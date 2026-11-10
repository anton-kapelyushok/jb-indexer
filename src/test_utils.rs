use std::collections::HashSet;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::internal::inverted_index::InvertedIndex;
use crate::internal::utils::canonical_path;
use crate::model::{
    FileAddress, Index, IndexConfig, IndexState, IndexStatusUpdate, TokenPredicate, TokenSet,
};

pub fn fa(s: &str) -> FileAddress {
    FileAddress::new(s)
}

pub fn file_address_of(path: &Path) -> FileAddress {
    FileAddress::new(canonical_path(path))
}

pub fn set<T: Eq + Hash>(items: impl IntoIterator<Item = T>) -> HashSet<T> {
    items.into_iter().collect()
}

pub fn tokens(items: &[&str]) -> TokenSet {
    items.iter().map(|s| s.to_string()).collect()
}

pub async fn with_timeout<T>(secs: u64, f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(secs), f)
        .await
        .expect("test timed out")
}

#[allow(dead_code)]
pub struct FolderSetup {
    pub poupa: PathBuf,
    pub loupa: PathBuf,
    pub nested_dir: PathBuf,
    pub nested_poupa: PathBuf,
}

pub fn initialize_directory(dir: &Path) -> FolderSetup {
    let poupa = dir.join("poupa");
    std::fs::File::create(&poupa).unwrap();
    let loupa = dir.join("loupa");
    std::fs::File::create(&loupa).unwrap();
    let nested_dir = dir.join("nestedDir");
    std::fs::create_dir(&nested_dir).unwrap();
    let nested_poupa = nested_dir.join("poupa");
    std::fs::File::create(&nested_poupa).unwrap();
    FolderSetup {
        poupa,
        loupa,
        nested_dir,
        nested_poupa,
    }
}

/// Config which splits lines by spaces.
pub struct TestConfig {
    pub enable_logging: Arc<AtomicBool>,
    pub enable_watcher: bool,
    pub panic_on_line: Option<String>,
}

impl Default for TestConfig {
    fn default() -> Self {
        TestConfig {
            enable_logging: Arc::new(AtomicBool::new(false)),
            enable_watcher: true,
            panic_on_line: None,
        }
    }
}

#[async_trait]
impl IndexConfig for TestConfig {
    fn enable_logging(&self) -> &Arc<AtomicBool> {
        &self.enable_logging
    }

    fn enable_watcher(&self) -> bool {
        self.enable_watcher
    }

    fn tokenize(&self, line: &str) -> Vec<String> {
        if self.panic_on_line.as_deref() == Some(line) {
            panic!("not an io error");
        }
        line.split(' ').map(str::to_string).collect()
    }

    fn find(&self, _index: Arc<dyn Index>, _query: String) -> BoxStream<'static, FileAddress> {
        futures::stream::empty().boxed()
    }

    fn matches(&self, line: &str, query: &str) -> bool {
        line.contains(query)
    }
}

/// In-memory index for config tests.
pub struct FakeIndex(Mutex<InvertedIndex>);

impl FakeIndex {
    pub fn from_documents(docs: &[(&str, &[&str])]) -> Self {
        let mut index = InvertedIndex::new(0.75);
        for (name, toks) in docs {
            index.add_or_update_document(fa(name), tokens(toks));
        }
        FakeIndex(Mutex::new(index))
    }

    /// token -> files
    pub fn from_files(by_token: &[(&str, &[&str])]) -> Self {
        let mut docs: std::collections::HashMap<&str, Vec<&str>> = Default::default();
        for (token, files) in by_token {
            for f in *files {
                docs.entry(f).or_default().push(token);
            }
        }
        let docs: Vec<(&str, Vec<&str>)> = docs.into_iter().collect();
        let refs: Vec<(&str, &[&str])> = docs.iter().map(|(f, t)| (*f, t.as_slice())).collect();
        Self::from_documents(&refs)
    }
}

#[async_trait]
impl Index for FakeIndex {
    async fn find_files_by_token(&self, token: &str) -> Vec<FileAddress> {
        self.0.lock().unwrap().find_files_by_token(token)
    }

    async fn find_tokens_matching_predicate(&self, predicate: TokenPredicate) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .find_tokens_matching_predicate(&*predicate)
    }

    async fn state(&self) -> IndexState {
        IndexState::broken()
    }

    fn status_updates(&self) -> BoxStream<'static, IndexStatusUpdate> {
        futures::stream::empty().boxed()
    }

    async fn compact(&self) {}
}
