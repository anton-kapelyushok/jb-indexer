use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use rustc_hash::FxHashSet;

use crate::internal::FileAddress;
use crate::model::{IndexConfig, TokenIndex};

type ErrorHandler = Box<dyn Fn(&anyhow::Error) + Send + Sync>;

/// Indexes lower-cased character trigrams of every line.
pub struct TrigramIndexConfig {
    enable_logging: AtomicBool,
    enable_watcher: bool,
    handle_watcher_error: ErrorHandler,
    handle_initial_file_sync_error: ErrorHandler,
}

pub fn trigram_index_config(enable_watcher: bool) -> TrigramIndexConfig {
    TrigramIndexConfig {
        enable_logging: AtomicBool::new(false),
        enable_watcher,
        handle_watcher_error: Box::new(|_| {}),
        handle_initial_file_sync_error: Box::new(|_| {}),
    }
}

impl TrigramIndexConfig {
    pub fn on_watcher_error(mut self, f: impl Fn(&anyhow::Error) + Send + Sync + 'static) -> Self {
        self.handle_watcher_error = Box::new(f);
        self
    }
    pub fn on_initial_file_sync_error(mut self, f: impl Fn(&anyhow::Error) + Send + Sync + 'static) -> Self {
        self.handle_initial_file_sync_error = Box::new(f);
        self
    }
}

pub(crate) fn tokenize(line: &str) -> Vec<String> {
    let lower = line.to_lowercase();
    if line.chars().count() < 3 {
        let mut s = lower;
        while s.chars().count() < 3 {
            s.push(' ');
        }
        return vec![s];
    }
    // char_indices boundaries let every trigram be a borrowed slice until the final copy
    let bounds: Vec<usize> = lower.char_indices().map(|(i, _)| i).chain(std::iter::once(lower.len())).collect();
    bounds.windows(4).map(|w| lower[w[0]..w[3]].to_owned()).collect()
}

impl IndexConfig for TrigramIndexConfig {
    fn enable_logging(&self) -> &AtomicBool {
        &self.enable_logging
    }

    fn enable_watcher(&self) -> bool {
        self.enable_watcher
    }

    fn tokenize(&self, line: &str) -> Vec<String> {
        tokenize(line)
    }

    fn find(&self, index: Arc<dyn TokenIndex>, query: String) -> BoxStream<'static, FileAddress> {
        async_stream::stream! {
            let index = &*index;
            let len = query.chars().count();
            if len < 3 {
                // 0: everything matches; 1-2: files with trigrams containing the query
                let tokens = if len == 0 {
                    index.find_tokens_matching_predicate(Box::new(|_| true)).await
                } else {
                    let q = query.clone();
                    index.find_tokens_matching_predicate(Box::new(move |t| t.contains(q.as_str()))).await
                };
                for t in tokens {
                    for f in index.find_files_by_token(&t).await { yield f; }
                }
                return;
            }

            let tokens = tokenize(&query);
            let mut files: FxHashSet<FileAddress> = index.find_files_by_token(&tokens[0]).await.into_iter().collect();
            for t in &tokens[1..] {
                if files.is_empty() { break; }
                let next: FxHashSet<FileAddress> = index.find_files_by_token(t).await.into_iter().collect();
                files.retain(|f| next.contains(f));
            }
            for f in files { yield f; }
        }
        .boxed()
    }

    fn matches(&self, line: &str, query: &str) -> bool {
        line.contains(query)
    }

    fn handle_watcher_error(&self, e: &anyhow::Error) {
        (self.handle_watcher_error)(e)
    }

    fn handle_initial_file_sync_error(&self, e: &anyhow::Error) {
        (self.handle_initial_file_sync_error)(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::word_index_config::tests::TestIndex;

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    #[test]
    fn should_tokenize_strings() {
        assert_eq!(
            sorted(tokenize("poUPa loupa")),
            sorted(["pou", "oup", "upa", "pa ", "a l", " lo", "lou", "oup", "upa"].map(String::from).to_vec())
        );
    }

    #[test]
    fn should_tokenize_small_strings() {
        assert_eq!(tokenize("ab "), vec!["ab "]);
        assert_eq!(tokenize("a  "), vec!["a  "]);
        assert_eq!(tokenize("   "), vec!["   "]);
        assert_eq!(tokenize("ab"), vec!["ab "]);
        assert_eq!(tokenize("Жё"), vec!["жё "]);
    }

    #[tokio::test]
    async fn should_be_able_to_search() {
        let index = TestIndex::shared(&[
            ("res1", &["pou", "oup", "upa"]),
            ("res2", &["pou", "oup"]),
            ("res3", &["pou", "upa"]),
            ("res4", &["pou", "oup"]),
            ("res5", &["oup", "upa"]),
            ("res6", &["pou", "oup", "upa"]),
        ]);
        let found: Vec<String> =
            trigram_index_config(false).find(index, "poUPa".into()).map(|f| f.path().to_owned()).collect().await;
        assert_eq!(sorted(found), vec!["res1", "res6"]);
    }
}
