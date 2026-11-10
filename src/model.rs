use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use async_trait::async_trait;
use futures::stream::BoxStream;

/// Error shared between several subscribers (status updates are broadcast, so errors must be cloneable).
pub type SharedError = Arc<anyhow::Error>;

/// Predicate used to select tokens from the index.
pub type TokenPredicate = Box<dyn Fn(&str) -> bool + Send + Sync>;

/// Canonical path of an indexed file.
///
/// Addresses emitted by the file system synchronization are interned, so two addresses of the
/// same file share the same allocation (see [`FileAddress::ptr_eq`]).
#[derive(Clone)]
pub struct FileAddress(Arc<str>);

impl FileAddress {
    pub fn new(path: impl Into<Arc<str>>) -> Self {
        FileAddress(path.into())
    }

    pub fn path(&self) -> &str {
        &self.0
    }

    /// Referential equality.
    pub fn ptr_eq(&self, other: &FileAddress) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn ref_count(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

impl PartialEq for FileAddress {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for FileAddress {}

impl Hash for FileAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl fmt::Debug for FileAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileAddress({:?})", &*self.0)
    }
}

impl fmt::Display for FileAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Read access to a text index.
#[async_trait]
pub trait Index: Send + Sync {
    async fn find_files_by_token(&self, token: &str) -> Vec<FileAddress>;
    async fn find_tokens_matching_predicate(&self, predicate: TokenPredicate) -> Vec<String>;
    async fn state(&self) -> IndexState;
    /// Stream of major index events. Replays the last few events, ends after [`IndexStatusUpdate::Failed`].
    fn status_updates(&self) -> BoxStream<'static, IndexStatusUpdate>;
    async fn compact(&self);
}

/// Customizes how files are tokenized and how queries are executed.
#[async_trait]
pub trait IndexConfig: Send + Sync + 'static {
    fn enable_logging(&self) -> &Arc<AtomicBool>;

    fn enable_watcher(&self) -> bool;

    /// Transforms a line into tokens for the index to store.
    fn tokenize(&self, line: &str) -> Vec<String>;

    /// Returns files that possibly match the query.
    fn find(&self, index: Arc<dyn Index>, query: String) -> BoxStream<'static, FileAddress>;

    /// Determines whether a line matches the query.
    fn matches(&self, line: &str, query: &str) -> bool;

    /// The watcher is restarted on error and the error is passed to the status updates stream.
    /// However that stream has a limited buffer and the error might be lost,
    /// use this callback to make sure you catch it.
    async fn handle_watcher_error(&self, _e: &anyhow::Error) {}

    /// Initial file sync can fail if directory content changes during iteration.
    /// It gets retried, but you might want to log this error.
    async fn handle_initial_file_sync_error(&self, _e: &anyhow::Error) {}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexSearchResult {
    pub path: String,
    pub line_no: usize,
    pub line: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexState {
    pub clock: i64,

    pub start_time: i64,
    pub last_restart_time: i64,

    pub indexed_files: usize,
    pub known_tokens: usize,
    pub watcher_started: bool,
    pub handled_file_events: u64,
    pub total_file_events: u64,
    pub files_discovered_by_watcher_during_initialization: u64,
    pub is_broken: bool,
    pub all_file_discovered: bool,
}

impl IndexState {
    pub fn is_in_sync(&self) -> bool {
        !self.is_broken
            && self.all_file_discovered
            && self.handled_file_events == self.total_file_events
    }

    pub fn pretty_print(&self) -> String {
        let progress_total = if !self.all_file_discovered {
            self.total_file_events
                .max(self.files_discovered_by_watcher_during_initialization)
        } else {
            self.total_file_events
        };
        let progress = if self.all_file_discovered {
            if progress_total == 0 {
                1.0
            } else {
                self.handled_file_events as f64 / self.total_file_events as f64
            }
        } else if progress_total == 0 {
            0.0
        } else {
            self.handled_file_events as f64 / progress_total as f64
        };
        format!(
            "in sync: {}\nprogress: {}/{} {}%\nindexed files: {}",
            self.is_in_sync(),
            self.handled_file_events,
            progress_total,
            (progress * 100.0) as i64,
            self.indexed_files,
        )
    }

    pub fn broken() -> Self {
        IndexState {
            clock: -1,
            is_broken: true,
            indexed_files: 0,
            known_tokens: 0,
            watcher_started: false,
            handled_file_events: 0,
            total_file_events: 0,
            start_time: -1,
            last_restart_time: -1,
            all_file_discovered: false,
            files_discovered_by_watcher_during_initialization: 0,
        }
    }
}

/// State transitions:
///
/// ```text
/// Initial -> Initializing -> [ WatcherStarted ] -> AllFilesDiscovered -> IndexInSync -> IndexOutOfSync-+
///                  ^                                                         ^                       |
///                  |                                                         |                       |
///       ReinitializingBecauseFileSyncFailed                                  +-----------------------+
///
/// + Failed - terminal state, can be reached from any state
/// ```
#[derive(Clone, Debug)]
pub enum IndexStatusUpdate {
    Initial { ts: i64 },
    Initializing { ts: i64 },
    WatcherStarted { ts: i64, status: IndexState },
    AllFilesDiscovered { ts: i64, status: IndexState },
    IndexInSync { ts: i64, status: IndexState },
    IndexOutOfSync { ts: i64, status: IndexState },
    ReinitializingBecauseFileSyncFailed { ts: i64, reason: SharedError },
    Failed { ts: i64, reason: SharedError },
}

impl IndexStatusUpdate {
    pub fn ts(&self) -> i64 {
        match self {
            IndexStatusUpdate::Initial { ts }
            | IndexStatusUpdate::Initializing { ts }
            | IndexStatusUpdate::WatcherStarted { ts, .. }
            | IndexStatusUpdate::AllFilesDiscovered { ts, .. }
            | IndexStatusUpdate::IndexInSync { ts, .. }
            | IndexStatusUpdate::IndexOutOfSync { ts, .. }
            | IndexStatusUpdate::ReinitializingBecauseFileSyncFailed { ts, .. }
            | IndexStatusUpdate::Failed { ts, .. } => *ts,
        }
    }
}

/// How an index or a search engine terminated.
#[derive(Clone, Debug)]
pub enum Completion {
    Cancelled,
    Failed(SharedError),
}

impl Completion {
    pub(crate) fn into_error(self) -> SharedError {
        match self {
            Completion::Cancelled => Arc::new(anyhow::anyhow!("cancelled")),
            Completion::Failed(e) => e,
        }
    }
}

pub(crate) type TokenSet = HashSet<String>;
