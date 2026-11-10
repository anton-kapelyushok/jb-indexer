use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::internal::FileAddress;

/// Shared, cloneable error (Kotlin: `Throwable`).
pub type Reason = Arc<anyhow::Error>;

pub type TokenPredicate = Box<dyn Fn(&str) -> bool + Send + Sync>;

/// The query half of [`crate::Index`] that an [`IndexConfig`] needs in order to find candidate files.
///
/// Kotlin's `IndexConfig.find` takes the full `Index`; splitting the query half out keeps the configs
/// unit-testable against a plain `InvertedIndex` (the Kotlin tests use mockk for this).
pub trait TokenIndex: Send + Sync {
    fn find_files_by_token<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Vec<FileAddress>>;
    fn find_tokens_matching_predicate(&self, predicate: TokenPredicate) -> BoxFuture<'_, Vec<String>>;
}

pub trait IndexConfig: Send + Sync + 'static {
    fn enable_logging(&self) -> &AtomicBool;

    fn enable_watcher(&self) -> bool;

    /// Transforms a line into tokens for the index to store.
    fn tokenize(&self, line: &str) -> Vec<String>;

    /// Returns files that may match the query.
    fn find(&self, index: Arc<dyn TokenIndex>, query: String) -> BoxStream<'static, FileAddress>;

    /// Determines if a line matches the query.
    fn matches(&self, line: &str, query: &str) -> bool;

    /// The watcher is restarted on error, and the error is passed to the status stream.
    /// That stream has a limited buffer, so the error might be lost; this hook always sees it.
    fn handle_watcher_error(&self, e: &anyhow::Error);

    /// The initial file sync can fail if the directory changes while it is being walked.
    /// It is retried, but you might want to log the error.
    fn handle_initial_file_sync_error(&self, e: &anyhow::Error);
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    pub handled_file_events: i64,
    pub total_file_events: i64,
    pub files_discovered_by_watcher_during_initialization: i64,
    pub is_broken: bool,
    pub all_file_discovered: bool,
}

impl IndexState {
    pub fn is_in_sync(&self) -> bool {
        !self.is_broken && self.all_file_discovered && self.handled_file_events == self.total_file_events
    }

    pub fn pretty_print(&self) -> String {
        let progress_total = if !self.all_file_discovered {
            self.total_file_events.max(self.files_discovered_by_watcher_during_initialization)
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
            self.indexed_files
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
///                                       ^                                      ^                       |
///                                       |                                      |                       |
///            ReinitializingBecauseFileSyncFailed                               +-----------------------+
///
/// + Failed - terminal state, can be reached from any state
/// ```
#[derive(Clone)]
pub enum IndexStatusUpdate {
    Initial { ts: i64 },
    Initializing { ts: i64 },
    WatcherStarted { ts: i64, status: IndexState },
    AllFilesDiscovered { ts: i64, status: IndexState },
    IndexInSync { ts: i64, status: IndexState },
    IndexOutOfSync { ts: i64, status: IndexState },
    ReinitializingBecauseFileSyncFailed { ts: i64, reason: Reason },
    Failed { ts: i64, reason: Reason },
}

impl IndexStatusUpdate {
    pub fn ts(&self) -> i64 {
        use IndexStatusUpdate::*;
        match self {
            Initial { ts }
            | Initializing { ts }
            | WatcherStarted { ts, .. }
            | AllFilesDiscovered { ts, .. }
            | IndexInSync { ts, .. }
            | IndexOutOfSync { ts, .. }
            | ReinitializingBecauseFileSyncFailed { ts, .. }
            | Failed { ts, .. } => *ts,
        }
    }

    pub fn status(&self) -> Option<&IndexState> {
        use IndexStatusUpdate::*;
        match self {
            WatcherStarted { status, .. }
            | AllFilesDiscovered { status, .. }
            | IndexInSync { status, .. }
            | IndexOutOfSync { status, .. } => Some(status),
            _ => None,
        }
    }

    pub fn is_in_sync(&self) -> bool {
        matches!(self, IndexStatusUpdate::IndexInSync { .. })
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, IndexStatusUpdate::Failed { .. })
    }
}

impl fmt::Debug for IndexStatusUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use IndexStatusUpdate::*;
        match self {
            Initial { ts } => write!(f, "Initial(ts={ts})"),
            Initializing { ts } => write!(f, "Initializing(ts={ts})"),
            WatcherStarted { ts, status } => write!(f, "WatcherStarted(ts={ts}, status={status:?})"),
            AllFilesDiscovered { ts, status } => write!(f, "AllFilesDiscovered(ts={ts}, status={status:?})"),
            IndexInSync { ts, status } => write!(f, "IndexInSync(ts={ts}, status={status:?})"),
            IndexOutOfSync { ts, status } => write!(f, "IndexOutOfSync(ts={ts}, status={status:?})"),
            ReinitializingBecauseFileSyncFailed { ts, reason } => {
                write!(f, "ReinitializingBecauseFileSyncFailed(ts={ts}, reason={reason:#})")
            }
            Failed { ts, reason } => write!(f, "Failed(ts={ts}, reason={reason:#})"),
        }
    }
}

/// How a long-running component (index / search engine) terminated. Kotlin: `Deferred` completion.
#[derive(Clone, Debug)]
pub enum Outcome {
    Cancelled,
    Failed(Reason),
}
