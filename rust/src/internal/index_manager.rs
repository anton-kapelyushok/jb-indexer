use std::fmt;
use std::sync::Arc;

use async_channel::Receiver;
use rustc_hash::FxHashMap;

use super::inverted_index::InvertedIndex;
use super::model::{FileUpdateRequest, StatusUpdate, UserRequest};
use super::utils::{debug_log, now_millis};
use crate::model::{IndexConfig, IndexState, IndexStatusUpdate};

/// Absolute (canonical) path of an indexed file. Cheap to clone; interned by `sync_fs`.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileAddress(pub Arc<str>);

impl FileAddress {
    pub fn new(path: impl Into<Arc<str>>) -> Self {
        FileAddress(path.into())
    }
    pub fn path(&self) -> &str {
        &self.0
    }
    /// Referential equality (Kotlin `===`), true for addresses produced by the same interner.
    pub fn ptr_eq(&self, other: &FileAddress) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for FileAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileAddress({})", self.0)
    }
}

impl fmt::Display for FileAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Owns the inverted index and serializes all reads/updates to it.
/// Returns only on error (a closed input channel), like the Kotlin coroutine.
pub async fn index_manager(
    user_requests: Receiver<UserRequest>,
    index_update_requests: Receiver<FileUpdateRequest>,
    status_updates: Receiver<StatusUpdate>,
    emit_status_update: impl Fn(IndexStatusUpdate) + Send + 'static,
    cfg: Arc<dyn IndexConfig>,
) -> anyhow::Result<()> {
    let mut index = IndexManager::new(Box::new(emit_status_update), InvertedIndex::default());
    let cfg = &*cfg;

    loop {
        // `biased` == Kotlin `select`, which prefers clauses in declaration order
        tokio::select! {
            biased;
            event = status_updates.recv() => {
                let event = event?;
                debug_log(cfg, || format!("statusUpdates: {event:?}"));
                match event {
                    StatusUpdate::WatcherDiscoveredFileDuringInitialization => {
                        index.handle_watcher_discovered_file_during_initialization()
                    }
                    StatusUpdate::WatcherStarted => index.handle_watcher_started(),
                    StatusUpdate::FileUpdated { .. } => index.handle_file_updated(),
                    StatusUpdate::AllFilesDiscovered => index.handle_all_files_discovered(),
                    StatusUpdate::FileSyncFailed { t, reason } => index.handle_file_sync_failed(t, reason),
                }
            }
            event = user_requests.recv() => {
                let event = event?;
                debug_log(cfg, || format!("userRequests: {event:?}"));
                index.handle_user_request(event);
            }
            event = index_update_requests.recv() => {
                let event = event?;
                debug_log(cfg, || format!("indexRequests: {event:?}"));
                index.handle_file_update_request(event);
            }
        }
    }
}

pub struct IndexManager {
    emit_status_update: Box<dyn Fn(IndexStatusUpdate) + Send>,
    pub inverted_index: InvertedIndex,
    clock: i64,
    logical_time_of_last_watcher_reset: i64,
    start_time: i64,
    last_restart_time: i64,
    watcher_started: bool,
    all_files_discovered: bool,
    sync_completed: bool,
    file_update_times: FxHashMap<super::FileAddress, i64>,
    files_discovered_by_watcher_during_initialization: i64,
    total_file_events: i64,
    handled_file_events: i64,
}

impl IndexManager {
    pub fn new(emit_status_update: Box<dyn Fn(IndexStatusUpdate) + Send>, inverted_index: InvertedIndex) -> Self {
        let start_time = now_millis();
        IndexManager {
            emit_status_update,
            inverted_index,
            clock: 0,
            logical_time_of_last_watcher_reset: 0,
            start_time,
            last_restart_time: start_time,
            watcher_started: false,
            all_files_discovered: false,
            sync_completed: false,
            file_update_times: FxHashMap::default(),
            files_discovered_by_watcher_during_initialization: 0,
            total_file_events: 0,
            handled_file_events: 0,
        }
    }

    fn emit(&self, u: IndexStatusUpdate) {
        (self.emit_status_update)(u)
    }

    pub fn handle_file_update_request(&mut self, event: FileUpdateRequest) {
        match event {
            FileUpdateRequest::UpdateFile { t, file_address, tokens, .. } => {
                if self.handle_file_sync_event(t, &file_address) {
                    self.inverted_index.add_or_update_document(file_address, tokens);
                }
            }
            FileUpdateRequest::RemoveFile { t, file_address, .. } => {
                if self.handle_file_sync_event(t, &file_address) {
                    self.inverted_index.remove_document(&file_address);
                }
            }
        }
    }

    pub fn handle_user_request(&mut self, event: UserRequest) {
        // a dropped receiver just means the caller lost interest
        match event {
            UserRequest::FindFilesByToken { query, result } => {
                let _ = result.send(self.inverted_index.find_files_by_token(&query));
            }
            UserRequest::FindTokensMatchingPredicate { predicate, result } => {
                let _ = result.send(self.inverted_index.find_tokens_matching_predicate(predicate));
            }
            UserRequest::Status { result } => {
                let _ = result.send(self.status());
            }
            UserRequest::Compact { result } => {
                self.inverted_index.compact();
                let _ = result.send(());
            }
        }
    }

    pub fn handle_watcher_started(&mut self) {
        self.watcher_started = true;
        self.emit(IndexStatusUpdate::WatcherStarted { ts: now_millis(), status: self.status() });
    }

    pub fn handle_all_files_discovered(&mut self) {
        let ts = now_millis();
        self.all_files_discovered = true;
        self.emit(IndexStatusUpdate::AllFilesDiscovered { ts, status: self.status() });
        if self.handled_file_events == self.total_file_events {
            self.emit(IndexStatusUpdate::IndexInSync { ts, status: self.status() });
        }
    }

    pub fn handle_file_updated(&mut self) {
        let was_in_sync = self.status().is_in_sync();
        self.clock += 1;
        self.total_file_events += 1;
        if was_in_sync {
            self.emit(IndexStatusUpdate::IndexOutOfSync { ts: now_millis(), status: self.status() });
        }
    }

    pub fn handle_watcher_discovered_file_during_initialization(&mut self) {
        self.files_discovered_by_watcher_during_initialization += 1;
    }

    pub fn handle_file_sync_failed(&mut self, t: i64, reason: crate::model::Reason) {
        self.clock += 1;
        self.watcher_started = false;
        self.all_files_discovered = false;
        self.last_restart_time = now_millis();
        self.file_update_times.clear();
        self.inverted_index.reset();
        self.files_discovered_by_watcher_during_initialization = 0;
        self.total_file_events = 0;
        self.handled_file_events = 0;
        self.logical_time_of_last_watcher_reset = t;

        self.emit(IndexStatusUpdate::ReinitializingBecauseFileSyncFailed { ts: now_millis(), reason });
    }

    /// Returns whether the event should be applied to the index.
    fn handle_file_sync_event(&mut self, t: i64, fa: &super::FileAddress) -> bool {
        self.clock += 1;
        // event from a generation before the last watcher reset
        if t < self.logical_time_of_last_watcher_reset {
            return false;
        }

        self.handled_file_events += 1;
        if self.all_files_discovered && self.handled_file_events == self.total_file_events {
            self.sync_completed = true;
            self.emit(IndexStatusUpdate::IndexInSync { ts: now_millis(), status: self.status() });
        }

        // a later event for the same file was already applied
        let last_update = self.file_update_times.get(fa).copied().unwrap_or(0);
        if t < last_update {
            return false;
        }
        self.file_update_times.insert(fa.clone(), t);
        true
    }

    pub fn status(&self) -> IndexState {
        IndexState {
            clock: self.clock,
            indexed_files: self.inverted_index.documents_count(),
            known_tokens: self.inverted_index.tokens_count(),
            watcher_started: self.watcher_started,
            all_file_discovered: self.all_files_discovered,
            handled_file_events: self.handled_file_events,
            total_file_events: self.total_file_events,
            files_discovered_by_watcher_during_initialization: self.files_discovered_by_watcher_during_initialization,
            start_time: self.start_time,
            last_restart_time: self.last_restart_time,
            is_broken: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use rustc_hash::FxHashSet;

    use super::*;
    use crate::internal::model::FileEventSource::{InitialSync, Watcher};

    type Events = Arc<Mutex<Vec<IndexStatusUpdate>>>;

    fn setup() -> (IndexManager, Events) {
        let events: Events = Default::default();
        let e = events.clone();
        let m = IndexManager::new(Box::new(move |u| e.lock().unwrap().push(u)), InvertedIndex::new(0.0));
        (m, events)
    }

    fn fa(s: &str) -> FileAddress {
        FileAddress::new(s)
    }

    fn update(t: i64, f: &str, tokens: &[&str]) -> FileUpdateRequest {
        FileUpdateRequest::UpdateFile {
            t,
            file_address: fa(f),
            tokens: tokens.iter().map(|s| s.to_string()).collect::<FxHashSet<_>>(),
            source: InitialSync,
        }
    }

    fn remove(t: i64, f: &str) -> FileUpdateRequest {
        FileUpdateRequest::RemoveFile { t, file_address: fa(f), source: Watcher }
    }

    fn take(events: &Events) -> Vec<IndexStatusUpdate> {
        std::mem::take(&mut *events.lock().unwrap())
    }

    #[test]
    fn should_track_file_events() {
        let (mut m, events) = setup();
        let mut clock = 1;

        m.handle_file_updated();
        assert_eq!((m.status().handled_file_events, m.status().total_file_events), (0, 1));
        m.handle_file_updated();
        assert_eq!((m.status().handled_file_events, m.status().total_file_events), (0, 2));

        m.handle_file_update_request(update(clock, "doc1.txt", &[]));
        clock += 1;
        assert_eq!((m.status().handled_file_events, m.status().total_file_events), (1, 2));
        assert!(take(&events).is_empty());

        m.handle_file_update_request(update(clock, "doc2.txt", &[]));
        clock += 1;
        assert_eq!((m.status().handled_file_events, m.status().total_file_events), (2, 2));
        // AllFilesDiscovered not received yet, there might be more: not in sync
        assert!(!m.status().is_in_sync());
        assert!(take(&events).is_empty());

        // found 3rd file
        m.handle_file_updated();
        // initial file sync finished, 1 event still in flight
        m.handle_all_files_discovered();
        assert!(!m.status().is_in_sync());
        let e = take(&events);
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], IndexStatusUpdate::AllFilesDiscovered { .. }));

        // read 3rd file: in sync
        m.handle_file_update_request(update(clock, "doc3.txt", &[]));
        clock += 1;
        assert!(m.status().is_in_sync());
        let e = take(&events);
        assert_eq!(e.len(), 1);
        assert!(e[0].is_in_sync());

        // some file updated: out of sync
        m.handle_file_updated();
        assert!(!m.status().is_in_sync());
        let e = take(&events);
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], IndexStatusUpdate::IndexOutOfSync { .. }));

        // update handled: in sync again
        m.handle_file_update_request(remove(clock, "doc3.txt"));
        let e = take(&events);
        assert_eq!(e.len(), 1);
        assert!(e[0].is_in_sync());
    }

    #[test]
    fn should_emit_all_files_discovered_and_in_sync_on_empty_directory() {
        let (mut m, events) = setup();
        m.handle_all_files_discovered();
        let e = take(&events);
        assert_eq!(e.len(), 2);
        assert!(matches!(e[0], IndexStatusUpdate::AllFilesDiscovered { .. }));
        assert!(e[1].is_in_sync());
    }

    #[test]
    fn should_ignore_stale_updates() {
        let (mut m, _) = setup();
        m.handle_file_update_request(update(2, "doc1.txt", &["a"]));
        assert_eq!(m.status().handled_file_events, 1);
        assert_eq!(m.inverted_index.find_files_by_token("a"), vec![fa("doc1.txt")]);

        // t=1 < t=2: ignored
        m.handle_file_update_request(update(1, "doc1.txt", &["b"]));
        assert_eq!(m.status().handled_file_events, 2);
        assert!(m.inverted_index.find_files_by_token("b").is_empty());

        // t=3 > t=2: applied
        m.handle_file_update_request(update(3, "doc1.txt", &["c"]));
        assert_eq!(m.status().handled_file_events, 3);
        assert_eq!(m.inverted_index.find_files_by_token("c"), vec![fa("doc1.txt")]);

        // stale remove ignored too
        m.handle_file_update_request(remove(2, "doc1.txt"));
        assert_eq!(m.inverted_index.documents_count(), 1);
    }

    #[test]
    fn should_reset_state_when_file_sync_failed() {
        let (mut m, events) = setup();
        m.handle_watcher_discovered_file_during_initialization();
        m.handle_watcher_discovered_file_during_initialization();
        m.handle_watcher_started();
        m.handle_file_updated();
        m.handle_file_update_request(update(1, "doc1.txt", &[]));
        m.handle_file_updated();
        m.handle_file_update_request(update(2, "doc2.txt", &[]));
        m.handle_all_files_discovered();
        let s = m.status();
        assert!(s.watcher_started && s.all_file_discovered);
        assert_eq!((s.total_file_events, s.handled_file_events), (2, 2));
        assert!(take(&events).last().unwrap().is_in_sync());

        m.handle_file_sync_failed(3, Arc::new(anyhow::anyhow!("boom")));
        let s = m.status();
        assert!(!s.watcher_started && !s.all_file_discovered);
        assert_eq!((s.total_file_events, s.handled_file_events), (0, 0));
        assert_eq!(s.indexed_files, 0);
        let e = take(&events);
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], IndexStatusUpdate::ReinitializingBecauseFileSyncFailed { .. }));
    }

    #[test]
    fn should_ignore_updates_happened_before_watcher_reset() {
        let (mut m, _) = setup();
        m.handle_watcher_started();
        m.handle_file_updated();
        m.handle_file_update_request(update(1, "doc1.txt", &[]));
        assert_eq!(m.inverted_index.documents_count(), 1);
        m.handle_file_updated();
        m.handle_all_files_discovered();

        m.handle_file_sync_failed(3, Arc::new(anyhow::anyhow!("boom")));

        // stale event from the previous generation
        m.handle_file_update_request(update(2, "doc2.txt", &[]));
        assert_eq!(m.inverted_index.documents_count(), 0);

        m.handle_watcher_started();
        m.handle_file_updated();
        m.handle_file_update_request(update(4, "doc1.txt", &[]));
        assert_eq!(m.inverted_index.documents_count(), 1);
    }
}
