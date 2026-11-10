use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::internal::inverted_index::{DocumentIndex, InvertedIndex};
use crate::internal::model::{FileUpdateRequest, StatusUpdate, UserRequest};
use crate::internal::utils::now_millis;
use crate::model::{FileAddress, IndexState, IndexStatusUpdate, SharedError};

/// Owns the inverted index and serializes all reads/updates to it.
pub(crate) async fn index_manager(
    user_requests: flume::Receiver<UserRequest>,
    index_update_requests: flume::Receiver<FileUpdateRequest>,
    status_updates: flume::Receiver<StatusUpdate>,
    emit_status_update: impl Fn(IndexStatusUpdate) + Send + 'static,
    enable_debug_log: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let mut index = IndexManager::new(
        emit_status_update,
        enable_debug_log.clone(),
        InvertedIndex::default(),
    );
    let debug_log = |s: String| {
        if enable_debug_log.load(Ordering::Relaxed) {
            println!("{s}")
        }
    };

    loop {
        // biased: status updates are always handled before index updates.
        // syncFs sends FileUpdated before the event itself, so total events are counted before handled ones.
        tokio::select! {
            biased;
            event = status_updates.recv_async() => {
                let event = event?;
                debug_log(format!("statusUpdates: {event:?}"));
                match event {
                    StatusUpdate::WatcherDiscoveredFileDuringInitialization =>
                        index.handle_watcher_discovered_file_during_initialization(),
                    StatusUpdate::WatcherStarted => index.handle_watcher_started(),
                    StatusUpdate::FileUpdated(_) => index.handle_file_updated(),
                    StatusUpdate::AllFilesDiscovered => index.handle_all_files_discovered(),
                    StatusUpdate::FileSyncFailed { t, reason } => index.handle_file_sync_failed(t, reason),
                }
            }
            event = user_requests.recv_async() => {
                let event = event?;
                debug_log(format!("userRequests: {event:?}"));
                index.handle_user_request(event);
            }
            event = index_update_requests.recv_async() => {
                let event = event?;
                debug_log(format!("indexRequests: {event:?}"));
                match event {
                    FileUpdateRequest::UpdateFile { t, file_address, tokens, .. } =>
                        index.handle_update_file_content_request(t, file_address, tokens),
                    FileUpdateRequest::RemoveFile { t, file_address, .. } =>
                        index.handle_remove_file_request(t, file_address),
                }
            }
        }
    }
}

pub(crate) struct IndexManager<E, I> {
    emit_status_update: E,
    enable_debug_log: Arc<AtomicBool>,
    inverted_index: I,

    clock: i64,
    logical_time_of_last_watcher_reset: i64,
    start_time: i64,
    last_restart_time: i64,
    watcher_started: bool,
    all_files_discovered: bool,
    sync_completed: bool,
    file_update_times: HashMap<FileAddress, i64>,
    files_discovered_by_watcher_during_initialization: u64,
    total_file_events: u64,
    handled_file_events: u64,
}

impl<E: Fn(IndexStatusUpdate), I: DocumentIndex> IndexManager<E, I> {
    pub fn new(
        emit_status_update: E,
        enable_debug_log: Arc<AtomicBool>,
        inverted_index: I,
    ) -> Self {
        let start_time = now_millis();
        IndexManager {
            emit_status_update,
            enable_debug_log,
            inverted_index,
            clock: 0,
            logical_time_of_last_watcher_reset: 0,
            start_time,
            last_restart_time: start_time,
            watcher_started: false,
            all_files_discovered: false,
            sync_completed: false,
            file_update_times: HashMap::new(),
            files_discovered_by_watcher_during_initialization: 0,
            total_file_events: 0,
            handled_file_events: 0,
        }
    }

    pub fn handle_update_file_content_request(
        &mut self,
        t: i64,
        fa: FileAddress,
        tokens: crate::model::TokenSet,
    ) {
        if self.handle_file_sync_event(t, &fa) {
            self.inverted_index.add_or_update_document(fa, tokens);
        }
    }

    pub fn handle_remove_file_request(&mut self, t: i64, fa: FileAddress) {
        if self.handle_file_sync_event(t, &fa) {
            self.inverted_index.remove_document(&fa);
        }
    }

    pub fn handle_user_request(&mut self, request: UserRequest) {
        // the requester may have gone away, ignore send errors
        match request {
            UserRequest::FindFilesByToken { query, result } => {
                let _ = result.send(self.inverted_index.find_files_by_token(&query));
            }
            UserRequest::FindTokensMatchingPredicate { predicate, result } => {
                let _ = result.send(
                    self.inverted_index
                        .find_tokens_matching_predicate(&*predicate),
                );
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
        let ts = now_millis();
        self.debug_log(|| format!("Watcher started after {} ms!", ts - self.start_time));
        self.watcher_started = true;
        (self.emit_status_update)(IndexStatusUpdate::WatcherStarted {
            ts,
            status: self.status(),
        });
    }

    pub fn handle_all_files_discovered(&mut self) {
        let ts = now_millis();
        self.all_files_discovered = true;
        self.debug_log(|| format!("All files discovered after {} ms!", ts - self.start_time));
        (self.emit_status_update)(IndexStatusUpdate::AllFilesDiscovered {
            ts,
            status: self.status(),
        });
        if self.handled_file_events == self.total_file_events {
            (self.emit_status_update)(IndexStatusUpdate::IndexInSync {
                ts,
                status: self.status(),
            });
        }
    }

    pub fn handle_file_updated(&mut self) {
        let was_in_sync = self.status().is_in_sync();
        self.clock += 1;
        self.total_file_events += 1;
        if was_in_sync {
            (self.emit_status_update)(IndexStatusUpdate::IndexOutOfSync {
                ts: now_millis(),
                status: self.status(),
            });
        }
    }

    pub fn handle_watcher_discovered_file_during_initialization(&mut self) {
        self.files_discovered_by_watcher_during_initialization += 1;
    }

    pub fn handle_file_sync_failed(&mut self, t: i64, reason: SharedError) {
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

        (self.emit_status_update)(IndexStatusUpdate::ReinitializingBecauseFileSyncFailed {
            ts: now_millis(),
            reason,
        });
    }

    /// Returns true if the event should be applied to the index.
    fn handle_file_sync_event(&mut self, t: i64, fa: &FileAddress) -> bool {
        self.clock += 1;
        // event belongs to a generation before the last watcher reset
        if t < self.logical_time_of_last_watcher_reset {
            return false;
        }

        self.handled_file_events += 1;
        if self.all_files_discovered && self.handled_file_events == self.total_file_events {
            if !self.sync_completed {
                self.sync_completed = true;
                let elapsed = now_millis() - self.start_time;
                self.debug_log(|| format!("Initial sync completed after {elapsed} ms!"));
            }
            (self.emit_status_update)(IndexStatusUpdate::IndexInSync {
                ts: now_millis(),
                status: self.status(),
            });
        }

        // a later event for this file was already handled
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
            files_discovered_by_watcher_during_initialization: self
                .files_discovered_by_watcher_during_initialization,
            start_time: self.start_time,
            last_restart_time: self.last_restart_time,
            is_broken: false,
        }
    }

    fn debug_log(&self, s: impl FnOnce() -> String) {
        if self.enable_debug_log.load(Ordering::Relaxed) {
            println!("{}", s());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::test_utils::fa;

    /// Records calls instead of storing documents.
    #[derive(Default)]
    struct RecordingIndex {
        added: Arc<std::sync::Mutex<Vec<FileAddress>>>,
        removed: Arc<std::sync::Mutex<Vec<FileAddress>>>,
    }

    impl DocumentIndex for RecordingIndex {
        fn add_or_update_document(&mut self, fa: FileAddress, _tokens: HashSet<String>) {
            self.added.lock().unwrap().push(fa)
        }
        fn remove_document(&mut self, fa: &FileAddress) {
            self.removed.lock().unwrap().push(fa.clone())
        }
        fn find_files_by_token(&self, _token: &str) -> Vec<FileAddress> {
            vec![]
        }
        fn find_tokens_matching_predicate(&self, _matches: &dyn Fn(&str) -> bool) -> Vec<String> {
            vec![]
        }
        fn reset(&mut self) {}
        fn compact(&mut self) {}
        fn tokens_count(&self) -> usize {
            0
        }
        fn documents_count(&self) -> usize {
            0
        }
    }

    struct Setup {
        manager: IndexManager<Box<dyn Fn(IndexStatusUpdate)>, RecordingIndex>,
        emitted: Arc<std::sync::Mutex<Vec<IndexStatusUpdate>>>,
        added: Arc<std::sync::Mutex<Vec<FileAddress>>>,
        removed: Arc<std::sync::Mutex<Vec<FileAddress>>>,
    }

    fn setup() -> Setup {
        let emitted = Arc::new(std::sync::Mutex::new(vec![]));
        let index = RecordingIndex::default();
        let added = index.added.clone();
        let removed = index.removed.clone();
        let e = emitted.clone();
        let manager = IndexManager::new(
            Box::new(move |u| e.lock().unwrap().push(u)) as Box<dyn Fn(IndexStatusUpdate)>,
            Arc::new(AtomicBool::new(false)),
            index,
        );
        Setup {
            manager,
            emitted,
            added,
            removed,
        }
    }

    fn update(m: &mut Setup, t: i64, name: &str) {
        m.manager
            .handle_update_file_content_request(t, fa(name), HashSet::new());
    }

    fn pop(s: &Setup) -> IndexStatusUpdate {
        s.emitted
            .lock()
            .unwrap()
            .pop()
            .expect("expected emitted event")
    }

    fn sync_failed(s: &mut Setup, t: i64) {
        s.manager
            .handle_file_sync_failed(t, Arc::new(anyhow::anyhow!("boom")));
    }

    #[test]
    fn should_track_file_events() {
        let mut s = setup();
        let mut clock = 1;
        // found 1 file
        s.manager.handle_file_updated();
        assert_eq!(s.manager.status().handled_file_events, 0);
        assert_eq!(s.manager.status().total_file_events, 1);

        // found 2 file
        s.manager.handle_file_updated();
        assert_eq!(s.manager.status().handled_file_events, 0);
        assert_eq!(s.manager.status().total_file_events, 2);

        // read 1 file
        update(&mut s, clock, "doc1.txt");
        clock += 1;
        assert_eq!(s.manager.status().handled_file_events, 1);
        assert_eq!(s.manager.status().total_file_events, 2);
        assert!(s.emitted.lock().unwrap().is_empty());

        // read 2 file
        update(&mut s, clock, "doc2.txt");
        clock += 1;
        assert_eq!(s.manager.status().handled_file_events, 2);
        assert_eq!(s.manager.status().total_file_events, 2);

        // AllFilesDiscovered was not received, there might be more, not in sync
        assert!(!s.manager.status().is_in_sync());
        assert!(s.emitted.lock().unwrap().is_empty());

        // found 3 file - there were more!
        s.manager.handle_file_updated();

        // finished initial file sync, still 1 event inflight
        s.manager.handle_all_files_discovered();
        assert!(!s.manager.status().is_in_sync());
        assert_eq!(s.emitted.lock().unwrap().len(), 1);
        assert!(matches!(
            pop(&s),
            IndexStatusUpdate::AllFilesDiscovered { .. }
        ));

        // read 3 file, no more in flight - we are in sync
        update(&mut s, clock, "doc3.txt");
        clock += 1;
        assert!(s.manager.status().is_in_sync());
        assert_eq!(s.emitted.lock().unwrap().len(), 1);
        assert!(matches!(pop(&s), IndexStatusUpdate::IndexInSync { .. }));

        // some file updated, out of sync again
        s.manager.handle_file_updated();
        assert!(!s.manager.status().is_in_sync());
        assert_eq!(s.emitted.lock().unwrap().len(), 1);
        assert!(matches!(pop(&s), IndexStatusUpdate::IndexOutOfSync { .. }));

        // handled an update - in sync now
        s.manager.handle_remove_file_request(clock, fa("doc3.txt"));
        assert_eq!(s.emitted.lock().unwrap().len(), 1);
        assert!(matches!(pop(&s), IndexStatusUpdate::IndexInSync { .. }));
    }

    #[test]
    fn should_emit_all_files_discovered_and_index_in_sync_events_on_empty_directory() {
        let mut s = setup();
        s.manager.handle_all_files_discovered();
        let emitted = s.emitted.lock().unwrap();
        assert_eq!(emitted.len(), 2);
        assert!(matches!(
            emitted[0],
            IndexStatusUpdate::AllFilesDiscovered { .. }
        ));
        assert!(matches!(emitted[1], IndexStatusUpdate::IndexInSync { .. }));
    }

    #[test]
    fn should_ignore_stale_updates() {
        let mut s = setup();
        let doc = fa("doc1.txt");
        s.manager.handle_remove_file_request(2, doc.clone());
        assert_eq!(s.manager.status().handled_file_events, 1);
        assert_eq!(*s.removed.lock().unwrap(), vec![doc.clone()]);

        // t=1 is less than t=2, ignore
        update(&mut s, 1, "doc1.txt");
        assert_eq!(s.manager.status().handled_file_events, 2);
        assert!(s.added.lock().unwrap().is_empty());

        // t=3 is greater than t=2, handle
        update(&mut s, 3, "doc1.txt");
        assert_eq!(s.manager.status().handled_file_events, 3);
        assert_eq!(*s.added.lock().unwrap(), vec![doc]);
    }

    #[test]
    fn should_reset_state_when_file_sync_failed() {
        let mut s = setup();
        // initialize normally
        s.manager
            .handle_watcher_discovered_file_during_initialization();
        s.manager
            .handle_watcher_discovered_file_during_initialization();
        s.manager.handle_watcher_started();
        s.manager.handle_file_updated();
        update(&mut s, 1, "doc1.txt");
        s.manager.handle_file_updated();
        update(&mut s, 2, "doc2.txt");
        s.manager.handle_all_files_discovered();
        let status = s.manager.status();
        assert!(status.watcher_started);
        assert!(status.all_file_discovered);
        assert_eq!(status.total_file_events, 2);
        assert_eq!(status.handled_file_events, 2);
        assert!(matches!(
            s.emitted.lock().unwrap().last(),
            Some(IndexStatusUpdate::IndexInSync { .. })
        ));
        s.emitted.lock().unwrap().clear();

        // watcher failed
        sync_failed(&mut s, 3);
        let status = s.manager.status();
        assert!(!status.watcher_started);
        assert!(!status.all_file_discovered);
        assert_eq!(status.total_file_events, 0);
        assert_eq!(status.handled_file_events, 0);
        assert_eq!(s.emitted.lock().unwrap().len(), 1);
        assert!(matches!(
            pop(&s),
            IndexStatusUpdate::ReinitializingBecauseFileSyncFailed { .. }
        ));
    }

    #[test]
    fn should_ignore_updates_happened_before_watcher_reset() {
        let mut s = setup();
        // starting normally
        s.manager
            .handle_watcher_discovered_file_during_initialization();
        s.manager
            .handle_watcher_discovered_file_during_initialization();
        s.manager.handle_watcher_started();
        s.manager.handle_file_updated();

        // event is not stale, handling it
        update(&mut s, 1, "doc1.txt");
        assert_eq!(*s.added.lock().unwrap(), vec![fa("doc1.txt")]);
        s.added.lock().unwrap().clear();

        s.manager.handle_file_updated();
        s.manager.handle_all_files_discovered();

        // watcher failed
        sync_failed(&mut s, 3);

        // stale event received
        update(&mut s, 2, "doc2.txt");
        assert!(s.added.lock().unwrap().is_empty());

        // reinitializing
        s.manager
            .handle_watcher_discovered_file_during_initialization();
        s.manager
            .handle_watcher_discovered_file_during_initialization();
        s.manager.handle_watcher_started();
        s.manager.handle_file_updated();

        // event happened after FileSyncFailed
        update(&mut s, 4, "doc1.txt");
        assert_eq!(*s.added.lock().unwrap(), vec![fa("doc1.txt")]);
    }
}
