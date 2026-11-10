use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use walkdir::WalkDir;

use crate::internal::fs_watcher::Watcher;
use crate::internal::model::{FileEventSource, FileEventType, FileSyncEvent, StatusUpdate};
use crate::internal::utils::{Interner, Latch, canonical_path, is_regular_file};
use crate::model::IndexConfig;

/// Emits directory content and then watches it for changes.
///
/// The watcher is started first and its events are buffered, then directory content is emitted,
/// then buffered and new watcher events are emitted. That way changes that happen during the initial sync
/// are not lost. If the watcher fails, everything is reinitialized and `FileSyncFailed` is sent.
/// Every event carries a logical timestamp so that stale events can be discarded downstream.
pub(crate) async fn sync_fs(
    cfg: Arc<dyn IndexConfig>,
    dir: PathBuf,
    file_sync_events: flume::Sender<FileSyncEvent>,
    status_updates: flume::Sender<StatusUpdate>,
    watcher: Arc<dyn Watcher>,
) -> anyhow::Result<()> {
    let clock = AtomicI64::new(0);
    let interner = Arc::new(Interner::default());

    loop {
        let watcher_started = Latch::new();
        let (watcher_events_tx, watcher_events) = flume::unbounded::<FileSyncEvent>();

        let mut watcher_fut: futures::future::BoxFuture<'_, anyhow::Result<()>> =
            if cfg.enable_watcher() {
                watcher.watch(
                    dir.clone(),
                    interner.clone(),
                    watcher_events_tx,
                    status_updates.clone(),
                    watcher_started.clone(),
                )
            } else {
                watcher_started.open();
                Box::pin(async move {
                    let _keep_channel_open = watcher_events_tx;
                    std::future::pending::<anyhow::Result<()>>().await
                })
            };

        let mut job = Box::pin(async {
            watcher_started.wait().await;
            // emit initial file events only after watcher started
            emit_initial_content(
                &dir,
                &*cfg,
                &clock,
                &interner,
                &file_sync_events,
                &status_updates,
            )
            .await?;
            // emit watcher events only after initial sync completed
            while let Ok(event) = watcher_events.recv_async().await {
                let event = FileSyncEvent {
                    t: clock.fetch_add(1, Ordering::SeqCst) + 1,
                    ..event
                };
                file_sync_events.send_async(event).await?;
            }
            anyhow::Ok(())
        });

        let mut job_done = false;
        let watcher_result = loop {
            tokio::select! {
                r = &mut watcher_fut => break r,
                r = &mut job, if !job_done => {
                    // initial sync gave up or downstream is gone
                    r?;
                    job_done = true;
                }
            }
        };
        let error = match watcher_result {
            Err(e) => e,
            Ok(()) => anyhow::anyhow!("Watcher completed without error for some reason"),
        };
        cfg.handle_watcher_error(&error).await;
        // no more events from this generation will be sent after this point
        drop(job);
        drop(watcher_fut);
        let t = clock.fetch_add(1, Ordering::SeqCst) + 1;
        status_updates
            .send_async(StatusUpdate::FileSyncFailed {
                t,
                reason: Arc::new(error),
            })
            .await?;
    }
}

pub(crate) async fn emit_initial_content(
    dir: &Path,
    cfg: &dyn IndexConfig,
    clock: &AtomicI64,
    interner: &Interner,
    file_sync_events: &flume::Sender<FileSyncEvent>,
    status_updates: &flume::Sender<StatusUpdate>,
) -> anyhow::Result<()> {
    const RETRY_COUNT: u64 = 10;
    for attempt in 1..=RETRY_COUNT {
        let walk_error =
            match emit_once(dir, clock, interner, file_sync_events, status_updates).await? {
                Ok(()) => break,
                Err(e) => e,
            };

        if !dir.exists() {
            let not_found = anyhow::anyhow!("directory not found: {}", canonical_path(dir));
            cfg.handle_initial_file_sync_error(&not_found).await;
            return Err(walk_error.context(not_found));
        }

        cfg.handle_initial_file_sync_error(&walk_error).await;
        if attempt == RETRY_COUNT {
            return Err(walk_error);
        }

        // the usual cause is someone deleting directory content while we are trying to index it,
        // there is a good chance it fails again if we retry immediately - back off
        tokio::time::sleep(Duration::from_secs(attempt - 1)).await;
    }

    status_updates
        .send_async(StatusUpdate::AllFilesDiscovered)
        .await?;
    Ok(())
}

/// Outer error: fatal (channel closed). Inner error: directory walk failed, can be retried.
async fn emit_once(
    dir: &Path,
    clock: &AtomicI64,
    interner: &Interner,
    file_sync_events: &flume::Sender<FileSyncEvent>,
    status_updates: &flume::Sender<StatusUpdate>,
) -> anyhow::Result<anyhow::Result<()>> {
    // walk on a blocking thread; it stops as soon as the receiver is dropped
    let (paths_tx, paths) = flume::bounded::<anyhow::Result<String>>(256);
    let root = dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(&root) {
            let item = match entry {
                Ok(entry) if is_regular_file(entry.path()) => Ok(canonical_path(entry.path())),
                Ok(_) => continue,
                Err(e) => Err(e.into()),
            };
            let failed = item.is_err();
            if paths_tx.send(item).is_err() || failed {
                return;
            }
        }
    });

    while let Ok(item) = paths.recv_async().await {
        let path = match item {
            Ok(path) => path,
            Err(e) => return Ok(Err(e)),
        };
        status_updates
            .send_async(StatusUpdate::FileUpdated(FileEventSource::InitialSync))
            .await?;
        let event = FileSyncEvent {
            t: clock.fetch_add(1, Ordering::SeqCst) + 1,
            file_address: interner.intern(path),
            source: FileEventSource::InitialSync,
            kind: FileEventType::Create,
        };
        file_sync_events.send_async(event).await?;
    }
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use async_trait::async_trait;

    use super::*;
    use crate::internal::fs_watcher::FsWatcher;
    use crate::internal::model::FileEventSource::{InitialSync, Watcher as WatcherSource};
    use crate::model::FileAddress;
    use crate::test_utils::{TestConfig, file_address_of, initialize_directory, with_timeout};

    struct Harness {
        status_updates: flume::Receiver<StatusUpdate>,
        status_updates_tx: flume::Sender<StatusUpdate>,
        file_sync_events: flume::Receiver<FileSyncEvent>,
        job: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    fn launch(dir: &Path, watcher: Arc<dyn Watcher>) -> Harness {
        // rendezvous channels make the test step through syncFs one event at a time
        let (status_updates_tx, status_updates) = flume::bounded(0);
        let (events_tx, file_sync_events) = flume::bounded(0);
        let job = tokio::spawn(sync_fs(
            Arc::new(TestConfig::default()),
            dir.to_path_buf(),
            events_tx,
            status_updates_tx.clone(),
            watcher,
        ));
        Harness {
            status_updates,
            status_updates_tx,
            file_sync_events,
            job,
        }
    }

    impl Harness {
        async fn status(&self) -> StatusUpdate {
            self.status_updates.recv_async().await.unwrap()
        }

        async fn receive_file_updated(&self, source: FileEventSource) {
            match self.status().await {
                StatusUpdate::FileUpdated(s) => assert_eq!(s, source),
                other => panic!("expected FileUpdated, got {other:?}"),
            }
        }

        async fn receive_file_sync_event(
            &self,
            t: i64,
            source: FileEventSource,
            kind: FileEventType,
        ) -> FileAddress {
            let event = self.file_sync_events.recv_async().await.unwrap();
            assert_eq!(event.t, t);
            assert_eq!(event.source, source);
            assert_eq!(event.kind, kind);
            event.file_address
        }

        async fn receive_watcher_discovered_file(&self) {
            let update = self.status().await;
            assert!(
                matches!(
                    update,
                    StatusUpdate::WatcherDiscoveredFileDuringInitialization
                ),
                "{update:?}"
            );
        }

        async fn receive_watcher_started(&self) {
            let update = self.status().await;
            assert!(matches!(update, StatusUpdate::WatcherStarted), "{update:?}");
        }

        async fn receive_all_files_discovered(&self) {
            let update = self.status().await;
            assert!(
                matches!(update, StatusUpdate::AllFilesDiscovered),
                "{update:?}"
            );
        }

        async fn receive_file_sync_failed(&self) -> i64 {
            match self.status().await {
                StatusUpdate::FileSyncFailed { t, .. } => t,
                other => panic!("expected FileSyncFailed, got {other:?}"),
            }
        }

        /// Pretends to be a watcher: sends initialization events and receives them back.
        async fn complete_watcher_initialization(&self, watcher_started: Latch) {
            for _ in 0..3 {
                self.send_and_receive(StatusUpdate::WatcherDiscoveredFileDuringInitialization)
                    .await;
            }
            self.send_and_receive(StatusUpdate::WatcherStarted).await;
            watcher_started.open();
        }

        async fn send_and_receive(&self, update: StatusUpdate) {
            let tx = self.status_updates_tx.clone();
            let send = tokio::spawn(async move { tx.send_async(update).await.unwrap() });
            self.status().await;
            send.await.unwrap();
        }

        async fn complete_initial_file_sync(&self, last_t: i64) {
            for i in 1..=3 {
                self.receive_file_updated(InitialSync).await;
                self.receive_file_sync_event(last_t + i, InitialSync, FileEventType::Create)
                    .await;
            }
            self.receive_all_files_discovered().await;
        }

        async fn cancel(self) {
            self.job.abort();
            assert!(self.job.await.unwrap_err().is_cancelled());
        }
    }

    /// Watcher that does nothing until the test breaks it.
    struct NoopWatcher {
        latches: flume::Sender<Latch>,
        breaks: flume::Receiver<anyhow::Error>,
    }

    struct NoopWatcherControls {
        latches: flume::Receiver<Latch>,
        breaks: flume::Sender<anyhow::Error>,
    }

    fn noop_watcher() -> (Arc<dyn Watcher>, NoopWatcherControls) {
        let (latches_tx, latches) = flume::unbounded();
        let (breaks, breaks_rx) = flume::unbounded();
        (
            Arc::new(NoopWatcher {
                latches: latches_tx,
                breaks: breaks_rx,
            }),
            NoopWatcherControls { latches, breaks },
        )
    }

    #[async_trait]
    impl Watcher for NoopWatcher {
        async fn watch(
            &self,
            _dir: PathBuf,
            _interner: Arc<Interner>,
            _file_sync_events: flume::Sender<FileSyncEvent>,
            _status_updates: flume::Sender<StatusUpdate>,
            watcher_started: Latch,
        ) -> anyhow::Result<()> {
            self.latches.send_async(watcher_started).await.unwrap();
            Err(self.breaks.recv_async().await.unwrap())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_initialize_and_run_with_correct_events() {
        with_timeout(5, async {
            let dir = tempfile::tempdir().unwrap();
            let setup = initialize_directory(dir.path());
            let h = launch(dir.path(), Arc::new(FsWatcher));

            let mut emitted_files = vec![];

            // watcher initialization
            for _ in 0..3 {
                h.receive_watcher_discovered_file().await;
            }
            h.receive_watcher_started().await;

            // file sync start
            h.receive_file_updated(InitialSync).await;
            emitted_files.push(
                h.receive_file_sync_event(1, InitialSync, FileEventType::Create)
                    .await,
            );
            h.receive_file_updated(InitialSync).await;
            emitted_files.push(
                h.receive_file_sync_event(2, InitialSync, FileEventType::Create)
                    .await,
            );
            h.receive_file_updated(InitialSync).await;

            // some file updated during initialization
            std::fs::remove_file(emitted_files[0].path()).unwrap();

            emitted_files.push(
                h.receive_file_sync_event(3, InitialSync, FileEventType::Create)
                    .await,
            );

            // AllFilesDiscovered and watcher's FileUpdated race with each other
            let mut all_files_discovered = false;
            let mut watcher_file_updated = false;
            for _ in 0..2 {
                match h.status().await {
                    StatusUpdate::AllFilesDiscovered => all_files_discovered = true,
                    StatusUpdate::FileUpdated(WatcherSource) => watcher_file_updated = true,
                    other => panic!("unexpected {other:?}"),
                }
            }
            assert!(all_files_discovered && watcher_file_updated);

            assert_eq!(
                emitted_files.iter().cloned().collect::<HashSet<_>>(),
                [&setup.loupa, &setup.poupa, &setup.nested_poupa]
                    .map(|p| file_address_of(p))
                    .into_iter()
                    .collect(),
            );

            // file updated during initialization was postponed until now
            let deleted = h
                .receive_file_sync_event(4, WatcherSource, FileEventType::Delete)
                .await;
            assert_eq!(deleted, emitted_files[0]);

            // created files are captured
            let volobuev = dir.path().join("volobuev");
            std::fs::File::create(&volobuev).unwrap();
            h.receive_file_updated(WatcherSource).await;
            let created = h
                .receive_file_sync_event(5, WatcherSource, FileEventType::Create)
                .await;
            assert_eq!(created, file_address_of(&volobuev));

            // updated files are also captured
            std::fs::write(&setup.nested_poupa, "some really important content update").unwrap();
            h.receive_file_updated(WatcherSource).await;
            let modified = h
                .receive_file_sync_event(6, WatcherSource, FileEventType::Modify)
                .await;
            assert_eq!(modified, file_address_of(&setup.nested_poupa));

            h.cancel().await;
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_cancel_during_initial_file_sync() {
        with_timeout(2, async {
            let dir = tempfile::tempdir().unwrap();
            initialize_directory(dir.path());
            let h = launch(dir.path(), Arc::new(FsWatcher));

            for _ in 0..3 {
                h.receive_watcher_discovered_file().await;
            }
            h.receive_watcher_started().await;

            h.receive_file_updated(InitialSync).await;
            h.receive_file_sync_event(1, InitialSync, FileEventType::Create)
                .await;

            // there are 2 more files, but cancel!
            h.cancel().await;
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_cancel_during_watcher_initializing() {
        with_timeout(2, async {
            let dir = tempfile::tempdir().unwrap();
            initialize_directory(dir.path());
            let h = launch(dir.path(), Arc::new(FsWatcher));

            h.receive_watcher_discovered_file().await;
            h.receive_watcher_discovered_file().await;

            // there is one more file, but cancel!
            h.cancel().await;
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn emitted_file_addresses_should_have_referential_equality() {
        with_timeout(2, async {
            let dir = tempfile::tempdir().unwrap();
            let poupa = dir.path().join("poupa");
            std::fs::File::create(&poupa).unwrap();
            let h = launch(dir.path(), Arc::new(FsWatcher));

            h.receive_watcher_discovered_file().await;
            h.receive_watcher_started().await;

            h.receive_file_updated(InitialSync).await;
            let first = h
                .receive_file_sync_event(1, InitialSync, FileEventType::Create)
                .await;
            h.receive_all_files_discovered().await;

            std::fs::remove_file(&poupa).unwrap();
            h.receive_file_updated(WatcherSource).await;
            let second = h
                .receive_file_sync_event(2, WatcherSource, FileEventType::Delete)
                .await;

            assert!(first.ptr_eq(&second));
            h.cancel().await;
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_be_able_to_reinitialize_when_watcher_error_happens_after_initial_file_sync() {
        with_timeout(2, async {
            let dir = tempfile::tempdir().unwrap();
            initialize_directory(dir.path());
            let (watcher, controls) = noop_watcher();
            let h = launch(dir.path(), watcher);

            let watcher_started = controls.latches.recv_async().await.unwrap();
            h.complete_watcher_initialization(watcher_started).await;

            // start file sync
            h.receive_file_updated(InitialSync).await;
            h.receive_file_sync_event(1, InitialSync, FileEventType::Create)
                .await;
            h.receive_file_updated(InitialSync).await;

            // watcher breaks
            controls.breaks.send(anyhow::anyhow!("poupa")).unwrap();
            assert_eq!(h.receive_file_sync_failed().await, 3);

            // watcher restarts, fs reinitializes
            let watcher_started = controls.latches.recv_async().await.unwrap();
            h.complete_watcher_initialization(watcher_started).await;
            h.complete_initial_file_sync(3).await;

            h.cancel().await;
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_be_able_to_reinitialize_when_watcher_error_happens_before_initial_file_sync() {
        with_timeout(2, async {
            let dir = tempfile::tempdir().unwrap();
            initialize_directory(dir.path());
            let (watcher, controls) = noop_watcher();
            let h = launch(dir.path(), watcher);

            // watcher initialization, never completed
            let _never_opened = controls.latches.recv_async().await.unwrap();
            h.send_and_receive(StatusUpdate::WatcherDiscoveredFileDuringInitialization)
                .await;
            h.send_and_receive(StatusUpdate::WatcherDiscoveredFileDuringInitialization)
                .await;

            // watcher breaks
            controls.breaks.send(anyhow::anyhow!("poupa")).unwrap();
            assert_eq!(h.receive_file_sync_failed().await, 1);

            // watcher restarts, fs reinitializes
            let watcher_started = controls.latches.recv_async().await.unwrap();
            h.complete_watcher_initialization(watcher_started).await;
            h.complete_initial_file_sync(1).await;

            h.cancel().await;
        })
        .await
    }
}
