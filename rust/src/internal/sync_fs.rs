use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_channel::Sender;
use futures::future::BoxFuture;
use walkdir::WalkDir;

use super::fs_watcher::Watcher;
use super::model::{FileEventSource, FileEventType, FileSyncEvent, StatusUpdate};
use super::utils::{canonical_path, Interner, Latch};
use crate::model::IndexConfig;

/// Emits the directory content and then file changes, restarting everything whenever the watcher fails.
///
/// The watcher is started first and its events are buffered; the initial directory walk is emitted after
/// the watcher is up, followed by the buffered and live watcher events. Every event carries a logical
/// timestamp so the index manager can drop stale ones. Returns only on a fatal error.
pub async fn sync_fs(
    cfg: Arc<dyn IndexConfig>,
    dir: PathBuf,
    file_sync_events: Sender<FileSyncEvent>,
    status_updates: Sender<StatusUpdate>,
    watcher: Arc<dyn Watcher>,
) -> anyhow::Result<Infallible> {
    let clock = AtomicI64::new(0);
    let interner = Arc::new(Interner::default());

    loop {
        let watcher_error =
            run_generation(&cfg, &dir, &file_sync_events, &status_updates, &*watcher, &clock, &interner).await?;
        cfg.handle_watcher_error(&watcher_error);
        // the generation future is dropped by now, so no more events from it can be sent
        let t = clock.fetch_add(1, Ordering::SeqCst) + 1;
        status_updates.send(StatusUpdate::FileSyncFailed { t, reason: Arc::new(watcher_error) }).await?;
    }
}

/// Runs one watcher + initial-sync generation. `Ok(e)` = the watcher failed with `e` (restart);
/// `Err` = fatal.
async fn run_generation(
    cfg: &Arc<dyn IndexConfig>,
    dir: &Path,
    file_sync_events: &Sender<FileSyncEvent>,
    status_updates: &Sender<StatusUpdate>,
    watcher: &dyn Watcher,
    clock: &AtomicI64,
    interner: &Arc<Interner>,
) -> anyhow::Result<anyhow::Error> {
    let started = Latch::default();
    let (watcher_tx, watcher_rx) = async_channel::unbounded::<FileSyncEvent>();

    let mut watcher_fut: BoxFuture<'static, anyhow::Result<()>> = if cfg.enable_watcher() {
        watcher.watch(dir.to_path_buf(), interner.clone(), watcher_tx.clone(), status_updates.clone(), started.clone())
    } else {
        started.complete();
        Box::pin(futures::future::pending())
    };

    let job = async {
        started.wait().await;
        // emit initial file events only after the watcher started
        emit_initial_content(dir, &**cfg, clock, interner, file_sync_events, status_updates).await?;
        // emit watcher events only after the initial sync completed
        while let Ok(ev) = watcher_rx.recv().await {
            let t = clock.fetch_add(1, Ordering::SeqCst) + 1;
            file_sync_events.send(FileSyncEvent { t, ..ev }).await?;
        }
        // unreachable while `watcher_tx` is alive; keeps the disabled-watcher case idle forever
        futures::future::pending::<anyhow::Result<Infallible>>().await
    };
    tokio::pin!(job);

    let watcher_result = tokio::select! {
        r = &mut watcher_fut => r,
        r = &mut job => return Err(match r { Err(e) => e, Ok(never) => match never {} }),
    };
    drop(watcher_tx);
    Ok(match watcher_result {
        Err(e) => e,
        Ok(()) => anyhow!("Watcher completed without exception for some reason"),
    })
}

pub async fn emit_initial_content(
    dir: &Path,
    cfg: &dyn IndexConfig,
    clock: &AtomicI64,
    interner: &Interner,
    file_sync_events: &Sender<FileSyncEvent>,
    status_updates: &Sender<StatusUpdate>,
) -> anyhow::Result<()> {
    const RETRY_COUNT: u64 = 10;
    for attempt in 1..=RETRY_COUNT {
        match walk_once(dir, clock, interner, file_sync_events, status_updates).await? {
            Ok(()) => break,
            Err(e) => {
                if !dir.exists() {
                    let e1 = anyhow!("{}: no such file or directory ({e:#})", canonical_path(dir));
                    cfg.handle_initial_file_sync_error(&e1);
                    return Err(e1);
                }
                cfg.handle_initial_file_sync_error(&e);
                if attempt == RETRY_COUNT {
                    return Err(e);
                }
                // usually someone is deleting directory content while we index it; retrying right away
                // would likely fail again, so back off
                tokio::time::sleep(Duration::from_secs(attempt - 1)).await;
            }
        }
    }
    status_updates.send(StatusUpdate::AllFilesDiscovered).await?;
    Ok(())
}

/// Outer error: fatal (channel closed). Inner error: walk failed, retryable.
async fn walk_once(
    dir: &Path,
    clock: &AtomicI64,
    interner: &Interner,
    file_sync_events: &Sender<FileSyncEvent>,
    status_updates: &Sender<StatusUpdate>,
) -> anyhow::Result<anyhow::Result<()>> {
    // the walk is blocking IO, so it runs on the blocking pool and streams canonical paths back;
    // it stops as soon as this future is dropped (send_blocking fails on a closed channel)
    let (tx, rx) = async_channel::bounded::<Result<String, walkdir::Error>>(256);
    let root = dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(&root) {
            let item = entry.map(|e| {
                let ft = e.file_type();
                let is_file = ft.is_file() || (ft.is_symlink() && e.path().is_file());
                is_file.then(|| canonical_path(e.path()))
            });
            let item = match item {
                Ok(None) => continue,
                Ok(Some(p)) => Ok(p),
                Err(e) => Err(e),
            };
            let failed = item.is_err();
            if tx.send_blocking(item).is_err() || failed {
                return;
            }
        }
    });

    while let Ok(item) = rx.recv().await {
        let path = match item {
            Ok(p) => p,
            Err(e) => return Ok(Err(e.into())),
        };
        status_updates.send(StatusUpdate::FileUpdated { source: FileEventSource::InitialSync }).await?;
        let t = clock.fetch_add(1, Ordering::SeqCst) + 1;
        file_sync_events
            .send(FileSyncEvent {
                t,
                file_address: interner.intern(path),
                source: FileEventSource::InitialSync,
                kind: FileEventType::Create,
            })
            .await?;
    }
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use async_channel::Receiver;
    use futures::stream::BoxStream;

    use super::*;
    use crate::internal::fs_watcher::FsWatcher;
    use crate::internal::FileAddress;
    use crate::model::TokenIndex;

    struct Cfg(AtomicBool);
    impl IndexConfig for Cfg {
        fn enable_logging(&self) -> &AtomicBool {
            &self.0
        }
        fn enable_watcher(&self) -> bool {
            true
        }
        fn tokenize(&self, _: &str) -> Vec<String> {
            vec![]
        }
        fn find(&self, _: Arc<dyn TokenIndex>, _: String) -> BoxStream<'static, FileAddress> {
            unimplemented!()
        }
        fn matches(&self, _: &str, _: &str) -> bool {
            false
        }
        fn handle_watcher_error(&self, _: &anyhow::Error) {}
        fn handle_initial_file_sync_error(&self, _: &anyhow::Error) {}
    }

    /// poupa, loupa, nestedDir/poupa
    fn initialize_directory(dir: &Path) -> [PathBuf; 3] {
        std::fs::create_dir(dir.join("nestedDir")).unwrap();
        let files = [dir.join("poupa"), dir.join("loupa"), dir.join("nestedDir/poupa")];
        for f in &files {
            std::fs::write(f, "").unwrap();
        }
        files
    }

    struct Harness {
        events: Receiver<FileSyncEvent>,
        status: Receiver<StatusUpdate>,
        status_tx: Sender<StatusUpdate>,
        job: tokio::task::JoinHandle<anyhow::Result<Infallible>>,
    }

    fn start(dir: &Path, watcher: Arc<dyn Watcher>) -> Harness {
        // file events rendezvous-like (capacity 1); status unbounded so the test can inspect it in order
        let (etx, erx) = async_channel::bounded(1);
        let (stx, srx) = async_channel::unbounded();
        let cfg: Arc<dyn IndexConfig> = Arc::new(Cfg(AtomicBool::new(false)));
        let job = tokio::spawn(sync_fs(cfg, dir.to_path_buf(), etx, stx.clone(), watcher));
        Harness { events: erx, status: srx, status_tx: stx, job }
    }

    async fn recv<T>(rx: &Receiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("timed out").unwrap()
    }

    impl Harness {
        async fn status(&self) -> StatusUpdate {
            recv(&self.status).await
        }
        async fn expect_status(&self, expected: &str) {
            let s = format!("{:?}", self.status().await);
            assert!(s.starts_with(expected), "expected {expected}, got {s}");
        }
        async fn event(&self, t: i64, source: FileEventSource, kind: FileEventType) -> FileAddress {
            let e = recv(&self.events).await;
            assert_eq!((e.t, e.source, e.kind), (t, source, kind), "{e:?}");
            e.file_address
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_initialize_and_run_with_correct_events() {
        let tmp = tempfile::tempdir().unwrap();
        let files = initialize_directory(tmp.path());
        let h = start(tmp.path(), Arc::new(FsWatcher));
        use FileEventSource::*;
        use FileEventType::*;

        for _ in 0..3 {
            h.expect_status("WatcherDiscoveredFileDuringInitialization").await;
        }
        h.expect_status("WatcherStarted").await;

        let mut emitted = vec![h.event(1, InitialSync, Create).await, h.event(2, InitialSync, Create).await];
        // a file changes during the initial sync...
        std::fs::remove_file(emitted[0].path()).unwrap();
        emitted.push(h.event(3, InitialSync, Create).await);
        let mut expected: Vec<String> = files.iter().map(|f| canonical_path(f)).collect();
        expected.sort();
        let mut got: Vec<String> = emitted.iter().map(|f| f.path().to_owned()).collect();
        got.sort();
        assert_eq!(got, expected);

        // ...and is reported only after it
        let deleted = h.event(4, Watcher, Delete).await;
        assert_eq!(deleted, emitted[0]);
        assert!(deleted.ptr_eq(&emitted[0]), "emitted file addresses are interned");

        let volobuev = tmp.path().join("volobuev");
        std::fs::write(&volobuev, "").unwrap();
        assert_eq!(h.event(5, Watcher, Create).await.path(), canonical_path(&volobuev));

        std::fs::write(&files[2], "some really important content update").unwrap();
        assert_eq!(h.event(6, Watcher, Modify).await.path(), canonical_path(&files[2]));

        for _ in 0..3 {
            h.expect_status("FileUpdated(InitialSync)").await;
        }
        h.expect_status("AllFilesDiscovered").await;
        for _ in 0..3 {
            h.expect_status("FileUpdated(Watcher)").await;
        }
        h.job.abort();
    }

    #[tokio::test]
    async fn should_cancel_during_initial_file_sync() {
        let tmp = tempfile::tempdir().unwrap();
        initialize_directory(tmp.path());
        let h = start(tmp.path(), Arc::new(FsWatcher));
        h.expect_status("WatcherDiscoveredFileDuringInitialization").await;
        h.event(1, FileEventSource::InitialSync, FileEventType::Create).await;
        h.job.abort();
        assert!(h.job.await.unwrap_err().is_cancelled());
    }

    /// Kotlin `noopWatcherSetup`: hands out the `started` latch and fails when told to.
    struct NoopWatcher {
        latches: Sender<Latch>,
        breaks: Receiver<anyhow::Error>,
    }

    impl Watcher for NoopWatcher {
        fn watch(
            &self,
            _: PathBuf,
            _: Arc<Interner>,
            _: Sender<FileSyncEvent>,
            _: Sender<StatusUpdate>,
            started: Latch,
        ) -> BoxFuture<'static, anyhow::Result<()>> {
            let (l, b) = (self.latches.clone(), self.breaks.clone());
            Box::pin(async move {
                l.send(started).await.unwrap();
                Err(b.recv().await.unwrap())
            })
        }
    }

    fn noop_watcher() -> (Arc<dyn Watcher>, Receiver<Latch>, Sender<anyhow::Error>) {
        let (ltx, lrx) = async_channel::unbounded();
        let (btx, brx) = async_channel::unbounded();
        (Arc::new(NoopWatcher { latches: ltx, breaks: brx }), lrx, btx)
    }

    impl Harness {
        async fn complete_watcher_initialization(&self, latch: Latch) {
            for _ in 0..3 {
                self.status_tx.send(StatusUpdate::WatcherDiscoveredFileDuringInitialization).await.unwrap();
                self.expect_status("WatcherDiscoveredFileDuringInitialization").await;
            }
            self.status_tx.send(StatusUpdate::WatcherStarted).await.unwrap();
            self.expect_status("WatcherStarted").await;
            latch.complete();
        }

        /// Skips leftovers of the failed generation; returns FileSyncFailed's t.
        async fn receive_file_sync_failed(&self) -> i64 {
            loop {
                match self.status().await {
                    StatusUpdate::FileSyncFailed { t, .. } => return t,
                    StatusUpdate::FileUpdated { .. } => continue,
                    other => panic!("unexpected {other:?}"),
                }
            }
        }

        async fn complete_initial_file_sync(&self, failed_t: i64) {
            let mut ts = Vec::new();
            while ts.len() < 3 {
                let e = recv(&self.events).await;
                if e.t < failed_t {
                    continue; // stale, buffered before the failure; the index manager drops these
                }
                assert_eq!((e.source, e.kind), (FileEventSource::InitialSync, FileEventType::Create));
                ts.push(e.t);
            }
            assert!(ts.windows(2).all(|w| w[0] < w[1]) && ts[0] > failed_t, "{ts:?} after {failed_t}");
            for _ in 0..3 {
                self.expect_status("FileUpdated(InitialSync)").await;
            }
            self.expect_status("AllFilesDiscovered").await;
        }
    }

    #[tokio::test]
    async fn should_reinitialize_when_watcher_error_happens_after_initial_file_sync() {
        let tmp = tempfile::tempdir().unwrap();
        initialize_directory(tmp.path());
        let (watcher, latches, breaks) = noop_watcher();
        let h = start(tmp.path(), watcher);

        h.complete_watcher_initialization(recv(&latches).await).await;
        h.complete_initial_file_sync(0).await;

        breaks.send(anyhow::anyhow!("poupa")).await.unwrap();
        let failed_t = h.receive_file_sync_failed().await;
        assert_eq!(failed_t, 4);

        h.complete_watcher_initialization(recv(&latches).await).await;
        h.complete_initial_file_sync(failed_t).await;
        h.job.abort();
    }

    #[tokio::test]
    async fn should_reinitialize_when_watcher_error_happens_during_initial_file_sync() {
        let tmp = tempfile::tempdir().unwrap();
        initialize_directory(tmp.path());
        let (watcher, latches, breaks) = noop_watcher();
        let h = start(tmp.path(), watcher);

        h.complete_watcher_initialization(recv(&latches).await).await;
        h.expect_status("FileUpdated(InitialSync)").await;
        h.event(1, FileEventSource::InitialSync, FileEventType::Create).await;

        breaks.send(anyhow::anyhow!("poupa")).await.unwrap();
        let failed_t = h.receive_file_sync_failed().await;

        h.complete_watcher_initialization(recv(&latches).await).await;
        h.complete_initial_file_sync(failed_t).await;
        h.job.abort();
    }

    #[tokio::test]
    async fn should_reinitialize_when_watcher_error_happens_before_initial_file_sync() {
        let tmp = tempfile::tempdir().unwrap();
        initialize_directory(tmp.path());
        let (watcher, latches, breaks) = noop_watcher();
        let h = start(tmp.path(), watcher);

        let _first = recv(&latches).await; // never completed
        breaks.send(anyhow::anyhow!("poupa")).await.unwrap();
        assert_eq!(h.receive_file_sync_failed().await, 1);

        h.complete_watcher_initialization(recv(&latches).await).await;
        h.complete_initial_file_sync(1).await;
        h.job.abort();
    }

    #[tokio::test]
    async fn should_fail_when_directory_does_not_exist() {
        let tmp = tempfile::tempdir().unwrap();
        let (watcher, latches, _breaks) = noop_watcher();
        let h = start(&tmp.path().join("missing"), watcher);
        recv(&latches).await.complete();
        let r = tokio::time::timeout(Duration::from_secs(5), h.job).await.unwrap().unwrap();
        assert!(r.is_err());
    }
}
