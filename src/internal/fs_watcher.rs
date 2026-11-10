use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use notify::event::{ModifyKind, RemoveKind, RenameMode};
use notify::{EventKind, RecursiveMode, Watcher as _};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::internal::model::{FileEventSource, FileEventType, FileSyncEvent, StatusUpdate};
use crate::internal::utils::{
    Interner, Latch, canonical_path, is_regular_file, send_blocking_cancellable,
};

#[async_trait]
pub(crate) trait Watcher: Send + Sync {
    /// Watches `dir`, sending change events (with `t = -1`) to `file_sync_events`.
    /// Opens `watcher_started` once the watcher is ready. Runs until the watcher fails.
    async fn watch(
        &self,
        dir: PathBuf,
        interner: Arc<Interner>,
        file_sync_events: flume::Sender<FileSyncEvent>,
        status_updates: flume::Sender<StatusUpdate>,
        watcher_started: Latch,
    ) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub(crate) struct WatcherOverflowError;

impl std::fmt::Display for WatcherOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("watcher overflow: some file system events were lost")
    }
}

impl std::error::Error for WatcherOverflowError {}

/// Watcher based on the `notify` crate (inotify / FSEvents / ReadDirectoryChangesW).
pub(crate) struct FsWatcher;

#[async_trait]
impl Watcher for FsWatcher {
    async fn watch(
        &self,
        dir: PathBuf,
        interner: Arc<Interner>,
        file_sync_events: flume::Sender<FileSyncEvent>,
        status_updates: flume::Sender<StatusUpdate>,
        watcher_started: Latch,
    ) -> anyhow::Result<()> {
        // stops blocking work and the event callback when this future is dropped
        let cancel = CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        let (errors_tx, errors_rx) = flume::unbounded::<anyhow::Error>();

        let init = {
            let cancel = cancel.clone();
            let status_updates = status_updates.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<notify::RecommendedWatcher> {
                let handler = EventHandler {
                    interner,
                    file_sync_events,
                    status_updates: status_updates.clone(),
                    errors: errors_tx,
                    cancel: cancel.clone(),
                };
                let mut watcher = notify::recommended_watcher(move |res| handler.handle(res))?;
                watcher.watch(&dir, RecursiveMode::Recursive)?;

                // report progress of watcher initialization: one event per file in the directory
                for entry in WalkDir::new(&dir).into_iter().filter_map(Result::ok) {
                    if cancel.is_cancelled() {
                        anyhow::bail!("watcher initialization cancelled");
                    }
                    if is_regular_file(entry.path())
                        && !send_blocking_cancellable(
                            &status_updates,
                            StatusUpdate::WatcherDiscoveredFileDuringInitialization,
                            &cancel,
                        )
                    {
                        anyhow::bail!("watcher initialization cancelled");
                    }
                }
                Ok(watcher)
            })
        };
        let watcher = init.await??;

        status_updates
            .send_async(StatusUpdate::WatcherStarted)
            .await?;
        watcher_started.open();

        let error = errors_rx
            .recv_async()
            .await
            .unwrap_or_else(|_| anyhow::anyhow!("watcher event handler stopped"));
        drop(watcher);
        Err(error)
    }
}

struct EventHandler {
    interner: Arc<Interner>,
    file_sync_events: flume::Sender<FileSyncEvent>,
    status_updates: flume::Sender<StatusUpdate>,
    errors: flume::Sender<anyhow::Error>,
    cancel: CancellationToken,
}

impl EventHandler {
    fn handle(&self, res: notify::Result<notify::Event>) {
        if self.cancel.is_cancelled() {
            return;
        }
        let event = match res {
            Ok(event) => event,
            Err(e) => {
                let _ = self.errors.send(e.into());
                return;
            }
        };
        if event.need_rescan() {
            let _ = self.errors.send(WatcherOverflowError.into());
            return;
        }

        match event.kind {
            EventKind::Create(_) => event.paths.iter().for_each(|p| self.created(p)),
            EventKind::Modify(ModifyKind::Name(mode)) => match (mode, event.paths.as_slice()) {
                (RenameMode::From, paths) => paths
                    .iter()
                    .for_each(|p| self.emit(p, FileEventType::Delete)),
                (RenameMode::To, paths) => paths.iter().for_each(|p| self.created(p)),
                (RenameMode::Both, [from, to]) => {
                    self.emit(from, FileEventType::Delete);
                    self.created(to);
                }
                (_, paths) => paths.iter().for_each(|p| {
                    if p.exists() {
                        self.created(p)
                    } else {
                        self.emit(p, FileEventType::Delete)
                    }
                }),
            },
            // last modified time is what matters, permission/ownership changes are ignored
            EventKind::Modify(ModifyKind::Metadata(_)) => {}
            EventKind::Modify(_) => event
                .paths
                .iter()
                .filter(|p| !p.is_dir())
                .for_each(|p| self.emit(p, FileEventType::Modify)),
            EventKind::Remove(RemoveKind::Folder) => {}
            EventKind::Remove(_) => event
                .paths
                .iter()
                .for_each(|p| self.emit(p, FileEventType::Delete)),
            EventKind::Access(_) | EventKind::Any | EventKind::Other => {}
        }
    }

    /// A created (or moved in) directory may already contain files, emit them all.
    fn created(&self, path: &Path) {
        if path.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(Result::ok) {
                if is_regular_file(entry.path()) {
                    self.emit(entry.path(), FileEventType::Create);
                }
            }
        } else {
            self.emit(path, FileEventType::Create);
        }
    }

    fn emit(&self, path: &Path, kind: FileEventType) {
        if !send_blocking_cancellable(
            &self.status_updates,
            StatusUpdate::FileUpdated(FileEventSource::Watcher),
            &self.cancel,
        ) {
            return;
        }
        let event = FileSyncEvent {
            t: -1,
            file_address: self.interner.intern(canonical_path(path)),
            source: FileEventSource::Watcher,
            kind,
        };
        send_blocking_cancellable(&self.file_sync_events, event, &self.cancel);
    }
}
