use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, bail};
use async_channel::Sender;
use futures::future::BoxFuture;
use notify::event::{EventKind, ModifyKind, RenameMode};
use notify::{RecursiveMode, Watcher as _};
use rustc_hash::FxHashMap;
use walkdir::WalkDir;

use super::model::{FileEventSource, FileEventType, FileSyncEvent, StatusUpdate};
use super::utils::{canonical_path, Interner, Latch};

pub trait Watcher: Send + Sync {
    /// Watches `dir`, forwarding file changes to `file_sync_events` (with `t = -1`; `sync_fs` assigns
    /// timestamps). Must complete `started` once watching is set up. Only ever completes with an error;
    /// dropping the future stops watching.
    fn watch(
        &self,
        dir: PathBuf,
        interner: Arc<Interner>,
        file_sync_events: Sender<FileSyncEvent>,
        status_updates: Sender<StatusUpdate>,
        started: Latch,
    ) -> BoxFuture<'static, anyhow::Result<()>>;
}

#[derive(Debug)]
pub struct WatcherOverflowError;

impl std::fmt::Display for WatcherOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("watcher event queue overflowed")
    }
}
impl std::error::Error for WatcherOverflowError {}

/// `notify` (inotify on Linux) based watcher.
///
/// Mirrors `io.methvin:directory-watcher` as used by the Kotlin version: on start it walks the tree and
/// records each file's mtime (emitting `WatcherDiscoveredFileDuringInitialization` per file), and uses those
/// "hashes" to tell creates from modifies, drop no-op modifies, and expand directory moves/deletes.
pub struct FsWatcher;

type Known = FxHashMap<PathBuf, Option<SystemTime>>;

fn mtime(p: &Path) -> Option<SystemTime> {
    std::fs::metadata(p).and_then(|m| m.modified()).ok()
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Watcher for FsWatcher {
    fn watch(
        &self,
        dir: PathBuf,
        interner: Arc<Interner>,
        file_sync_events: Sender<FileSyncEvent>,
        status_updates: Sender<StatusUpdate>,
        started: Latch,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            let (raw_tx, mut raw_rx) = tokio::sync::mpsc::unbounded_channel::<notify::Result<notify::Event>>();
            let cancelled = Arc::new(AtomicBool::new(false));
            let _cancel_guard = CancelOnDrop(cancelled.clone());

            // registering watches and walking the tree are blocking; the walk takes a while on big repos
            // and must be cancellable, so it checks the flag set when this future is dropped
            let status = status_updates.clone();
            let init_dir = dir.clone();
            let (watcher, mut known) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                let mut w = notify::recommended_watcher(move |res| {
                    let _ = raw_tx.send(res);
                })?;
                w.watch(&init_dir, RecursiveMode::Recursive)?;
                let mut known = Known::default();
                for entry in WalkDir::new(&init_dir).into_iter().flatten() {
                    if cancelled.load(Ordering::Relaxed) {
                        bail!("cancelled");
                    }
                    if entry.file_type().is_file() {
                        status.send_blocking(StatusUpdate::WatcherDiscoveredFileDuringInitialization)?;
                        known.insert(entry.path().to_path_buf(), mtime(entry.path()));
                    }
                }
                Ok((Arc::new(std::sync::Mutex::new(w)), known))
            })
            .await??;

            status_updates.send(StatusUpdate::WatcherStarted).await?;
            started.complete();

            let emit = |path: &Path, kind: FileEventType| {
                let ev = FileSyncEvent {
                    t: -1,
                    file_address: interner.intern(canonical_path(path)),
                    source: FileEventSource::Watcher,
                    kind,
                };
                let status_updates = &status_updates;
                let file_sync_events = &file_sync_events;
                async move {
                    status_updates.send(StatusUpdate::FileUpdated { source: FileEventSource::Watcher }).await?;
                    file_sync_events.send(ev).await?;
                    anyhow::Ok(())
                }
            };

            while let Some(first) = raw_rx.recv().await {
                // take everything already queued as one batch (e.g. truncate + write of a single save)
                let mut batch = vec![first];
                while let Ok(more) = raw_rx.try_recv() {
                    batch.push(more);
                }
                let mut changes = Vec::new();
                for res in batch {
                    let event = res?;
                    if event.need_rescan() {
                        return Err(WatcherOverflowError.into());
                    }
                    for path in &event.paths {
                        if is_new_dir(&event.kind, path) {
                            // notify registers watches on new directories only after emitting the batch;
                            // register now so nothing created after the walk below can slip through
                            let (w, p) = (watcher.clone(), path.clone());
                            tokio::task::spawn_blocking(move || w.lock().unwrap().watch(&p, RecursiveMode::Recursive))
                                .await?
                                .ok(); // the directory may already be gone again
                        }
                        changes.extend(translate(&mut known, &event.kind, path));
                    }
                }
                for (p, kind) in coalesce(changes) {
                    emit(&p, kind).await?;
                }
            }
            Err(anyhow!("watcher stopped delivering events"))
        })
    }
}

fn is_new_dir(kind: &EventKind, path: &Path) -> bool {
    matches!(kind, EventKind::Create(_) | EventKind::Modify(ModifyKind::Name(RenameMode::To | RenameMode::Any)))
        && path.is_dir()
}

/// Merges several changes of the same file within one batch into one, keeping first-seen order.
/// The indexer reads the file's current content anyway, so only "gone" vs "there (new or changed)" matters.
fn coalesce(changes: Vec<(PathBuf, FileEventType)>) -> Vec<(PathBuf, FileEventType)> {
    let mut order: Vec<(PathBuf, FileEventType, FileEventType)> = Vec::new();
    let mut pos: FxHashMap<PathBuf, usize> = FxHashMap::default();
    for (p, kind) in changes {
        match pos.get(&p) {
            Some(&i) => order[i].2 = kind,
            None => {
                pos.insert(p.clone(), order.len());
                order.push((p, kind, kind));
            }
        }
    }
    order
        .into_iter()
        .map(|(p, first, last)| {
            let kind = match (first, last) {
                (_, FileEventType::Delete) => FileEventType::Delete,
                (FileEventType::Create, _) => FileEventType::Create,
                _ => FileEventType::Modify,
            };
            (p, kind)
        })
        .collect()
}

/// Turns a raw notify event into file-level CREATE/MODIFY/DELETE events, updating `known`.
fn translate(known: &mut Known, kind: &EventKind, path: &Path) -> Vec<(PathBuf, FileEventType)> {
    let mut out = Vec::new();
    match kind {
        EventKind::Create(_) | EventKind::Modify(ModifyKind::Name(RenameMode::To)) => appeared(known, path, &mut out),
        EventKind::Modify(ModifyKind::Data(_) | ModifyKind::Metadata(_) | ModifyKind::Any | ModifyKind::Other) => {
            if path.is_file() {
                appeared(known, path, &mut out)
            }
        }
        EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
            disappeared(known, path, &mut out)
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::Any | RenameMode::Other)) => {
            if path.exists() {
                appeared(known, path, &mut out)
            } else {
                disappeared(known, path, &mut out)
            }
        }
        // Name(Both) is reported alongside separate From/To events
        _ => {}
    }
    out
}

fn appeared(known: &mut Known, path: &Path, out: &mut Vec<(PathBuf, FileEventType)>) {
    let mut upsert = |p: &Path| {
        let m = mtime(p);
        match known.insert(p.to_path_buf(), m) {
            None => out.push((p.to_path_buf(), FileEventType::Create)),
            Some(prev) if prev != m => out.push((p.to_path_buf(), FileEventType::Modify)),
            Some(_) => {}
        }
    };
    if path.is_dir() {
        // files created in a new directory before its watch was registered would be missed otherwise
        for e in WalkDir::new(path).into_iter().flatten() {
            if e.file_type().is_file() {
                upsert(e.path());
            }
        }
    } else if path.is_file() {
        upsert(path);
    }
}

fn disappeared(known: &mut Known, path: &Path, out: &mut Vec<(PathBuf, FileEventType)>) {
    if known.remove(path).is_some() {
        out.push((path.to_path_buf(), FileEventType::Delete));
        return;
    }
    // a directory went away: drop everything under it
    let gone: Vec<PathBuf> = known.keys().filter(|k| k.starts_with(path)).cloned().collect();
    for p in gone {
        known.remove(&p);
        out.push((p, FileEventType::Delete));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use FileEventType::*;

    #[test]
    fn coalesces_changes_per_file() {
        let a = PathBuf::from("/a");
        let b = PathBuf::from("/b");
        let got = coalesce(vec![(a.clone(), Modify), (b.clone(), Create), (a.clone(), Modify), (b.clone(), Modify)]);
        assert_eq!(got, vec![(a.clone(), Modify), (b.clone(), Create)]);
        let got = coalesce(vec![(a.clone(), Create), (a.clone(), Delete), (b.clone(), Delete), (b.clone(), Create)]);
        assert_eq!(got, vec![(a, Delete), (b, Modify)]);
    }
}
