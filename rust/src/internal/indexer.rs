use std::path::Path;
use std::sync::Arc;

use async_channel::{Receiver, Sender};
use rustc_hash::FxHashSet;

use super::model::{FileEventType, FileSyncEvent, FileUpdateRequest};
use super::utils::{debug_log, read_lines};
use crate::model::IndexConfig;

const MAX_FILE_SIZE: u64 = 10_000_000;

/// Reads and tokenizes files named by sync events, forwarding the result to the index manager.
/// Several of these run in parallel, sharing one MPMC input channel.
pub async fn indexer(
    cfg: Arc<dyn IndexConfig>,
    file_sync_events: Receiver<FileSyncEvent>,
    index_update_requests: Sender<FileUpdateRequest>,
) -> anyhow::Result<()> {
    while let Ok(event) = file_sync_events.recv().await {
        debug_log(&*cfg, || format!("indexer: {event:?}"));
        let request = match event.kind {
            FileEventType::Create | FileEventType::Modify => handle_updated(&cfg, event).await?,
            FileEventType::Delete => {
                FileUpdateRequest::RemoveFile { t: event.t, file_address: event.file_address, source: event.source }
            }
        };
        index_update_requests.send(request).await?;
    }
    Ok(())
}

async fn handle_updated(cfg: &Arc<dyn IndexConfig>, event: FileSyncEvent) -> anyhow::Result<FileUpdateRequest> {
    let cfg2 = cfg.clone();
    let fa = event.file_address.clone();
    // blocking read + CPU-bound tokenization off the async workers (Kotlin: withContext(Dispatchers.IO));
    // a panic in tokenize surfaces as a JoinError and fails the index, like a non-IO exception in Kotlin
    let tokens = tokio::task::spawn_blocking(move || -> std::io::Result<FxHashSet<String>> {
        let path = Path::new(fa.path());
        if std::fs::metadata(path)?.len() > MAX_FILE_SIZE {
            // file too large, skip
            return Ok(FxHashSet::default());
        }
        let mut tokens = FxHashSet::default();
        for line in read_lines(path)? {
            tokens.extend(cfg2.tokenize(&line));
        }
        Ok(tokens)
    })
    .await?;

    let tokens = tokens.unwrap_or_else(|e| {
        debug_log(&**cfg, || format!("Failed to read {}: {e}", event.file_address));
        FxHashSet::default()
    });
    Ok(FileUpdateRequest::UpdateFile { t: event.t, file_address: event.file_address, tokens, source: event.source })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use futures::stream::BoxStream;

    use super::*;
    use crate::internal::model::FileEventSource;
    use crate::internal::utils::canonical_path;
    use crate::internal::FileAddress;
    use crate::model::TokenIndex;

    /// tokenize = split(" "); panics on "a vot on" when `explode` is set
    struct SpaceConfig {
        logging: AtomicBool,
        explode: bool,
    }

    impl IndexConfig for SpaceConfig {
        fn enable_logging(&self) -> &AtomicBool {
            &self.logging
        }
        fn enable_watcher(&self) -> bool {
            false
        }
        fn tokenize(&self, line: &str) -> Vec<String> {
            if self.explode && line == "a vot on" {
                panic!("not io exception");
            }
            line.split(' ').map(str::to_owned).collect()
        }
        fn find(&self, _: Arc<dyn TokenIndex>, _: String) -> BoxStream<'static, FileAddress> {
            unimplemented!()
        }
        fn matches(&self, line: &str, query: &str) -> bool {
            line.contains(query)
        }
        fn handle_watcher_error(&self, _: &anyhow::Error) {}
        fn handle_initial_file_sync_error(&self, _: &anyhow::Error) {}
    }

    fn setup(
        explode: bool,
    ) -> (Sender<FileSyncEvent>, Receiver<FileUpdateRequest>, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let cfg: Arc<dyn IndexConfig> = Arc::new(SpaceConfig { logging: AtomicBool::new(false), explode });
        let (etx, erx) = async_channel::bounded(1);
        let (rtx, rrx) = async_channel::bounded(1);
        let job = tokio::spawn(indexer(cfg, erx, rtx));
        (etx, rrx, job)
    }

    fn event(t: i64, path: &str, kind: FileEventType) -> FileSyncEvent {
        FileSyncEvent { t, file_address: FileAddress::new(path), source: FileEventSource::InitialSync, kind }
    }

    #[tokio::test]
    async fn should_emit_remove_file_request_on_delete_event() {
        let (etx, rrx, job) = setup(false);
        etx.send(event(10, "doc1.txt", FileEventType::Delete)).await.unwrap();
        match rrx.recv().await.unwrap() {
            FileUpdateRequest::RemoveFile { t, file_address, .. } => {
                assert_eq!(t, 10);
                assert_eq!(file_address, FileAddress::new("doc1.txt"));
            }
            other => panic!("unexpected {other:?}"),
        }
        job.abort();
    }

    #[tokio::test]
    async fn should_emit_update_file_request_on_update_event() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("poupa");
        std::fs::write(&file, "volobuev gde tvoi mech\na vot on").unwrap();
        let path = canonical_path(&file);

        let (etx, rrx, job) = setup(false);
        etx.send(event(10, &path, FileEventType::Create)).await.unwrap();
        match rrx.recv().await.unwrap() {
            FileUpdateRequest::UpdateFile { t, file_address, tokens, .. } => {
                assert_eq!(t, 10);
                assert_eq!(file_address.path(), path);
                for tok in ["volobuev", "gde", "tvoi", "mech", "a", "vot", "on"] {
                    assert!(tokens.contains(tok), "{tok}");
                }
            }
            other => panic!("unexpected {other:?}"),
        }
        job.abort();
    }

    #[tokio::test]
    async fn should_emit_empty_update_on_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = canonical_path(&dir.path().join("poupa"));

        let (etx, rrx, job) = setup(false);
        etx.send(event(10, &path, FileEventType::Create)).await.unwrap();
        match rrx.recv().await.unwrap() {
            FileUpdateRequest::UpdateFile { t, tokens, .. } => {
                assert_eq!(t, 10);
                assert!(tokens.is_empty());
            }
            other => panic!("unexpected {other:?}"),
        }
        job.abort();
    }

    #[tokio::test]
    async fn should_fail_on_error_other_than_io() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("poupa");
        std::fs::write(&file, "volobuev gde tvoi mech\na vot on").unwrap();

        let (etx, _rrx, job) = setup(true);
        etx.send(event(10, &canonical_path(&file), FileEventType::Create)).await.unwrap();
        assert!(job.await.unwrap().is_err());
    }
}
