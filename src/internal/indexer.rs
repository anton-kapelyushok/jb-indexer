use std::path::Path;
use std::sync::Arc;

use crate::internal::model::{FileEventType, FileSyncEvent, FileUpdateRequest};
use crate::internal::utils::{debug_log, lines, read_text};
use crate::model::{IndexConfig, TokenSet};

const MAX_FILE_SIZE: u64 = 10_000_000;

/// Reads files from sync events, tokenizes them and passes the result to the index manager.
/// Several indexers run in parallel, sharing the same input channel.
pub(crate) async fn indexer(
    cfg: Arc<dyn IndexConfig>,
    file_sync_events: flume::Receiver<FileSyncEvent>,
    index_update_requests: flume::Sender<FileUpdateRequest>,
) -> anyhow::Result<()> {
    while let Ok(event) = file_sync_events.recv_async().await {
        debug_log(&*cfg, || format!("indexer: {event:?}"));
        let request =
            match event.kind {
                FileEventType::Create | FileEventType::Modify => {
                    let cfg = cfg.clone();
                    let fa = event.file_address.clone();
                    // a panic in tokenize is propagated as an error (it is not an IO error)
                    let tokens = tokio::task::spawn_blocking(move || {
                        read_tokens(&*cfg, Path::new(fa.path()))
                    })
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("indexer failed to tokenize {}: {e}", event.file_address)
                    })?;
                    FileUpdateRequest::UpdateFile {
                        t: event.t,
                        file_address: event.file_address,
                        tokens,
                        source: event.source,
                    }
                }
                FileEventType::Delete => FileUpdateRequest::RemoveFile {
                    t: event.t,
                    file_address: event.file_address,
                    source: event.source,
                },
            };
        index_update_requests.send_async(request).await?;
    }
    Ok(())
}

fn read_tokens(cfg: &dyn IndexConfig, path: &Path) -> TokenSet {
    let read = || -> std::io::Result<TokenSet> {
        if std::fs::metadata(path)?.len() > MAX_FILE_SIZE {
            // file too large, skip
            return Ok(TokenSet::new());
        }
        let text = read_text(path)?;
        Ok(lines(&text).flat_map(|line| cfg.tokenize(line)).collect())
    };
    read().unwrap_or_else(|e| {
        debug_log(cfg, || format!("Failed to read {}: {e}", path.display()));
        TokenSet::new()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::model::FileEventSource;
    use crate::test_utils::{TestConfig, fa, file_address_of, with_timeout};

    struct Setup {
        file_sync_events: flume::Sender<FileSyncEvent>,
        index_update_requests: flume::Receiver<FileUpdateRequest>,
        job: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    fn launch(cfg: TestConfig) -> Setup {
        let (file_sync_events, rx) = flume::bounded(0);
        let (tx, index_update_requests) = flume::bounded(0);
        let job = tokio::spawn(indexer(Arc::new(cfg), rx, tx));
        Setup {
            file_sync_events,
            index_update_requests,
            job,
        }
    }

    #[tokio::test]
    async fn should_emit_remove_file_request_on_remove_event() {
        with_timeout(1, async {
            let s = launch(TestConfig::default());
            s.file_sync_events
                .send_async(FileSyncEvent {
                    t: 10,
                    file_address: fa("doc1.txt"),
                    source: FileEventSource::Watcher,
                    kind: FileEventType::Delete,
                })
                .await
                .unwrap();

            match s.index_update_requests.recv_async().await.unwrap() {
                FileUpdateRequest::RemoveFile {
                    t, file_address, ..
                } => {
                    assert_eq!(t, 10);
                    assert_eq!(file_address, fa("doc1.txt"));
                }
                other => panic!("unexpected {other:?}"),
            }
            s.job.abort();
        })
        .await
    }

    #[tokio::test]
    async fn should_emit_update_file_request_on_update_event() {
        with_timeout(1, async {
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("poupa");
            std::fs::write(&file, "volobuev gde tvoi mech\na vot on").unwrap();
            let s = launch(TestConfig::default());

            s.file_sync_events
                .send_async(FileSyncEvent {
                    t: 10,
                    file_address: file_address_of(&file),
                    source: FileEventSource::InitialSync,
                    kind: FileEventType::Create,
                })
                .await
                .unwrap();

            match s.index_update_requests.recv_async().await.unwrap() {
                FileUpdateRequest::UpdateFile {
                    t,
                    file_address,
                    tokens,
                    ..
                } => {
                    assert_eq!(t, 10);
                    assert_eq!(file_address, file_address_of(&file));
                    for token in ["volobuev", "gde", "tvoi", "mech", "a", "vot", "on"] {
                        assert!(tokens.contains(token), "{token}");
                    }
                }
                other => panic!("unexpected {other:?}"),
            }
            s.job.abort();
        })
        .await
    }

    #[tokio::test]
    async fn should_emit_update_file_request_with_empty_content_on_missing_file() {
        with_timeout(1, async {
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("poupa");
            let s = launch(TestConfig::default());

            s.file_sync_events
                .send_async(FileSyncEvent {
                    t: 10,
                    file_address: file_address_of(&file),
                    source: FileEventSource::InitialSync,
                    kind: FileEventType::Create,
                })
                .await
                .unwrap();

            match s.index_update_requests.recv_async().await.unwrap() {
                FileUpdateRequest::UpdateFile {
                    t,
                    file_address,
                    tokens,
                    ..
                } => {
                    assert_eq!(t, 10);
                    assert_eq!(file_address, file_address_of(&file));
                    assert!(tokens.is_empty());
                }
                other => panic!("unexpected {other:?}"),
            }
            s.job.abort();
        })
        .await
    }

    #[tokio::test]
    async fn should_fail_on_error_other_than_io_error() {
        with_timeout(1, async {
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("poupa");
            std::fs::write(&file, "volobuev gde tvoi mech\na vot on").unwrap();
            let s = launch(TestConfig {
                panic_on_line: Some("a vot on".into()),
                ..TestConfig::default()
            });

            s.file_sync_events
                .send_async(FileSyncEvent {
                    t: 10,
                    file_address: file_address_of(&file),
                    source: FileEventSource::InitialSync,
                    kind: FileEventType::Create,
                })
                .await
                .unwrap();

            assert!(s.job.await.unwrap().is_err());
        })
        .await
    }
}
