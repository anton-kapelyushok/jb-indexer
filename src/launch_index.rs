use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::internal::fs_watcher::FsWatcher;
use crate::internal::index_manager::index_manager;
use crate::internal::indexer::indexer;
use crate::internal::model::UserRequest;
use crate::internal::sync_fs::sync_fs;
use crate::internal::utils::now_millis;
use crate::model::{
    Completion, FileAddress, Index, IndexConfig, IndexState, IndexStatusUpdate, TokenPredicate,
};
use crate::status_bus::StatusBus;

const INDEXERS: usize = 4;

/// Running index. Cheap to clone; the index is cancelled when the last handle is dropped.
#[derive(Clone)]
pub struct IndexHandle {
    inner: Arc<IndexInner>,
}

struct IndexInner {
    cancel: CancellationToken,
    user_requests: flume::Sender<UserRequest>,
    status: StatusBus,
    completion: watch::Receiver<Option<Completion>>,
}

impl Drop for IndexInner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Starts building an index of `dir` in background tasks on the current tokio runtime
/// and keeps it in sync with the file system.
///
/// Must be called from within a tokio runtime.
pub fn launch_index(dir: impl Into<PathBuf>, cfg: Arc<dyn IndexConfig>) -> IndexHandle {
    let dir = dir.into();
    let (user_requests_tx, user_requests) = flume::bounded(0);
    let (index_update_requests_tx, index_update_requests) = flume::bounded(0);
    let (status_updates_tx, status_updates) = flume::unbounded();
    let (file_sync_events_tx, file_sync_events) = flume::unbounded();
    let status = StatusBus::new();
    status.emit(IndexStatusUpdate::Initializing { ts: now_millis() });

    let mut tasks = JoinSet::new();
    tasks.spawn(sync_fs(
        cfg.clone(),
        dir,
        file_sync_events_tx,
        status_updates_tx,
        Arc::new(FsWatcher),
    ));
    for _ in 0..INDEXERS {
        tasks.spawn(indexer(
            cfg.clone(),
            file_sync_events.clone(),
            index_update_requests_tx.clone(),
        ));
    }
    drop(index_update_requests_tx);
    {
        let status = status.clone();
        tasks.spawn(index_manager(
            user_requests,
            index_update_requests,
            status_updates,
            move |update| status.emit(update),
            cfg.enable_logging().clone(),
        ));
    }

    let cancel = CancellationToken::new();
    let (completion_tx, completion) = watch::channel(None);
    {
        let cancel = cancel.clone();
        let status = status.clone();
        tokio::spawn(async move {
            let result = tokio::select! {
                _ = cancel.cancelled() => Completion::Cancelled,
                e = first_failure(&mut tasks) => Completion::Failed(Arc::new(e)),
            };
            tasks.shutdown().await;
            status.emit(IndexStatusUpdate::Failed {
                ts: now_millis(),
                reason: result.clone().into_error(),
            });
            let _ = completion_tx.send(Some(result));
        });
    }

    IndexHandle {
        inner: Arc::new(IndexInner {
            cancel,
            user_requests: user_requests_tx,
            status,
            completion,
        }),
    }
}

/// Waits for the first task to fail. Tasks are not supposed to complete normally.
pub(crate) async fn first_failure(tasks: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Error {
    loop {
        match tasks.join_next().await {
            Some(Ok(Ok(()))) => continue,
            Some(Ok(Err(e))) => return e,
            Some(Err(e)) => return anyhow::anyhow!("task failed: {e}"),
            None => return anyhow::anyhow!("Index terminated without error?"),
        }
    }
}

impl IndexHandle {
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
    }

    pub fn is_completed(&self) -> bool {
        self.inner.completion.borrow().is_some()
    }

    /// Waits until the index terminates.
    pub async fn join(&self) -> Completion {
        let mut completion = self.inner.completion.clone();
        match completion.wait_for(Option::is_some).await {
            Ok(c) => c.clone().unwrap(),
            Err(_) => Completion::Cancelled,
        }
    }

    pub(crate) fn cancellation_token(&self) -> &CancellationToken {
        &self.inner.cancel
    }

    /// Sends a request to the index manager. Returns None if the index terminated meanwhile.
    async fn request<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> UserRequest) -> Option<T> {
        let (tx, rx) = oneshot::channel();
        let request = async {
            self.inner.user_requests.send_async(make(tx)).await.ok()?;
            rx.await.ok()
        };
        tokio::select! {
            r = request => r,
            _ = self.inner.cancel.cancelled() => None,
            _ = self.join() => None,
        }
    }
}

#[async_trait]
impl Index for IndexHandle {
    async fn find_files_by_token(&self, token: &str) -> Vec<FileAddress> {
        let query = token.to_string();
        self.request(|result| UserRequest::FindFilesByToken { query, result })
            .await
            .unwrap_or_default()
    }

    async fn find_tokens_matching_predicate(&self, predicate: TokenPredicate) -> Vec<String> {
        self.request(|result| UserRequest::FindTokensMatchingPredicate { predicate, result })
            .await
            .unwrap_or_default()
    }

    async fn state(&self) -> IndexState {
        self.request(|result| UserRequest::Status { result })
            .await
            .unwrap_or_else(IndexState::broken)
    }

    fn status_updates(&self) -> BoxStream<'static, IndexStatusUpdate> {
        self.inner.status.subscribe()
    }

    async fn compact(&self) {
        self.request(|result| UserRequest::Compact { result }).await;
    }
}
