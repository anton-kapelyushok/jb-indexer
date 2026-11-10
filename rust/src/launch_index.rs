use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_channel::Sender;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::internal::fs_watcher::{FsWatcher, Watcher};
use crate::internal::index_manager::index_manager;
use crate::internal::indexer::indexer;
use crate::internal::model::UserRequest;
use crate::internal::sync_fs::sync_fs;
use crate::internal::utils::{now_millis, StatusBus};
use crate::internal::FileAddress;
use crate::model::{IndexConfig, IndexState, IndexStatusUpdate, Outcome, TokenIndex, TokenPredicate};

const INDEXERS: usize = 4;

/// Handle to a running index. Cheap to clone; the index is cancelled when [`Index::cancel`] is called
/// or the last handle is dropped (Kotlin: a `Deferred` child of the caller's scope).
#[derive(Clone)]
pub struct Index {
    inner: Arc<IndexInner>,
}

struct IndexInner {
    user_requests: Sender<UserRequest>,
    status: Arc<StatusBus>,
    token: CancellationToken,
    done: watch::Receiver<Option<Outcome>>,
    _cancel_on_drop: DropGuard,
}

/// Starts indexing `dir` in the background. Must be called from within a tokio runtime.
pub fn launch_index(dir: impl Into<PathBuf>, cfg: Arc<dyn IndexConfig>) -> Index {
    launch_index_with_watcher(dir.into(), cfg, Arc::new(FsWatcher))
}

pub(crate) fn launch_index_with_watcher(dir: PathBuf, cfg: Arc<dyn IndexConfig>, watcher: Arc<dyn Watcher>) -> Index {
    let (user_tx, user_rx) = async_channel::bounded(1);
    let (update_tx, update_rx) = async_channel::bounded(1);
    let (status_tx, status_rx) = async_channel::unbounded();
    let (sync_tx, sync_rx) = async_channel::unbounded();
    let status = Arc::new(StatusBus::default());
    status.emit(IndexStatusUpdate::Initializing { ts: now_millis() });

    let token = CancellationToken::new();
    let (done_tx, done_rx) = watch::channel(None);

    let mut set: JoinSet<anyhow::Result<()>> = JoinSet::new();
    {
        let (cfg, dir) = (cfg.clone(), dir.clone());
        let (sync_tx, status_tx) = (sync_tx.clone(), status_tx.clone());
        set.spawn(async move { sync_fs(cfg, dir, sync_tx, status_tx, watcher).await.map(|n| match n {}) });
    }
    for _ in 0..INDEXERS {
        set.spawn(indexer(cfg.clone(), sync_rx.clone(), update_tx.clone()));
    }
    {
        let status = status.clone();
        set.spawn(index_manager(user_rx, update_rx, status_rx, move |u| status.emit(u), cfg.clone()));
    }
    drop((sync_tx, sync_rx, update_tx, status_tx));

    let supervisor_token = token.clone();
    let supervisor_status = status.clone();
    tokio::spawn(async move {
        let outcome = supervise(&mut set, &supervisor_token).await;
        supervisor_token.cancel();
        set.shutdown().await;
        let reason = match &outcome {
            Outcome::Cancelled => Arc::new(anyhow!("Index was cancelled")),
            Outcome::Failed(e) => e.clone(),
        };
        supervisor_status.emit(IndexStatusUpdate::Failed { ts: now_millis(), reason });
        let _ = done_tx.send(Some(outcome));
    });

    Index {
        inner: Arc::new(IndexInner {
            user_requests: user_tx,
            status,
            _cancel_on_drop: token.clone().drop_guard(),
            token,
            done: done_rx,
        }),
    }
}

/// Waits until cancelled or until any child fails (structured concurrency: one failure fails all).
pub(crate) async fn supervise(set: &mut JoinSet<anyhow::Result<()>>, token: &CancellationToken) -> Outcome {
    let children = async {
        while let Some(r) = set.join_next().await {
            match r {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => return Outcome::Failed(Arc::new(e)),
                Err(join_err) => return Outcome::Failed(Arc::new(anyhow!("task panicked: {join_err}"))),
            }
        }
        Outcome::Failed(Arc::new(anyhow!("terminated without exception?")))
    };
    tokio::select! {
        _ = token.cancelled() => Outcome::Cancelled,
        o = children => o,
    }
}

impl Index {
    pub async fn state(&self) -> IndexState {
        self.request(|result| UserRequest::Status { result }).await.unwrap_or_else(IndexState::broken)
    }

    /// Status updates: the last 16 are replayed, then live ones; the stream ends after `Failed`.
    pub fn status_stream(&self) -> BoxStream<'static, IndexStatusUpdate> {
        self.inner.status.subscribe()
    }

    pub async fn compact(&self) {
        self.request(|result| UserRequest::Compact { result }).await;
    }

    pub async fn find_files_by_token(&self, token: &str) -> Vec<FileAddress> {
        let query = token.to_owned();
        self.request(|result| UserRequest::FindFilesByToken { query, result }).await.unwrap_or_default()
    }

    pub async fn find_tokens_matching_predicate(&self, predicate: TokenPredicate) -> Vec<String> {
        self.request(|result| UserRequest::FindTokensMatchingPredicate { predicate, result }).await.unwrap_or_default()
    }

    pub fn cancel(&self) {
        self.inner.token.cancel();
    }

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.inner.token
    }

    pub fn is_finished(&self) -> bool {
        self.inner.done.borrow().is_some()
    }

    /// Waits for the index to terminate (it only does so when cancelled or failed).
    pub async fn join(&self) -> Outcome {
        let mut rx = self.inner.done.clone();
        let r = rx.wait_for(|o| o.is_some()).await.ok().and_then(|o| o.clone());
        r.unwrap_or(Outcome::Cancelled)
    }

    /// Sends a request to the index manager. `None` if the index is (or gets) cancelled meanwhile —
    /// the Kotlin version needs `withIndexContext` for this, here the dropped oneshot does it too.
    async fn request<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> UserRequest) -> Option<T> {
        let (tx, rx) = oneshot::channel();
        let req = make(tx);
        with_cancellation(&self.inner.token, async {
            self.inner.user_requests.send(req).await.ok()?;
            rx.await.ok()
        })
        .await
    }
}

pub(crate) async fn with_cancellation<T>(token: &CancellationToken, f: impl Future<Output = Option<T>>) -> Option<T> {
    tokio::select! {
        biased;
        _ = token.cancelled() => None,
        r = f => r,
    }
}

impl TokenIndex for Index {
    fn find_files_by_token<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Vec<FileAddress>> {
        Box::pin(Index::find_files_by_token(self, token))
    }

    fn find_tokens_matching_predicate(&self, predicate: TokenPredicate) -> BoxFuture<'_, Vec<String>> {
        Box::pin(Index::find_tokens_matching_predicate(self, predicate))
    }
}
