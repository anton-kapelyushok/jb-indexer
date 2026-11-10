use std::sync::Arc;

use anyhow::anyhow;
use async_channel::Sender;
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::internal::model::SearchInFileRequest;
use crate::internal::search_in_file::search_in_file;
use crate::internal::utils::distinct;
use crate::internal::FileAddress;
use crate::launch_index::{supervise, with_cancellation, Index};
use crate::model::{IndexConfig, IndexSearchResult, IndexState, IndexStatusUpdate, Outcome, TokenIndex};

const SEARCH_WORKERS: usize = 4;
const FILES_IN_FLIGHT: usize = 4;

/// Handle to a running search engine. Cheap to clone; cancelled by [`SearchEngine::cancel`] or when the
/// last handle is dropped. Terminates when the underlying index does.
#[derive(Clone)]
pub struct SearchEngine {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: Arc<dyn IndexConfig>,
    index: Index,
    requests: Sender<SearchInFileRequest>,
    token: CancellationToken,
    done: watch::Receiver<Option<Outcome>>,
    _cancel_on_drop: DropGuard,
}

pub fn launch_search_engine(cfg: Arc<dyn IndexConfig>, index: Index) -> SearchEngine {
    let (req_tx, req_rx) = async_channel::bounded(1);
    let token = CancellationToken::new();
    let (done_tx, done_rx) = watch::channel(None);

    let mut set: JoinSet<anyhow::Result<()>> = JoinSet::new();
    for _ in 0..SEARCH_WORKERS {
        set.spawn(search_in_file(cfg.clone(), req_rx.clone()));
    }
    {
        let index = index.clone();
        set.spawn(async move {
            Err(match index.join().await {
                Outcome::Cancelled => anyhow!("Underlying index was canceled"),
                Outcome::Failed(e) => anyhow!("Underlying index failed with exception: {e:#}"),
            })
        });
    }

    let t = token.clone();
    tokio::spawn(async move {
        let outcome = supervise(&mut set, &t).await;
        t.cancel();
        set.shutdown().await;
        let _ = done_tx.send(Some(outcome));
    });

    SearchEngine {
        inner: Arc::new(Inner {
            cfg,
            index,
            requests: req_tx,
            _cancel_on_drop: token.clone().drop_guard(),
            token,
            done: done_rx,
        }),
    }
}

impl SearchEngine {
    pub async fn index_state(&self) -> IndexState {
        self.inner.index.state().await
    }

    pub fn index_status_updates(&self) -> BoxStream<'static, IndexStatusUpdate> {
        self.inner.index.status_stream()
    }

    /// Lazily finds all lines containing `query`. Candidate files come from the index; up to 4 of them are
    /// scanned concurrently by the worker pool. Ends early (silently) if the engine is cancelled.
    pub fn find(&self, query: &str) -> BoxStream<'static, IndexSearchResult> {
        let inner = &self.inner;
        let index: Arc<dyn TokenIndex> = Arc::new(inner.index.clone());
        let query: Arc<str> = query.into();
        let (requests, token) = (inner.requests.clone(), inner.token.clone());

        let q = query.clone();
        let t = token.clone();
        distinct(inner.cfg.find(index, query.to_string()))
            .map(move |fa| search_one(requests.clone(), t.clone(), fa, q.clone()))
            .buffer_unordered(FILES_IN_FLIGHT)
            .flat_map(stream::iter)
            .take_until(token.cancelled_owned())
            .boxed()
    }

    /// Cancels both the search engine and the index.
    pub fn cancel_all(&self) {
        self.inner.index.cancel();
        self.cancel();
    }

    /// Cancels only the search engine.
    pub fn cancel(&self) {
        self.inner.token.cancel();
    }

    pub async fn join(&self) -> Outcome {
        let mut rx = self.inner.done.clone();
        let r = rx.wait_for(|o| o.is_some()).await.ok().and_then(|o| o.clone());
        r.unwrap_or(Outcome::Cancelled)
    }
}

async fn search_one(
    requests: Sender<SearchInFileRequest>,
    token: CancellationToken,
    fa: FileAddress,
    query: Arc<str>,
) -> Vec<IndexSearchResult> {
    let (tx, rx) = oneshot::channel();
    with_cancellation(&token, async {
        requests.send(SearchInFileRequest { fa, query, result: tx }).await.ok()?;
        rx.await.ok()
    })
    .await
    .unwrap_or_default()
}
