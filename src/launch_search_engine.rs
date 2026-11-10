use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::internal::model::SearchInFileRequest;
use crate::internal::search_in_file::search_in_file;
use crate::internal::utils::distinct;
use crate::launch_index::{IndexHandle, first_failure};
use crate::model::{
    Completion, Index, IndexConfig, IndexSearchResult, IndexState, IndexStatusUpdate,
};

const SEARCH_WORKERS: usize = 4;
const SEARCH_CONCURRENCY: usize = 4;

/// Executes search queries against an index, in parallel.
/// Cheap to clone; cancelled when the last handle is dropped.
#[derive(Clone)]
pub struct SearchEngine {
    inner: Arc<SearchEngineInner>,
}

struct SearchEngineInner {
    cfg: Arc<dyn IndexConfig>,
    index: IndexHandle,
    cancel: CancellationToken,
    search_in_file_requests: flume::Sender<SearchInFileRequest>,
    completion: watch::Receiver<Option<Completion>>,
}

impl Drop for SearchEngineInner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Starts a search engine on top of `index`. Terminates when the index terminates.
///
/// Must be called from within a tokio runtime.
pub fn launch_search_engine(cfg: Arc<dyn IndexConfig>, index: IndexHandle) -> SearchEngine {
    let (requests_tx, requests) = flume::bounded(0);
    let mut workers = JoinSet::new();
    for _ in 0..SEARCH_WORKERS {
        workers.spawn(search_in_file(cfg.clone(), requests.clone()));
    }

    let cancel = CancellationToken::new();
    let (completion_tx, completion) = watch::channel(None);
    {
        let cancel = cancel.clone();
        let index = index.clone();
        tokio::spawn(async move {
            let result = tokio::select! {
                _ = cancel.cancelled() => Completion::Cancelled,
                c = index.join() => Completion::Failed(Arc::new(match c {
                    Completion::Cancelled => anyhow::anyhow!("Underlying index was cancelled"),
                    Completion::Failed(e) => anyhow::anyhow!("Underlying index failed: {e:#}"),
                })),
                e = first_failure(&mut workers) => Completion::Failed(Arc::new(e)),
            };
            // do not keep the index alive
            drop(index);
            workers.shutdown().await;
            let _ = completion_tx.send(Some(result));
        });
    }

    SearchEngine {
        inner: Arc::new(SearchEngineInner {
            cfg,
            index,
            cancel,
            search_in_file_requests: requests_tx,
            completion,
        }),
    }
}

impl SearchEngine {
    pub async fn index_state(&self) -> IndexState {
        self.inner.index.state().await
    }

    pub fn index_status_updates(&self) -> BoxStream<'static, IndexStatusUpdate> {
        self.inner.index.status_updates()
    }

    pub fn index(&self) -> &IndexHandle {
        &self.inner.index
    }

    /// Finds all lines matching `query`. Results are streamed as soon as they are found, in no particular order.
    /// The stream ends early if the search engine is cancelled.
    pub fn find(&self, query: &str) -> BoxStream<'static, IndexSearchResult> {
        if self.inner.cancel.is_cancelled() {
            return futures::stream::empty().boxed();
        }
        let index: Arc<dyn Index> = Arc::new(self.inner.index.clone());
        let candidates = distinct(self.inner.cfg.find(index, query.to_string()));
        let requests = self.inner.search_in_file_requests.clone();
        let query: Arc<str> = query.into();
        let cancel = self.inner.cancel.clone();
        let index_cancel = self.inner.index.cancellation_token().clone();

        candidates
            .map(move |fa| {
                let requests = requests.clone();
                let query = query.clone();
                async move {
                    let (result, rx) = oneshot::channel();
                    if requests
                        .send_async(SearchInFileRequest { fa, query, result })
                        .await
                        .is_err()
                    {
                        return vec![];
                    }
                    rx.await.unwrap_or_default()
                }
            })
            .buffer_unordered(SEARCH_CONCURRENCY)
            .flat_map(futures::stream::iter)
            .take_until(async move {
                tokio::select! {
                    _ = cancel.cancelled() => {},
                    _ = index_cancel.cancelled() => {},
                }
            })
            .boxed()
    }

    /// Cancels the search engine, the index keeps running.
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
    }

    /// Cancels both the search engine and the index.
    pub fn cancel_all(&self) {
        self.inner.index.cancel();
        self.cancel();
    }

    pub fn is_completed(&self) -> bool {
        self.inner.completion.borrow().is_some()
    }

    /// Waits until the search engine terminates.
    pub async fn join(&self) -> Completion {
        let mut completion = self.inner.completion.clone();
        match completion.wait_for(Option::is_some).await {
            Ok(c) => c.clone().unwrap(),
            Err(_) => Completion::Cancelled,
        }
    }
}
