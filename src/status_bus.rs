use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::broadcast;

use crate::model::IndexStatusUpdate;

const REPLAY: usize = 16; // is enough for everyone
const CAPACITY: usize = 1024;

/// Broadcasts status updates, replaying the last few to new subscribers (slow subscribers lose updates).
#[derive(Clone)]
pub(crate) struct StatusBus {
    inner: Arc<Mutex<BusState>>,
}

struct BusState {
    replay: VecDeque<IndexStatusUpdate>,
    tx: broadcast::Sender<IndexStatusUpdate>,
}

impl StatusBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(CAPACITY);
        StatusBus {
            inner: Arc::new(Mutex::new(BusState {
                replay: VecDeque::new(),
                tx,
            })),
        }
    }

    pub fn emit(&self, update: IndexStatusUpdate) {
        let mut state = self.inner.lock().unwrap();
        if state.replay.len() == REPLAY {
            state.replay.pop_front();
        }
        state.replay.push_back(update.clone());
        let _ = state.tx.send(update);
    }

    /// Replayed and live updates, ends right after `Failed`.
    pub fn subscribe(&self) -> BoxStream<'static, IndexStatusUpdate> {
        let (replay, rx) = {
            let state = self.inner.lock().unwrap();
            (state.replay.clone(), state.tx.subscribe())
        };
        let live = futures::stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(update) => return Some((update, rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        let updates = futures::stream::iter(replay).chain(live).boxed();
        // end right after Failed, without waiting for another update
        futures::stream::unfold(Some(updates), |updates| async move {
            let mut updates = updates?;
            let update = updates.next().await?;
            let next = if matches!(update, IndexStatusUpdate::Failed { .. }) {
                None
            } else {
                Some(updates)
            };
            Some((update, next))
        })
        .boxed()
    }
}
