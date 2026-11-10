//! Simple text search library: a text index builder and a search query executor.
//!
//! ```no_run
//! use std::sync::Arc;
//! use futures::StreamExt;
//! use jb_indexer::*;
//!
//! # async fn example() {
//! // use word index (trigram index can also be used)
//! let cfg: Arc<dyn IndexConfig> = Arc::new(word_index_config().with_watcher(true));
//!
//! let index = launch_index(".", cfg.clone());
//! let search_engine = launch_search_engine(cfg, index.clone());
//!
//! // wait until it is in sync
//! let mut updates = search_engine.index_status_updates();
//! while let Some(update) = updates.next().await {
//!     if matches!(update, IndexStatusUpdate::IndexInSync { .. }) { break; }
//! }
//!
//! let mut results = search_engine.find("some query");
//! while let Some(result) = results.next().await {
//!     println!("{result:?}");
//! }
//! search_engine.cancel_all();
//! # }
//! ```

mod config_common;
mod internal;
mod launch_index;
mod launch_search_engine;
mod model;
mod status_bus;
mod trigram_index_config;
mod word_index_config;

#[cfg(test)]
mod test_utils;

pub use internal::inverted_index::InvertedIndex;
pub use launch_index::{IndexHandle, launch_index};
pub use launch_search_engine::{SearchEngine, launch_search_engine};
pub use model::{
    Completion, FileAddress, Index, IndexConfig, IndexSearchResult, IndexState, IndexStatusUpdate,
    SharedError, TokenPredicate,
};
pub use trigram_index_config::{TrigramIndexConfig, trigram_index_config};
pub use word_index_config::{WordIndexConfig, word_index_config};
