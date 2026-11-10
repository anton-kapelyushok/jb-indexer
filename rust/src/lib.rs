//! Rust + tokio port of the Kotlin `indexer.core` library.
//!
//! The module layout mirrors the Kotlin sources one-to-one:
//!
//! | Kotlin (`lib/src/main/kotlin/indexer/core`) | Rust (`src`)                     |
//! |---------------------------------------------|----------------------------------|
//! | `model.kt`                                  | `model.rs`                       |
//! | `launchIndex.kt`                            | `launch_index.rs`                |
//! | `launchSearchEngine.kt`                     | `launch_search_engine.rs`        |
//! | `wordIndexConfig.kt`                        | `word_index_config.rs`           |
//! | `trigramIndexConfig.kt`                     | `trigram_index_config.rs`        |
//! | `internal/*.kt`                             | `internal/*.rs`                  |
//!
//! Coroutines map onto tokio tasks, `Channel`s onto `async_channel` (MPMC, like Kotlin channels),
//! `CompletableDeferred` onto `oneshot`, `MutableSharedFlow(replay = 16)` onto a small replaying
//! broadcast bus, and `Job.cancel()` onto `CancellationToken`.

pub mod internal;
mod launch_index;
mod launch_search_engine;
mod model;
mod trigram_index_config;
mod word_index_config;

pub use internal::FileAddress;
pub use launch_index::{launch_index, Index};
pub use launch_search_engine::{launch_search_engine, SearchEngine};
pub use model::*;
pub use trigram_index_config::{trigram_index_config, TrigramIndexConfig};
pub use word_index_config::{word_index_config, WordIndexConfig};
