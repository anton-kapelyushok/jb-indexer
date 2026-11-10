//! Internals; `pub` only so integration tests and benchmarks can reach them (Kotlin: `internal`).

pub mod fs_watcher;
pub mod index_manager;
pub mod indexer;
pub mod inverted_index;
pub mod model;
pub mod search_in_file;
pub mod sync_fs;
pub mod utils;

pub use index_manager::FileAddress;
