# File Indexer

```
Please implement a library for simple text search. Imagine, the library will then be used by other people.



This library is supposed to consist of two parts: text index builder and search query executor.



Text index builder should:



Be able to build a text index for a given folder in a file system. As an example of such a folder, you may use https://github.com/JetBrains/intellij-community repository. The type of the index doesn't matter much. For example, that might be a trigram index.
Provide the value of the progress while building the index. Clients of the library should be able to implement UI on the basis of that.
Build the index using several threads in parallel.
Be cancellable. It should be possible to interrupt indexing.
(Optional) Be incremental. It would be nice if the builder would be able to listen to the file system changes and update the index accordingly.


Search query executor should:



Find a position in files for a given string.
Be able to process search requests in parallel.


Please also cover the library with a set of unit-tests. Your code should not use third-party indexing libraries. To implement the library, you can use any JVM language and any build systems, but we would appreciate you choosing Kotlin and Gradle.
```

Implemented in Rust (originally Kotlin/Gradle).

## Build & run

```shell
cargo test                       # unit + end-to-end tests
cargo run --release -- [--trigram] [--no-watcher] [DIR]
```

The CLI indexes `DIR` (default `.`) and reads commands from stdin:
`find <query>`, `status`, `compact`, `memory`, `enable-logging` (empty line disables it), `error`, `help`, `stop`.
Typing any line while a search runs interrupts it.

## Usage example

```rust
use std::sync::Arc;
use futures::StreamExt;
use jb_indexer::*;

// use word index (trigram index can also be used)
let cfg: Arc<dyn IndexConfig> = Arc::new(word_index_config().with_watcher(true));

// launch index on the current tokio runtime
let index = launch_index(".", cfg.clone());

// launch search engine on top of it
let search_engine = launch_search_engine(cfg, index.clone());

// show progress
let state = search_engine.index_state().await;
let percent = if state.total_file_events == 0 { 0.0 }
    else { 100.0 * state.handled_file_events as f64 / state.total_file_events as f64 };
println!("Indexing progress: {percent}%");

// wait until it is in sync
let mut updates = search_engine.index_status_updates();
while let Some(update) = updates.next().await {
    if matches!(update, IndexStatusUpdate::IndexInSync { .. }) { break; }
}

// execute some query
let mut results = search_engine.find("some query");
while let Some(result) = results.next().await { println!("{result:?}"); }

// cancel both search engine and index
search_engine.cancel_all();

// or you can cancel the search engine separately, without cancelling the index
search_engine.cancel();

// cancelling the index terminates the search engine
index.cancel();
```

`IndexHandle` and `SearchEngine` are cheap to clone; dropping the last clone cancels them.
`join()` waits for termination and returns a `Completion` (`Cancelled` or `Failed(error)`).

## Design

All components are tokio tasks communicating over channels (`flume`).

Search is performed utilizing `InvertedIndex`.

InvertedIndex is owned by the `index_manager` task - it serializes concurrent update/read requests and keeps track of
index state in regard of directory state. `IndexHandle` exposes `find_files_by_token` and
`find_tokens_matching_predicate` for querying it (the `Index` trait).

Directory is tracked by the `sync_fs` task which emits directory content and watches for changes

- the `notify` crate is used for directory watching (inotify / FSEvents / ReadDirectoryChangesW)
- We initialize watcher first, buffer its events, emit directory content. After that we emit buffered and new watcher
  events. That way changes happened during file sync will be captured by watcher and processed by downstream tasks.
- Watcher can report an overflow (events lost). In this case we lost track of repository contents and need to reinitialize
- Sometimes, when directory is changed during initial file sync, the walk fails. In this case we retry file
  sync (with a backoff).
- Every emitted event has a logical timestamp to prevent saving stale events

File sync events are handled by 4 `indexer` tasks running in parallel. They read file content, tokenize it and pass
documents to the `index_manager` task. Files larger than 10 MB are indexed as empty.

`launch_index` spawns `index_manager`, `sync_fs`, `indexer` tasks, establishes communication between them and
exposes the index via `IndexHandle`. If any task fails, the whole index fails.

`launch_search_engine` accepts the `IndexHandle`. It accepts user queries, asks the index for possible document
candidates and passes them to a pool of `search_in_file` workers to return exact matches. It exposes this
functionality via `SearchEngine`.

To keep track of the index state, `SearchEngine::index_state` returns the state at the moment of a call,
and `SearchEngine::index_status_updates` subscribes to major index events (the last 16 are replayed to new subscribers).

Search and tokenization can be customized by implementing the `IndexConfig` trait. `word_index_config` and
`trigram_index_config` are implemented.
