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

## Usage example

```kotlin
// use word index (trigram index can also be used)
val cfg = indexer.core.wordIndexConfig(enableWatcher = true)

// launch index in current scope
val index = launchIndex(Path("."), cfg)

// launch searchEngine in current scope
val searchEngine = launchSearchEngine(cfg, index)

// show progress
searchEngine.indexState().let {
    val percent = if (it.totalFileEvents == 0L) 0.0
    else 100.0 * it.handledFileEvents / it.totalFileEvents
    println("Indexing progress: $percent%")
}

// wait until it is in sync
searchEngine.indexStatusUpdates().first { it is IndexStatusUpdate.IndexInSync }

// execute some query
searchEngine.find("some query").collect { println(it) }

// cancel both searchEngine and index 
searchEngine.cancelAll()

// or you can cancel searchEngine separately, without cancelling index
searchEngine.cancel()

// cancelling index will lead to searchEngine cancellation
index.cancel()
```

## Design

Search is performed utilizing `InvertedIndex`.

InvertedIndex is managed by `indexManager` coroutine - it handles concurrent update/read queries and keeps track of index
state in regard of directory state. It exposes `findFilesByToken` and `findTokensMatchingPredicate` methods for querying
it

Directory is tracked with syncFs coroutine which emits directory content and watches for changes

- I use `io.methvin:directory-watcher` library for directory watching
    - on my machine`FileSystems.getDefault().newWatchService()` returns `PollingWatchService`, which is
      really slow, this one is faster
- We initialize watcher first, buffer its events, emit directory content. After that we emit buffered and new watcher
  events. That way changes happened during file sync will be captured by watcher and processed by downstream coroutine.
- Watcher can throw `overflown` event. In this case we lost track of repository contents and need to reinitialize
- Sometimes, when directory is changed during initial file sync, the process can fail. In this case we retry file
  sync.
- Every emitted event has a logical timestamp to prevent saving stale events

File sync events are handled by `indexer` coroutine, which runs in parallel. It reads file content, parses it and passes
parsed documents to `indexManager` coroutine

`launchIndex` coroutine launches `indexManager`, `syncFs`, `indexer` coroutines, establishes communication between them and
exposes the index via `Index` interface

`launchSearchIndex` accepts `Index` returned by `launchIndex` coroutine. It accepts user queries, communicates with
index
for possible document candidates and passes them to `searchInFile` worker pool to return exact matches. It exposes this
functionality via `SearchEngine` interface

To keep track search engine state `SearchEngine.indexState` is used to get state at the moment of a call,
and `SearchEngine.indexStatusUpdates` to subscribe for major index events

Search and parse behavior can be customized by implementing IndexConfig interface. `wordIndexConfig` and
`trigramIndexConfig` are implemented.

## Rust port

A Rust + tokio port with the same structure, plus a performance comparison against this Kotlin version, lives in [`rust/`](rust/README.md).
