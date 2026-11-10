# File Indexer — Rust + tokio port

A port of the Kotlin library in `../lib` that keeps the same structure. It has the same components, channels, message types, status state machine and index algorithms. Each Kotlin source file has a Rust counterpart:

| Kotlin (`lib/src/main/kotlin/indexer/core`) | Rust (`src`) |
|---|---|
| `model.kt` (`Index`, `SearchEngine`, `IndexConfig`, `IndexState`, `IndexStatusUpdate`) | `model.rs` |
| `launchIndex.kt` | `launch_index.rs` |
| `launchSearchEngine.kt` | `launch_search_engine.rs` |
| `wordIndexConfig.kt` / `trigramIndexConfig.kt` | `word_index_config.rs` / `trigram_index_config.rs` |
| `internal/indexManager.kt`, `InvertedIndex.kt`, `syncFs.kt`, `FsWatcher.kt`, `indexer.kt`, `searchInFile.kt`, `model.kt`, `utils.kt` | `internal/*.rs` (same names, snake_case) |
| `app/src/main/kotlin/Main.kt` | `src/bin/indexer_app.rs` |

## Usage

```rust
let cfg: Arc<dyn IndexConfig> = Arc::new(word_index_config(/* enable_watcher */ true));
let index = launch_index(".", cfg.clone());
let search_engine = launch_search_engine(cfg, index.clone());

// progress
let s = search_engine.index_state().await;
println!("{}", s.pretty_print());

// wait until in sync
let mut updates = search_engine.index_status_updates();
while let Some(u) = updates.next().await { if u.is_in_sync() { break } }

// query (lazy stream)
let mut results = search_engine.find("some query");
while let Some(r) = results.next().await { println!("{}:{} {}", r.path, r.line_no, r.line) }

search_engine.cancel();      // only the engine
search_engine.cancel_all();  // engine + index
index.cancel();              // index (the engine terminates with it)
```

`cargo run --release --bin indexer-app -- DIR [word|trigram] [true|false]` starts the interactive CLI, which has the same commands as `Main.kt`.

## Kotlin → Rust mapping

| Kotlin | Rust |
|---|---|
| coroutine launched in a scope (`launch` / `async`) | tokio task in a `JoinSet`, watched by a supervisor task. One child failing fails all of them. |
| `Job.cancel()` / structured cancellation | `CancellationToken`. Dropping the last `Index` / `SearchEngine` handle also cancels. |
| `Channel` (MPMC, rendezvous or unlimited) | `async_channel` (MPMC). Rendezvous becomes capacity 1, because there is no capacity 0. |
| `select { }` (biased, in clause order) | `tokio::select! { biased; ... }` |
| `CompletableDeferred` reply | `oneshot`. A dropped sender replaces Kotlin's `withIndexContext` "stuck await" workaround. |
| `MutableSharedFlow(replay = 16, DROP_OLDEST)` | `StatusBus`: a 16-item replay buffer plus `broadcast`. Lagging subscribers skip ahead. |
| `Flow` / `flatMapMerge(4)` | `Stream` / `buffer_unordered(4)` |
| `withContext(Dispatchers.IO)` | `spawn_blocking` for file reads, tokenization and directory walks |
| `io.methvin:directory-watcher` | `notify` (inotify), plus methvin-style mtime "hashing", directory expansion and per-batch coalescing |
| Guava weak interner | `Interner`, which purges unused `Arc<str>` lazily |

### Behaviour differences

- **Unicode trigrams.** Kotlin's `windowed(3)` works on UTF-16 code units, so emoji are split into surrogate halves. Rust uses Unicode scalar values. On the benchmark corpus this changes the vocabulary by 2 of 69k trigrams and doesn't change any query result.
- **Watcher events.** `notify` emits a directory's Create event before it watches the new directory. The Rust watcher therefore registers the directory itself before walking it, otherwise files created right after `mkdir` can be lost; I hit this as a flaky e2e test. It also coalesces each batch of raw events per path, the way JDK's WatchService coalesces repeated `ENTRY_MODIFY`, so one save produces one Modify.
- **Search engine failure.** `launchSearchEngine.kt` builds, but never throws, the exception for "underlying index failed". The Rust engine always terminates when its index does.
- **Inherited limitation (unchanged).** The word config tokenizes on Java's ASCII `\W`. A query with no ASCII word characters (for example, Cyrillic text or `""`) can't find files that contain none either. The trigram config doesn't have this problem.

## Tests

`cargo test` runs 34 tests. These are ports of the Kotlin `InvertedIndexTest`, `IndexManagerTest`, `IndexerKtTest`, `SyncFsKtTest` (using a controllable mock watcher), `WordIndexConfigKtTest`, `TrigramIndexConfigKtTest` and the `LaunchSearchEngineKtTest` e2e (`tests/e2e.rs`, run for both configs), plus tests for cancellation, missing directories and event coalescing. 20 consecutive full runs passed.

## Performance vs Kotlin

The harnesses are identical: `src/bin/bench.rs` and `../app/src/main/kotlin/Bench.kt`. Scripts are in `../bench`, raw data in `../bench/results/results.jsonl`, and the full tables in `../bench/results/summary.md`.

**Setup:** sparse checkout of intellij-community (`platform/{util,core-api,core-impl,analysis-api,editor-ui-api}`): 4,562 files, 37 MB, warm page cache. **1 vCPU, ~900 MB RAM** VM. OpenJDK 17 (`-Xmx384m`, default GC), Kotlin 1.8.20, coroutines 1.7.1. Rust 1.99 release build. Each configuration was run in 3 processes, and the table shows medians. Every query returned identical match counts and checksums from both implementations.

| config | metric | Kotlin | Rust | Rust advantage |
|---|---|---:|---:|---:|
| word | time to `IndexInSync` | 4611 ms | 836 ms | **5.5x** |
| word | peak RSS | 138 MB | 37 MB | **3.7x** |
| word | 12 queries, warm, sequential | 696 ms | 263 ms | **2.6x** |
| word | throughput, 8 concurrent clients | 13.5 q/s | 23.7 q/s | **1.8x** |
| trigram | time to `IndexInSync` | 5971 ms | 2786 ms | **2.1x** |
| trigram | peak RSS | 210 MB | 60 MB | **3.5x** |
| trigram | 12 queries, warm, sequential | 489 ms | 250 ms | **2.0x** |
| trigram | throughput, 8 concurrent clients | 16.0 q/s | 24.7 q/s | **1.5x** |

With the watcher enabled, results are about the same (see `summary.md`). Single-query speedups range from 1.5x to 12x. The biggest gains are on selective queries, where Kotlin's fixed per-query overhead dominates: `xyzzy_definitely_absent` takes 7.3 ms vs 0.7 ms in the word index. The smallest gains are on queries with many matches, like `if (` (21k matches), where both spend most of their time reading and scanning candidate files.

**Caveats:**
- The machine has **one core**. That means the parallel parts (4 indexers, 4 search workers, the parallel `compact`) were not tested for scaling, and "concurrent throughput" measures overlap and overhead, not parallel speedup.
- The JVM's RSS is mostly runtime and heap headroom. After a full GC, the live heap is only 19 MB (word) and 35 MB (trigram). The JVM also pays JIT warm-up: cold query times are up to 10x higher than warm ones on Kotlin.
- Both implementations do the same work per file: `canonicalize`, strict-UTF-8 read, per-line tokenize into owned strings, a per-file token set, and messages through the same channels. I didn't add optimizations to the port, so the numbers compare runtimes, not algorithms.
- Gradle gets OOM-killed on this VM, so Kotlin was compiled with `../bench/build_kotlin.sh`. That script runs the same Kotlin compiler on Gradle's resolved jars.

Reproduce:

```sh
bench/build_kotlin.sh                         # or ./gradlew :app:installDist on a bigger machine
(cd rust && cargo build --release)
bench/run.sh /path/to/corpus 3 5 8            # repeats, warm iterations per query, concurrent clients
python3 bench/summarize.py
```
