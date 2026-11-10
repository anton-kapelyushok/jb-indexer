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

- **Unicode trigrams.** Kotlin's `windowed(3)` works on UTF-16 code units, so emoji are split into surrogate halves. Rust uses Unicode scalar values. On the original sparse IntelliJ benchmark corpus this changes the vocabulary by 2 of 69k trigrams and doesn't change any query result.
- **Watcher events.** `notify` emits a directory's Create event before it watches the new directory. The Rust watcher therefore registers the directory itself before walking it, otherwise files created right after `mkdir` can be lost; I hit this as a flaky e2e test. It also coalesces each batch of raw events per path, the way JDK's WatchService coalesces repeated `ENTRY_MODIFY`, so one save produces one Modify.
- **Search engine failure.** `launchSearchEngine.kt` builds, but never throws, the exception for "underlying index failed". The Rust engine always terminates when its index does.
- **Inherited limitation (unchanged).** The word config tokenizes on Java's ASCII `\W`. A query with no ASCII word characters (for example, Cyrillic text or `""`) can't find files that contain none either. The trigram config doesn't have this problem.

## Tests

`cargo test` runs 34 tests. These are ports of the Kotlin `InvertedIndexTest`, `IndexManagerTest`, `IndexerKtTest`, `SyncFsKtTest` (using a controllable mock watcher), `WordIndexConfigKtTest`, `TrigramIndexConfigKtTest` and the `LaunchSearchEngineKtTest` e2e (`tests/e2e.rs`, run for both configs), plus tests for cancellation, missing directories and event coalescing. 20 consecutive full runs passed.

## Performance vs Kotlin

### Rust source corpus follow-up (2026-11-10)

This environment has **1 vCPU and 1.74 GiB RAM**, without swap, so multicore
scaling remains unmeasured. The Intel Xeon VM runs Ubuntu 26.04.1 LTS,
OpenJDK 17.0.21 and Rust 1.99.1; Kotlin is 1.8.20, coroutines 1.7.1.
The full IntelliJ corpus was cloned (289,729 files / 1,037,998,078 bytes,
commit `b39788da0256d63df1fba0b14f4b29ff250757df`), but its first run was
cancelled when the user requested a smaller repository. No completed IntelliJ
measurements were obtained.

The replacement is a shallow checkout of **rust-lang/rust**, without submodule
initialization: **64,417 files / 230,512,043 bytes**, excluding `.git`.
Raw results, exact corpus commit, machine details and the 12 corpus-specific
queries are in [`../bench/results/rust-corpus-2026-11-10/`](../bench/results/rust-corpus-2026-11-10/).
Both implementations use the same query file. The indexers also traverse `.git`
(they have no Git-specific exclusion): including metadata, the tree contains
64,446 files / 291,871,225 bytes, and both report **64,441 indexed files**;
strict UTF-8 reads reject binary files. The runner pre-reads the source files;
caches are not dropped between processes. These numbers cannot be compared
directly with the original IntelliJ subset because the queries and corpus differ.

Both runtimes use one worker: `BENCH_THREADS=1` for Rust and
`-XX:ActiveProcessorCount=1 -Dkotlinx.coroutines.scheduler.core.pool.size=1`
for Kotlin, with `-Xmx1200m` and default GC. The JVM's maximum heap is headroom,
not its live memory consumption; live heap after full GC is reported separately.

Medians over three processes, watcher disabled (five warm runs per query,
eight concurrent clients):

| config | metric | Kotlin | Rust |
|---|---|---:|---:|
| word | indexing | 11,316 ms | 3,298 ms |
| word | peak process RSS | 553 MiB | 460 MiB |
| word | sum of 12 warm query medians | 4,810 ms | 4,746 ms |
| word | concurrent throughput | 2.6 q/s | 2.7 q/s |
| trigram | indexing | 23,931 ms | 15,140 ms |
| trigram | peak process RSS | 1,278 MiB | 539 MiB |
| trigram | sum of 12 warm query medians | 3,147 ms | 3,840 ms |
| trigram | concurrent throughput | 3.1 q/s | 3.4 q/s |

Rust indexes 3.4x faster for words and 1.6x faster for trigrams. Warm word
latency is essentially tied; **Kotlin's aggregate warm trigram latency is
1.22x faster**. Rust's concurrent throughput is only slightly higher, unlike
the large indexing advantage. Kotlin's live heap after full GC is **116 MiB
(word) / 183 MiB (trigram)**: its peak RSS includes substantial heap headroom
and runtime memory. These are single-core observations; overlapping eight
clients does not demonstrate multicore scaling.

Validation: Rust's 34 tests passed in three consecutive runs; `cargo clippy
--all-targets` and `cargo fmt --check` passed, and the release build succeeded.
Gradle built `:app:installDist`. The initial Kotlin test suite passed 27 of 28
tests; `TrigramIndexConfigKtTest.should be able to search()` timed out after
one second. Running that class again without competing build work reproduced
the timeout. No Kotlin library or test behaviour was changed.

Reproduction commands and build/test details are in the results directory's
[README](../bench/results/rust-corpus-2026-11-10/README.md); the complete
[summary](../bench/results/rust-corpus-2026-11-10/summary.md) includes watcher-enabled
runs and per-query latencies.

The runner supports `BENCH_OUT` and `BENCH_QUERIES` to preserve earlier data
and select a shared query file. The summary validates indexed file counts and
query counts/checksums across **both** watcher settings before publishing
comparisons.

### Original sparse-corpus results (1 vCPU, ~900 MB RAM)

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
