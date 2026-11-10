# Follow-up: benchmark the Rust port against the Kotlin indexer on a real machine

## Context

This repo contains a Kotlin text-search library (`lib/`, with a CLI in `app/`; see the top-level `README.md`). On branch
`air/repository-purpose-overview-request-4c428306-d` it was ported to **Rust + tokio** in `rust/`. The port keeps
the same structure: the same components, channels, message types, status state machine and algorithms, with one
Rust file per Kotlin file. Read `rust/README.md` first. It has the Kotlin→Rust mapping, the known behaviour
differences and the first benchmark results.

Those first results came from a **1 vCPU / ~900 MB RAM VM**. They show Rust ahead on every metric: 2–5.5x faster
initial indexing, about 3.5x less memory and 1.5–2.6x faster queries. They are still weak, though:

- With a single core, nothing parallel was measured: the 4 indexers, 4 search workers, the parallel
  `InvertedIndex.compact` and concurrent queries.
- The corpus was small: a 4.5k-file, 37 MB sparse subset of intellij-community.
- Gradle was OOM-killed, so **the Kotlin unit tests were never run**. Kotlin was compiled with
  `bench/build_kotlin.sh`, which calls the Kotlin compiler directly on jars Gradle had already downloaded.

Your job is to produce trustworthy numbers on a bigger machine and update the docs.

## Tasks

1. **Verify both builds and test suites.**
   - `./gradlew :lib:test :app:installDist`. Report any Kotlin test failures. They were never run, so failures may
     already exist and have nothing to do with this work. Don't "fix" Kotlin behaviour just to match Rust.
   - `cd rust && cargo test`. All 34 tests should pass; run the suite several times to check for flakiness, since
     the watcher tests depend on timing. Also run `cargo clippy --all-targets` and `cargo fmt --check`.
   - `bench/run.sh` expects `bench/.kotlin-classpath`, which `bench/build_kotlin.sh` writes. If you build with
     Gradle instead, generate that file from `app/build/install/app/lib/*` or adapt `run.sh`.

2. **Benchmark on a real corpus.**
   - Clone the full `https://github.com/JetBrains/intellij-community` (`--depth 1` is fine). That is the corpus the
     assignment names. Write down the file count and size.
   - Pick a Kotlin `-Xmx` that the full corpus fits in (the default 384m won't). Set it with
     `JAVA_OPTS="-Xmx..." bench/run.sh ...` and record what you used. The trigram index is the memory-heavy one.
   - Run `bench/run.sh <corpus> 3 5 8`, then `python3 bench/summarize.py`. The summary script asserts that the two
     implementations return identical result counts and checksums for every query; investigate any mismatch.
     The only known difference is Unicode trigrams: Kotlin splits emoji into UTF-16 surrogate halves. It changes a
     handful of trigrams and no query results.
   - Measure **parallel scaling**. Run the Rust bench with `BENCH_THREADS=1,2,4,8,…` (this sets the tokio worker
     count). Constrain the JVM the same way: `-XX:ActiveProcessorCount=N`, plus
     `-Dkotlinx.coroutines.scheduler.core.pool.size=N` if needed. Alternatively, use `taskset -c` for both.
     Report indexing time and concurrent throughput against core count.
   - Optional: compare with the page cache cold (drop caches between runs) as well as warm.
   - Optional: measure watcher startup cost separately. Both watchers walk the whole tree on start.

3. **Update the docs with the new numbers.**
   - Replace or extend the "Performance vs Kotlin" section in `rust/README.md`, and commit the new
     `bench/results/*`. Keep the 1-vCPU numbers as a separate, clearly labelled section rather than deleting them.
   - State the machine (CPU model, cores, RAM), the OS, JDK, Rust and Kotlin versions, the corpus commit and the JVM
     flags.
   - Be honest. If Kotlin wins anywhere (for example, if it scales better), say so. If the JVM's RSS is mostly
     heap headroom, report the live heap after GC as well; the bench already prints `heap_after_gc_kb`.

## Constraints

- Keep the two implementations doing equivalent work. Don't optimise only the Rust side, because the point is
  to compare runtimes, not algorithms. If you want to show optimisation headroom, keep it separate and label it,
  for example as an extra row or a feature flag.
- Don't change `lib/` behaviour. `app/src/main/kotlin/Bench.kt` and `rust/src/bin/bench.rs` must stay identical
  in what they measure.
- The root `.gitignore` ignores every `bin/` directory. `rust/.gitignore` re-includes `src/bin/`, so keep that.
- Commit to this branch. Don't push to `master`.

## Known issues worth knowing about (don't silently "fix" these)

- The word config tokenizes on Java's ASCII `\W`. A query with no ASCII word characters (Cyrillic text, or `""`)
  can't find files that have no ASCII tokens either. This is inherited from Kotlin and documented.
- The Rust watcher registers new directories before walking them, because `notify` emits the directory's create
  event before it adds the watch. It also coalesces each batch of raw events per path. Both changes are
  deliberate; see `rust/README.md`.
