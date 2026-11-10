# Rust source corpus benchmark, 2026-11-10

See `machine.json` for the machine, corpus commit, file counts and runtime flags;
`queries.txt` is shared by both implementations. The trailing spaces in
`impl `, `return ` and `if ` are intentional parts of the search strings.
Submodules were not initialized.
The original IntelliJ attempt was cancelled at the user's request to select a
smaller repository. No completed IntelliJ runs are included in these results.

## Validation

- Three consecutive `cargo test` runs: 30 unit tests + 4 integration tests passed each time.
- `cargo clippy --all-targets`: passed without warnings.
- `cargo fmt --check`: passed.
- `cargo build --release`: passed.
- Gradle `:lib:test :app:installDist`: 27 of 28 tests passed; the build stopped at
  the failing test task. `TrigramIndexConfigKtTest.should be able to search()`
  hit its one-second coroutine timeout. Rerunning that class without competing
  build processes reproduced the timeout (2 passed, 1 failed).
- A separate Gradle `:app:installDist` succeeded.

Gradle used `--no-daemon --max-workers=1 -Dorg.gradle.jvmargs=-Xmx512m` for the
initial suite, and `-Xmx384m -Pkotlin.compiler.execution.strategy=in-process`
for the separate app build and test rerun. Library/test behavior was not changed.

## Reproduction

Build both implementations and generate `bench/.kotlin-classpath` first (see
`prompt.md` / `bench/build_kotlin.sh`). Run each process serially, with no build
processes competing for resources:

```sh
BENCH_OUT="$PWD/bench/results/rust-corpus-2026-11-10/results.jsonl" \
BENCH_QUERIES="$PWD/bench/results/rust-corpus-2026-11-10/queries.txt" \
BENCH_THREADS=1 \
JAVA_OPTS='-Xmx1200m -XX:ActiveProcessorCount=1 -Dkotlinx.coroutines.scheduler.core.pool.size=1' \
bench/run.sh /tmp/rust-corpus 3 5 8
python3 bench/summarize.py bench/results/rust-corpus-2026-11-10/results.jsonl \
  > bench/results/rust-corpus-2026-11-10/summary.md
```

The runner pre-reads files excluding `.git`; the indexers themselves traverse
`.git` too. No caches were dropped. Neither implementation was optimized.
A single vCPU cannot provide multicore scaling measurements. Eight concurrent
clients measure overlap and scheduling on this machine, not parallel speedup.

## Completion note

All 24 timed processes completed, with exactly three repetitions per group.
`summary.md` validates identical file counts and query counts/checksums across
both implementations and watcher settings. The shell runner reported a trailing
parse error after writing the last result because its comments were edited while
it was executing. The timed commands and saved data were complete; the current
script passes `bash -n`. The exact process log is preserved in `run.log`.

A subsequent smoke run of the current runner on `lib/src/test/resources/testdata`
completed all eight mode/watcher/implementation combinations with exit code 0
and identical query counts/checksums (`1` repetition, `1` warm iteration, `1` client).
