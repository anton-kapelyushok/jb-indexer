#!/usr/bin/env bash
# Runs the Kotlin and Rust benchmark harnesses on the same corpus and writes one JSON line per run.
# Usage: bench/run.sh CORPUS_DIR [REPEATS] [ITERS] [CONCURRENCY]
# Optional: BENCH_OUT selects the output file; BENCH_QUERIES selects a shared query file.
#   prerequisites: bench/build_kotlin.sh (or gradle installDist) and `cargo build --release` in rust/
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CORPUS="$1"; REPEATS="${2:-3}"; ITERS="${3:-5}"; CONC="${4:-8}"
OUT="${BENCH_OUT:-$ROOT/bench/results/results.jsonl}"; mkdir -p "$(dirname "$OUT")"; : > "$OUT"
JAVA_OPTS="${JAVA_OPTS:--Xmx384m}"
QUERIES="${BENCH_QUERIES:-$ROOT/bench/queries.txt}"
TIME_FILE=$(mktemp)
trap 'rm -f "$TIME_FILE"' EXIT
# Pre-read source files; available RAM determines how much remains cached.
find "$CORPUS" -type f -not -path '*/.git/*' -exec cat {} + > /dev/null
for mode in word trigram; do for watcher in false true; do for rep in $(seq 1 "$REPEATS"); do
  for impl in rust kotlin; do
    if [ "$impl" = rust ]; then cmd=("$ROOT/rust/target/release/bench")
    else cmd=(java $JAVA_OPTS -Duser.language=en -Duser.country=US -cp "$(cat "$ROOT/bench/.kotlin-classpath")" BenchKt); fi
    json=$(/usr/bin/time -f '%e %M' -o "$TIME_FILE" "${cmd[@]}" "$CORPUS" "$mode" "$watcher" "$QUERIES" "$ITERS" "$CONC")
    read -r wall maxrss < "$TIME_FILE"
    echo "${json%\}},\"rep\":$rep,\"process_wall_s\":$wall,\"process_maxrss_kb\":$maxrss}" >> "$OUT"
    echo "$impl $mode watcher=$watcher rep=$rep wall=${wall}s maxrss=${maxrss}KB"
  done
done; done; done
