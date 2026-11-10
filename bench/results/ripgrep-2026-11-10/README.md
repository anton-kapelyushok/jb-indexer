# ripgrep baseline, 2026-11-10

Comparison against the existing Kotlin/Rust benchmark on the same rust-lang/rust
checkout and the same twelve literal, case-sensitive queries. See `summary.md`
for the comparison and its limits, `machine.json` for ripgrep flags/version and
scope preparation, `validation.json` for checked result counts/checksums, and
`results.jsonl` for all latency samples, throughput, and RSS probes.

The corpus, machine and indexer raw results are documented in
`../rust-corpus-2026-11-10/`. The matching corpus commit is
`6f1c7ee0ec0ea64802f4b7a184b63b7efb645b3b`.

Reproduce from the repository root with ripgrep and GNU time installed:

```sh
python3 bench/ripgrep.py /tmp/rust-corpus \
  bench/results/rust-corpus-2026-11-10/results.jsonl \
  bench/results/rust-corpus-2026-11-10/queries.txt \
  bench/results/ripgrep-2026-11-10
python3 bench/summarize_ripgrep.py \
  bench/results/rust-corpus-2026-11-10/results.jsonl \
  bench/results/ripgrep-2026-11-10/results.jsonl \
  > bench/results/ripgrep-2026-11-10/summary.md
```

The benchmark first checks every query against all prior Kotlin/Rust runs and
stops on any mismatch. Its one-time file validation reads the searchable corpus,
warming the page cache. Each timed query then launches a single one-thread
ripgrep process against the original directory, including hidden/ignored files.
Invalid UTF-8 files are explicitly excluded, and a 10 MB limit matches indexing.
Globs are evaluated with the corpus as the working directory. Ripgrep's default
LF-based output is reconciled with the indexers' CR/LF/CRLF line numbering during
validation; the timed command searches the original files without rewriting them.

The first timing of each repetition and query is followed by five warm timings
and a separate GNU time RSS probe. Eight concurrent clients each execute the
query suite sequentially. Results from the validation pass are not included in
latency measurements. The original indexer measurements were collected earlier
in this session, rather than interleaved with these ripgrep measurements.
