# ripgrep vs the file indexers

Same Rust source checkout and 12 literal, case-sensitive queries. One vCPU; warm page cache attempted, no cache drops. Medians over three repetitions, five warm timings per query, eight concurrent clients. Indexer rows use watchers disabled.

| implementation | indexing ms | 12 warm queries ms | concurrent q/s | peak process RSS MiB |
|---|---:|---:|---:|---:|
| kotlin, word | 11316 | 4810 | 2.6 | 552.9 |
| rust, word | 3298 | 4746 | 2.7 | 460.1 |
| kotlin, trigram | 23931 | 3147 | 3.1 | 1277.6 |
| rust, trigram | 15140 | 3840 | 3.4 | 539.2 |
| ripgrep CLI | none | 3453 | 3.7 | 6.9 |

| query | matches | Kotlin word ms | Rust word ms | Kotlin trigram ms | Rust trigram ms | ripgrep ms |
|---|---:|---:|---:|---:|---:|---:|
| `fn main` | 32540 | 956.13 | 598.22 | 551.40 | 578.19 | 337.48 |
| `pub fn` | 49021 | 690.40 | 595.87 | 341.81 | 356.62 | 295.23 |
| `impl ` | 54290 | 513.00 | 550.18 | 390.07 | 485.31 | 327.77 |
| `#[test]` | 15360 | 352.29 | 424.47 | 70.26 | 93.89 | 263.60 |
| `use std::` | 19562 | 397.29 | 451.67 | 322.97 | 395.98 | 283.48 |
| `return ` | 40568 | 378.10 | 426.41 | 338.38 | 441.50 | 286.41 |
| `TODO` | 394 | 65.78 | 64.52 | 42.08 | 53.71 | 253.69 |
| `xyzzy_definitely_absent` | 0 | 13.65 | 3.83 | 0.28 | 0.05 | 252.47 |
| `unsafe {` | 30163 | 267.14 | 291.66 | 199.41 | 256.71 | 277.41 |
| `Option<` | 28680 | 326.34 | 344.85 | 270.89 | 355.03 | 277.84 |
| `Result<` | 15774 | 331.94 | 380.48 | 153.68 | 205.57 | 267.41 |
| `if ` | 123417 | 518.18 | 613.61 | 466.06 | 616.99 | 329.84 |

Approximate indexing amortization, assuming a uniform mix of these 12 queries, steady warm latency, no updates, and ignoring ripgrep’s eligibility prepass:

| indexer | searches to recover initial indexing time |
|---|---:|
| kotlin, word | does not amortize under this query mix |
| rust, word | does not amortize under this query mix |
| kotlin, trigram | 941 |
| rust, trigram | does not amortize under this query mix |

All 12 query counts and checksums match every recorded Kotlin/Rust run. Ripgrep validation also accounts for CR-only line endings when reconstructing the indexers’ line numbers.

Interpretation limits:

- Ripgrep 15.2.0 searches the original directory each time, including hidden/ignored files and `.git`, with literal case-sensitive matching and one thread. A one-time strict-UTF8 prepass excludes the same invalid files the indexers cannot search; files above 10 MB are skipped. Its preparation cost is recorded in `machine.json`, separately from query latency. The corpus was not modified.
- Ripgrep timing includes process startup, directory traversal, and formatting matching lines to `/dev/null`. The library benchmarks consume results in-process without CLI serialization. This is a practical CLI-versus-library comparison, not a pure search-loop comparison. Validation and the separate RSS probes are outside query timings.
- Ripgrep peak RSS is the median of each repetition’s largest per-query process peak. It excludes the Python measurement driver and does not measure aggregate memory for concurrent processes. Indexer RSS includes the persistent index and runtime. See the earlier report for Kotlin live heap after GC.
- Eight ripgrep clients use eight one-thread processes; indexer clients share a worker pool. All compete on one vCPU; no multicore scaling is measured.
- The first timing in each repetition is recorded as `cold_ms` for consistency with the existing schema, but is not a filesystem-cold measurement. Raw warm samples and per-query RSS probes are preserved.
