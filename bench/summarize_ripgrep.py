#!/usr/bin/env python3
"""Summarize ripgrep.py output alongside the recorded Kotlin/Rust benchmark."""
import collections
import json
import math
from pathlib import Path
import statistics as st
import sys

baseline_path, rg_path = map(Path, sys.argv[1:3])
base = [json.loads(l) for l in baseline_path.read_text().splitlines()]
runs = [json.loads(l) for l in rg_path.read_text().splitlines()]
assert len(runs) == 3 and sorted(r['rep'] for r in runs) == [1, 2, 3]
groups = collections.defaultdict(list)
for r in base:
    if not r['watcher']:
        groups[(r['mode'], r['impl'])].append(r)
expected = [(q['q'], q['count'], q['checksum']) for q in base[0]['queries']]
for r in base + runs:
    assert [(q['q'], q['count'], q['checksum']) for q in r['queries']] == expected
assert all(len(rows) == 3 for rows in groups.values())

def med(rows, key):
    return st.median(r[key] for r in rows)

def suite(rows):
    return sum(st.median(r['queries'][i]['median_ms'] for r in rows) for i in range(len(expected)))

rg_suite = suite(runs)
print('# ripgrep vs the file indexers\n')
print('Same Rust source checkout and 12 literal, case-sensitive queries. One vCPU; warm page cache attempted, no cache drops. Medians over three repetitions, five warm timings per query, eight concurrent clients. Indexer rows use watchers disabled.\n')
print('| implementation | indexing ms | 12 warm queries ms | concurrent q/s | peak process RSS MiB |')
print('|---|---:|---:|---:|---:|')
for mode in ['word', 'trigram']:
    for impl in ['kotlin', 'rust']:
        rows = groups[(mode, impl)]
        print(f'| {impl}, {mode} | {med(rows, "index_ms"):.0f} | {suite(rows):.0f} | {med(rows, "concurrent_qps"):.1f} | {med(rows, "process_maxrss_kb")/1024:.1f} |')
print(f'| ripgrep CLI | none | {rg_suite:.0f} | {med(runs, "concurrent_qps"):.1f} | {med(runs, "peak_rss_kb")/1024:.1f} |\n')
print('| query | matches | Kotlin word ms | Rust word ms | Kotlin trigram ms | Rust trigram ms | ripgrep ms |')
print('|---|---:|---:|---:|---:|---:|---:|')
for i, (query, count, _) in enumerate(expected):
    values = [st.median(r['queries'][i]['median_ms'] for r in groups[(mode, impl)])
              for mode in ['word', 'trigram'] for impl in ['kotlin', 'rust']]
    values.append(st.median(r['queries'][i]['median_ms'] for r in runs))
    print(f'| `{query}` | {count} | ' + ' | '.join(f'{v:.2f}' for v in values) + ' |')
print('\nApproximate indexing amortization, assuming a uniform mix of these 12 queries, steady warm latency, no updates, and ignoring ripgrep’s eligibility prepass:\n')
print('| indexer | searches to recover initial indexing time |')
print('|---|---:|')
for mode in ['word', 'trigram']:
    for impl in ['kotlin', 'rust']:
        rows = groups[(mode, impl)]
        saving = (rg_suite - suite(rows)) / len(expected)
        n = str(math.ceil(med(rows, 'index_ms') / saving)) if saving > 0 else 'does not amortize under this query mix'
        print(f'| {impl}, {mode} | {n} |')
print('''
All 12 query counts and checksums match every recorded Kotlin/Rust run. Ripgrep validation also accounts for CR-only line endings when reconstructing the indexers’ line numbers.

Interpretation limits:

- Ripgrep 15.2.0 searches the original directory each time, including hidden/ignored files and `.git`, with literal case-sensitive matching and one thread. A one-time strict-UTF8 prepass excludes the same invalid files the indexers cannot search; files above 10 MB are skipped. Its preparation cost is recorded in `machine.json`, separately from query latency. The corpus was not modified.
- Ripgrep timing includes process startup, directory traversal, and formatting matching lines to `/dev/null`. The library benchmarks consume results in-process without CLI serialization. This is a practical CLI-versus-library comparison, not a pure search-loop comparison. Validation and the separate RSS probes are outside query timings.
- Ripgrep peak RSS is the median of each repetition’s largest per-query process peak. It excludes the Python measurement driver and does not measure aggregate memory for concurrent processes. Indexer RSS includes the persistent index and runtime. See the earlier report for Kotlin live heap after GC.
- Eight ripgrep clients use eight one-thread processes; indexer clients share a worker pool. All compete on one vCPU; no multicore scaling is measured.
- The first timing in each repetition is recorded as `cold_ms` for consistency with the existing schema, but is not a filesystem-cold measurement. Raw warm samples and per-query RSS probes are preserved.
'''.rstrip())
