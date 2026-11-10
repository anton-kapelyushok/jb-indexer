#!/usr/bin/env python3
"""Compare a one-thread ripgrep CLI with recorded indexer runs; see --help."""
import argparse
import base64
import bisect
import concurrent.futures
import glob
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import tempfile
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('corpus', type=Path)
    p.add_argument('baseline', type=Path)
    p.add_argument('queries', type=Path)
    p.add_argument('out', type=Path)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--iterations', type=int, default=5)
    p.add_argument('--clients', type=int, default=8)
    args = p.parse_args()
    root = args.corpus.resolve()
    queries = [q for q in args.queries.read_text().splitlines() if q]
    baseline = [json.loads(l) for l in args.baseline.read_text().splitlines()]
    expected = {q['q']: (q['count'], q['checksum']) for q in baseline[0]['queries']}
    for r in baseline:
        assert {q['q']: (q['count'], q['checksum']) for q in r['queries']} == expected
    assert set(queries) == set(expected)
    rg = shutil.which('rg')
    assert rg, 'ripgrep is required'

    # Pre-validate exactly as indexing does: canonical paths, <=10 MB, strict UTF-8.
    start = time.perf_counter()
    files = set()
    rejected = {'too_large': 0, 'invalid_utf8': 0}
    excluded_paths = []
    cr_ends = {}
    for directory, _, names in os.walk(root):
        for name in names:
            path = Path(directory, name)
            if not path.is_file():
                continue
            path = path.resolve()
            if path.stat().st_size > 10_000_000:
                rejected['too_large'] += 1
                continue
            data = path.read_bytes()
            try:
                data.decode('utf-8')
            except UnicodeDecodeError:
                rejected['invalid_utf8'] += 1
                excluded_paths.append(str(path.relative_to(root)))
                continue
            files.add(str(path))
            if b'\r' in data.replace(b'\r\n', b''):
                cr_ends[str(path)] = [m.end() for m in re.finditer(rb'\r\n|\r|\n', data)]
    prep_ms = (time.perf_counter() - start) * 1000
    # A single rg process traverses the tree for each query. Explicit globs make
    # its input universe match strict UTF-8 indexing without changing the files.
    batches = [[str(root)]]
    common = [rg, '--no-config', '--threads', '1', '-uuu', '--fixed-strings',
              '--text', '--encoding', 'none', '--color', 'never',
              '--max-filesize', '10000000']
    for path in sorted(set(excluded_paths)):
        common += ['--glob', '!' + glob.escape(path)]
    args.out.mkdir(parents=True, exist_ok=True)
    metadata = {
        'rg_version': subprocess.check_output([rg, '--version'], text=True),
        'corpus': str(root), 'baseline': str(args.baseline),
        'queries': queries, 'eligible_files': len(files), 'excluded': rejected,
        'preparation_ms': prep_ms, 'argv_batches_per_query': len(batches),
        'common_flags': common[1:], 'timed_output_flags': ['--line-number', '--with-filename', '--null', '--no-heading'],
        'repeats': args.repeats, 'iterations': args.iterations, 'clients': args.clients,
        'timing': 'CLI wall time including startup, directory traversal, and formatting matching lines; stdout=/dev/null; validation outside timing',
        'scope': 'Each query traverses the original directory, including .git; invalid-UTF8 exclusions discovered in a one-time prepass; max file size 10MB; equivalence validated',
        'excluded_paths': sorted(set(excluded_paths)),
        'concurrency': 'Eight client threads launch one-thread rg processes; this is process concurrency, whereas the indexers share a worker pool',
        'cache': 'Manifest preparation reads eligible files first; no cache drops',
    }
    (args.out / 'machine.json').write_text(json.dumps(metadata, indent=2) + '\n')

    def decode(value):
        return value['text'].encode() if 'text' in value else base64.b64decode(value['bytes'])

    # Validate actual matches, counting matching lines rather than occurrences.
    validation = []
    for query in queries:
        count = checksum = 0
        for batch in batches:
            proc = subprocess.run(common + ['--json', '-e', query, '--'] + batch,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=root)
            assert proc.returncode in (0, 1), proc.stderr.decode(errors='replace')
            for raw in proc.stdout.splitlines():
                event = json.loads(raw)
                if event['type'] != 'match':
                    continue
                d = event['data']
                path = os.fsdecode(decode(d['path']))
                line_no = d['line_number']
                if path in cr_ends:
                    line_no = bisect.bisect_right(cr_ends[path], d['absolute_offset']) + 1
                # The indexers split CR, LF and CRLF, unlike rg's LF-only output.
                for i, line in enumerate(re.split(rb'\r\n|\r|\n', decode(d['lines']))):
                    if query.encode() in line:
                        count += 1
                        checksum += line_no + i + len(os.fsencode(path))
        assert (count, checksum) == expected[query], (query, (count, checksum), expected[query])
        validation.append({'q': query, 'count': count, 'checksum': checksum})
        print(f'validated {query!r}: {count} matching lines', flush=True)
    (args.out / 'validation.json').write_text(json.dumps(validation, indent=2) + '\n')

    def run(query, rss=False):
        start = time.perf_counter()
        peaks = []
        with tempfile.TemporaryDirectory(prefix='rg-time-') as tmp:
            for batch in batches:
                cmd = common + ['--line-number', '--with-filename', '--null', '--no-heading', '-e', query, '--'] + batch
                if rss:
                    cmd = ['/usr/bin/time', '-f', '%M', '-o', tmp + '/rss'] + cmd
                proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, cwd=root)
                assert proc.returncode in (0, 1), proc.stderr.decode(errors='replace')
                if rss:
                    peaks.append(int(Path(tmp, 'rss').read_text().splitlines()[-1]))
        return (time.perf_counter() - start) * 1000, max(peaks, default=0)

    with (args.out / 'results.jsonl').open('w') as output:
        for rep in range(1, args.repeats + 1):
            rows = []
            for query in queries:
                cold, _ = run(query)
                times = [run(query)[0] for _ in range(args.iterations)]
                _, peak = run(query, rss=True)  # Separate RSS probe excludes time-wrapper overhead from latency.
                count, checksum = expected[query]
                rows.append(dict(q=query, count=count, checksum=checksum, cold_ms=cold,
                                 median_ms=statistics.median(times), warm_ms=times, peak_rss_kb=peak))
            def client(_):
                for query in queries:
                    run(query)
            start = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
                list(pool.map(client, range(args.clients)))
            elapsed = (time.perf_counter() - start) * 1000
            row = dict(impl='ripgrep', rep=rep, queries=rows, concurrent_ms=elapsed,
                       concurrent_qps=args.clients * len(queries) / (elapsed / 1000),
                       peak_rss_kb=max(q['peak_rss_kb'] for q in rows))
            output.write(json.dumps(row) + '\n')
            output.flush()
            print(f'rep {rep}: warm suite={sum(q["median_ms"] for q in rows):.0f} ms; throughput={row["concurrent_qps"]:.2f} q/s', flush=True)


if __name__ == '__main__':
    main()
