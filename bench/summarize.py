"""Summarizes bench/results/results.jsonl (medians over repetitions) as markdown."""
import json, statistics as st, sys, collections
rows = [json.loads(l) for l in open(sys.argv[1] if len(sys.argv) > 1 else 'bench/results/results.jsonl')]
g = collections.defaultdict(list)
for r in rows: g[(r['mode'], r['watcher'], r['impl'])].append(r)
# Validate every configuration, including watcher-enabled runs, before printing claims.
for mode in ['word', 'trigram']:
    baseline = g[(mode, False, 'kotlin')][0]
    expected = [(q['q'], q['count'], q['checksum']) for q in baseline['queries']]
    for watcher in [False, True]:
        for impl in ['kotlin', 'rust']:
            runs = g[(mode, watcher, impl)]
            assert runs, (mode, watcher, impl, "missing runs")
            for run in runs:
                assert run['indexed_files'] == baseline['indexed_files'], (mode, watcher, impl, "file count")
                assert [(q['q'], q['count'], q['checksum']) for q in run['queries']] == expected, (mode, watcher, impl, "query mismatch")
med = lambda xs: st.median(xs)
def m(rs, f): return med([f(r) for r in rs])

print("| config | metric | Kotlin | Rust | Comparison |\n|---|---|---:|---:|---:|")
for mode in ['word', 'trigram']:
    for w in [False, True]:
        k, r = g[(mode, w, 'kotlin')], g[(mode, w, 'rust')]
        cfg = f"{mode}, watcher {'on' if w else 'off'}"
        def line(name, f, unit, lower_better=True, fmt="{:.0f}"):
            kv, rv = m(k, f), m(r, f)
            ratio = kv / rv if lower_better else rv / kv
            winner = "Rust" if ratio >= 1 else "Kotlin"
            advantage = ratio if ratio >= 1 else 1 / ratio
            description = "faster" if unit in ("ms", "s") else ("less" if lower_better else "more")
            print(f"| {cfg} | {name} | {fmt.format(kv)} {unit} | {fmt.format(rv)} {unit} | {winner} **{advantage:.1f}x** {description} |")
        line("time to IndexInSync", lambda x: x['index_ms'], "ms")
        line("process peak RSS", lambda x: x['process_maxrss_kb'] / 1024, "MiB")
        line("concurrent throughput (8 clients)", lambda x: x['concurrent_qps'], "q/s", lower_better=False, fmt="{:.1f}")
        line("process wall (JVM/binary start → exit)", lambda x: x['process_wall_s'], "s", fmt="{:.1f}")
print()
for mode in ['word', 'trigram']:
    k, r = g[(mode, False, 'kotlin')], g[(mode, False, 'rust')]
    print(f"\n**Per-query latency, {mode} index (median of 5 warm runs, median over 3 processes)**\n")
    print("| query | matches | Kotlin cold ms | Rust cold ms | Kotlin warm ms | Rust warm ms | Kotlin/Rust warm latency |\n|---|---:|---:|---:|---:|---:|---:|")
    for i, q in enumerate(k[0]['queries']):
        assert all((x['queries'][i]['count'], x['queries'][i]['checksum']) == (q['count'], q['checksum']) for x in k + r), q['q']
        kc, rc = m(k, lambda x: x['queries'][i]['cold_ms']), m(r, lambda x: x['queries'][i]['cold_ms'])
        kw, rw = m(k, lambda x: x['queries'][i]['median_ms']), m(r, lambda x: x['queries'][i]['median_ms'])
        print(f"| `{q['q']}` | {q['count']} | {kc:.1f} | {rc:.1f} | {kw:.1f} | {rw:.1f} | {kw / max(rw, 0.01):.1f}x |")
    tk = sum(m(k, lambda x: x['queries'][i]['median_ms']) for i in range(len(k[0]['queries'])))
    tr = sum(m(r, lambda x: x['queries'][i]['median_ms']) for i in range(len(r[0]['queries'])))
    print(f"| **all 12 queries** | | | | **{tk:.0f}** | **{tr:.0f}** | **{tk/tr:.1f}x** |")
k = g[('trigram', False, 'kotlin')]
print(f"\nKotlin JVM heap after full GC once indexed (live data): word {m(g[('word',False,'kotlin')], lambda x: x['heap_after_gc_kb'])/1024:.0f} MiB, trigram {m(k, lambda x: x['heap_after_gc_kb'])/1024:.0f} MiB")
print("All query result counts + checksums identical across implementations and runs: OK")
