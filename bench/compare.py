import json, sys
r=json.load(open(sys.argv[1])); k=json.load(open(sys.argv[2]))
for key in ['index_ms','indexed_files','known_tokens','peak_rss_kb','concurrent_qps']: print(f"{key:20} rust={r[key]:>12} kotlin={k[key]:>12}")
for a,b in zip(r['queries'],k['queries']):
    ok = (a['count'],a['checksum'])==(b['count'],b['checksum'])
    print(f"{a['q'][:36]:38} cnt r={a['count']:>6} k={b['count']:>6} {'OK' if ok else 'MISMATCH'}  med r={a['median_ms']:>8} k={b['median_ms']:>8}")
