//! Benchmark harness; `app/src/main/kotlin/Bench.kt` is the identical Kotlin harness.
//!
//! Usage: bench DIR word|trigram WATCHER(true|false) QUERIES_FILE ITERATIONS CONCURRENCY
//!
//! Measures: time to the first IndexInSync, process RSS, per-query cold/median latency (plus result count
//! and a checksum so both implementations can be checked for identical results), concurrent query throughput.

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use indexer::internal::utils::rss_kb;
use indexer::*;

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

async fn run_query(se: &SearchEngine, q: &str) -> (usize, u64) {
    let mut s = se.find(q);
    let (mut count, mut checksum) = (0usize, 0u64);
    while let Some(r) = s.next().await {
        count += 1;
        checksum += r.line_no as u64 + r.path.len() as u64;
    }
    (count, checksum)
}

fn main() -> anyhow::Result<()> {
    let threads = std::env::var("BENCH_THREADS").ok().and_then(|s| s.parse().ok());
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = threads {
        rt.worker_threads(n);
    }
    rt.enable_all().build()?.block_on(bench())
}

async fn bench() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let (dir, mode, watcher) = (&a[0], a[1].as_str(), a[2] == "true");
    let queries: Vec<String> =
        std::fs::read_to_string(&a[3])?.lines().filter(|l| !l.is_empty()).map(str::to_owned).collect();
    let iters: usize = a[4].parse()?;
    let conc: usize = a[5].parse()?;
    let cfg: Arc<dyn IndexConfig> = match mode {
        "trigram" => Arc::new(trigram_index_config(watcher)),
        _ => Arc::new(word_index_config(watcher)),
    };

    let t0 = Instant::now();
    let index = launch_index(dir, cfg.clone());
    let se = launch_search_engine(cfg.clone(), index.clone());
    let mut updates = se.index_status_updates();
    let mut all_discovered_ms = -1.0;
    while let Some(u) = updates.next().await {
        match u {
            IndexStatusUpdate::AllFilesDiscovered { .. } => all_discovered_ms = ms(t0),
            IndexStatusUpdate::IndexInSync { .. } => break,
            IndexStatusUpdate::Failed { reason, .. } => anyhow::bail!("index failed: {reason:#}"),
            _ => {}
        }
    }
    let index_ms = ms(t0);
    let state = se.index_state().await;
    let (rss_after_index, _) = rss_kb();

    let mut qjson = Vec::new();
    for q in &queries {
        let t = Instant::now();
        let (count, checksum) = run_query(&se, q).await;
        let cold = ms(t);
        let mut times = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            run_query(&se, q).await;
            times.push(ms(t));
        }
        times.sort_by(f64::total_cmp);
        let median = times.get(times.len() / 2).copied().unwrap_or(cold);
        qjson.push(format!(
            "{{\"q\":{},\"count\":{count},\"checksum\":{checksum},\"cold_ms\":{cold:.2},\"median_ms\":{median:.2}}}",
            json_str(q)
        ));
    }

    let t = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..conc {
        let (se, queries) = (se.clone(), queries.clone());
        handles.push(tokio::spawn(async move {
            for q in &queries {
                run_query(&se, q).await;
            }
        }));
    }
    for h in handles {
        h.await?;
    }
    let conc_ms = ms(t);
    let (rss_end, hwm) = rss_kb();

    println!(
        "{{\"impl\":\"rust\",\"mode\":\"{mode}\",\"watcher\":{watcher},\"index_ms\":{index_ms:.1},\
\"all_files_discovered_ms\":{all_discovered_ms:.1},\"indexed_files\":{},\"known_tokens\":{},\
\"rss_after_index_kb\":{rss_after_index},\"rss_end_kb\":{rss_end},\"peak_rss_kb\":{hwm},\
\"concurrent_ms\":{conc_ms:.1},\"concurrent_qps\":{:.1},\"queries\":[{}]}}",
        state.indexed_files,
        state.known_tokens,
        (conc * queries.len()) as f64 / (conc_ms / 1000.0),
        qjson.join(",")
    );
    se.cancel_all();
    Ok(())
}
