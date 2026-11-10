//! Port of `app/src/main/kotlin/Main.kt`: interactive CLI over the index.
//!
//! Usage: indexer-app [DIR] [word|trigram] [watcher=true|false]

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use indexer::internal::utils::rss_kb;
use indexer::*;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = args.first().cloned().unwrap_or_else(|| ".".into());
    let enable_watcher = args.get(2).map(|s| s != "false").unwrap_or(true);
    let cfg: Arc<dyn IndexConfig> = match args.get(1).map(String::as_str) {
        Some("trigram") => Arc::new(trigram_index_config(enable_watcher)),
        _ => Arc::new(word_index_config(enable_watcher)),
    };

    let stdin = start_std_reader_daemon();

    let index = launch_index(&dir, cfg.clone());
    let search_engine = launch_search_engine(cfg.clone(), index.clone());
    launch_status_display(&search_engine);

    let r = run_cmd_handler(stdin, &search_engine, &index, &*cfg).await;
    search_engine.cancel_all();
    r
}

fn launch_status_display(search_engine: &SearchEngine) {
    let mut updates = search_engine.index_status_updates();
    tokio::spawn(async move {
        let mut was_in_sync = false;
        while let Some(update) = updates.next().await {
            use IndexStatusUpdate::*;
            match &update {
                Initial { .. } => println!("Index start!"),
                Initializing { .. } => println!("Initializing index!"),
                IndexInSync { ts, status } => {
                    if !was_in_sync {
                        was_in_sync = true;
                        println!("Initial sync completed after {}ms!", ts - status.last_restart_time);
                    } else {
                        println!("Index in sync again!");
                    }
                }
                IndexOutOfSync { .. } => println!("Index out of sync!"),
                AllFilesDiscovered { ts, status } => {
                    println!("All files discovered after {}ms!", ts - status.last_restart_time)
                }
                Failed { reason, .. } => println!("Index failed with exception {reason:#}"),
                WatcherStarted { ts, status } => {
                    println!("Watcher started after after {}ms!", ts - status.last_restart_time)
                }
                ReinitializingBecauseFileSyncFailed { reason, .. } => {
                    was_in_sync = false;
                    println!("File sync failed with {reason:#} - reinitializing it");
                }
            }
            println!();
        }
    });
}

fn start_std_reader_daemon() -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel(1);
    std::thread::spawn(move || {
        for line in std::io::stdin().lines() {
            let Ok(line) = line else { break };
            if tx.blocking_send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn memory() -> String {
    let (rss, _) = rss_kb();
    format!("{} MB", rss / 1000)
}

async fn run_cmd_handler(
    mut input: mpsc::Receiver<String>,
    search_engine: &SearchEngine,
    index: &Index,
    cfg: &dyn IndexConfig,
) -> anyhow::Result<()> {
    let help = "Available commands: find stop enable-logging status gc memory compact error help";
    println!("{help}\n");

    while let Some(prompt) = input.recv().await {
        let start = Instant::now();
        match prompt.as_str() {
            "stop" => break,
            "help" => println!("{help}"),
            "enable-logging" => cfg.enable_logging().store(true, Ordering::Relaxed),
            "" => cfg.enable_logging().store(false, Ordering::Relaxed),
            "status" => {
                let state = search_engine.index_state().await;
                println!("{state:?}\n\n{}", state.pretty_print());
            }
            "gc" => println!("no GC in Rust; RSS {}", memory()),
            "memory" => println!("{}", memory()),
            "compact" => index.compact().await,
            "error" => anyhow::bail!("error"),
            p if p.starts_with("find ") => {
                let query = p["find ".len()..].to_owned();
                let search = async {
                    let initial = search_engine.index_state().await;
                    let in_sync_before = initial.is_in_sync();
                    if !in_sync_before {
                        println!("Directory is not fully indexed yet, results might be incomplete or outdated\n");
                    }
                    let mut results = search_engine.find(&query).take(20);
                    while let Some(r) = results.next().await {
                        println!("{}:{}", r.path, r.line_no);
                        let line = if r.line.chars().count() > 100 {
                            format!("{}...", r.line.chars().take(101).collect::<String>())
                        } else {
                            r.line
                        };
                        println!("{line}\n");
                    }
                    if in_sync_before && initial != search_engine.index_state().await {
                        println!(
                            "Directory content has changed during search, results might be incomplete or outdated\n"
                        );
                    }
                };
                tokio::select! {
                    _ = search => {}
                    _ = input.recv() => println!("Search interrupted!"),
                }
            }
            _ => println!("Unrecognized command!"),
        }
        println!("====== {}ms\n", start.elapsed().as_millis());
    }
    Ok(())
}
