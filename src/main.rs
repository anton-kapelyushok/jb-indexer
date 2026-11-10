use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use futures::StreamExt;
use jb_indexer::*;
use tokio::sync::mpsc;

const HELP: &str =
    "Available commands: find <query>, stop, enable-logging, status, compact, memory, error, help";

fn usage() -> ! {
    eprintln!("Usage: jb-indexer [--trigram] [--no-watcher] [DIR]");
    std::process::exit(2)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut dir = ".".to_string();
    let mut trigram = false;
    let mut watcher = true;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--trigram" => trigram = true,
            "--no-watcher" => watcher = false,
            "-h" | "--help" => usage(),
            s if s.starts_with('-') => usage(),
            s => dir = s.to_string(),
        }
    }

    let stdin = start_stdin_reader();

    let cfg: Arc<dyn IndexConfig> = if trigram {
        Arc::new(trigram_index_config().with_watcher(watcher))
    } else {
        Arc::new(word_index_config().with_watcher(watcher))
    };

    let index = launch_index(dir, cfg.clone());
    let search_engine = launch_search_engine(cfg.clone(), index.clone());
    let status_display = tokio::spawn(display_status(search_engine.clone()));

    let result = run_cmd_handler(stdin, &search_engine, &index, &*cfg).await;

    search_engine.cancel_all();
    status_display.abort();
    result
}

async fn display_status(search_engine: SearchEngine) {
    let mut was_in_sync = false;
    let mut updates = search_engine.index_status_updates();
    while let Some(update) = updates.next().await {
        match update {
            IndexStatusUpdate::Initial { .. } => println!("Index start!"),
            IndexStatusUpdate::Initializing { .. } => println!("Initializing index!"),
            IndexStatusUpdate::IndexInSync { ts, status } => {
                if !was_in_sync {
                    was_in_sync = true;
                    println!(
                        "Initial sync completed after {}ms!",
                        ts - status.last_restart_time
                    );
                } else {
                    println!("Index in sync again!");
                }
            }
            IndexStatusUpdate::IndexOutOfSync { .. } => println!("Index out of sync!"),
            IndexStatusUpdate::AllFilesDiscovered { ts, status } => {
                println!(
                    "All files discovered after {}ms!",
                    ts - status.last_restart_time
                )
            }
            IndexStatusUpdate::Failed { reason, .. } => {
                println!("Index failed with error {reason:?}")
            }
            IndexStatusUpdate::WatcherStarted { ts, status } => {
                println!("Watcher started after {}ms!", ts - status.last_restart_time)
            }
            IndexStatusUpdate::ReinitializingBecauseFileSyncFailed { reason, .. } => {
                was_in_sync = false;
                println!("File sync failed with {reason:?} - reinitializing it");
            }
        }
        println!();
    }
}

/// Reads stdin lines on a dedicated thread.
fn start_stdin_reader() -> mpsc::Receiver<String> {
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

/// Resident memory of the process, in MB (Linux only).
fn memory_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1000)
}

fn print_memory() {
    match memory_mb() {
        Some(mb) => println!("{mb} MB"),
        None => println!("memory usage is not available on this platform"),
    }
}

async fn run_cmd_handler(
    mut input: mpsc::Receiver<String>,
    search_engine: &SearchEngine,
    index: &IndexHandle,
    cfg: &dyn IndexConfig,
) -> anyhow::Result<()> {
    println!("{HELP}");
    println!();

    while let Some(prompt) = input.recv().await {
        let start = Instant::now();
        match prompt.as_str() {
            "stop" => break,
            "help" => println!("{HELP}"),
            "enable-logging" => cfg.enable_logging().store(true, Ordering::Relaxed),
            "" => cfg.enable_logging().store(false, Ordering::Relaxed),
            "status" => {
                let state = search_engine.index_state().await;
                println!("{state:?}");
                println!();
                println!("{}", state.pretty_print());
            }
            "memory" | "gc" => print_memory(),
            "compact" => {
                index.compact().await;
                print_memory();
            }
            "error" => anyhow::bail!("error"),
            p if p.starts_with("find ") => {
                let query = &p["find ".len()..];
                tokio::select! {
                    _ = find(search_engine, query) => {}
                    _ = input.recv() => println!("Search interrupted!"),
                }
            }
            _ => println!("Unrecognized command!"),
        }

        println!("====== {}ms", start.elapsed().as_millis());
        println!();
    }
    Ok(())
}

async fn find(search_engine: &SearchEngine, query: &str) {
    let initial_state = search_engine.index_state().await;
    let is_in_sync_before_search = initial_state.is_in_sync();
    if !is_in_sync_before_search {
        println!("Directory is not fully indexed yet, results might be incomplete or outdated");
        println!();
    }

    let mut results = search_engine.find(query).take(20);
    while let Some(IndexSearchResult {
        path,
        line_no,
        line,
    }) = results.next().await
    {
        println!("{path}:{line_no}");
        if line.chars().count() > 100 {
            println!("{}...", line.chars().take(101).collect::<String>());
        } else {
            println!("{line}");
        }
        println!();
    }

    if is_in_sync_before_search && initial_state != search_engine.index_state().await {
        println!(
            "Directory content has changed during search, results might be incomplete or outdated"
        );
        println!();
    }
}
