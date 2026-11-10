//! Port of `LaunchSearchEngineKtTest.e2e`: index the shared Kotlin test data with a live watcher,
//! compare every query to a brute-force scan, then mutate files and check the index follows.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use indexer::internal::utils::{canonical_path, read_lines};
use indexer::*;
use walkdir::WalkDir;

fn copy_dir(from: &Path, to: &Path) {
    for e in WalkDir::new(from) {
        let e = e.unwrap();
        let target = to.join(e.path().strip_prefix(from).unwrap());
        if e.file_type().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
        } else {
            std::fs::copy(e.path(), &target).unwrap();
        }
    }
}

fn dumb_search(dir: &Path, query: &str) -> BTreeSet<IndexSearchResult> {
    let mut out = BTreeSet::new();
    for e in WalkDir::new(dir).into_iter().flatten().filter(|e| e.file_type().is_file()) {
        for (i, line) in read_lines(e.path()).unwrap_or_default().into_iter().enumerate() {
            if line.contains(query) {
                out.insert(IndexSearchResult { path: canonical_path(e.path()), line_no: i + 1, line });
            }
        }
    }
    out
}

async fn check_query(dir: &Path, se: &SearchEngine, query: &str) -> BTreeSet<IndexSearchResult> {
    let expected = dumb_search(dir, query);
    let actual: BTreeSet<_> = se.find(query).collect().await;
    assert_eq!(actual, expected, "query {query:?}");
    actual
}

/// Runs `f`, then waits until the index has processed the resulting change and is in sync again.
async fn capture_update(se: &SearchEngine, f: impl FnOnce()) {
    let before = se.index_state().await.clock;
    let mut updates = se.index_status_updates();
    f();
    let r = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(u) = updates.next().await {
            if u.is_in_sync() && u.status().unwrap().clock > before {
                break;
            }
        }
        // a single write can produce more than one watcher event; let them settle
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if se.index_state().await.is_in_sync() {
                break;
            }
        }
    })
    .await;
    if r.is_err() {
        panic!("index did not catch up: before={before} now={:?}", se.index_state().await);
    }
}

/// `full_text`: the word config only knows ASCII `\W`-separated tokens (same as the Kotlin version, whose
/// e2e only covers trigrams), so queries without such tokens can't find files that have none of them.
async fn e2e(cfg: Arc<dyn IndexConfig>, full_text: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("dirToWatch");
    copy_dir(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../lib/src/test/resources/testdata"), &dir);

    cfg.enable_logging().store(std::env::var("E2E_LOG").is_ok(), std::sync::atomic::Ordering::Relaxed);
    let index = launch_index(&dir, cfg.clone());
    let se = launch_search_engine(cfg, index);
    let mut updates = se.index_status_updates();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(u) = updates.next().await {
            if u.is_in_sync() {
                break;
            }
        }
    })
    .await
    .expect("initial sync");

    for q in ["poupa", "path", "date = { statu", "async(CoroutineName", "fun ", "a", "val tokens = "] {
        check_query(&dir, &se, q).await;
    }
    if full_text {
        check_query(&dir, &se, "").await;
    }

    if full_text {
        let q = "Каждому гарантируется свобода мысли и слова";
        assert!(!check_query(&dir, &se, q).await.is_empty());
        capture_update(&se, || std::fs::remove_file(dir.join("статья 29.txt")).unwrap()).await;
        assert!(check_query(&dir, &se, q).await.is_empty());

        let q = "Граждане Российской Федерации имеют право собираться мирно без оружия";
        assert!(!check_query(&dir, &se, q).await.is_empty());
        capture_update(&se, || std::fs::write(dir.join("статья 31.txt"), "Колобок повесился").unwrap()).await;
        assert!(check_query(&dir, &se, q).await.is_empty());
        assert!(!check_query(&dir, &se, "олобо").await.is_empty());
    } else {
        let q = "fun CoroutineScope.launchIndex(";
        assert!(!check_query(&dir, &se, q).await.is_empty());
        capture_update(&se, || std::fs::remove_file(dir.join("kotlin/indexer/core/launchIndex.kt.txt")).unwrap()).await;
        assert!(check_query(&dir, &se, q).await.is_empty());

        let q = "class InvertedIndex(";
        assert!(!check_query(&dir, &se, q).await.is_empty());
        let f = dir.join("kotlin/indexer/core/internal/InvertedIndex.kt.txt");
        capture_update(&se, || std::fs::write(&f, "kolobok povesilsya").unwrap()).await;
        assert!(check_query(&dir, &se, q).await.is_empty());
        assert!(!check_query(&dir, &se, "olobo").await.is_empty());
    }

    // new directory with a file in it
    capture_update(&se, || {
        std::fs::create_dir(dir.join("fresh")).unwrap();
        std::fs::write(dir.join("fresh/new.txt"), "brand new volobuev").unwrap();
    })
    .await;
    assert!(!check_query(&dir, &se, "new volobuev").await.is_empty());

    se.cancel_all();
    assert!(matches!(se.join().await, Outcome::Cancelled | Outcome::Failed(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_trigram() {
    e2e(Arc::new(trigram_index_config(true)), true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_word() {
    e2e(Arc::new(word_index_config(true)), false).await;
}

#[tokio::test]
async fn cancelling_index_cancels_search_engine_and_ends_status_stream() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("a.txt"), "hello").unwrap();
    let cfg: Arc<dyn IndexConfig> = Arc::new(word_index_config(false));
    let index = launch_index(tmp.path(), cfg.clone());
    let se = launch_search_engine(cfg, index.clone());
    let mut updates = se.index_status_updates();

    index.cancel();
    assert!(matches!(index.join().await, Outcome::Cancelled));
    assert!(matches!(se.join().await, Outcome::Failed(_)));
    // after Failed the stream terminates
    let last = tokio::time::timeout(Duration::from_secs(5), async {
        let mut last = None;
        while let Some(u) = updates.next().await {
            last = Some(u);
        }
        last
    })
    .await
    .unwrap();
    assert!(last.unwrap().is_failed());
    // queries against a dead index return the "broken"/empty defaults instead of hanging
    assert!(index.state().await.is_broken);
    assert_eq!(se.find("hello").count().await, 0);
}

#[tokio::test]
async fn missing_directory_fails_index() {
    let cfg: Arc<dyn IndexConfig> = Arc::new(word_index_config(false));
    let index = launch_index("/definitely/not/here", cfg);
    let outcome = tokio::time::timeout(Duration::from_secs(5), index.join()).await.unwrap();
    assert!(matches!(outcome, Outcome::Failed(_)), "{outcome:?}");
}
