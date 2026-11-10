use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use jb_indexer::*;

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Reference implementation: scan every file.
fn dumb_search(dir: &Path, query: &str) -> HashSet<IndexSearchResult> {
    let mut result = HashSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        if path.is_dir() {
            stack.extend(std::fs::read_dir(&path).unwrap().map(|e| e.unwrap().path()));
            continue;
        }
        let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
        let canonical = std::fs::canonicalize(&path)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        for (idx, line) in text.lines().enumerate() {
            if line.contains(query) {
                result.insert(IndexSearchResult {
                    path: canonical.clone(),
                    line_no: idx + 1,
                    line: line.to_string(),
                });
            }
        }
    }
    result
}

async fn check_query(
    dir: &Path,
    search_engine: &SearchEngine,
    query: &str,
) -> HashSet<IndexSearchResult> {
    let expected = dumb_search(dir, query);
    let actual: HashSet<_> = search_engine.find(query).collect().await;
    assert_eq!(actual, expected, "query: {query}");
    actual
}

async fn wait_for_in_sync_after(search_engine: &SearchEngine, clock: i64) {
    let mut updates = search_engine.index_status_updates();
    while let Some(update) = updates.next().await {
        if let IndexStatusUpdate::IndexInSync { status, .. } = update
            && status.clock > clock {
                return;
            }
    }
    panic!("status stream ended");
}

/// Applies a file system change and waits until the index has caught up with it.
async fn capture_update(search_engine: &SearchEngine, change: impl FnOnce()) {
    let state = search_engine.index_state().await;
    assert!(state.is_in_sync());
    change();
    wait_for_in_sync_after(search_engine, state.clock).await;
    // a single write can produce several file system events, let them all arrive
    tokio::time::sleep(Duration::from_millis(200)).await;
    while !search_engine.index_state().await.is_in_sync() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_in_sync(search_engine: &SearchEngine) {
    let mut updates = search_engine.index_status_updates();
    while let Some(update) = updates.next().await {
        if matches!(update, IndexStatusUpdate::IndexInSync { .. }) {
            return;
        }
    }
    panic!("status stream ended");
}

async fn run_e2e(cfg: Arc<dyn IndexConfig>, check_empty_query: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("dirToWatch");
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata"),
        &dir,
    );

    let index = launch_index(&dir, cfg.clone());
    let search_engine = launch_search_engine(cfg, index);
    wait_in_sync(&search_engine).await;

    check_query(&dir, &search_engine, "poupa").await;
    check_query(&dir, &search_engine, "path").await;
    check_query(&dir, &search_engine, "date = { statu").await;
    check_query(&dir, &search_engine, "async(CoroutineName").await;
    if check_empty_query {
        // word index can't find files without any words for an empty query
        check_query(&dir, &search_engine, "").await;
    }
    check_query(&dir, &search_engine, "a").await;
    check_query(&dir, &search_engine, "{ }").await;
    check_query(&dir, &search_engine, "Конституци").await;

    let q = "Каждому гарантируется свобода мысли и слова";
    assert!(!check_query(&dir, &search_engine, q).await.is_empty());
    capture_update(&search_engine, || {
        std::fs::remove_file(dir.join("статья 29.txt")).unwrap()
    })
    .await;
    assert!(check_query(&dir, &search_engine, q).await.is_empty());

    let q = "Граждане Российской Федерации имеют право собираться мирно без оружия";
    assert!(!check_query(&dir, &search_engine, q).await.is_empty());
    capture_update(&search_engine, || {
        std::fs::write(dir.join("статья 31.txt"), "Колобок повесился").unwrap()
    })
    .await;
    assert!(check_query(&dir, &search_engine, q).await.is_empty());
    assert!(!check_query(&dir, &search_engine, "олобо").await.is_empty());

    // new directories are picked up
    capture_update(&search_engine, || {
        let tmp_new = tmp.path().join("newDir");
        std::fs::create_dir(&tmp_new).unwrap();
        std::fs::write(tmp_new.join("new.txt"), "brand new content").unwrap();
        std::fs::rename(&tmp_new, dir.join("newDir")).unwrap();
    })
    .await;
    assert!(
        !check_query(&dir, &search_engine, "brand new")
            .await
            .is_empty()
    );

    search_engine.cancel_all();
    assert!(matches!(
        search_engine.index().join().await,
        Completion::Cancelled
    ));
    assert!(
        search_engine
            .find("poupa")
            .collect::<Vec<_>>()
            .await
            .is_empty()
    );
    assert!(search_engine.index_state().await.is_broken);
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_trigram() {
    tokio::time::timeout(
        Duration::from_secs(20),
        run_e2e(Arc::new(trigram_index_config()), true),
    )
    .await
    .expect("timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_word() {
    tokio::time::timeout(
        Duration::from_secs(20),
        run_e2e(Arc::new(word_index_config()), false),
    )
    .await
    .expect("timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn index_without_watcher_and_search_engine_cancellation() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("a.txt"), "hello world\nsecond line").unwrap();
    let cfg: Arc<dyn IndexConfig> = Arc::new(trigram_index_config().with_watcher(false));
    let index = launch_index(tmp.path(), cfg.clone());
    let search_engine = launch_search_engine(cfg, index.clone());
    tokio::time::timeout(Duration::from_secs(5), wait_in_sync(&search_engine))
        .await
        .unwrap();

    let results: Vec<_> = search_engine.find("world").collect().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].line_no, 1);
    assert_eq!(results[0].line, "hello world");

    // cancelling the search engine does not cancel the index
    search_engine.cancel();
    assert!(matches!(search_engine.join().await, Completion::Cancelled));
    assert!(
        search_engine
            .find("world")
            .collect::<Vec<_>>()
            .await
            .is_empty()
    );
    assert!(!index.is_completed());
    assert_eq!(index.state().await.indexed_files, 1);

    // cancelling the index terminates a search engine
    let cfg: Arc<dyn IndexConfig> = Arc::new(trigram_index_config().with_watcher(false));
    let search_engine = launch_search_engine(cfg, index.clone());
    index.cancel();
    assert!(matches!(search_engine.join().await, Completion::Failed(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn index_fails_on_missing_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let errors = Arc::new(std::sync::Mutex::new(vec![]));
    let errors2 = errors.clone();
    let cfg: Arc<dyn IndexConfig> = Arc::new(
        word_index_config()
            .with_watcher(false)
            .on_initial_file_sync_error(move |e| errors2.lock().unwrap().push(e.to_string())),
    );
    let index = launch_index(tmp.path().join("missing"), cfg);
    let completion = tokio::time::timeout(Duration::from_secs(5), index.join())
        .await
        .unwrap();
    assert!(matches!(completion, Completion::Failed(_)));
    assert_eq!(errors.lock().unwrap().len(), 1);

    let last = index.status_updates().collect::<Vec<_>>().await;
    assert!(matches!(
        last.last(),
        Some(IndexStatusUpdate::Failed { .. })
    ));
}
