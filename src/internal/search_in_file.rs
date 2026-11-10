use std::path::Path;
use std::sync::Arc;

use crate::internal::model::SearchInFileRequest;
use crate::internal::utils::{debug_log, lines, read_text};
use crate::model::{IndexConfig, IndexSearchResult};

/// Worker that finds exact matches of a query in candidate files.
pub(crate) async fn search_in_file(
    cfg: Arc<dyn IndexConfig>,
    requests: flume::Receiver<SearchInFileRequest>,
) -> anyhow::Result<()> {
    while let Ok(request) = requests.recv_async().await {
        let cfg = cfg.clone();
        let fa = request.fa.clone();
        let query = request.query.clone();
        let results = tokio::task::spawn_blocking(move || search(&*cfg, fa.path(), &query)).await?;
        // requester may have gone away
        let _ = request.result.send(results);
    }
    Ok(())
}

fn search(cfg: &dyn IndexConfig, path: &str, query: &str) -> Vec<IndexSearchResult> {
    match read_text(Path::new(path)) {
        Ok(text) => lines(&text)
            .enumerate()
            .filter(|(_, line)| cfg.matches(line, query))
            .map(|(idx, line)| IndexSearchResult {
                path: path.to_string(),
                line_no: idx + 1,
                line: line.to_string(),
            })
            .collect(),
        Err(e) => {
            debug_log(cfg, || format!("Failed to read {path}: {e}"));
            vec![]
        }
    }
}
