use std::path::Path;
use std::sync::Arc;

use async_channel::Receiver;

use super::model::SearchInFileRequest;
use super::utils::{debug_log, read_lines};
use crate::model::{IndexConfig, IndexSearchResult};

/// Worker that scans candidate files for exact matches. Several of these share one MPMC request channel.
pub async fn search_in_file(cfg: Arc<dyn IndexConfig>, requests: Receiver<SearchInFileRequest>) -> anyhow::Result<()> {
    while let Ok(request) = requests.recv().await {
        let cfg2 = cfg.clone();
        let fa = request.fa.clone();
        let query = request.query.clone();
        let results = tokio::task::spawn_blocking(move || match read_lines(Path::new(fa.path())) {
            Ok(lines) => lines
                .into_iter()
                .enumerate()
                .filter(|(_, line)| cfg2.matches(line, &query))
                .map(|(idx, line)| IndexSearchResult { path: fa.path().to_owned(), line_no: idx + 1, line })
                .collect(),
            Err(e) => {
                debug_log(&*cfg2, || format!("Failed to read {}: {e}", fa.path()));
                Vec::new()
            }
        })
        .await?;

        let _ = request.result.send(results);
    }
    Ok(())
}
