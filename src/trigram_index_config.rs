use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::config_common::{
    CommonConfig, files_by_tokens, impl_config_builder, impl_config_common_methods,
};
use crate::model::{FileAddress, Index, IndexConfig};

/// Index of lowercase character trigrams. Supports arbitrary substring queries.
pub struct TrigramIndexConfig {
    common: CommonConfig,
}

/// Trigram index config. The watcher (incremental index updates) is enabled by default.
pub fn trigram_index_config() -> TrigramIndexConfig {
    TrigramIndexConfig {
        common: CommonConfig::new(true),
    }
}

impl_config_builder!(TrigramIndexConfig);

fn tokenize(line: &str) -> Vec<String> {
    let chars: Vec<char> = line.to_lowercase().chars().collect();
    if chars.len() < 3 {
        let mut token: String = chars.into_iter().collect();
        while token.chars().count() < 3 {
            token.push(' ');
        }
        return vec![token];
    }
    chars.windows(3).map(|w| w.iter().collect()).collect()
}

#[async_trait]
impl IndexConfig for TrigramIndexConfig {
    impl_config_common_methods!();

    async fn handle_watcher_error(&self, e: &anyhow::Error) {
        self.common.watcher_error(e)
    }

    async fn handle_initial_file_sync_error(&self, e: &anyhow::Error) {
        self.common.initial_file_sync_error(e)
    }

    fn tokenize(&self, line: &str) -> Vec<String> {
        tokenize(line)
    }

    fn find(&self, index: Arc<dyn Index>, query: String) -> BoxStream<'static, FileAddress> {
        async_stream::stream! {
            match query.chars().count() {
                0 => {
                    // everything matches
                    let tokens = index.find_tokens_matching_predicate(Box::new(|_| true)).await;
                    for await fa in files_by_tokens(index, tokens) {
                        yield fa;
                    }
                }
                1 | 2 => {
                    // files with trigrams containing the query match
                    let q = query.to_lowercase();
                    let tokens = index.find_tokens_matching_predicate(Box::new(move |t| t.contains(&q))).await;
                    for await fa in files_by_tokens(index, tokens) {
                        yield fa;
                    }
                }
                _ => {
                    let tokens = tokenize(&query);
                    let mut files: HashSet<FileAddress> =
                        index.find_files_by_token(&tokens[0]).await.into_iter().collect();
                    for token in &tokens[1..] {
                        if files.is_empty() {
                            break;
                        }
                        let new_files: HashSet<FileAddress> =
                            index.find_files_by_token(token).await.into_iter().collect();
                        files.retain(|f| new_files.contains(f));
                    }
                    for fa in files {
                        yield fa;
                    }
                }
            }
        }
        .boxed()
    }

    fn matches(&self, line: &str, query: &str) -> bool {
        line.contains(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{FakeIndex, fa, set};

    #[test]
    fn should_tokenize_strings() {
        let mut tokens = tokenize("poUPa loupa");
        tokens.sort();
        let mut expected = vec![
            "pou", "oup", "upa", "pa ", "a l", " lo", "lou", "oup", "upa",
        ];
        expected.sort();
        assert_eq!(tokens, expected);
    }

    #[test]
    fn should_tokenize_small_strings() {
        assert_eq!(tokenize("ab "), vec!["ab "]);
        assert_eq!(tokenize("a  "), vec!["a  "]);
        assert_eq!(tokenize("   "), vec!["   "]);
        assert_eq!(tokenize("ab"), vec!["ab "]);
        assert_eq!(tokenize(""), vec!["   "]);
    }

    #[tokio::test]
    async fn should_be_able_to_search() {
        let index = FakeIndex::from_files(&[
            ("pou", &["res1", "res2", "res3", "res4", "res6"]), // no 5
            ("oup", &["res1", "res2", "res4", "res5", "res6"]), // no 3
            ("upa", &["res1", "res3", "res5", "res6"]),         // no 2 4
        ]);
        let found = trigram_index_config()
            .find(Arc::new(index), "poUPa".into())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(set(found), set([fa("res1"), fa("res6")]));
    }

    #[tokio::test]
    async fn short_queries_are_case_insensitive_on_candidates() {
        let index = FakeIndex::from_documents(&[("1", &["ab "]), ("2", &["xyz"])]);
        let found = trigram_index_config()
            .find(Arc::new(index), "Ab".into())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(set(found), set([fa("1")]));
    }
}
