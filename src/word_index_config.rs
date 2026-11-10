use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::config_common::{
    CommonConfig, files_by_tokens, impl_config_builder, impl_config_common_methods,
};
use crate::model::{FileAddress, Index, IndexConfig};

/// Index of lowercase words (sequences of letters, digits and underscores).
pub struct WordIndexConfig {
    common: CommonConfig,
}

/// Word index config. The watcher (incremental index updates) is enabled by default.
pub fn word_index_config() -> WordIndexConfig {
    WordIndexConfig {
        common: CommonConfig::new(true),
    }
}

impl_config_builder!(WordIndexConfig);

/// Anything but (Unicode) letters, digits and underscore.
fn is_separator(c: char) -> bool {
    !(c.is_alphanumeric() || c == '_')
}

fn tokenize(line: &str) -> Vec<String> {
    line.split(is_separator)
        .map(|it| it.trim().to_lowercase())
        .filter(|it| !it.is_empty())
        .collect()
}

async fn file_set(index: &dyn Index, token: &str) -> HashSet<FileAddress> {
    index.find_files_by_token(token).await.into_iter().collect()
}

#[async_trait]
impl IndexConfig for WordIndexConfig {
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
            let search_tokens = tokenize(&query);
            // whether the query starts/ends on a word boundary (so the first/last token is a full word)
            let start_token_is_full = query.chars().next().is_some_and(is_separator);
            let end_token_is_full = query.chars().last().is_some_and(is_separator);

            match search_tokens.len() {
                0 => {
                    // index won't help us here, emit everything we have
                    let tokens = index.find_tokens_matching_predicate(Box::new(|_| true)).await;
                    for await fa in files_by_tokens(index, tokens) {
                        yield fa;
                    }
                }
                1 => {
                    let search_token = search_tokens[0].to_lowercase();
                    let tokens = match (start_token_is_full, end_token_is_full) {
                        (true, true) => vec![search_token],
                        (false, false) => {
                            index.find_tokens_matching_predicate(Box::new(move |t| t.contains(&search_token))).await
                        }
                        (false, true) => {
                            index.find_tokens_matching_predicate(Box::new(move |t| t.ends_with(&search_token))).await
                        }
                        (true, false) => {
                            index.find_tokens_matching_predicate(Box::new(move |t| t.starts_with(&search_token))).await
                        }
                    };
                    for await fa in files_by_tokens(index, tokens) {
                        yield fa;
                    }
                }
                2 => {
                    let start_token = search_tokens[0].clone();
                    let end_token = search_tokens[1].clone();

                    let start_full_match = file_set(&*index, &start_token).await;
                    let end_full_match = file_set(&*index, &end_token).await;

                    for fa in start_full_match.intersection(&end_full_match) {
                        yield fa.clone();
                    }

                    let end_starts_with = if !end_token_is_full {
                        let t = end_token.clone();
                        index.find_tokens_matching_predicate(Box::new(move |it| it.starts_with(&t))).await
                    } else {
                        vec![]
                    };
                    let mut end_starts_with_files = HashSet::new();
                    for token in end_starts_with.into_iter().collect::<HashSet<_>>() {
                        for fa in index.find_files_by_token(&token).await {
                            if start_full_match.contains(&fa) {
                                yield fa.clone();
                            }
                            end_starts_with_files.insert(fa);
                        }
                    }

                    let start_ends_with = if !start_token_is_full {
                        let t = start_token.clone();
                        index.find_tokens_matching_predicate(Box::new(move |it| it.ends_with(&t))).await
                    } else {
                        vec![]
                    };
                    let mut start_ends_with_files = HashSet::new();
                    for token in start_ends_with.into_iter().collect::<HashSet<_>>() {
                        for fa in index.find_files_by_token(&token).await {
                            if end_full_match.contains(&fa) {
                                yield fa.clone();
                            }
                            start_ends_with_files.insert(fa);
                        }
                    }

                    for fa in start_ends_with_files.intersection(&end_starts_with_files) {
                        yield fa.clone();
                    }
                }
                n => {
                    // inner tokens are always full words, the outer ones only on a word boundary
                    let first_core_token = if start_token_is_full { 0 } else { 1 };
                    let last_core_token = if end_token_is_full { n - 1 } else { n - 2 };
                    let core_tokens = &search_tokens[first_core_token..=last_core_token];

                    let mut files = file_set(&*index, &core_tokens[0]).await;
                    for token in &core_tokens[1..] {
                        if files.is_empty() {
                            break;
                        }
                        let new_files = file_set(&*index, token).await;
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
    use crate::test_utils::{FakeIndex, fa};

    async fn find(index: &Arc<FakeIndex>, query: &str) -> HashSet<FileAddress> {
        let index: Arc<dyn Index> = index.clone();
        word_index_config()
            .find(index, query.into())
            .collect()
            .await
    }

    fn assert_contains_all(found: &HashSet<FileAddress>, names: &[&str]) {
        for name in names {
            assert!(found.contains(&fa(name)), "expected {name} in {found:?}");
        }
    }

    fn assert_contains_none(found: &HashSet<FileAddress>, names: &[&str]) {
        for name in names {
            assert!(
                !found.contains(&fa(name)),
                "did not expect {name} in {found:?}"
            );
        }
    }

    #[test]
    fn should_tokenize_strings() {
        let mut tokens = tokenize("pOUpa    loupa volobueV mech    ");
        tokens.sort();
        assert_eq!(tokens, vec!["loupa", "mech", "poupa", "volobuev"]);
    }

    #[test]
    fn should_tokenize_non_ascii_words() {
        assert_eq!(
            tokenize("Каждому гарантируется, naïve_x"),
            vec!["каждому", "гарантируется", "naïve_x"]
        );
    }

    #[tokio::test]
    async fn should_search_one_token_query() {
        let index = Arc::new(FakeIndex::from_documents(&[
            ("contains", &["poupa"]),
            ("startsWith", &["oupa"]),
            ("endsWith", &["pou"]),
            ("equal", &["ou"]),
            ("never", &["volobuev"]),
        ]));

        let found = find(&index, "ou").await;
        assert_contains_all(&found, &["contains", "startsWith", "endsWith", "equal"]);

        let found = find(&index, "ou ").await;
        assert_contains_all(&found, &["endsWith", "equal"]);
        assert_contains_none(&found, &["contains", "startsWith"]);

        let found = find(&index, " ou").await;
        assert_contains_all(&found, &["startsWith", "equal"]);
        assert_contains_none(&found, &["contains", "endsWith"]);

        let found = find(&index, " ou ").await;
        assert_contains_all(&found, &["equal"]);
        assert_contains_none(&found, &["contains", "startsWith", "endsWith"]);
    }

    #[tokio::test]
    async fn should_search_2_tokens_query() {
        let index = Arc::new(FakeIndex::from_documents(&[
            ("1", &["__ou", "pa__"]),
            ("2", &["ou", "pa__"]),
            ("3", &["__ou", "pa"]),
            ("4", &["ou", "pa"]),
            ("5", &["oupa"]),
            ("6", &["paou"]),
        ]));

        let found = find(&index, "ou pa").await;
        assert_contains_all(&found, &["1", "2", "3", "4", "6"]);
        assert_contains_none(&found, &["5"]);

        let found = find(&index, " ou pa").await;
        assert_contains_all(&found, &["2", "4"]);
        assert_contains_none(&found, &["1", "3", "5", "6"]);

        let found = find(&index, "ou pa ").await;
        assert_contains_all(&found, &["3", "4"]);
        assert_contains_none(&found, &["1", "2", "5", "6"]);

        let found = find(&index, " ou pa ").await;
        assert_contains_all(&found, &["4"]);
        assert_contains_none(&found, &["1", "2", "3", "5", "6"]);
    }

    #[tokio::test]
    async fn should_search_multiple_tokens_query() {
        let index = Arc::new(FakeIndex::from_documents(&[
            ("1", &["volobuev", "__ou", "pa__"]),
            ("2", &["volobuev", "ou", "pa__"]),
            ("3", &["volobuev", "__ou", "pa"]),
            ("4", &["volobuev", "ou", "pa"]),
            ("5", &["volobuev", "oupa"]),
            ("6", &["volobuev", "paou"]),
            ("7", &["__ou", "pa__"]),
            ("8", &["ou", "pa__"]),
            ("9", &["__ou", "pa"]),
            ("10", &["ou", "pa"]),
            ("11", &["oupa"]),
            ("12", &["paou"]),
        ]));
        let never = ["7", "8", "9", "10", "11", "12"];

        let found = find(&index, "ou volobuev pa").await;
        assert_contains_all(&found, &["1", "2", "3", "4", "6"]);
        assert_contains_none(&found, &never);

        let found = find(&index, " ou volobuev pa").await;
        assert_contains_all(&found, &["2", "4"]);
        assert_contains_none(&found, &["5"]);
        assert_contains_none(&found, &never);

        let found = find(&index, "ou volobuev pa ").await;
        assert_contains_all(&found, &["3", "4"]);
        assert_contains_none(&found, &["5"]);
        assert_contains_none(&found, &never);

        let found = find(&index, " ou volobuev pa ").await;
        assert_contains_all(&found, &["4"]);
        assert_contains_none(&found, &["5"]);
        assert_contains_none(&found, &never);
    }
}
