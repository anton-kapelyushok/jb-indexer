use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use rustc_hash::FxHashSet;

use crate::internal::FileAddress;
use crate::model::{IndexConfig, TokenIndex};

type ErrorHandler = Box<dyn Fn(&anyhow::Error) + Send + Sync>;

/// Indexes lower-cased words, split on `\W+` (ASCII word characters, like Java's regex `\W`).
pub struct WordIndexConfig {
    enable_logging: AtomicBool,
    enable_watcher: bool,
    handle_watcher_error: ErrorHandler,
    handle_initial_file_sync_error: ErrorHandler,
}

pub fn word_index_config(enable_watcher: bool) -> WordIndexConfig {
    WordIndexConfig {
        enable_logging: AtomicBool::new(false),
        enable_watcher,
        handle_watcher_error: Box::new(|_| {}),
        handle_initial_file_sync_error: Box::new(|_| {}),
    }
}

impl WordIndexConfig {
    pub fn on_watcher_error(mut self, f: impl Fn(&anyhow::Error) + Send + Sync + 'static) -> Self {
        self.handle_watcher_error = Box::new(f);
        self
    }
    pub fn on_initial_file_sync_error(mut self, f: impl Fn(&anyhow::Error) + Send + Sync + 'static) -> Self {
        self.handle_initial_file_sync_error = Box::new(f);
        self
    }
}

/// Java regex `\W`: anything but `[a-zA-Z0-9_]`.
fn is_non_word(c: char) -> bool {
    !(c.is_ascii_alphanumeric() || c == '_')
}

pub(crate) fn tokenize(line: &str) -> Vec<String> {
    line.split(is_non_word).filter(|s| !s.is_empty()).map(|s| s.to_ascii_lowercase()).collect()
}

async fn files_for_tokens(index: &dyn TokenIndex, tokens: &[String]) -> Vec<FileAddress> {
    let mut out = Vec::new();
    for t in tokens {
        out.extend(index.find_files_by_token(t).await);
    }
    out
}

async fn file_set(index: &dyn TokenIndex, token: &str) -> FxHashSet<FileAddress> {
    index.find_files_by_token(token).await.into_iter().collect()
}

impl IndexConfig for WordIndexConfig {
    fn enable_logging(&self) -> &AtomicBool {
        &self.enable_logging
    }

    fn enable_watcher(&self) -> bool {
        self.enable_watcher
    }

    fn tokenize(&self, line: &str) -> Vec<String> {
        tokenize(line)
    }

    fn find(&self, index: Arc<dyn TokenIndex>, query: String) -> BoxStream<'static, FileAddress> {
        async_stream::stream! {
            let index = &*index;
            let search_tokens = tokenize(&query);
            // is the first / last query token known to be complete (surrounded by non-word chars)?
            let start_full = query.chars().next().is_some_and(is_non_word);
            let end_full = query.chars().last().is_some_and(is_non_word);

            match search_tokens.len() {
                0 => {
                    // the index won't help here, emit everything we have
                    let tokens = index.find_tokens_matching_predicate(Box::new(|_| true)).await;
                    for t in tokens {
                        for f in index.find_files_by_token(&t).await { yield f; }
                    }
                }
                1 => {
                    let st = search_tokens[0].clone();
                    let tokens = match (start_full, end_full) {
                        (true, true) => vec![st],
                        (false, false) => index.find_tokens_matching_predicate(Box::new(move |t| t.contains(st.as_str()))).await,
                        (false, true) => index.find_tokens_matching_predicate(Box::new(move |t| t.ends_with(st.as_str()))).await,
                        (true, false) => index.find_tokens_matching_predicate(Box::new(move |t| t.starts_with(st.as_str()))).await,
                    };
                    for t in tokens {
                        for f in index.find_files_by_token(&t).await { yield f; }
                    }
                }
                2 => {
                    let (start, end) = (search_tokens[0].clone(), search_tokens[1].clone());
                    let start_full_match = file_set(index, &start).await;
                    let end_full_match = file_set(index, &end).await;

                    for f in start_full_match.intersection(&end_full_match) { yield f.clone(); }

                    let end_starts_with = if !end_full {
                        let e = end.clone();
                        index.find_tokens_matching_predicate(Box::new(move |t| t.starts_with(e.as_str()))).await
                    } else { vec![] };
                    let mut end_starts_with_files = FxHashSet::default();
                    for f in files_for_tokens(index, &end_starts_with).await {
                        if start_full_match.contains(&f) { yield f.clone(); }
                        end_starts_with_files.insert(f);
                    }

                    let start_ends_with = if !start_full {
                        let s = start.clone();
                        index.find_tokens_matching_predicate(Box::new(move |t| t.ends_with(s.as_str()))).await
                    } else { vec![] };
                    let mut start_ends_with_files = FxHashSet::default();
                    for f in files_for_tokens(index, &start_ends_with).await {
                        if end_full_match.contains(&f) { yield f.clone(); }
                        start_ends_with_files.insert(f);
                    }

                    for f in start_ends_with_files.intersection(&end_starts_with_files) { yield f.clone(); }
                }
                _ => {
                    // partial first/last tokens can't be looked up directly; intersect over the complete ones
                    let first = if start_full { 0 } else { 1 };
                    let last = if end_full { search_tokens.len() - 1 } else { search_tokens.len() - 2 };
                    let core = &search_tokens[first..=last];

                    let mut files = file_set(index, &core[0]).await;
                    for t in &core[1..] {
                        if files.is_empty() { break; }
                        let next = file_set(index, t).await;
                        files.retain(|f| next.contains(f));
                    }
                    for f in files { yield f; }
                }
            }
        }
        .boxed()
    }

    fn matches(&self, line: &str, query: &str) -> bool {
        line.contains(query)
    }

    fn handle_watcher_error(&self, e: &anyhow::Error) {
        (self.handle_watcher_error)(e)
    }

    fn handle_initial_file_sync_error(&self, e: &anyhow::Error) {
        (self.handle_initial_file_sync_error)(e)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    use futures::future::BoxFuture;

    use super::*;
    use crate::internal::inverted_index::InvertedIndex;
    use crate::model::TokenPredicate;

    /// Plain InvertedIndex behind the TokenIndex trait (Kotlin tests: mockk).
    pub struct TestIndex(pub Mutex<InvertedIndex>);

    impl TestIndex {
        pub fn shared(docs: &[(&str, &[&str])]) -> Arc<dyn TokenIndex> {
            let mut ii = InvertedIndex::default();
            for (fa, tokens) in docs {
                ii.add_or_update_document(FileAddress::new(*fa), tokens.iter().map(|s| s.to_string()).collect());
            }
            Arc::new(TestIndex(Mutex::new(ii)))
        }
    }

    impl TokenIndex for TestIndex {
        fn find_files_by_token<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Vec<FileAddress>> {
            let r = self.0.lock().unwrap().find_files_by_token(token);
            Box::pin(async move { r })
        }
        fn find_tokens_matching_predicate(&self, p: TokenPredicate) -> BoxFuture<'_, Vec<String>> {
            let r = self.0.lock().unwrap().find_tokens_matching_predicate(p);
            Box::pin(async move { r })
        }
    }

    async fn find(index: &Arc<dyn TokenIndex>, q: &str) -> FxHashSet<String> {
        word_index_config(false).find(index.clone(), q.into()).map(|f| f.path().to_owned()).collect().await
    }

    fn check(found: &FxHashSet<String>, all: &[&str], none: &[&str]) {
        for f in all {
            assert!(found.contains(*f), "expected {f} in {found:?}");
        }
        for f in none {
            assert!(!found.contains(*f), "unexpected {f} in {found:?}");
        }
    }

    #[test]
    fn should_tokenize_strings() {
        let mut t = tokenize("pOUpa    loupa volobueV mech    ");
        t.sort();
        assert_eq!(t, vec!["loupa", "mech", "poupa", "volobuev"]);
    }

    #[tokio::test]
    async fn should_search_one_token_query() {
        let index = TestIndex::shared(&[
            ("contains", &["poupa"]),
            ("startsWith", &["oupa"]),
            ("endsWith", &["pou"]),
            ("equal", &["ou"]),
            ("never", &["volobuev"]),
        ]);
        check(&find(&index, "ou").await, &["contains", "startsWith", "endsWith", "equal"], &["never"]);
        check(&find(&index, "ou ").await, &["endsWith", "equal"], &["contains", "startsWith"]);
        check(&find(&index, " ou").await, &["startsWith", "equal"], &["contains", "endsWith"]);
        check(&find(&index, " ou ").await, &["equal"], &["contains", "startsWith", "endsWith"]);
    }

    #[tokio::test]
    async fn should_search_2_tokens_query() {
        let index = TestIndex::shared(&[
            ("1", &["__ou", "pa__"]),
            ("2", &["ou", "pa__"]),
            ("3", &["__ou", "pa"]),
            ("4", &["ou", "pa"]),
            ("5", &["oupa"]),
            ("6", &["paou"]),
        ]);
        check(&find(&index, "ou pa").await, &["1", "2", "3", "4", "6"], &["5"]);
        check(&find(&index, " ou pa").await, &["2", "4"], &["1", "3", "5", "6"]);
        check(&find(&index, "ou pa ").await, &["3", "4"], &["1", "2", "5", "6"]);
        check(&find(&index, " ou pa ").await, &["4"], &["1", "2", "3", "5", "6"]);
    }

    #[tokio::test]
    async fn should_search_multiple_tokens_query() {
        let index = TestIndex::shared(&[
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
        ]);
        let none = ["7", "8", "9", "10", "11", "12"];
        check(&find(&index, "ou volobuev pa").await, &["1", "2", "3", "4", "6"], &none);
        check(&find(&index, " ou volobuev pa").await, &["2", "4"], &[&none[..], &["5"]].concat());
        check(&find(&index, "ou volobuev pa ").await, &["3", "4"], &[&none[..], &["5"]].concat());
        check(&find(&index, " ou volobuev pa ").await, &["4"], &[&none[..], &["5"]].concat());
    }
}
