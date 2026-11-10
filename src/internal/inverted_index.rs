use std::collections::HashMap;

use crate::model::{FileAddress, TokenSet};

/// Operations of the index storage used by the index manager.
pub(crate) trait DocumentIndex: Send {
    fn add_or_update_document(&mut self, fa: FileAddress, tokens: TokenSet);
    fn remove_document(&mut self, fa: &FileAddress);
    fn find_files_by_token(&self, token: &str) -> Vec<FileAddress>;
    fn find_tokens_matching_predicate(&self, matches: &dyn Fn(&str) -> bool) -> Vec<String>;
    fn reset(&mut self);
    fn compact(&mut self);
    fn tokens_count(&self) -> usize;
    fn documents_count(&self) -> usize;
}

/// token -> list of file refs. Removed/updated documents leave dead refs in the lists,
/// which are cleaned up by `compact` (automatically, when the share of alive entries drops below `load_factor`).
pub struct InvertedIndex {
    load_factor: f64,
    data: HashMap<String, Vec<u32>>,
    last_fa_ref: u32,
    fa_by_ref: HashMap<u32, FileAddress>,
    ref_by_fa: HashMap<FileAddress, u32>,
    entries_count_by_ref: HashMap<u32, usize>,
    alive_entries: usize,
    total_entries: usize,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new(0.75)
    }
}

impl InvertedIndex {
    pub fn new(load_factor: f64) -> Self {
        InvertedIndex {
            load_factor,
            data: HashMap::new(),
            last_fa_ref: 0,
            fa_by_ref: HashMap::new(),
            ref_by_fa: HashMap::new(),
            entries_count_by_ref: HashMap::new(),
            alive_entries: 0,
            total_entries: 0,
        }
    }

    pub fn alive_entries(&self) -> usize {
        self.alive_entries
    }

    pub fn total_entries(&self) -> usize {
        self.total_entries
    }

    pub fn tokens_count(&self) -> usize {
        self.data.len()
    }

    pub fn documents_count(&self) -> usize {
        self.ref_by_fa.len()
    }

    pub fn add_or_update_document(&mut self, fa: FileAddress, tokens: TokenSet) {
        let prev_entries_count = self.remove_previous_file_ref(&fa);

        let fa_ref = self.last_fa_ref;
        self.last_fa_ref += 1;
        self.fa_by_ref.insert(fa_ref, fa.clone());
        self.ref_by_fa.insert(fa, fa_ref);
        self.entries_count_by_ref.insert(fa_ref, tokens.len());

        self.alive_entries = self.alive_entries - prev_entries_count + tokens.len();
        self.total_entries += tokens.len();

        for token in tokens {
            self.data
                .entry(token)
                .or_insert_with(|| Vec::with_capacity(1))
                .push(fa_ref);
        }

        if self.total_entries != 0
            && (self.alive_entries as f64) / (self.total_entries as f64) < self.load_factor
        {
            self.compact();
        }
    }

    pub fn remove_document(&mut self, fa: &FileAddress) {
        let prev_entries_count = self.remove_previous_file_ref(fa);
        self.alive_entries -= prev_entries_count;
    }

    pub fn find_files_by_token(&self, token: &str) -> Vec<FileAddress> {
        self.data
            .get(token)
            .map(|refs| {
                refs.iter()
                    .filter_map(|r| self.fa_by_ref.get(r).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn find_tokens_matching_predicate(&self, matches: &dyn Fn(&str) -> bool) -> Vec<String> {
        self.data.keys().filter(|k| matches(k)).cloned().collect()
    }

    pub fn reset(&mut self) {
        *self = InvertedIndex::new(self.load_factor);
    }

    pub fn compact(&mut self) {
        let mut last_key = 1u32;
        let remapped: HashMap<u32, u32> = self
            .fa_by_ref
            .keys()
            .map(|&k| {
                let v = last_key;
                last_key += 1;
                (k, v)
            })
            .collect();
        self.last_fa_ref = last_key;
        self.fa_by_ref = std::mem::take(&mut self.fa_by_ref)
            .into_iter()
            .map(|(k, v)| (remapped[&k], v))
            .collect();
        for v in self.ref_by_fa.values_mut() {
            *v = remapped[v];
        }
        self.entries_count_by_ref = std::mem::take(&mut self.entries_count_by_ref)
            .into_iter()
            .map(|(k, v)| (remapped[&k], v))
            .collect();

        let mut entries: Vec<(String, Vec<u32>)> =
            std::mem::take(&mut self.data).into_iter().collect();
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = entries.len() / threads + 1;
        std::thread::scope(|scope| {
            for chunk in entries.chunks_mut(chunk_size) {
                let remapped = &remapped;
                scope.spawn(move || {
                    for (_, refs) in chunk {
                        let mut new_refs: Vec<u32> = refs
                            .iter()
                            .filter_map(|r| remapped.get(r).copied())
                            .collect();
                        new_refs.shrink_to_fit();
                        *refs = new_refs;
                    }
                });
            }
        });
        self.data = entries
            .into_iter()
            .filter(|(_, refs)| !refs.is_empty())
            .collect();

        self.total_entries = self.alive_entries;
    }

    fn remove_previous_file_ref(&mut self, fa: &FileAddress) -> usize {
        match self.ref_by_fa.remove(fa) {
            Some(prev_ref) => {
                self.fa_by_ref.remove(&prev_ref);
                self.entries_count_by_ref.remove(&prev_ref).unwrap_or(0)
            }
            None => 0,
        }
    }
}

impl DocumentIndex for InvertedIndex {
    fn add_or_update_document(&mut self, fa: FileAddress, tokens: TokenSet) {
        InvertedIndex::add_or_update_document(self, fa, tokens)
    }
    fn remove_document(&mut self, fa: &FileAddress) {
        InvertedIndex::remove_document(self, fa)
    }
    fn find_files_by_token(&self, token: &str) -> Vec<FileAddress> {
        InvertedIndex::find_files_by_token(self, token)
    }
    fn find_tokens_matching_predicate(&self, matches: &dyn Fn(&str) -> bool) -> Vec<String> {
        InvertedIndex::find_tokens_matching_predicate(self, matches)
    }
    fn reset(&mut self) {
        InvertedIndex::reset(self)
    }
    fn compact(&mut self) {
        InvertedIndex::compact(self)
    }
    fn tokens_count(&self) -> usize {
        InvertedIndex::tokens_count(self)
    }
    fn documents_count(&self) -> usize {
        InvertedIndex::documents_count(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{fa, set, tokens};

    fn create_index() -> InvertedIndex {
        InvertedIndex::new(0.0)
    }

    #[test]
    fn can_add_documents() {
        let mut index = create_index();
        index.add_or_update_document(fa("doc1.txt"), tokens(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), tokens(&["loupa", "volobuev"]));

        assert_eq!(index.documents_count(), 2);
        assert_eq!(index.alive_entries(), 4);
        assert_eq!(index.total_entries(), 4);

        assert_eq!(
            set(index.find_files_by_token("poupa")),
            set([fa("doc1.txt")])
        );
        assert_eq!(
            set(index.find_files_by_token("loupa")),
            set([fa("doc1.txt"), fa("doc2.txt")])
        );
        assert_eq!(
            set(index.find_files_by_token("volobuev")),
            set([fa("doc2.txt")])
        );
    }

    #[test]
    fn can_update_documents() {
        let mut index = create_index();
        index.add_or_update_document(fa("doc1.txt"), tokens(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), tokens(&["loupa", "volobuev"]));
        index.add_or_update_document(fa("doc1.txt"), tokens(&["mech", "volobuev"]));

        assert_eq!(index.alive_entries(), 4);
        assert_eq!(index.total_entries(), 6);

        assert_eq!(index.documents_count(), 2);
        assert_eq!(
            set(index.find_files_by_token("mech")),
            set([fa("doc1.txt")])
        );
        assert_eq!(
            set(index.find_files_by_token("volobuev")),
            set([fa("doc1.txt"), fa("doc2.txt")])
        );
        assert_eq!(
            set(index.find_files_by_token("loupa")),
            set([fa("doc2.txt")])
        );
    }

    #[test]
    fn can_remove_documents() {
        let mut index = create_index();
        index.add_or_update_document(fa("doc1.txt"), tokens(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), tokens(&["loupa", "volobuev"]));
        index.remove_document(&fa("doc1.txt"));

        assert_eq!(index.documents_count(), 1);
        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 4);
        assert!(index.find_files_by_token("poupa").is_empty());
        assert_eq!(
            set(index.find_files_by_token("loupa")),
            set([fa("doc2.txt")])
        );
        assert_eq!(
            set(index.find_files_by_token("volobuev")),
            set([fa("doc2.txt")])
        );
    }

    #[test]
    fn can_compact() {
        let mut index = create_index();
        index.add_or_update_document(fa("doc1.txt"), tokens(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), tokens(&["loupa", "volobuev"]));
        index.remove_document(&fa("doc1.txt"));

        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 4);

        index.compact();

        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 2);
        assert_eq!(index.tokens_count(), 2);

        assert!(index.find_files_by_token("poupa").is_empty());
        assert_eq!(
            set(index.find_files_by_token("loupa")),
            set([fa("doc2.txt")])
        );
        assert_eq!(
            set(index.find_files_by_token("volobuev")),
            set([fa("doc2.txt")])
        );

        // index keeps working after compaction
        index.add_or_update_document(fa("doc3.txt"), tokens(&["loupa"]));
        assert_eq!(
            set(index.find_files_by_token("loupa")),
            set([fa("doc2.txt"), fa("doc3.txt")])
        );
    }

    #[test]
    fn should_compact_on_update_if_load_factor_threshold_is_reached() {
        let mut index = InvertedIndex::new(0.75);
        index.add_or_update_document(fa("doc1.txt"), tokens(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), tokens(&["loupa", "volobuev"]));
        index.add_or_update_document(fa("doc1.txt"), tokens(&[]));

        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 2);
    }
}
