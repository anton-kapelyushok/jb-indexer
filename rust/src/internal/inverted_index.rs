use rustc_hash::{FxHashMap, FxHashSet};

use super::FileAddress;

/// token -> list of file refs. File refs of updated/removed documents are left behind as garbage and
/// dropped by [`InvertedIndex::compact`], which runs automatically once the garbage ratio crosses `load_factor`.
pub struct InvertedIndex {
    load_factor: f64,
    data: FxHashMap<String, Vec<u32>>,
    last_fa_ref: u32,
    fa_by_ref: FxHashMap<u32, FileAddress>,
    ref_by_fa: FxHashMap<FileAddress, u32>,
    entries_by_ref: FxHashMap<u32, usize>,
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
            data: FxHashMap::default(),
            last_fa_ref: 0,
            fa_by_ref: FxHashMap::default(),
            ref_by_fa: FxHashMap::default(),
            entries_by_ref: FxHashMap::default(),
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

    pub fn add_or_update_document(&mut self, fa: FileAddress, tokens: FxHashSet<String>) {
        let prev_entries = self.remove_previous_file_ref(&fa);

        let fa_ref = self.last_fa_ref;
        self.last_fa_ref += 1;
        self.fa_by_ref.insert(fa_ref, fa.clone());
        self.ref_by_fa.insert(fa, fa_ref);
        self.entries_by_ref.insert(fa_ref, tokens.len());

        self.alive_entries = self.alive_entries - prev_entries + tokens.len();
        self.total_entries += tokens.len();

        for token in tokens {
            self.data.entry(token).or_insert_with(|| Vec::with_capacity(1)).push(fa_ref);
        }

        if self.total_entries != 0 && (self.alive_entries as f64 / self.total_entries as f64) < self.load_factor {
            self.compact();
        }
    }

    pub fn remove_document(&mut self, fa: &FileAddress) {
        let prev = self.remove_previous_file_ref(fa);
        self.alive_entries -= prev;
    }

    pub fn find_files_by_token(&self, token: &str) -> Vec<FileAddress> {
        match self.data.get(token) {
            Some(refs) => refs.iter().filter_map(|r| self.fa_by_ref.get(r).cloned()).collect(),
            None => Vec::new(),
        }
    }

    pub fn find_tokens_matching_predicate(&self, matches: impl Fn(&str) -> bool) -> Vec<String> {
        self.data.keys().filter(|k| matches(k)).cloned().collect()
    }

    pub fn reset(&mut self) {
        self.data.clear();
        self.last_fa_ref = 0;
        self.fa_by_ref.clear();
        self.ref_by_fa.clear();
        self.entries_by_ref.clear();
        self.alive_entries = 0;
        self.total_entries = 0;
    }

    pub fn compact(&mut self) {
        let mut last_key = 1u32;
        let remapped: FxHashMap<u32, u32> = self
            .fa_by_ref
            .keys()
            .map(|&k| {
                let v = last_key;
                last_key += 1;
                (k, v)
            })
            .collect();
        self.last_fa_ref = last_key;
        self.fa_by_ref = self.fa_by_ref.drain().map(|(k, v)| (remapped[&k], v)).collect();
        for v in self.ref_by_fa.values_mut() {
            *v = remapped[v];
        }
        self.entries_by_ref = self.entries_by_ref.drain().map(|(k, v)| (remapped[&k], v)).collect();

        // remap posting lists in parallel, one chunk per core (Kotlin: runBlocking(Dispatchers.Default))
        let mut lists: Vec<&mut Vec<u32>> = self.data.values_mut().collect();
        let chunks = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let chunk_size = lists.len() / chunks + 1;
        let remapped = &remapped;
        std::thread::scope(|s| {
            for chunk in lists.chunks_mut(chunk_size) {
                s.spawn(move || {
                    for list in chunk {
                        let new: Vec<u32> = list.iter().filter_map(|r| remapped.get(r).copied()).collect();
                        **list = new;
                    }
                });
            }
        });
        self.data.retain(|_, v| !v.is_empty());

        self.total_entries = self.alive_entries;
    }

    fn remove_previous_file_ref(&mut self, fa: &FileAddress) -> usize {
        match self.ref_by_fa.remove(fa) {
            Some(prev) => {
                self.fa_by_ref.remove(&prev);
                self.entries_by_ref.remove(&prev).unwrap_or(0)
            }
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fa(s: &str) -> FileAddress {
        FileAddress::new(s)
    }
    fn set(v: &[&str]) -> FxHashSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }
    fn sorted(mut v: Vec<FileAddress>) -> Vec<FileAddress> {
        v.sort();
        v
    }

    #[test]
    fn can_add_documents() {
        let mut index = InvertedIndex::new(0.0);
        index.add_or_update_document(fa("doc1.txt"), set(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), set(&["loupa", "volobuev"]));

        assert_eq!(index.documents_count(), 2);
        assert_eq!(index.alive_entries(), 4);
        assert_eq!(index.total_entries(), 4);
        assert_eq!(index.find_files_by_token("poupa"), vec![fa("doc1.txt")]);
        assert_eq!(sorted(index.find_files_by_token("loupa")), vec![fa("doc1.txt"), fa("doc2.txt")]);
        assert_eq!(index.find_files_by_token("volobuev"), vec![fa("doc2.txt")]);
    }

    #[test]
    fn can_update_documents() {
        let mut index = InvertedIndex::new(0.0);
        index.add_or_update_document(fa("doc1.txt"), set(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), set(&["loupa", "volobuev"]));
        index.add_or_update_document(fa("doc1.txt"), set(&["mech", "volobuev"]));

        assert_eq!(index.alive_entries(), 4);
        assert_eq!(index.total_entries(), 6);
        assert_eq!(index.documents_count(), 2);
        assert_eq!(index.find_files_by_token("mech"), vec![fa("doc1.txt")]);
        assert_eq!(sorted(index.find_files_by_token("volobuev")), vec![fa("doc1.txt"), fa("doc2.txt")]);
        assert_eq!(index.find_files_by_token("loupa"), vec![fa("doc2.txt")]);
    }

    #[test]
    fn can_remove_documents() {
        let mut index = InvertedIndex::new(0.0);
        index.add_or_update_document(fa("doc1.txt"), set(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), set(&["loupa", "volobuev"]));
        index.remove_document(&fa("doc1.txt"));

        assert_eq!(index.documents_count(), 1);
        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 4);
        assert!(index.find_files_by_token("poupa").is_empty());
        assert_eq!(index.find_files_by_token("loupa"), vec![fa("doc2.txt")]);
        assert_eq!(index.find_files_by_token("volobuev"), vec![fa("doc2.txt")]);
    }

    #[test]
    fn can_compact() {
        let mut index = InvertedIndex::new(0.0);
        index.add_or_update_document(fa("doc1.txt"), set(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), set(&["loupa", "volobuev"]));
        index.remove_document(&fa("doc1.txt"));
        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 4);

        index.compact();

        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 2);
        assert!(index.find_files_by_token("poupa").is_empty());
        assert_eq!(index.tokens_count(), 2, "empty posting lists are dropped");
        assert_eq!(index.find_files_by_token("loupa"), vec![fa("doc2.txt")]);
        assert_eq!(index.find_files_by_token("volobuev"), vec![fa("doc2.txt")]);
    }

    #[test]
    fn should_compact_on_update_if_load_factor_threshold_is_reached() {
        let mut index = InvertedIndex::new(0.75);
        index.add_or_update_document(fa("doc1.txt"), set(&["poupa", "loupa"]));
        index.add_or_update_document(fa("doc2.txt"), set(&["loupa", "volobuev"]));
        index.add_or_update_document(fa("doc1.txt"), set(&[]));

        assert_eq!(index.alive_entries(), 2);
        assert_eq!(index.total_entries(), 2);
    }
}
