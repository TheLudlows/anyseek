use ahash::{AHashMap, AHashSet};
use seekpower_posting::PostingEntry;

pub struct WriteBuffer {
    pub postings: AHashMap<u64, Vec<PostingEntry>>,
    pub documents: AHashMap<u64, Vec<u8>>,
    pub doc_lengths: AHashMap<u64, Vec<(u16, u16)>>,
    pub deletions: AHashSet<u64>,
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self {
            postings: AHashMap::new(),
            documents: AHashMap::new(),
            doc_lengths: AHashMap::new(),
            deletions: AHashSet::new(),
        }
    }
}
