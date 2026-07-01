use ahash::{AHashMap, AHashSet};
use seekpower_posting::PostingEntry;
use seekpower_storage::TermId;

/// Write buffer for accumulating writes before commit.
/// Uses term_id (u32) instead of term_hash for postings, enabling
/// term recovery for highlighting and preventing hash collisions.
pub struct WriteBuffer {
    /// term_id → posting entries (replaces term_hash → entries)
    pub postings: AHashMap<TermId, Vec<PostingEntry>>,
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
