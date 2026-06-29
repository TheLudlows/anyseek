// storm-db ranking module: MinHeap top-k, BM25F scoring, IDF

/// BM25 parameter K (term frequency saturation).
pub const K_BM25: f32 = 1.2;
/// BM25 parameter B (document length normalization).
pub const B_BM25: f32 = 0.75;

/// Source of a search result.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultSource {
    Lexical,
    Vector,
    Hybrid,
}

/// A single search result with its score.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub doc_id: u64,
    pub score: f32,
    pub source: ResultSource,
}

impl SearchResult {
    pub fn new(doc_id: u64, score: f32, source: ResultSource) -> Self {
        Self {
            doc_id,
            score,
            source,
        }
    }
}

/// Lookup table: compressed doc length (u8) → real doc length (f32).
/// Mapping: index i maps to 2^(i-128), so 128 → 1.0.
pub const DOCUMENT_LENGTH_COMPRESSION: [f32; 256] = {
    let mut table = [0.0f32; 256];
    table[128] = 1.0;
    // Fill forward from 129 to 255 (multiply by 2 each step).
    let mut i = 129;
    while i < 256 {
        table[i] = table[i - 1] * 2.0;
        i += 1;
    }
    // Fill backward from 127 to 0 (divide by 2 each step).
    let mut j: i32 = 127;
    while j >= 0 {
        table[j as usize] = table[(j + 1) as usize] / 2.0;
        j -= 1;
    }
    table
};

/// A binary min-heap for collecting top-k results.
/// The root is the *minimum* score, which enables O(1) WAND pruning.
pub struct MinHeap {
    elements: Vec<SearchResult>,
    capacity: usize,
}

impl MinHeap {
    /// Create a new heap that holds at most `capacity` elements.
    pub fn new(capacity: usize) -> Self {
        Self {
            elements: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Return the number of elements currently in the heap.
    #[inline]
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Return true if the heap is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Return true if the heap has reached its capacity.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.elements.len() == self.capacity
    }

    /// Add a result to the heap.
    /// - If not full: push and bubble up. Returns true.
    /// - If full and score > root: replace root and bubble down. Returns true.
    /// - If full and score <= root: reject. Returns false.
    pub fn add(&mut self, result: SearchResult) -> bool {
        if self.capacity == 0 {
            return false;
        }
        if !self.is_full() {
            self.elements.push(result);
            self.heapify_up(self.elements.len() - 1);
            true
        } else if result.score > self.elements[0].score {
            self.elements[0] = result;
            self.heapify_down(0);
            true
        } else {
            false
        }
    }

    /// Return the minimum score (root) if the heap is non-empty.
    pub fn peek_min(&self) -> Option<f32> {
        self.elements.first().map(|r| r.score)
    }

    /// Drain the heap and return results sorted descending by score.
    pub fn into_sorted_vec(mut self) -> Vec<SearchResult> {
        // Repeatedly extract min from the end (swap root with last).
        let mut result = Vec::with_capacity(self.elements.len());
        while !self.elements.is_empty() {
            let last_idx = self.elements.len() - 1;
            self.elements.swap(0, last_idx);
            let min_elem = self.elements.pop().unwrap();
            result.push(min_elem);
            if !self.elements.is_empty() {
                self.heapify_down(0);
            }
        }
        result.reverse(); // descending by score
        result
    }

    // --- internal helpers (0-based binary min-heap) ---

    fn heapify_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.elements[idx].score < self.elements[parent].score {
                self.elements.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    fn heapify_down(&mut self, mut idx: usize) {
        loop {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            let mut smallest = idx;

            if left < self.elements.len()
                && self.elements[left].score < self.elements[smallest].score
            {
                smallest = left;
            }
            if right < self.elements.len()
                && self.elements[right].score < self.elements[smallest].score
            {
                smallest = right;
            }
            if smallest != idx {
                self.elements.swap(idx, smallest);
                idx = smallest;
            } else {
                break;
            }
        }
    }
}

/// Compute the IDF component for a term.
///
/// Formula: `ln((N - df + 0.5) / (df + 0.5) + 1.0)`
/// where N = doc_count, df = doc_freq.
pub fn compute_idf(doc_count: u64, doc_freq: u32) -> f32 {
    ((doc_count as f32 - doc_freq as f32 + 0.5) / (doc_freq as f32 + 0.5) + 1.0).ln()
}

/// Compute a BM25F field score.
///
/// - `tf`: term frequency in this field.
/// - `doc_length_compressed`: compressed document length index (0-255).
/// - `boost`: field weight / boost factor.
/// - `idf`: precomputed IDF value.
/// - `avg_doc_length`: average document length in the collection.
pub fn compute_bm25f(
    tf: u32,
    doc_length_compressed: u8,
    boost: f32,
    idf: f32,
    avg_doc_length: f32,
) -> f32 {
    let doc_len = DOCUMENT_LENGTH_COMPRESSION[doc_length_compressed as usize];
    let bm25_component =
        K_BM25 * (1.0 - B_BM25 + B_BM25 * doc_len / avg_doc_length);
    let tf_f = tf as f32;
    boost * idf * (tf_f * (K_BM25 + 1.0) / (tf_f + bm25_component))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MinHeap tests ---

    #[test]
    fn test_minheap_add_below_capacity() {
        let mut heap = MinHeap::new(5);
        assert!(heap.add(SearchResult::new(1, 10.0, ResultSource::Lexical)));
        assert!(heap.add(SearchResult::new(2, 5.0, ResultSource::Lexical)));
        assert!(heap.add(SearchResult::new(3, 8.0, ResultSource::Lexical)));
        assert_eq!(heap.len(), 3);
        assert!(!heap.is_full());
    }

    #[test]
    fn test_minheap_add_above_capacity_evicts_min() {
        let mut heap = MinHeap::new(3);
        assert!(heap.add(SearchResult::new(1, 10.0, ResultSource::Lexical)));
        assert!(heap.add(SearchResult::new(2, 5.0, ResultSource::Lexical)));
        assert!(heap.add(SearchResult::new(3, 8.0, ResultSource::Lexical)));
        assert!(heap.is_full());
        // This should evict the root (5.0) since 20.0 > 5.0
        assert!(heap.add(SearchResult::new(4, 20.0, ResultSource::Lexical)));
        assert_eq!(heap.len(), 3);
        assert!(!heap.is_empty());
        // Root should now be 8.0 (the new minimum after eviction)
        assert_eq!(heap.peek_min(), Some(8.0));
    }

    #[test]
    fn test_minheap_add_above_capacity_rejects_lower() {
        let mut heap = MinHeap::new(3);
        assert!(heap.add(SearchResult::new(1, 10.0, ResultSource::Lexical)));
        assert!(heap.add(SearchResult::new(2, 5.0, ResultSource::Lexical)));
        assert!(heap.add(SearchResult::new(3, 8.0, ResultSource::Lexical)));
        // 3.0 < root (5.0), should be rejected
        assert!(!heap.add(SearchResult::new(4, 3.0, ResultSource::Lexical)));
        assert_eq!(heap.len(), 3);
        assert_eq!(heap.peek_min(), Some(5.0));
    }

    #[test]
    fn test_minheap_peek_min() {
        let mut heap = MinHeap::new(5);
        assert!(heap.add(SearchResult::new(1, 10.0, ResultSource::Lexical)));
        assert_eq!(heap.peek_min(), Some(10.0));
        assert!(heap.add(SearchResult::new(2, 3.0, ResultSource::Lexical)));
        assert_eq!(heap.peek_min(), Some(3.0));
    }

    #[test]
    fn test_minheap_peek_min_empty() {
        let heap: MinHeap = MinHeap::new(5);
        assert_eq!(heap.peek_min(), None);
    }

    #[test]
    fn test_minheap_into_sorted_vec() {
        let mut heap = MinHeap::new(10);
        for (i, score) in [7.0, 2.0, 9.0, 1.0, 5.0, 8.0, 3.0].iter().enumerate() {
            assert!(heap.add(SearchResult::new(i as u64, *score, ResultSource::Lexical)));
        }
        let sorted = heap.into_sorted_vec();
        assert_eq!(sorted.len(), 7);
        // Verify descending order
        for i in 1..sorted.len() {
            assert!(sorted[i - 1].score >= sorted[i].score);
        }
        assert_eq!(sorted[0].score, 9.0);
        assert_eq!(sorted[6].score, 1.0);
    }

    #[test]
    fn test_minheap_root_is_smallest() {
        let mut heap = MinHeap::new(20);
        let scores = [42.0, 17.0, 99.0, 3.0, 55.0, 8.0, 31.0, 1.0, 73.0, 15.0];
        for (i, &s) in scores.iter().enumerate() {
            assert!(heap.add(SearchResult::new(i as u64, s, ResultSource::Lexical)));
        }
        let expected_min = scores.iter().cloned().fold(f32::INFINITY, f32::min);
        assert_eq!(heap.peek_min(), Some(expected_min));
    }

    #[test]
    fn test_minheap_capacity_zero() {
        let mut heap = MinHeap::new(0);
        assert!(!heap.add(SearchResult::new(1, 10.0, ResultSource::Lexical)));
        assert_eq!(heap.len(), 0);
    }

    // --- BM25F tests ---

    #[test]
    fn test_bm25f_score_increases_with_tf() {
        let s1 = compute_bm25f(1, 128, 1.0, 2.0, 100.0);
        let s2 = compute_bm25f(5, 128, 1.0, 2.0, 100.0);
        let s3 = compute_bm25f(10, 128, 1.0, 2.0, 100.0);
        assert!(s1 < s2, "tf=1 ({s1}) should be < tf=5 ({s2})");
        assert!(s2 < s3, "tf=5 ({s2}) should be < tf=10 ({s3})");
    }

    #[test]
    fn test_bm25f_score_decreases_with_doc_length() {
        // Short doc (index 128 → length 1.0)
        let s_short = compute_bm25f(5, 128, 1.0, 2.0, 100.0);
        // Long doc (index 138 → length 1024.0)
        let s_long = compute_bm25f(5, 138, 1.0, 2.0, 100.0);
        assert!(
            s_short > s_long,
            "short doc ({s_short}) should score > long doc ({s_long})"
        );
    }

    #[test]
    fn test_bm25f_score_increases_with_idf() {
        let s1 = compute_bm25f(5, 128, 1.0, 1.0, 100.0);
        let s2 = compute_bm25f(5, 128, 1.0, 5.0, 100.0);
        assert!(s1 < s2, "lower idf ({s1}) should be < higher idf ({s2})");
    }

    // --- IDF tests ---

    #[test]
    fn test_idf_rare_term_positive() {
        // Rare term: appears in 1 of 1000 docs
        let idf = compute_idf(1000, 1);
        assert!(idf > 0.0, "rare term IDF should be positive, got {idf}");
        // Should be relatively large
        assert!(idf > 5.0, "rare term IDF should be large, got {idf}");
    }

    #[test]
    fn test_idf_common_term_low() {
        // Common term: appears in 500 of 1000 docs
        let idf = compute_idf(1000, 500);
        assert!(
            idf < 1.0,
            "common term IDF should be low, got {idf}"
        );
    }

    #[test]
    fn test_idf_decreases_with_freq() {
        let idf1 = compute_idf(1000, 1);
        let idf2 = compute_idf(1000, 10);
        let idf3 = compute_idf(1000, 100);
        assert!(idf1 > idf2, "idf for df=1 should > df=10");
        assert!(idf2 > idf3, "idf for df=10 should > df=100");
    }

    // --- DOCUMENT_LENGTH_COMPRESSION tests ---

    #[test]
    fn test_doc_length_compression_table_128_is_one() {
        assert!(
            (DOCUMENT_LENGTH_COMPRESSION[128] - 1.0).abs() < 1e-6,
            "table[128] should be ≈ 1.0, got {}",
            DOCUMENT_LENGTH_COMPRESSION[128]
        );
    }

    #[test]
    fn test_doc_length_compression_monotonic() {
        for i in 1..256 {
            assert!(
                DOCUMENT_LENGTH_COMPRESSION[i] > DOCUMENT_LENGTH_COMPRESSION[i - 1],
                "table should be strictly increasing at index {i}"
            );
        }
    }
}
