/// Search result from any search mode.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub doc_id: u64,
    pub score: f32,
    pub result_type: ResultType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultType {
    Lexical,
    Vector,
    Hybrid,
}

// ---------------------------------------------------------------------------
// Document length compression
// ---------------------------------------------------------------------------

/// Compression table mapping u8 index → f32 document length.
/// Index 128 → 1.0 (neutral value).
pub fn document_length_compression() -> &'static [f32; 256] {
    static TABLE: std::sync::OnceLock<[f32; 256]> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [0.0f32; 256];
        let mut i = 0usize;
        while i < 256 {
            table[i] = 2.0_f32.powi(i as i32 - 128);
            i += 1;
        }
        table
    })
}

/// Compress a document length (token count) to u8.
pub fn compress_doc_length(length: f32) -> u8 {
    if length <= 0.0 {
        return 0;
    }
    let log2 = length.log2();
    let compressed = (log2 + 128.0).round() as i32;
    compressed.clamp(0, 255) as u8
}

/// Decompress a u8 back to approximate document length.
pub fn decompress_doc_length(compressed: u8) -> f32 {
    document_length_compression()[compressed as usize]
}

// ---------------------------------------------------------------------------
// IDF computation
// ---------------------------------------------------------------------------

/// Compute IDF for a term.
/// IDF = ln((N - df + 0.5) / (df + 0.5) + 1)
pub fn compute_idf(total_docs: u64, doc_freq: u32) -> f32 {
    let n = total_docs as f32;
    let df = doc_freq as f32;
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

// ---------------------------------------------------------------------------
// BM25F scorer
// ---------------------------------------------------------------------------

pub struct Bm25fScorer {
    pub k: f32,
    pub b: f32,
}

impl Bm25fScorer {
    pub fn new() -> Self {
        Self { k: 1.2, b: 0.75 }
    }

    /// Compute BM25F score for a single term in a single field.
    pub fn score_field(&self, tf: u32, dl: f32, avgdl: f32, boost: f32, idf: f32) -> f32 {
        let tf = tf as f32;
        let numerator = tf * (self.k + 1.0);
        let denominator = tf + self.k * (1.0 - self.b + self.b * dl / avgdl);
        boost * idf * numerator / denominator
    }

    /// Compute BM25F score across multiple fields for a single term.
    pub fn score_multi_field(&self, fields: &[FieldScoreInput], idf: f32) -> f32 {
        fields
            .iter()
            .map(|f| self.score_field(f.tf, f.dl, f.avgdl, f.boost, idf))
            .sum()
    }
}

impl Default for Bm25fScorer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct FieldScoreInput {
    pub tf: u32,
    pub dl: f32,
    pub avgdl: f32,
    pub boost: f32,
}

// ---------------------------------------------------------------------------
// MinHeap for top-k
// ---------------------------------------------------------------------------

/// Min-heap for tracking top-k search results.
pub struct MinHeap {
    heap: Vec<SearchResult>,
    capacity: usize,
}

impl MinHeap {
    pub fn new(capacity: usize) -> Self {
        Self {
            heap: Vec::with_capacity(capacity.max(1)),
            capacity: capacity.max(1),
        }
    }

    /// Insert a result. If heap is full and score <= min, it's discarded.
    pub fn push(&mut self, result: SearchResult) {
        if self.heap.len() < self.capacity {
            self.heap.push(result);
            self.sift_up(self.heap.len() - 1);
        } else if result.score > self.min_score() {
            self.heap[0] = result;
            self.sift_down(0);
        }
    }

    pub fn is_full(&self) -> bool {
        self.heap.len() >= self.capacity
    }

    /// Get the minimum score in the heap. Returns f32::MAX if empty.
    pub fn min_score(&self) -> f32 {
        self.heap.first().map_or(f32::MAX, |r| r.score)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Consume the heap and return results sorted by score descending.
    pub fn into_sorted_vec(self) -> Vec<SearchResult> {
        let mut vec = self.heap;
        let mut result = Vec::with_capacity(vec.len());
        while !vec.is_empty() {
            result.push(vec.swap_remove(0));
            if !vec.is_empty() {
                Self::sift_down_static(&mut vec, 0);
            }
        }
        result
    }

    fn sift_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.heap[idx].score < self.heap[parent].score {
                self.heap.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, idx: usize) {
        Self::sift_down_static(&mut self.heap, idx);
    }

    fn sift_down_static(heap: &mut Vec<SearchResult>, mut idx: usize) {
        let len = heap.len();
        loop {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            let mut smallest = idx;

            if left < len && heap[left].score < heap[smallest].score {
                smallest = left;
            }
            if right < len && heap[right].score < heap[smallest].score {
                smallest = right;
            }
            if smallest != idx {
                heap.swap(idx, smallest);
                idx = smallest;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_idf_basic() {
        let idf = compute_idf(1000, 10);
        assert!((idf - 4.5574).abs() < 0.001, "idf was {}", idf);
    }

    #[test]
    fn test_compute_idf_all_docs() {
        let idf = compute_idf(100, 100);
        assert!(idf > 0.0);
        assert!(idf < 0.01);
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        for &len in &[1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0] {
            let compressed = compress_doc_length(len);
            let decompressed = decompress_doc_length(compressed);
            let error = (decompressed - len).abs() / len;
            assert!(error < 0.02, "length={}, compressed={}, decompressed={}, error={}",
                    len, compressed, decompressed, error);
        }
    }

    #[test]
    fn test_compress_doc_length_neutral() {
        assert_eq!(compress_doc_length(1.0), 128);
        assert_eq!(decompress_doc_length(128), 1.0);
    }

    #[test]
    fn test_bm25f_score_field_basic() {
        let scorer = Bm25fScorer::new();
        let score = scorer.score_field(1, 10.0, 10.0, 1.0, 1.0);
        assert!((score - 1.0).abs() < 1e-6, "score was {}", score);
    }

    #[test]
    fn test_bm25f_score_field_higher_tf() {
        let scorer = Bm25fScorer::new();
        let score1 = scorer.score_field(1, 10.0, 10.0, 1.0, 1.0);
        let score2 = scorer.score_field(5, 10.0, 10.0, 1.0, 1.0);
        assert!(score2 > score1);
    }

    #[test]
    fn test_bm25f_score_field_longer_doc_penalty() {
        let scorer = Bm25fScorer::new();
        let score_short = scorer.score_field(1, 5.0, 10.0, 1.0, 1.0);
        let score_long = scorer.score_field(1, 20.0, 10.0, 1.0, 1.0);
        assert!(score_short > score_long);
    }

    #[test]
    fn test_bm25f_score_field_zero_tf() {
        let scorer = Bm25fScorer::new();
        let score = scorer.score_field(0, 10.0, 10.0, 1.0, 1.0);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_minheap_push_and_sorted() {
        let mut heap = MinHeap::new(5);
        heap.push(SearchResult { doc_id: 1, score: 3.0, result_type: ResultType::Lexical });
        heap.push(SearchResult { doc_id: 2, score: 1.0, result_type: ResultType::Lexical });
        heap.push(SearchResult { doc_id: 3, score: 5.0, result_type: ResultType::Vector });
        heap.push(SearchResult { doc_id: 4, score: 2.0, result_type: ResultType::Hybrid });
        heap.push(SearchResult { doc_id: 5, score: 4.0, result_type: ResultType::Lexical });

        assert_eq!(heap.len(), 5);
        assert!(heap.is_full());
        assert_eq!(heap.min_score(), 1.0);

        let sorted = heap.into_sorted_vec();
        assert_eq!(sorted.len(), 5);
        assert_eq!(sorted[0].score, 5.0);
        assert_eq!(sorted[4].score, 1.0);
    }

    #[test]
    fn test_minheap_discards_low_scores_when_full() {
        let mut heap = MinHeap::new(3);
        heap.push(SearchResult { doc_id: 1, score: 5.0, result_type: ResultType::Lexical });
        heap.push(SearchResult { doc_id: 2, score: 3.0, result_type: ResultType::Lexical });
        heap.push(SearchResult { doc_id: 3, score: 1.0, result_type: ResultType::Lexical });

        assert!(heap.is_full());
        heap.push(SearchResult { doc_id: 4, score: 4.0, result_type: ResultType::Lexical });
        assert_eq!(heap.min_score(), 3.0);

        heap.push(SearchResult { doc_id: 5, score: 2.0, result_type: ResultType::Lexical });
        assert_eq!(heap.len(), 3);

        let sorted = heap.into_sorted_vec();
        assert_eq!(sorted[0].score, 5.0);
        assert_eq!(sorted[1].score, 4.0);
        assert_eq!(sorted[2].score, 3.0);
    }
}
