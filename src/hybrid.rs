use crate::ranking::{ResultSource, SearchResult};
use std::collections::HashMap;

/// RRF constant K — same value as SeekStorm.
pub const RRF_K: f32 = 0.6;

/// Reciprocal Rank Fusion of lexical and vector search results.
///
/// 1. Sort each input by score descending, assign 1-based ranks.
/// 2. Build a map from doc_id to (lexical_rank, vector_rank).
/// 3. Fuse: `score = 1/(K + rank_lex) + 1/(K + rank_vec)`.
/// 4. Sort by fused score descending; set source=Hybrid for docs in both sets.
pub fn hybrid_search(
    lexical_results: Vec<SearchResult>,
    vector_results: Vec<SearchResult>,
) -> Vec<SearchResult> {
    // --- 1. Sort by score descending and assign 1-based ranks ---

    let mut lexical_sorted = lexical_results;
    lexical_sorted.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    let mut vector_sorted = vector_results;
    vector_sorted.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    // --- 2. Build rank maps ---

    let lexical_ranks: HashMap<u64, usize> = lexical_sorted
        .iter()
        .enumerate()
        .map(|(i, r)| (r.doc_id, i + 1)) // 1-based rank
        .collect();

    let vector_ranks: HashMap<u64, usize> = vector_sorted
        .iter()
        .enumerate()
        .map(|(i, r)| (r.doc_id, i + 1))
        .collect();

    // --- 3. Collect all unique doc_ids ---

    let mut all_doc_ids: HashMap<u64, ()> = HashMap::new();
    for &id in lexical_ranks.keys() {
        all_doc_ids.insert(id, ());
    }
    for &id in vector_ranks.keys() {
        all_doc_ids.insert(id, ());
    }

    // --- 4. Compute RRF fused score ---

    let mut fused: Vec<SearchResult> = all_doc_ids
        .into_keys()
        .map(|doc_id| {
            let rank_lex = lexical_ranks.get(&doc_id);
            let rank_vec = vector_ranks.get(&doc_id);

            let score = match (rank_lex, rank_vec) {
                (Some(&rl), Some(&rv)) => {
                    1.0 / (RRF_K + rl as f32) + 1.0 / (RRF_K + rv as f32)
                }
                (Some(&rl), None) => 1.0 / (RRF_K + rl as f32),
                (None, Some(&rv)) => 1.0 / (RRF_K + rv as f32),
                (None, None) => 0.0, // unreachable
            };

            let source = match (rank_lex, rank_vec) {
                (Some(_), Some(_)) => ResultSource::Hybrid,
                (Some(_), None) => ResultSource::Lexical,
                (None, Some(_)) => ResultSource::Vector,
                (None, None) => ResultSource::Hybrid, // unreachable
            };

            SearchResult::new(doc_id, score, source)
        })
        .collect();

    // --- 5. Sort by fused score descending ---

    fused.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    fused
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(doc_id: u64, score: f32) -> SearchResult {
        SearchResult::new(doc_id, score, ResultSource::Lexical)
    }

    fn vec_res(doc_id: u64, score: f32) -> SearchResult {
        SearchResult::new(doc_id, score, ResultSource::Vector)
    }

    /// Overlapping doc_ids: doc 1 appears in both sets.
    /// Lexical: doc1=0.9 (rank 1), doc2=0.5 (rank 2)
    /// Vector:  doc1=0.8 (rank 1), doc3=0.3 (rank 2)
    /// doc1 fused = 1/(0.6+1) + 1/(0.6+1) = 2/1.6 = 1.25
    /// doc2 fused = 1/(0.6+2) = 1/2.6
    /// doc3 fused = 1/(0.6+2) = 1/2.6
    #[test]
    fn test_overlapping_doc_ids() {
        let lexical = vec![lex(1, 0.9), lex(2, 0.5)];
        let vector = vec![vec_res(1, 0.8), vec_res(3, 0.3)];

        let result = hybrid_search(lexical, vector);

        assert_eq!(result.len(), 3);

        // doc1 should be first with the highest fused score
        assert_eq!(result[0].doc_id, 1);
        assert!((result[0].score - 1.25).abs() < 1e-5, "doc1 score = {}", result[0].score);
        assert_eq!(result[0].source, ResultSource::Hybrid);

        // doc2 and doc3 have equal fused scores (same rank 2 in their single list)
        let scores_23: Vec<f32> = result[1..].iter().map(|r| r.score).collect();
        assert!(scores_23.iter().all(|&s| (s - 1.0 / 2.6).abs() < 1e-5));

        // Sources should be Lexical for doc2, Vector for doc3
        let doc2 = result.iter().find(|r| r.doc_id == 2).unwrap();
        let doc3 = result.iter().find(|r| r.doc_id == 3).unwrap();
        assert_eq!(doc2.source, ResultSource::Lexical);
        assert_eq!(doc3.source, ResultSource::Vector);
    }

    /// Non-overlapping: each doc appears in only one set.
    #[test]
    fn test_non_overlapping_doc_ids() {
        let lexical = vec![lex(1, 0.9), lex(2, 0.5)];
        let vector = vec![vec_res(3, 0.8), vec_res(4, 0.3)];

        let result = hybrid_search(lexical, vector);

        assert_eq!(result.len(), 4);

        // doc1 (lexical rank 1): 1/(0.6+1) = 0.625
        // doc3 (vector rank 1):  1/(0.6+1) = 0.625
        // doc2 (lexical rank 2): 1/(0.6+2) = 0.384615...
        // doc4 (vector rank 2):  1/(0.6+2) = 0.384615...

        let doc1 = result.iter().find(|r| r.doc_id == 1).unwrap();
        let doc2 = result.iter().find(|r| r.doc_id == 2).unwrap();
        let doc3 = result.iter().find(|r| r.doc_id == 3).unwrap();
        let doc4 = result.iter().find(|r| r.doc_id == 4).unwrap();

        assert!((doc1.score - 1.0 / 1.6).abs() < 1e-5);
        assert!((doc3.score - 1.0 / 1.6).abs() < 1e-5);
        assert!((doc2.score - 1.0 / 2.6).abs() < 1e-5);
        assert!((doc4.score - 1.0 / 2.6).abs() < 1e-5);

        // Only one rank contributes for each
        assert_eq!(doc1.source, ResultSource::Lexical);
        assert_eq!(doc2.source, ResultSource::Lexical);
        assert_eq!(doc3.source, ResultSource::Vector);
        assert_eq!(doc4.source, ResultSource::Vector);
    }

    /// Empty lexical results: all docs come from vector only.
    #[test]
    fn test_empty_lexical_results() {
        let lexical = vec![];
        let vector = vec![vec_res(10, 0.7), vec_res(20, 0.4)];

        let result = hybrid_search(lexical, vector);

        assert_eq!(result.len(), 2);
        // Both from vector only
        assert!(result.iter().all(|r| r.source == ResultSource::Vector));

        // doc10 rank 1, doc20 rank 2
        let doc10 = result.iter().find(|r| r.doc_id == 10).unwrap();
        let doc20 = result.iter().find(|r| r.doc_id == 20).unwrap();
        assert!((doc10.score - 1.0 / 1.6).abs() < 1e-5);
        assert!((doc20.score - 1.0 / 2.6).abs() < 1e-5);
        // doc10 should rank higher
        assert!(doc10.score > doc20.score);
    }

    /// Empty vector results: all docs come from lexical only.
    #[test]
    fn test_empty_vector_results() {
        let lexical = vec![lex(10, 0.7), lex(20, 0.4)];
        let vector = vec![];

        let result = hybrid_search(lexical, vector);

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|r| r.source == ResultSource::Lexical));

        let doc10 = result.iter().find(|r| r.doc_id == 10).unwrap();
        let doc20 = result.iter().find(|r| r.doc_id == 20).unwrap();
        assert!((doc10.score - 1.0 / 1.6).abs() < 1e-5);
        assert!((doc20.score - 1.0 / 2.6).abs() < 1e-5);
    }

    /// Known calculation: doc with lexical rank 1 + vector rank 1
    /// => score = 1/(0.6+1) + 1/(0.6+1) = 2/1.6 = 1.25
    #[test]
    fn test_known_calculation_rank1_both() {
        let lexical = vec![lex(42, 0.99)];
        let vector = vec![vec_res(42, 0.95)];

        let result = hybrid_search(lexical, vector);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].doc_id, 42);
        assert!((result[0].score - 1.25).abs() < 1e-5);
        assert_eq!(result[0].source, ResultSource::Hybrid);
    }
}
