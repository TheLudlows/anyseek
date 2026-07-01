//! SeekPower search engine: query execution and hybrid search fusion.
//!
//! This crate provides the query engine for SeekPower, supporting lexical search (BM25F),
//! vector search (ANN), and hybrid search with reciprocal rank fusion (RRF).

use std::collections::HashMap;

/// Search request containing query parameters and configuration.
#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub query_string: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub search_mode: SearchMode,
    pub query_type: QueryType,
    pub ann_mode: seekpower_vector::AnnMode,
    pub offset: usize,
    pub length: usize,
    pub field_filter: Option<FieldFilter>,
}

impl Default for SearchRequest {
    fn default() -> Self {
        Self {
            query_string: None,
            query_vector: None,
            search_mode: SearchMode::Lexical,
            query_type: QueryType::Union,
            ann_mode: seekpower_vector::AnnMode::All,
            offset: 0,
            length: 10,
            field_filter: None,
        }
    }
}

/// Search mode: lexical, vector, or hybrid.
#[derive(Debug, Clone)]
pub enum SearchMode {
    Lexical,
    Vector { ann_mode: seekpower_vector::AnnMode },
    Hybrid { ann_mode: seekpower_vector::AnnMode, w_lexical: f32, w_vector: f32 },
}

/// Query type: union (OR), intersection (AND), or phrase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Union,
    Intersection,
    Phrase,
}

/// Search response containing results and metadata.
#[derive(Debug, Clone)]
pub struct SearchResponse {
    pub query: String,
    pub result_count: usize,
    pub results: Vec<seekpower_ranking::SearchResult>,
}

/// Field filter for restricting search results.
#[derive(Debug, Clone)]
pub struct FieldFilter {
    pub field: String,
    pub condition: FilterCondition,
}

/// Filter condition types.
#[derive(Debug, Clone)]
pub enum FilterCondition {
    Equals(serde_json::Value),
    Range { min: Option<serde_json::Value>, max: Option<serde_json::Value> },
    In(Vec<serde_json::Value>),
}

/// Trait for fusion strategies that combine lexical and vector results.
pub trait FusionStrategy: Send + Sync {
    fn fuse(
        &self,
        lexical_results: Vec<seekpower_ranking::SearchResult>,
        vector_results: Vec<seekpower_ranking::SearchResult>,
    ) -> Vec<seekpower_ranking::SearchResult>;
}

/// Reciprocal Rank Fusion (RRF) strategy.
///
/// Combines rankings by computing: score = Σ w_i / (k + rank_i)
pub struct RrfFusion {
    pub k: u32,
    pub w_lexical: f32,
    pub w_vector: f32,
}

impl RrfFusion {
    pub fn new() -> Self {
        Self { k: 60, w_lexical: 1.0, w_vector: 1.0 }
    }
}

impl Default for RrfFusion {
    fn default() -> Self { Self::new() }
}

impl FusionStrategy for RrfFusion {
    fn fuse(
        &self,
        lexical_results: Vec<seekpower_ranking::SearchResult>,
        vector_results: Vec<seekpower_ranking::SearchResult>,
    ) -> Vec<seekpower_ranking::SearchResult> {
        let mut scores: HashMap<u64, f32> = HashMap::new();

        for (rank, result) in lexical_results.iter().enumerate() {
            let rrf = self.w_lexical / (self.k as f32 + rank as f32 + 1.0);
            *scores.entry(result.doc_id).or_insert(0.0) += rrf;
        }

        for (rank, result) in vector_results.iter().enumerate() {
            let rrf = self.w_vector / (self.k as f32 + rank as f32 + 1.0);
            *scores.entry(result.doc_id).or_insert(0.0) += rrf;
        }

        let mut fused: Vec<seekpower_ranking::SearchResult> = scores
            .into_iter()
            .map(|(doc_id, score)| seekpower_ranking::SearchResult {
                doc_id,
                score,
                result_type: seekpower_ranking::ResultType::Hybrid,
            })
            .collect();

        fused.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        fused
    }
}

/// Query engine that executes searches and fuses results.
pub struct QueryEngine {
    pub fusion: Box<dyn FusionStrategy>,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self { fusion: Box::new(RrfFusion::new()) }
    }

    pub fn with_fusion(mut self, fusion: Box<dyn FusionStrategy>) -> Self {
        self.fusion = fusion;
        self
    }

    /// Execute a lexical search using BM25F scoring.
    pub fn search_lexical(
        &self,
        posting_refs: &[&seekpower_posting::PostingListBlock],
        delete_set: &std::collections::HashSet<u64>,
        query_type: QueryType,
        avg_doc_length: f32,
        boosts: &[f32],
        idfs: &[f32],
        k: usize,
    ) -> Vec<seekpower_ranking::SearchResult> {
        let scorer = seekpower_ranking::Bm25fScorer::new();
        let mut heap = seekpower_ranking::MinHeap::new(k);

        match query_type {
            QueryType::Union => {
                for (idx, pl) in posting_refs.iter().enumerate() {
                    let entries = seekpower_posting::PostingCodec::decode(pl);
                    let idf = idfs.get(idx).copied().unwrap_or(0.0);
                    let boost = boosts.get(idx).copied().unwrap_or(1.0);
                    for entry in entries {
                        if delete_set.contains(&entry.doc_id) { continue; }
                        let tf = entry.positions.len() as u32;
                        let dl = avg_doc_length;
                        let score = scorer.score_field(tf, dl, avg_doc_length, boost, idf);
                        heap.push(seekpower_ranking::SearchResult {
                            doc_id: entry.doc_id,
                            score,
                            result_type: seekpower_ranking::ResultType::Lexical,
                        });
                    }
                }
            }
            QueryType::Intersection => {
                if posting_refs.is_empty() { return Vec::new(); }
                let all_entries: Vec<Vec<u64>> = posting_refs.iter().map(|pl| {
                    seekpower_posting::PostingCodec::decode(pl)
                        .iter()
                        .filter(|e| !delete_set.contains(&e.doc_id))
                        .map(|e| e.doc_id)
                        .collect()
                }).collect();
                let mut candidates = all_entries[0].clone();
                for set in &all_entries[1..] {
                    let set_h: std::collections::HashSet<u64> = set.iter().copied().collect();
                    candidates.retain(|id| set_h.contains(id));
                }
                for doc_id in candidates {
                    let mut total_score = 0.0f32;
                    for (idx, pl) in posting_refs.iter().enumerate() {
                        let entries = seekpower_posting::PostingCodec::decode(pl);
                        if let Some(entry) = entries.iter().find(|e| e.doc_id == doc_id) {
                            let idf = idfs.get(idx).copied().unwrap_or(0.0);
                            let boost = boosts.get(idx).copied().unwrap_or(1.0);
                            let tf = entry.positions.len() as u32;
                            total_score += scorer.score_field(tf, avg_doc_length, avg_doc_length, boost, idf);
                        }
                    }
                    heap.push(seekpower_ranking::SearchResult {
                        doc_id, score: total_score, result_type: seekpower_ranking::ResultType::Lexical,
                    });
                }
            }
            QueryType::Phrase => {
                return self.search_lexical(posting_refs, delete_set, QueryType::Intersection, avg_doc_length, boosts, idfs, k);
            }
        }

        heap.into_sorted_vec()
    }

    /// Execute a vector search using the provided index.
    pub async fn search_vector<V: seekpower_vector::VectorIndex>(
        &self,
        vector_index: &V,
        query: &[f32],
        k: usize,
        ann_mode: &seekpower_vector::AnnMode,
        delete_set: &std::collections::HashSet<u64>,
    ) -> Vec<seekpower_ranking::SearchResult> {
        vector_index.search(query, k, ann_mode, delete_set).await
            .into_iter()
            .map(|r| seekpower_ranking::SearchResult {
                doc_id: r.doc_id,
                score: r.score,
                result_type: seekpower_ranking::ResultType::Vector,
            })
            .collect()
    }

    /// Execute a hybrid search by fusing lexical and vector results.
    pub async fn search_hybrid(
        &self,
        lexical_results: Vec<seekpower_ranking::SearchResult>,
        vector_results: Vec<seekpower_ranking::SearchResult>,
    ) -> Vec<seekpower_ranking::SearchResult> {
        self.fusion.fuse(lexical_results, vector_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_request_default() {
        let req = SearchRequest::default();
        assert_eq!(req.offset, 0);
        assert_eq!(req.length, 10);
        assert!(req.query_string.is_none());
        assert!(req.query_vector.is_none());
        assert!(matches!(req.search_mode, SearchMode::Lexical));
        assert!(matches!(req.query_type, QueryType::Union));
        assert!(matches!(req.ann_mode, seekpower_vector::AnnMode::All));
        assert!(req.field_filter.is_none());
    }

    #[test]
    fn test_rrf_fusion_basic() {
        let fusion = RrfFusion::new();
        let lexical = vec![
            seekpower_ranking::SearchResult { doc_id: 1, score: 10.0, result_type: seekpower_ranking::ResultType::Lexical },
            seekpower_ranking::SearchResult { doc_id: 2, score: 5.0, result_type: seekpower_ranking::ResultType::Lexical },
        ];
        let vector = vec![
            seekpower_ranking::SearchResult { doc_id: 2, score: 8.0, result_type: seekpower_ranking::ResultType::Vector },
            seekpower_ranking::SearchResult { doc_id: 3, score: 3.0, result_type: seekpower_ranking::ResultType::Vector },
        ];
        let fused = fusion.fuse(lexical, vector);
        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].doc_id, 2);
        assert!(matches!(fused[0].result_type, seekpower_ranking::ResultType::Hybrid));
    }

    #[test]
    fn test_rrf_fusion_weights() {
        let fusion = RrfFusion { k: 60, w_lexical: 2.0, w_vector: 1.0 };
        let lexical = vec![
            seekpower_ranking::SearchResult { doc_id: 1, score: 10.0, result_type: seekpower_ranking::ResultType::Lexical },
        ];
        let vector = vec![
            seekpower_ranking::SearchResult { doc_id: 1, score: 10.0, result_type: seekpower_ranking::ResultType::Vector },
        ];
        let fused = fusion.fuse(lexical, vector);
        assert_eq!(fused.len(), 1);
        let expected = 2.0 / 61.0 + 1.0 / 61.0;
        assert!((fused[0].score - expected).abs() < 1e-6);
    }

    #[test]
    fn test_query_engine_lexical_search() {
        let engine = QueryEngine::new();
        let entries = vec![
            seekpower_posting::PostingEntry {
                doc_id: 1,
                field_id: 0,
                positions: vec![1, 5, 10],
            },
            seekpower_posting::PostingEntry {
                doc_id: 2,
                field_id: 0,
                positions: vec![2],
            },
        ];
        let block = seekpower_posting::PostingCodec::encode(&entries);
        let posting_refs = vec![&block];
        let delete_set = std::collections::HashSet::new();
        let boosts = vec![1.0];
        let idfs = vec![1.0];
        let results = engine.search_lexical(&posting_refs, &delete_set, QueryType::Union, 10.0, &boosts, &idfs, 10);
        assert_eq!(results.len(), 2);
        assert!(results[0].score > 0.0);
    }

    #[test]
    fn test_search_response_construction() {
        let response = SearchResponse {
            query: "test".to_string(),
            result_count: 2,
            results: vec![
                seekpower_ranking::SearchResult { doc_id: 1, score: 5.0, result_type: seekpower_ranking::ResultType::Lexical },
                seekpower_ranking::SearchResult { doc_id: 2, score: 3.0, result_type: seekpower_ranking::ResultType::Lexical },
            ],
        };
        assert_eq!(response.query, "test");
        assert_eq!(response.result_count, 2);
        assert_eq!(response.results.len(), 2);
    }
}
