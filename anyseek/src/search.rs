//! Query configuration and Record's private vector-component execution.
use crate::{INDEX_RUNTIME, index::IndexArc, min_heap::{Result, result_ordering_root},
    vector::{Embedding, Inference, Quantization, SearchVectorShard},
    vector_similarity::{AnnMode, QuantizedVector, VectorSimilarity, normalize_f32, normalize_f32_simd,
        quantize_f32_to_i8, quantize_f32_to_i8_simd}};
use serde::{Serialize, Deserialize};
use std::ops::Range;
/// Specifies the default QueryType: The following query types are supported:
/// - **Union** (OR, disjunction),
/// - **Intersection** (AND, conjunction),
/// - **Phrase** (""),
/// - **Not** (-).
///
/// The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
/// Intersection, AND `+`
///
/// ```rust ,no_run
/// use anyseek::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="+red +apple".to_string();
/// ```
/// ```rust ,no_run
/// use anyseek::search::QueryType;
/// let query_type=QueryType::Intersection;
/// let query_string="red apple".to_string();
/// ```
/// Union, OR
/// ```rust ,no_run
/// use anyseek::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="red apple".to_string();
/// ```
/// Phrase `""`
/// ```rust ,no_run
/// use anyseek::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="\"red apple\"".to_string();
/// ```
/// ```rust ,no_run
/// use anyseek::search::QueryType;
/// let query_type=QueryType::Phrase;
/// let query_string="red apple".to_string();
/// ```
/// Except, minus, NOT `-`
/// ```rust ,no_run
/// use anyseek::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="apple -red".to_string();
/// ```
/// Mixed phrase and intersection
/// ```rust ,no_run
/// use anyseek::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="+\"the who\" +uk".to_string();
/// ```
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum QueryType {
    /// Union (OR, disjunction)
    #[default]
    Union = 0,
    /// Intersection (AND, conjunction)
    Intersection = 1,
    /// Phrase ("")
    Phrase = 2,
    /// Not (-)
    Not = 3,
}

/// Specifies the default QueryMode: The following query modes are supported:
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum SearchMode {
    /// Lexical search mode: Search results are retrieved based on exact matches of query terms with the indexed terms.
    #[default]
    Lexical,
    /// Vector search mode: Search results are retrieved based on the similarity of query vectors with the indexed vectors.
    Vector {
        /// Include only vectors with similarity scores above the specified threshold
        /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
        /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
        similarity_threshold: Option<f32>,
        /// Specifies in which clusters to search for ANN results.
        ann_mode: AnnMode,
    },
    /// Hybrid search mode: Search results are retrieved based on a combination of lexical and vector search.
    /// The relevance score of search results is calculated based on RRF (Reciprocal Rank Fusion) of the result positions in lexical and vector search.
    Hybrid {
        /// optional threshold to filter out low similarity scores
        /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
        /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
        similarity_threshold: Option<f32>,
        /// Specifies in which clusters to search for ANN results.
        ann_mode: AnnMode,
        /// Fusion weights and candidate budgets; omitted JSON fields use defaults.
        #[serde(default)]
        rrf: RrfOptions,
    },
}

/// Weighted reciprocal rank fusion settings. Ranks start at zero; absent hits contribute zero.
#[derive(PartialEq, Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RrfOptions {
    /// Positive finite smoothing constant. One-based k=60 corresponds to 61 here.
    pub constant: f32,
    /// Finite nonnegative keyword contribution weight.
    pub keyword_weight: f32,
    /// Finite nonnegative vector contribution weight.
    pub vector_weight: f32,
    /// Positive global keyword candidate cap; None uses offset + length.
    pub keyword_candidates: Option<usize>,
    /// Positive global vector candidate cap; None uses offset + length.
    pub vector_candidates: Option<usize>,
}

impl Default for RrfOptions {
    fn default() -> Self {
        Self {
            constant: 0.6,
            keyword_weight: 1.0,
            vector_weight: 1.0,
            keyword_candidates: None,
            vector_candidates: None,
        }
    }
}

/// Shared zero-based rank arithmetic; callers validate finite weight/constant.
pub(crate) fn rrf_contribution(weight: f32, constant: f32, rank: usize) -> f32 {
    weight / (constant + rank as f32)
}

impl RrfOptions {
    /// Reject invalid inputs and combinations capable of overflowing the fused f32 score.
    /// One zero weight disables that channel's contribution; both zero is invalid.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !self.constant.is_finite() || self.constant <= 0.0 {
            return Err("RRF constant must be finite and positive".into());
        }
        if [self.keyword_weight, self.vector_weight].iter().any(|w| !w.is_finite() || *w < 0.0)
            || (self.keyword_weight == 0.0 && self.vector_weight == 0.0) {
            return Err("RRF weights must be finite, nonnegative, and not both zero".into());
        }
        if !(self.keyword_weight / self.constant + self.vector_weight / self.constant).is_finite() {
            return Err("RRF maximum fused score must be finite".into());
        }
        if self.keyword_candidates == Some(0) || self.vector_candidates == Some(0) {
            return Err("RRF candidate budgets must be positive".into());
        }
        Ok(())
    }
}

/// The following result types are supported:
/// - **Count** (count all results that match the query, but returning top-k results is not required)
/// - **Topk** (returns the top-k results per query, but counting all results that match the query is not required)
/// - **TopkCount** (returns the top-k results per query + count all results that match the query)
#[derive(Default, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum ResultType {
    /// Count all results that match the query, without returning top-k results
    Count = 0,
    /// Return the top-k results per query, without counting all results that match the query
    Topk = 1,
    /// Return the top-k results per query and count all results that match the query
    #[default]
    TopkCount = 2,
}

/// Explanation of one candidate channel; ranks are zero-based in its bounded window.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ChannelExplanation {
    /// lexical, vector, relation, or browse.
    pub channel: String,
    /// Position before final fusion pagination (not a corpus-wide recall guarantee).
    pub rank: usize,
    /// score, requested_order, stable_adjacency, or browse_order.
    pub rank_basis: String,
    /// Unmodified channel engine score; absent for adjacency/browse.
    pub raw_score: Option<f32>,
    /// Weighted reciprocal-rank contribution; absent outside fusion.
    pub rrf_contribution: Option<f32>,
}

/// Contains the results returned when searching the index.
#[derive(Default, Debug, Deserialize, Serialize, Clone)]
pub struct ResultObject {
    /// Per-returned-ordinal hybrid diagnostics, collected without altering ranking.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub explanations: std::collections::BTreeMap<usize, Vec<ChannelExplanation>>,
    /// Invalid query input. Callers must check this before consuming search results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Search query string
    pub original_query: String,
    /// Search query string, unchanged from the caller input
    pub query: String,
    /// Vector of lexical search query terms.
    pub query_terms: Vec<String>,
    /// Number of returned search results. Identical to results.len()
    pub result_count: usize,

    /// Total number of search results that match the query
    /// result_count_total is only accurate if result_type=TopkCount or ResultType=Count, but not for ResultType=Topk
    pub result_count_total: usize,

    ///number of vectors observed during search
    pub observed_vector_count: usize,

    ///number of clusters observed during search
    pub observed_cluster_count: usize,

    /// List of search results: doc ID and BM25 score
    pub results: Vec<Result>,
}

/// FacetFilter:
/// either numerical range facet filter (range start/end) or
/// string facet filter (vector of strings) at least one (boolean OR) must match.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum FacetFilter {
    /// Int range filter
    Int {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<i64>,
    },
    /// Timestamp range filter, Unix timestamp: the number of seconds since 1 January 1970
    Timestamp {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<i64>,
    },
    /// Float range filter
    Float {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<f64>,
    },
    /// String filter
    String {
        /// field name
        field: String,
        /// filter: array of facet string values
        filter: Vec<String>,
    },
    /// StringSet filter
    StringSet {
        /// field name
        field: String,
        /// filter: array of facet string values
        filter: Vec<String>,
    },
}

pub(crate) use crate::metadata_filter::FilterSparse;

/// Specifies the sort order for the search results.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum SortOrder {
    /// Ascending sort order
    Ascending = 0,
    /// Descending sort order
    Descending = 1,
}

/// Specifies the sort order for the search results.
#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ResultSort {
    /// name of the facet field to sort by
    pub field: String,
    /// Sort order: Ascending or Descending
    pub order: SortOrder,
}

/// Specifies the sort order for the search results.
#[derive(Clone, Serialize)]
pub(crate) struct ResultSortIndex {
    /// Index/ID of the facet field to sort by
    pub idx: usize,
    /// Sort order: Ascending or Descending
    pub order: SortOrder,
}

pub(crate) async fn search_with_counts(
    index: &IndexArc,
    query_string: String,
    query_vector: Option<Embedding>,
    search_mode: SearchMode,
    offset: usize,
    length: usize,
    result_type: ResultType,
    include_uncommitted: bool,
    field_filter: Vec<String>,
    facet_filter: Vec<FacetFilter>,
    result_sort: Vec<ResultSort>,
    component_counts: &mut Vec<usize>,
) -> ResultObject {
        if !matches!(search_mode, SearchMode::Vector { .. }) {
            return ResultObject { error: Some("vector component cannot execute lexical queries".into()), ..Default::default() };
        }
        let index_ref = index.read().await;
        if let Err(error) =
            crate::metadata_filter::validate_filters(&index_ref.schema_map, &facet_filter)
        {
            return ResultObject {
                error: Some(error),
                ..Default::default()
            };
        }
        if let Err(error) = crate::vector::validate_query_vector(
            &index_ref.meta.inference,
            &search_mode,
            query_vector.as_ref(),
        ) {
            return ResultObject {
                error: Some(error),
                ..Default::default()
            };
        }
        if length == 0 || offset.checked_add(length).is_none()
        {
            return ResultObject {
                error: Some("vector search requires a positive, bounded result window".into()),
                ..Default::default()
            };
        }
        let query_vector = if index_ref.is_vector_indexing
        {
            Some(if let Some(mut qv) = query_vector {
                if index_ref.vector_similarity == VectorSimilarity::Cosine
                    && matches!(index_ref.meta.inference, Inference::External { .. })
                    && let Embedding::F32(ref mut fvecs) = qv
                {
                    if index_ref.is_simd {
                        unsafe {
                            normalize_f32_simd(fvecs);
                        }
                    } else {
                        normalize_f32(fvecs);
                    }
                };

                if (index_ref.quantization == Quantization::ScalarQuantizationI8
                    || index_ref.quantization == Quantization::TurboQuantI8)
                    && let Embedding::F32(ref fvecs) = qv
                {
                    match (
                        index_ref.vector_similarity,
                        index_ref.quantization,
                        index_ref.is_simd,
                    ) {
                        (VectorSimilarity::Cosine, Quantization::ScalarQuantizationI8, true) => {
                            (unsafe { quantize_f32_to_i8_simd(fvecs) }, 1.0, 0.0, 0, 0)
                        }
                        (VectorSimilarity::Cosine, Quantization::ScalarQuantizationI8, false) => {
                            (quantize_f32_to_i8(fvecs), 1.0, 0.0, 0, 0)
                        }

                        (VectorSimilarity::Dot, Quantization::ScalarQuantizationI8, true) => {
                            let quantized_vector = QuantizedVector::new_scale_simd(fvecs);
                            (
                                Embedding::I8(quantized_vector.data),
                                quantized_vector.scale,
                                quantized_vector.norm,
                                0,
                                0,
                            )
                        }
                        (VectorSimilarity::Dot, Quantization::ScalarQuantizationI8, false) => {
                            let quantized_vector = QuantizedVector::new_scale(fvecs);
                            (
                                Embedding::I8(quantized_vector.data),
                                quantized_vector.scale,
                                quantized_vector.norm,
                                0,
                                0,
                            )
                        }
                        (VectorSimilarity::Euclidean, Quantization::ScalarQuantizationI8, true) => {
                            let non_affine =
                                index_ref.shard_vec[0].read().await.max_vector_value == f32::MIN;
                            if non_affine {
                                let quantized_vector = QuantizedVector::new_scale_norm_simd(fvecs);
                                (
                                    Embedding::I8(quantized_vector.data),
                                    quantized_vector.scale,
                                    quantized_vector.norm,
                                    0,
                                    0,
                                )
                            } else {
                                let mut min_vector_value =
                                    index_ref.shard_vec[0].read().await.min_vector_value;
                                let mut max_vector_value =
                                    index_ref.shard_vec[0].read().await.max_vector_value;
                                let quantized_vector = QuantizedVector::new_scale_norm_affine_simd(
                                    &mut min_vector_value,
                                    &mut max_vector_value,
                                    fvecs,
                                );
                                (
                                    Embedding::I8(quantized_vector.data),
                                    quantized_vector.scale,
                                    quantized_vector.norm,
                                    quantized_vector.zero_point,
                                    quantized_vector.sum_q,
                                )
                            }
                        }

                        (_, Quantization::TurboQuantI8, true) => {
                            let quantized_vector =
                                index_ref.turbo_quant.quantize_f32_i8_simd(fvecs);

                            (
                                Embedding::I8(quantized_vector.data),
                                quantized_vector.scale,
                                quantized_vector.norm,
                                quantized_vector.zero_point,
                                quantized_vector.sum_q,
                            )
                        }
                        (
                            VectorSimilarity::Euclidean,
                            Quantization::ScalarQuantizationI8,
                            false,
                        ) => {
                            let non_affine =
                                index_ref.shard_vec[0].read().await.max_vector_value == f32::MIN;
                            if non_affine {
                                let quantized_vector = QuantizedVector::new_scale_norm(fvecs);
                                (
                                    Embedding::I8(quantized_vector.data),
                                    quantized_vector.scale,
                                    quantized_vector.norm,
                                    0,
                                    0,
                                )
                            } else {
                                let mut min_vector_value =
                                    index_ref.shard_vec[0].read().await.min_vector_value;
                                let mut max_vector_value =
                                    index_ref.shard_vec[0].read().await.max_vector_value;
                                let quantized_vector = QuantizedVector::new_scale_norm_affine(
                                    &mut min_vector_value,
                                    &mut max_vector_value,
                                    fvecs,
                                );
                                (
                                    Embedding::I8(quantized_vector.data),
                                    quantized_vector.scale,
                                    quantized_vector.norm,
                                    quantized_vector.zero_point,
                                    quantized_vector.sum_q,
                                )
                            }
                        }

                        (_, Quantization::TurboQuantI8, false) => {
                            let quantized_vector = index_ref.turbo_quant.quantize_f32_i8(fvecs);
                            (
                                Embedding::I8(quantized_vector.data),
                                quantized_vector.scale,
                                quantized_vector.norm,
                                quantized_vector.zero_point,
                                quantized_vector.sum_q,
                            )
                        }
                        (_, Quantization::None, _) => (qv, 0.0, 0.0, 0, 0),
                    }
                } else {
                    (qv, 0.0, 0.0, 0, 0)
                }
            } else {
                return ResultObject {
                    error: Some("query_vector is required for Vector/Hybrid search".into()),
                    ..Default::default()
                };
            })
        } else {
            None
        };


        let SearchMode::Vector { similarity_threshold, ann_mode } = search_mode else { unreachable!() };
        let shard_number = index_ref.shard_vec.len();
        let mut tasks = Vec::new();
        for (slot, shard) in index_ref.shard_vec.iter().enumerate() {
            let shard = shard.clone();
            let query = query_vector.clone();
            let ann = ann_mode.clone();
            let fields = field_filter.clone();
            let filters = facet_filter.clone();
            tasks.push(INDEX_RUNTIME.handle().spawn(crate::resources::inherit(async move {
                #[cfg(test)] crate::record::query_test_gate().await;
                let mut result = shard.search_vector_shard(query, offset + length, include_uncommitted,
                    similarity_threshold, ann, fields, filters).await;
                for hit in &mut result.results { hit.doc_id = hit.doc_id * shard_number + slot; }
                result
            })));
        }
        let mut result = ResultObject { original_query: query_string.clone(), query: query_string,
            ..Default::default() };
        let mut failure = None;
        for task in tasks {
            match task.await {
                Ok(mut channel) => {
                    component_counts.push(channel.result_count_total);
                    result.result_count_total += channel.result_count_total;
                    result.observed_vector_count += channel.observed_vector_count;
                    result.observed_cluster_count += channel.observed_cluster_count;
                    if let Some(error) = channel.error { failure.get_or_insert(error); }
                    if result_type != ResultType::Count { result.results.append(&mut channel.results); }
                }
                Err(error) => { failure.get_or_insert(error.to_string()); }
            }
        }
        if failure.is_some() { result.error = failure; result.results.clear(); return result; }
        let shards = futures::future::join_all(index_ref.shard_vec.iter().map(|shard| shard.read())).await;
        result.results.sort_by(|a,b| crate::min_heap::rank_with_shards(&shards,b.score,b.doc_id,a.score,a.doc_id));
        result.results.truncate(offset + length);
        let sorts = result_sort.iter().filter_map(|sort| Some(ResultSortIndex {
            idx: match sort.field.as_str() { "_id" => usize::MAX, "_score" => usize::MAX-1,
                _ => *shards[0].facets_map.get(&sort.field)? }, order: sort.order.clone(),
        })).collect::<Vec<_>>();
        if !sorts.is_empty() {
            result.results.sort_by(|a,b| result_ordering_root(&shards,shard_number,false,&sorts,*b,*a));
        }
        result.results = result.results.into_iter().skip(offset).take(length).collect();
        result.result_count = result.results.len();
        result
}
