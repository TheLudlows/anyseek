//! Query configuration and Record's private vector-component execution.
use crate::record::RecordError;
use crate::{index::VectorIndexArc, min_heap::Result,
    vector::{Embedding, Inference, Quantization, SearchVectorSegment},
    vector_similarity::{AnnMode, QuantizedVector, VectorSimilarity, normalize_f32, normalize_f32_simd,
        quantize_f32_to_i8, quantize_f32_to_i8_simd}};
use serde::{Serialize, Deserialize};
use std::ops::Range;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::time::Instant;

/// Logical visits shared by every opted-in consumer of one request. Existing
/// leases still bound concurrent memory and physical I/O independently.
pub(crate) struct QueryWork {
    remaining: AtomicUsize,
    reads: AtomicUsize,
    bytes: AtomicUsize,
    deadline: Instant,
    #[cfg(test)]
    statistics_postings: AtomicUsize,
    #[cfg(test)]
    statistics_usage: std::sync::Mutex<Option<LexicalStatisticsUsage>>,
    #[cfg(test)]
    pub(crate) vector_visits: VectorVisits,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct VectorVisits {
    pub(crate) levels: AtomicUsize,
    pub(crate) header_clusters: AtomicUsize,
    pub(crate) medoids: AtomicUsize,
    pub(crate) clusters: AtomicUsize,
    pub(crate) sealed_members: AtomicUsize,
    pub(crate) tail_members: AtomicUsize,
    pub(crate) vector_ranges: AtomicUsize,
    pub(crate) vector_bytes: AtomicUsize,
    pub(crate) remaining_after_medoids: AtomicUsize,
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LexicalStatisticsUsage {
    pub(crate) initial_visits: usize,
    pub(crate) initial_ranges: usize,
    pub(crate) initial_bytes: usize,
    pub(crate) visits: usize,
    pub(crate) postings: usize,
    pub(crate) ranges: usize,
    pub(crate) bytes: usize,
}

tokio::task_local! { static QUERY_READS: Arc<QueryWork>; }

impl QueryWork {
    pub(crate) fn new(visits: usize, reads: usize, bytes: usize, deadline: Instant) -> Self {
        Self {
            remaining: AtomicUsize::new(visits),
            reads: AtomicUsize::new(reads),
            bytes: AtomicUsize::new(bytes),
            deadline,
            #[cfg(test)]
            statistics_postings: AtomicUsize::new(0),
            #[cfg(test)]
            statistics_usage: std::sync::Mutex::new(None),
            #[cfg(test)]
            vector_visits: Default::default(),
        }
    }

    /// Meter synchronous positioned reads on this request-owned worker.
    pub(crate) fn with_reads<T>(self: Arc<Self>, work: impl FnOnce() -> T) -> T {
        QUERY_READS.sync_scope(self, work)
    }

    pub(crate) fn current() -> Option<Arc<Self>> {
        QUERY_READS.try_with(Arc::clone).ok()
    }

    /// Check a range before allocating its buffer or entering I/O admission.
    /// The file leaf still charges the range when it starts the positioned read.
    pub(crate) fn check_read(bytes: usize) -> std::io::Result<bool> {
        QUERY_READS
            .try_with(|work| {
                work.charge(0).map_err(std::io::Error::other)?;
                if work.reads.load(Ordering::Relaxed) == 0
                    || work.bytes.load(Ordering::Relaxed) < bytes
                {
                    return Err(Self::read_budget_error());
                }
                Ok(true)
            })
            .unwrap_or(Ok(false))
    }

    fn read_budget_error() -> std::io::Error {
        std::io::Error::other(RecordError::Rejected(
            "shared query positioned read budget exhausted".into(),
        ))
    }

    pub(crate) fn charge_read(bytes: usize) -> std::io::Result<()> {
        QUERY_READS
            .try_with(|work| {
                work.charge(0).map_err(std::io::Error::other)?;
                for (counter, amount) in [(&work.reads, 1), (&work.bytes, bytes)] {
                    counter
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                            remaining.checked_sub(amount)
                        })
                        .map_err(|_| Self::read_budget_error())?;
                }
                Ok(())
            })
            .unwrap_or(Ok(()))
    }

    pub(crate) fn charge(&self, visits: usize) -> std::result::Result<(), RecordError> {
        if Instant::now() >= self.deadline {
            return Err(RecordError::Busy);
        }
        if visits == 0 {
            return Ok(());
        }
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(visits)
            })
            .map_err(|_| RecordError::Rejected("shared query work budget exhausted".into()))?;
        Ok(())
    }

    pub(crate) fn charge_statistics_postings(
        &self,
        postings: usize,
    ) -> std::result::Result<(), RecordError> {
        self.charge(postings)?;
        #[cfg(test)]
        self.statistics_postings
            .fetch_add(postings, Ordering::Relaxed);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn record_lexical_statistics(
        &self,
        before_visits: usize,
        (reads, bytes): (usize, usize),
    ) {
        let (after_reads, after_bytes) = self.remaining_io();
        *self.statistics_usage.lock().unwrap() = Some(LexicalStatisticsUsage {
            initial_visits: before_visits,
            initial_ranges: reads,
            initial_bytes: bytes,
            visits: before_visits - self.remaining(),
            postings: self.statistics_postings.swap(0, Ordering::Relaxed),
            ranges: reads - after_reads,
            bytes: bytes - after_bytes,
        });
    }

    #[cfg(test)]
    pub(crate) fn take_lexical_statistics(&self) -> Option<LexicalStatisticsUsage> {
        self.statistics_usage.lock().unwrap().take()
    }

    #[cfg(test)]
    pub(crate) fn remaining(&self) -> usize {
        self.remaining.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn remaining_io(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

/// Stable insertion sort with one shared work unit per comparison/swap.
/// A refusal leaves an intermediate order that the caller must discard.
pub(crate) fn sort_with_work<T>(
    items: &mut [T],
    work: &QueryWork,
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
) -> std::result::Result<(), RecordError> {
    for end in 1..items.len() {
        for slot in (1..=end).rev() {
            work.charge(1)?;
            if !compare(&items[slot - 1], &items[slot]).is_gt() {
                break;
            }
            items.swap(slot - 1, slot);
        }
    }
    Ok(())
}

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

/// Weighted rank fusion for a mixed event-time query.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TimeRrfOptions {
    /// Finite positive smoothing constant in the zero-based rank formula.
    pub constant: f32,
    /// Finite nonnegative event-time weight.
    pub time_weight: f32,
    /// Finite nonnegative lexical weight.
    pub keyword_weight: f32,
    /// Finite nonnegative exact-vector weight when a vector is supplied.
    pub vector_weight: f32,
}

impl Default for TimeRrfOptions {
    fn default() -> Self {
        let rrf = RrfOptions::default();
        Self {
            constant: rrf.constant,
            time_weight: 1.0,
            keyword_weight: rrf.keyword_weight,
            vector_weight: rrf.vector_weight,
        }
    }
}

impl TimeRrfOptions {
    pub(crate) fn validate(self, lexical: bool, vector: bool) -> std::result::Result<(), String> {
        if !self.constant.is_finite() || self.constant <= 0.0 {
            return Err("RRF constant must be finite and positive".into());
        }
        let weights = [self.time_weight, self.keyword_weight, self.vector_weight];
        if weights
            .iter()
            .any(|weight| !weight.is_finite() || *weight < 0.0)
        {
            return Err("RRF weights must be finite and nonnegative".into());
        }
        let enabled = [
            self.time_weight,
            if lexical { self.keyword_weight } else { 0.0 },
            if vector { self.vector_weight } else { 0.0 },
        ];
        if enabled.iter().all(|&weight| weight == 0.0) {
            return Err("at least one participating RRF weight must be positive".into());
        }
        validate_rrf_maximum(self.constant, &enabled)
    }
}

/// Shared zero-based rank arithmetic; callers validate finite weight/constant.
pub(crate) fn rrf_contribution(weight: f32, constant: f32, rank: usize) -> f32 {
    weight / (constant + rank as f32)
}

// Check the maximum in the same f32 precision and lane order as fusion.
pub(crate) fn validate_rrf_maximum(constant: f32, enabled_weights: &[f32]) -> std::result::Result<(), String> {
    let max = enabled_weights.iter()
        .map(|&weight| rrf_contribution(weight, constant, 0))
        .sum::<f32>();
    if !max.is_finite() {
        return Err("RRF maximum fused score must be finite".into());
    }
    Ok(())
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
        validate_rrf_maximum(self.constant, &[self.keyword_weight, self.vector_weight])?;
        if self.keyword_candidates == Some(0) || self.vector_candidates == Some(0) {
            return Err("RRF candidate budgets must be positive".into());
        }
        Ok(())
    }
}

#[cfg(test)]
#[test]
fn rrf_maximum_checks_all_enabled_rank_zero_contributions() {
    let weight = 1.2e38f32;
    let legacy = RrfOptions { constant: 1.0, keyword_weight: weight, vector_weight: weight, ..RrfOptions::default() };
    assert!(RrfOptions::default().validate().is_ok());
    assert!(legacy.validate().is_ok());
    assert!(validate_rrf_maximum(1.0, &[weight, weight, weight]).is_err());
    assert!(RrfOptions { keyword_weight: f32::MAX, vector_weight: 0.0, ..legacy }.validate().is_ok());
    assert!(RrfOptions { keyword_weight: f32::MAX, ..legacy }.validate().is_err());
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
    index: &VectorIndexArc,
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
    work: Option<Arc<QueryWork>>,
) -> std::result::Result<ResultObject, RecordError> {
        if let Some(work) = &work { work.charge(0)?; }
        if !matches!(search_mode, SearchMode::Vector { .. }) {
            return Err(RecordError::Rejected("vector component cannot execute lexical queries".into()));
        }
        let index_ref = index.read().await;
        if let Err(error) =
            crate::metadata_filter::validate_filters(&index_ref.schema_map, &facet_filter)
        {
            return Err(RecordError::Rejected(error));
        }
        if let Err(error) = crate::vector::validate_query_vector(
            &index_ref.meta.inference,
            &search_mode,
            query_vector.as_ref(),
        ) {
            return Err(RecordError::Rejected(error));
        }
        if length == 0 || offset.checked_add(length).is_none()
        {
            return Err(RecordError::Rejected("vector search requires a positive, bounded result window".into()));
        }
        let ann_work = work.as_deref().filter(|_| matches!(&search_mode,
            SearchMode::Vector { ann_mode, .. } if *ann_mode != AnnMode::All));
        if let Some(work) = ann_work {
            if !field_filter.is_empty() || !result_sort.is_empty() {
                return Err(RecordError::Rejected("metered ANN requires score-ordered Top-K over the configured vector fields".into()));
            }
            let SearchMode::Vector { ann_mode: AnnMode::Nprobe(probes), .. } = &search_mode else {
                return Err(RecordError::Rejected("metered ANN requires a positive nprobe".into()));
            };
            let Some(Embedding::F32(values)) = &query_vector else {
                return Err(RecordError::Rejected("metered ANN requires an unquantized F32 embedding".into()));
            };
            if *probes == 0 || index_ref.quantization != Quantization::None {
                return Err(RecordError::Rejected("metered ANN requires a positive nprobe and unquantized F32 vectors".into()));
            }
            // Charge normalization and per-shard copies before doing either.
            let units = values.len().div_ceil(64);
            work.charge(2 * units + index_ref.segments.len().saturating_mul(1 + units))?;
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
                                index_ref.segments[0].vector.read().await.max_vector_value == f32::MIN;
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
                                    index_ref.segments[0].vector.read().await.min_vector_value;
                                let mut max_vector_value =
                                    index_ref.segments[0].vector.read().await.max_vector_value;
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
                                index_ref.segments[0].vector.read().await.max_vector_value == f32::MIN;
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
                                    index_ref.segments[0].vector.read().await.min_vector_value;
                                let mut max_vector_value =
                                    index_ref.segments[0].vector.read().await.max_vector_value;
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
                return Err(RecordError::Rejected("query_vector is required for Vector/Hybrid search".into()));
            })
        } else {
            None
        };


        let SearchMode::Vector { similarity_threshold, ann_mode } = search_mode else { unreachable!() };
        let segment_count = index_ref.segments.len();
        let _merge_workspace = if work.is_some() {
            let bytes = (segment_count as u64).checked_mul((offset + length) as u64)
                .and_then(|rows| rows.checked_mul(128))
                .ok_or_else(|| RecordError::Rejected("vector result workspace overflow".into()))?;
            Some(crate::record::build_budget::admit_memory(bytes, crate::resources::Scope::current().as_ref())?)
        } else {
            None
        };
        let jobs = index_ref.segments.iter().enumerate().map(|(slot, shard)| {
            let shard = shard.vector.clone();
            let query = query_vector.clone();
            let ann = ann_mode.clone();
            let fields = field_filter.clone();
            let filters = facet_filter.clone();
            let work = work.clone();
            async move {
                #[cfg(test)] crate::record::query_test_gate().await;
                let mut result = shard.search_vector_segment(query, offset + length, include_uncommitted,
                    similarity_threshold, ann, fields, filters, work.as_deref()).await?;
                for hit in &mut result.results { hit.doc_id = hit.doc_id * segment_count + slot; }
                Ok(result)
            }
        });
        let channels = if work.is_some() {
            // The strict scorer runs on the request's owned blocking worker.
            // A budget failure drops pending shard futures, with no detached work.
            futures::future::try_join_all(jobs).await?
        } else {
            let tasks: Vec<_> = jobs.map(crate::resources::spawn).collect();
            let mut channels = Vec::new();
            let mut failure = None;
            for task in tasks {
                match task.await {
                    Ok(Ok(channel)) => channels.push(channel),
                    Ok(Err(error)) => { failure.get_or_insert(error); }
                    Err(error) => { failure.get_or_insert(RecordError::Storage(error.to_string())); }
                }
            }
            if let Some(error) = failure { return Err(error); }
            channels
        };
        let mut result = ResultObject { original_query: query_string.clone(), query: query_string,
            ..Default::default() };
        if let Some(work) = &work {
            let visits = channels.iter().try_fold(0usize, |total, channel| total.checked_add(channel.results.len()))
                .ok_or_else(|| RecordError::Rejected("vector merge work overflow".into()))?;
            work.charge(visits)?;
        }
        let mut counts = Vec::with_capacity(channels.len());
        for mut channel in channels {
            counts.push(channel.result_count_total);
            result.result_count_total += channel.result_count_total;
            result.observed_vector_count += channel.observed_vector_count;
            result.observed_cluster_count += channel.observed_cluster_count;
            if result_type != ResultType::Count {
                result.results.append(&mut channel.results);
            }
        }
        let shards = futures::future::join_all(index_ref.segments.iter().map(|segment| segment.vector.read())).await;
        if let Some(work) = &work { work.charge(0)?; }
        let compare = |a: &Result, b: &Result| crate::min_heap::rank_with_segments(&shards,b.score,b.doc_id,a.score,a.doc_id);
        if let Some(work) = ann_work {
            sort_with_work(&mut result.results, work, compare)?;
        } else {
            result.results.sort_by(compare);
        }
        result.results.truncate(offset + length);
        let sorts = result_sort.iter().filter_map(|sort| Some(ResultSortIndex {
            idx: match sort.field.as_str() { "_id" => usize::MAX, "_score" => usize::MAX-1,
                _ => *shards[0].facets_map.get(&sort.field)? }, order: sort.order.clone(),
        })).collect::<Vec<_>>();
        if !sorts.is_empty() {
            result.results = crate::min_heap::sort_results(&shards, false, &sorts, result.results)?;
        }
        result.results = result.results.into_iter().skip(offset).take(length).collect();
        result.result_count = result.results.len();
        if let Some(work) = &work { work.charge(0)?; }
        component_counts.extend(counts);
        Ok(result)
}
