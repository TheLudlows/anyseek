use crate::INDEX_RUNTIME;
use crate::index::{DOCUMENT_LENGTH_COMPRESSION, FieldType, NgramType, Shard, ShardArc};
use crate::iterator::{search_iterator_index, search_iterator_shard};
use crate::min_heap::{Result, rank_ordering, result_ordering_root};
use crate::tokenizer::tokenizer;
use crate::union::{union_docid_2, union_docid_3};
use crate::utils::{
    read_f32, read_f64, read_i8, read_i16, read_i32, read_i64, read_u8, read_u16, read_u32,
    read_u64,
};
#[cfg(feature = "vb")]
use crate::vector::ResultSource;
use crate::vector::{Embedding, Inference, Quantization, SearchVectorShard};
use crate::vector_similarity::{
    AnnMode, QuantizedVector, VectorSimilarity, normalize_f32, normalize_f32_simd,
    quantize_f32_to_i8, quantize_f32_to_i8_simd,
};
use crate::{
    index::{
        BlockObjectIndex, DUMMY_VEC, DUMMY_VEC_8, Index, IndexArc, LexicalSimilarity,
        MAX_POSITIONS_PER_TERM, NonUniquePostingListObjectQuery, NonUniqueTermObject,
        PostingListObjectIndex, PostingListObjectQuery, QueueObject, SPEEDUP_FLAG, SegmentIndex,
        TermObject, get_max_score,
    },
    intersection::intersection_blockid,
    min_heap::MinHeap,
    single::single_blockid,
    union::union_blockid,
};

use ahash::{AHashMap, AHashSet};
use itertools::Itertools;
use num::FromPrimitive;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::ops::Range;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{cmp, mem};

/// Specifies the default QueryType: The following query types are supported:
/// - **Union** (OR, disjunction),
/// - **Intersection** (AND, conjunction),
/// - **Phrase** (""),
/// - **Not** (-).
///
/// The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
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
    /// Opt-in weighted fusion. Existing Hybrid literals retain legacy defaults.
    WeightedHybrid {
        /// Optional vector similarity threshold.
        similarity_threshold: Option<f32>,
        /// Vector cluster probing policy, independent of candidate budgets.
        ann_mode: AnnMode,
        /// Validated fusion and candidate settings.
        rrf: RrfOptions,
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
    /// Positive per-shard keyword TopK budget and global fusion cap; None uses the legacy window.
    pub keyword_candidates: Option<usize>,
    /// Positive per-shard vector TopK budget and global fusion cap; None uses the legacy window.
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

pub(crate) struct SearchResult<'a> {
    pub topk_candidates: MinHeap<'a>,
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

/// FacetValue: Facet field value types
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub enum FacetValue {
    /// Boolean value
    Bool(bool),
    /// Unsigned 8-bit integer
    U8(u8),
    /// Unsigned 16-bit integer
    U16(u16),
    /// Unsigned 32-bit integer
    U32(u32),
    /// Unsigned 64-bit integer
    U64(u64),
    /// Signed 8-bit integer
    I8(i8),
    /// Signed 16-bit integer
    I16(i16),
    /// Signed 32-bit integer
    I32(i32),
    /// Signed 64-bit integer
    I64(i64),
    /// Unix timestamp: the number of seconds since 1 January 1970
    Timestamp(i64),
    /// 32-bit floating point number
    F32(f32),
    /// 64-bit floating point number
    F64(f64),
    /// String value
    String(String),
    /// String set value
    StringSet(Vec<String>),
    /// No value
    #[default]
    None,
}

impl Index {
    /// get_facet_value: Returns value from facet field for a doc_id even if schema stored=false (field not stored in document JSON).
    /// Facet fields are more compact than fields stored in document JSON.
    /// Strings are stored more compact as indices to a unique term dictionary. Numbers are stored binary, not as strings.
    /// Facet fields are faster because no document loading, decompression and JSON decoding is required.
    /// Facet fields are always memory mapped, internally always stored with fixed byte length layout, regardless of string size.
    #[inline]
    pub async fn get_facet_value(self: &Index, field: &str, doc_id: usize) -> FacetValue {
        let shard_id = doc_id % self.shard_number;
        let doc_id_shard = doc_id / self.shard_number;

        self.shard_vec[shard_id]
            .read()
            .await
            .get_facet_value_shard(field, doc_id_shard)
    }
}

impl Shard {
    #[inline]
    pub(crate) fn get_facet_value_shard(self: &Shard, field: &str, doc_id: usize) -> FacetValue {
        if let Some(field_idx) = self.facets_map.get(field) {
            match &self.facets[*field_idx].field_type {
                FieldType::U8 => {
                    let facet_value = &self.facets_file_mmap
                        [(self.facets_size_sum * doc_id) + self.facets[*field_idx].offset];
                    FacetValue::U8(*facet_value)
                }
                FieldType::U16 => {
                    let facet_value = read_u16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::U16(facet_value)
                }
                FieldType::U32 => {
                    let facet_value = read_u32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::U32(facet_value)
                }
                FieldType::U64 => {
                    let facet_value = read_u64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::U64(facet_value)
                }
                FieldType::I8 => {
                    let facet_value = read_i8(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I8(facet_value)
                }
                FieldType::I16 => {
                    let facet_value = read_i16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I16(facet_value)
                }
                FieldType::I32 => {
                    let facet_value = read_i32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I32(facet_value)
                }
                FieldType::I64 => {
                    let facet_value = read_i64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::I64(facet_value)
                }
                FieldType::Timestamp => {
                    let facet_value = read_i64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::Timestamp(facet_value)
                }
                FieldType::F32 => {
                    let facet_value = read_f32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::F32(facet_value)
                }
                FieldType::F64 => {
                    let facet_value = read_f64(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );
                    FacetValue::F64(facet_value)
                }

                FieldType::String16 => {
                    let facet_id = read_u16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );

                    let facet_value = self.facets[*field_idx]
                        .values
                        .get_index((facet_id).into())
                        .unwrap();

                    FacetValue::String(facet_value.1[0].clone())
                }

                FieldType::StringSet16 => {
                    let facet_id = read_u16(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );

                    let facet_value = self.facets[*field_idx]
                        .values
                        .get_index((facet_id).into())
                        .unwrap();

                    FacetValue::StringSet(facet_value.1.clone())
                }

                FieldType::String32 => {
                    let facet_id = read_u32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );

                    let facet_value = self.facets[*field_idx]
                        .values
                        .get_index(facet_id as usize)
                        .unwrap();

                    FacetValue::String(facet_value.1[0].clone())
                }

                FieldType::StringSet32 => {
                    let facet_id = read_u32(
                        &self.facets_file_mmap,
                        (self.facets_size_sum * doc_id) + self.facets[*field_idx].offset,
                    );

                    let facet_value = self.facets[*field_idx]
                        .values
                        .get_index(facet_id as usize)
                        .unwrap();

                    FacetValue::StringSet(facet_value.1.clone())
                }

                _ => FacetValue::None,
            }
        } else {
            FacetValue::None
        }
    }
}

/// FacetFilter:
/// either numerical range facet filter (range start/end) or
/// string facet filter (vector of strings) at least one (boolean OR) must match.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum FacetFilter {
    /// U8 range filter
    U8 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<u8>,
    },
    /// U16 range filter
    U16 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<u16>,
    },
    /// U32 range filter
    U32 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<u32>,
    },
    /// U64 range filter
    U64 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<u64>,
    },
    /// I8 range filter
    I8 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<i8>,
    },
    /// I16 range filter
    I16 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<i16>,
    },
    /// I32 range filter
    I32 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<i32>,
    },
    /// I64 range filter
    I64 {
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
    /// F32 range filter
    F32 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<f32>,
    },
    /// F64 range filter
    F64 {
        /// field name
        field: String,
        /// filter: range start, range end
        filter: Range<f64>,
    },
    /// String16 filter
    String16 {
        /// field name
        field: String,
        /// filter: array of facet string values
        filter: Vec<String>,
    },
    /// StringSet16 filter
    StringSet16 {
        /// field name
        field: String,
        /// filter: array of facet string values
        filter: Vec<String>,
    },
    /// String32 filter
    String32 {
        /// field name
        field: String,
        /// filter: array of facet string values
        filter: Vec<String>,
    },
    /// StringSet32 filter
    StringSet32 {
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

#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
/// Search the index for all indexed documents, both for committed and uncommitted documents.
/// The latter enables true realtime search: documents are available for search in exact the same millisecond they are indexed.
///
/// Arguments:
/// * `query_string`: query string `+` `-` `""` search operators are recognized.
/// * `query_type_default`: Specifiy default QueryType:
///   * **Union**, disjunction, OR,
///   * **Intersection**,  conjunction, AND, `+`,
///   * **Phrase** `""`,
///   * **Not**, except, minus `-`.
///
/// The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (`+` `-` `""`).
///
/// Boolean queries are specified in the search method either via the query_type parameter or via operator chars within the query parameter.
/// The interpretation of operator chars within the query string (set `query_type=QueryType::Union`) allows to specify advanced search operations via a simple search box.
///
/// Intersection, AND `+`
///
/// ```rust ,no_run
/// use seekpower::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="+red +apple".to_string();
/// ```
/// ```rust ,no_run
/// use seekpower::search::QueryType;
/// let query_type=QueryType::Intersection;
/// let query_string="red apple".to_string();
/// ```
/// Union, OR
/// ```rust ,no_run
/// use seekpower::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="red apple".to_string();
/// ```
/// Phrase `""`
/// ```rust ,no_run
/// use seekpower::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="\"red apple\"".to_string();
/// ```
/// ```rust ,no_run
/// use seekpower::search::QueryType;
/// let query_type=QueryType::Phrase;
/// let query_string="red apple".to_string();
/// ```
/// Except, minus, NOT `-`
/// ```rust ,no_run
/// use seekpower::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="apple -red".to_string();
/// ```
/// Mixed phrase and intersection
/// ```rust ,no_run
/// use seekpower::search::QueryType;
/// let query_type=QueryType::Union;
/// let query_string="+\"the who\" +uk".to_string();
/// ```
/// * `offset`: offset of search results to return.
/// * `length`: number of search results to return.
///   With length=0, resultType::TopkCount will be automatically downgraded to resultType::Count, returning the number of results only, without returning the results itself.
///
/// * `result_type`: type of search results to return: Count, Topk, TopkCount.
/// * `include_uncommitted`: true realtime search: include indexed documents which where not yet committed into search results.
/// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If set to Vec::new() then all indexed fields are searched.
/// * `facet_filter`: Search results are filtered to documents matching specific string values or numerical ranges in the facet fields. If set to Vec::new() then result are not facet filtered.
///   The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields at least one (boolean OR) of the stated values.
///   Examples:
///   facet_filter=vec![FacetFilter::String{field:"language".into(),filter:vec!["german".into()]},FacetFilter::String{field:"brand".into(),filter:vec!["apple".into(),"google".into()]}];
///   facet_filter=vec![FacetFilter::U8{field:"age".into(),filter: 21..65}];
///
/// * `result_sort`: Sort field and order: Search results are sorted by the specified facet field, either in ascending or descending order.
///   If no sort field is specified, then the search results are sorted by rank in descending order per default.
///   Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
///   The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
///   until the n-th field value is either not equal or the last field is reached.
///   A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
///   Sort is only enabled on facet fields that are defined in schema at create_index!
///   Examples:
///   result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending},ResultSort {field: "language".into(), order: SortOrder::Ascending}];
///
pub trait Search {
    /// Search the index for all indexed documents, both for committed and uncommitted documents.
    /// The latter enables true realtime search: documents are available for search in exact the same millisecond they are indexed.
    ///
    /// Arguments:
    /// * `query_string`: query string `+` `-` `""` search operators are recognized.
    /// * `query_type_default`: Specifiy default QueryType:
    ///   * **Union**, disjunction, OR,
    ///   * **Intersection**,  conjunction, AND, `+`,
    ///   * **Phrase** `""`,
    ///   * **Not**, except, minus `-`.
    ///
    /// The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (`+` `-` `""`).
    ///
    /// Boolean queries are specified in the search method either via the query_type parameter or via operator chars within the query parameter.
    /// The interpretation of operator chars within the query string (set `query_type=QueryType::Union`) allows to specify advanced search operations via a simple search box.
    ///
    /// Intersection, AND `+`
    ///
    /// ```rust ,no_run
    /// use seekpower::search::QueryType;
    /// let query_type=QueryType::Union;
    /// let query_string="+red +apple".to_string();
    /// ```
    /// ```rust ,no_run
    /// use seekpower::search::QueryType;
    /// let query_type=QueryType::Intersection;
    /// let query_string="red apple".to_string();
    /// ```
    /// Union, OR
    /// ```rust ,no_run
    /// use seekpower::search::QueryType;
    /// let query_type=QueryType::Union;
    /// let query_string="red apple".to_string();
    /// ```
    /// Phrase `""`
    /// ```rust ,no_run
    /// use seekpower::search::QueryType;
    /// let query_type=QueryType::Union;
    /// let query_string="\"red apple\"".to_string();
    /// ```
    /// ```rust ,no_run
    /// use seekpower::search::QueryType;
    /// let query_type=QueryType::Phrase;
    /// let query_string="red apple".to_string();
    /// ```
    /// Except, minus, NOT `-`
    /// ```rust ,no_run
    /// use seekpower::search::QueryType;
    /// let query_type=QueryType::Union;
    /// let query_string="apple -red".to_string();
    /// ```
    /// Mixed phrase and intersection
    /// ```rust ,no_run
    /// use seekpower::search::QueryType;
    /// let query_type=QueryType::Union;
    /// let query_string="+\"the who\" +uk".to_string();
    /// ```
    /// * `offset`: offset of search results to return.
    /// * `length`: number of search results to return.
    ///   With length=0, resultType::TopkCount will be automatically downgraded to resultType::Count, returning the number of results only, without returning the results itself.
    ///
    /// * `result_type`: type of search results to return: Count, Topk, TopkCount.
    /// * `include_uncommitted`: true realtime search: include indexed documents which where not yet committed into search results.
    /// * `field_filter`: Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If set to Vec::new() then all indexed fields are searched.
    /// * `facet_filter`: Search results are filtered to documents matching specific string values or numerical ranges in the facet fields. If set to Vec::new() then result are not facet filtered.
    ///   The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields at least one (boolean OR) of the stated values.
    ///   Examples:
    ///   facet_filter=vec![FacetFilter::String{field:"language".into(),filter:vec!["german".into()]},FacetFilter::String{field:"brand".into(),filter:vec!["apple".into(),"google".into()]}];
    ///   facet_filter=vec![FacetFilter::U8{field:"age".into(),filter: 21..65}];
    ///
    /// * `result_sort`: Sort field and order: Search results are sorted by the specified facet field, either in ascending or descending order.
    ///   If no sort field is specified, then the search results are sorted by rank in descending order per default.
    ///   Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
    ///   The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
    ///   until the n-th field value is either not equal or the last field is reached.
    ///   A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
    ///   Sort is only enabled on facet fields that are defined in schema at create_index!
    ///   Examples:
    ///   result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending},ResultSort {field: "language".into(), order: SortOrder::Ascending}];
    ///
    async fn search(
        &self,
        query_string: String,
        query_vector: Option<Embedding>,
        query_type_default: QueryType,
        search_mode: SearchMode,
        enable_empty_query: bool,
        offset: usize,
        length: usize,
        result_type: ResultType,
        include_uncommitted: bool,
        field_filter: Vec<String>,

        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    ) -> ResultObject;
}

impl Search for IndexArc {
    async fn search(
        &self,
        query_string: String,
        query_vector: Option<Embedding>,
        query_type_default: QueryType,
        search_mode: SearchMode,
        enable_empty_query: bool,
        offset: usize,
        length: usize,
        result_type: ResultType,
        include_uncommitted: bool,
        field_filter: Vec<String>,

        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    ) -> ResultObject {
        let rrf = match &search_mode {
            SearchMode::WeightedHybrid { rrf, .. } => *rrf,
            _ => RrfOptions::default(),
        };
        if let Err(error) = rrf.validate() {
            return ResultObject {
                error: Some(error),
                ..Default::default()
            };
        }
        let index_ref = self.read().await;
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
        if !matches!(search_mode, SearchMode::Lexical)
            && (length == 0 || offset.checked_add(length).is_none())
        {
            return ResultObject {
                error: Some("vector search requires a positive, bounded result window".into()),
                ..Default::default()
            };
        }
        let original_query = query_string.clone();

        if enable_empty_query
            && index_ref.shard_vec[0].read().await.active.is_none()
            && query_string.is_empty()
            && query_vector.is_none()
            && facet_filter.is_empty()
            && (result_sort.is_empty()
                || (result_sort.len() == 1
                    && (result_sort.first().unwrap().field == "_id"
                        || result_sort.first().unwrap().field == "_score")))
        {
            return search_iterator_index(
                self,
                offset,
                length,
                result_type,
                include_uncommitted,
                result_sort,
            )
            .await;
        }

        if index_ref.shard_number == 1 && matches!(search_mode, SearchMode::Lexical) {
            let mut result_object = index_ref.shard_vec[0]
                .search_lexical_shard(
                    query_string.clone(),
                    query_type_default,
                    enable_empty_query,
                    offset,
                    length,
                    result_type,
                    include_uncommitted,
                    field_filter,
                    facet_filter,
                    result_sort,
                )
                .await;
            result_object.original_query = original_query;
            result_object.query = query_string.clone();
            return result_object;
        }

        let mut result_object_list = Vec::new();
        let shard_number = index_ref.shard_number;
        let aggregate_results = result_type != ResultType::Count;

        let query_vector = if index_ref.is_vector_indexing
            && !matches!(search_mode, SearchMode::Lexical)
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

        for shard in index_ref.shard_vec.iter() {
            let query_string_clone = query_string.clone();
            let query_vector_clone = query_vector.clone();
            let shard_clone = shard.clone();
            let query_type_clone = query_type_default.clone();
            let query_mode_clone = search_mode.clone();
            let result_type_clone = result_type.clone();
            let field_filter_clone = field_filter.clone();

            let facet_filter_clone = facet_filter.clone();
            let result_sort_clone = result_sort.clone();
            let shard_id = shard.read().await.meta.id;

            result_object_list.push(INDEX_RUNTIME.handle().spawn(async move {
                #[cfg(test)]
                crate::record::query_test_gate().await;
                match query_mode_clone {
                    SearchMode::Lexical => {
                        let mut rlo_lexical = shard_clone
                            .search_lexical_shard(
                                query_string_clone,
                                query_type_clone,
                                enable_empty_query,
                                0,
                                offset + length,
                                result_type_clone,
                                include_uncommitted,
                                field_filter_clone,
                                facet_filter_clone,
                                result_sort_clone,
                            )
                            .await;

                        if aggregate_results {
                            for result in rlo_lexical.results.iter_mut() {
                                result.doc_id = (result.doc_id * shard_number) + shard_id as usize;
                            }
                        }
                        (Some(rlo_lexical), None)
                    }
                    SearchMode::Vector {
                        similarity_threshold,
                        ann_mode: cluster_search,
                    } => {
                        let mut rlo_vector = shard_clone
                            .search_vector_shard(
                                query_vector_clone,
                                offset + length,
                                include_uncommitted,
                                similarity_threshold,
                                cluster_search,
                                field_filter_clone,
                                facet_filter_clone,
                            )
                            .await;

                        if aggregate_results {
                            for result in rlo_vector.results.iter_mut() {
                                result.doc_id = (result.doc_id * shard_number) + shard_id as usize;
                            }
                        }
                        (None, Some(rlo_vector))
                    }
                    SearchMode::Hybrid { similarity_threshold, ann_mode }
                    | SearchMode::WeightedHybrid { similarity_threshold, ann_mode, .. } => {
                        let mut rlo_lexical = shard_clone
                            .search_lexical_shard(
                                query_string_clone.clone(),
                                query_type_clone,
                                enable_empty_query,
                                0,
                                rrf.keyword_candidates.unwrap_or(offset + length),
                                result_type_clone,
                                include_uncommitted,
                                field_filter_clone.clone(),
                                facet_filter_clone.clone(),
                                result_sort_clone,
                            )
                            .await;

                        if aggregate_results {
                            for result in rlo_lexical.results.iter_mut() {
                                result.doc_id = (result.doc_id * shard_number) + shard_id as usize;
                            }
                        }
                        let mut rlo_vector = shard_clone
                            .search_vector_shard(
                                query_vector_clone,
                                rrf.vector_candidates.unwrap_or(offset + length),
                                include_uncommitted,
                                similarity_threshold,
                                ann_mode,
                                field_filter_clone,
                                facet_filter_clone,
                            )
                            .await;

                        if aggregate_results {
                            for result in rlo_vector.results.iter_mut() {
                                result.doc_id = (result.doc_id * shard_number) + shard_id as usize;
                            }
                        }
                        (Some(rlo_lexical), Some(rlo_vector))
                    }
                }
            }));
        }

        let mut result_object: ResultObject = Default::default();

        let mut result_object_results_lexical: Vec<Result> = Vec::new();
        let mut result_object_results_vector: Vec<Result> = Vec::new();

        for result_object_shard in result_object_list {
            let mut rlo_shard_hybrid_options = result_object_shard.await.unwrap();

            match search_mode {
                SearchMode::Lexical => {
                    let rlo_shard_lexical = rlo_shard_hybrid_options.0.as_mut().unwrap();
                    if aggregate_results {
                        result_object_results_lexical.append(&mut rlo_shard_lexical.results)
                    };
                    result_object.result_count_total += rlo_shard_lexical.result_count_total;
                    if result_object.query_terms.is_empty() {
                        result_object.query_terms = rlo_shard_lexical.query_terms.clone()
                    };
                }
                SearchMode::Vector {
                    similarity_threshold: _,
                    ann_mode: _,
                } => {
                    let rlo_shard_vector = rlo_shard_hybrid_options.1.as_mut().unwrap();
                    if aggregate_results {
                        result_object_results_vector.append(&mut rlo_shard_vector.results)
                    };
                    result_object.observed_vector_count += rlo_shard_vector.observed_vector_count;
                    result_object.observed_cluster_count += rlo_shard_vector.observed_cluster_count;
                    result_object.result_count_total += rlo_shard_vector.result_count_total;
                    if result_object.query_terms.is_empty() {
                        result_object.query_terms = query_string
                            .to_lowercase()
                            .split_whitespace()
                            .map(|s| s.trim_matches(['\"', '+', '-']).to_string())
                            .filter(|s| !s.is_empty())
                            .collect::<Vec<String>>();
                    };
                }
                SearchMode::Hybrid { .. } | SearchMode::WeightedHybrid { .. } => {
                    let rlo_shard_lexical = rlo_shard_hybrid_options.0.as_mut().unwrap();
                    let rlo_shard_vector = rlo_shard_hybrid_options.1.as_mut().unwrap();
                    if aggregate_results {
                        result_object_results_lexical.append(&mut rlo_shard_lexical.results);
                        result_object_results_vector.append(&mut rlo_shard_vector.results);
                    }
                    result_object.result_count_total += rlo_shard_lexical
                        .result_count_total
                        .max(rlo_shard_vector.result_count_total);
                    result_object.observed_vector_count += rlo_shard_vector.observed_vector_count;
                    result_object.observed_cluster_count += rlo_shard_vector.observed_cluster_count;
                    if result_object.query_terms.is_empty() {
                        result_object.query_terms = rlo_shard_lexical.query_terms.clone()
                    };
                }
            };
        }
        let rank_shards = futures::future::join_all(index_ref.shard_vec.iter().map(|s| s.read())).await;
        let rank = |a, a_id, b, b_id| if rank_shards.len() == 1 {
            rank_shards[0].rank(a, a_id, b, b_id)
        } else { rank_ordering(a, a_id, b, b_id) };
        if aggregate_results {
            match search_mode {
                SearchMode::Lexical => {
                    #[cfg(feature = "vb")]
                    result_object_results_lexical
                        .iter_mut()
                        .for_each(|r| r.lexical_score = r.score);
                    #[cfg(feature = "vb")]
                    result_object_results_lexical
                        .iter_mut()
                        .for_each(|r| r.source = ResultSource::Lexical);

                    result_object.results = result_object_results_lexical;
                }
                SearchMode::Vector {
                    similarity_threshold: _,
                    ann_mode: _,
                } => {
                    result_object.results = result_object_results_vector;
                }
                SearchMode::Hybrid { .. } | SearchMode::WeightedHybrid { .. } => {
                    let k = rrf.constant;
                    let mut rrf_results: AHashMap<usize, Result> = AHashMap::new();
                    for (i, result) in result_object_results_lexical
                        .iter()
                        .sorted_by(|a, b| rank(b.score, b.doc_id, a.score, a.doc_id))
                        .take(if rrf.keyword_weight == 0.0 {
                            0
                        } else {
                            rrf.keyword_candidates.unwrap_or(usize::MAX)
                        })
                        .enumerate()
                    {
                        result_object.explanations.entry(result.doc_id).or_default().push(ChannelExplanation{channel:"lexical".into(),rank:i,rank_basis:"score".into(),raw_score:Some(result.score),rrf_contribution:Some(rrf_contribution(rrf.keyword_weight,k,i))});
                        rrf_results.insert(
                            result.doc_id,
                            Result {
                                doc_id: result.doc_id,
                                score: rrf_contribution(rrf.keyword_weight, k, i),
                                #[cfg(feature = "vb")]
                                lexical_score: result.score,
                                #[cfg(feature = "vb")]
                                source: ResultSource::Lexical,
                                ..Default::default()
                            },
                        );
                    }
                    for (i, result) in result_object_results_vector
                        .iter()
                        .sorted_by(|a, b| rank(b.score, b.doc_id, a.score, a.doc_id))
                        .take(if rrf.vector_weight == 0.0 {
                            0
                        } else {
                            rrf.vector_candidates.unwrap_or(usize::MAX)
                        })
                        .enumerate()
                    {
                        let rrf_score = rrf_contribution(rrf.vector_weight, k, i);
                        result_object.explanations.entry(result.doc_id).or_default().push(ChannelExplanation{channel:"vector".into(),rank:i,rank_basis:"score".into(),raw_score:Some(result.score),rrf_contribution:Some(rrf_score)});

                        #[cfg(feature = "vb")]
                        rrf_results
                            .entry(result.doc_id)
                            .and_modify(|e| {
                                e.score += rrf_score;

                                e.field_id = result.field_id;
                                e.chunk_id = result.chunk_id;
                                e.level_id = result.level_id;
                                e.shard_id = result.shard_id;
                                e.cluster_id = result.cluster_id;
                                e.cluster_score = result.cluster_score;
                                e.vector_score = result.vector_score;
                                e.source = ResultSource::Hybrid;
                            })
                            .or_insert(Result {
                                doc_id: result.doc_id,
                                score: rrf_score,

                                field_id: result.field_id,
                                chunk_id: result.chunk_id,
                                level_id: result.level_id,
                                shard_id: result.shard_id,
                                cluster_id: result.cluster_id,
                                cluster_score: result.cluster_score,
                                vector_score: result.vector_score,
                                lexical_score: 0.0,
                                source: ResultSource::Vector,
                            });

                        #[cfg(not(feature = "vb"))]
                        rrf_results
                            .entry(result.doc_id)
                            .and_modify(|e| {
                                e.score += rrf_score;
                            })
                            .or_insert(Result {
                                doc_id: result.doc_id,
                                score: rrf_score,
                            });
                    }
                    result_object.results = rrf_results.into_values().collect();
                }
            };
        }

        if aggregate_results {
            let mut result_sort_index: Vec<ResultSortIndex> = Vec::new();
            if !result_sort.is_empty() {
                for rs in result_sort.iter() {
                    if rs.field == "_id" {
                        result_sort_index.push(ResultSortIndex {
                            idx: usize::MAX,
                            order: rs.order.clone(),
                        });
                        continue;
                    }
                    if rs.field == "_score" {
                        result_sort_index.push(ResultSortIndex {
                            idx: usize::MAX - 1,
                            order: rs.order.clone(),
                        });
                        continue;
                    }

                    if let Some(idx) = rank_shards[0]
                        .facets_map
                        .get(&rs.field)
                    {
                        result_sort_index.push(ResultSortIndex {
                            idx: *idx,
                            order: rs.order.clone(),
                        });
                    }
                }
                result_object.results.sort_by(|a, b| {
                    result_ordering_root(
                        &rank_shards,
                        shard_number,
                        query_string.is_empty() && query_vector.is_none(),
                        &result_sort_index,
                        *b,
                        *a,
                    )
                });
            } else {
                if rank_shards.len() == 1 && rank_shards[0].active.is_some() {
                    result_object.results.sort_by(|a, b| rank(b.score, b.doc_id, a.score, a.doc_id));
                } else if query_string.is_empty() && query_vector.is_none() {
                    result_object
                        .results
                        .sort_by_key(|b| cmp::Reverse(b.doc_id));
                } else {
                    result_object
                        .results
                        .sort_by(|a, b| rank(b.score, b.doc_id, a.score, a.doc_id));
                }
            }

            if offset > 0 {
                result_object.results = if offset >= result_object.results.len() {
                    Vec::new()
                } else {
                    result_object.results.split_off(offset)
                };
            }

            if result_object.results.len() > length {
                result_object.results.truncate(length);
            }
            let returned:AHashSet<_>=result_object.results.iter().map(|r|r.doc_id).collect();
            result_object.explanations.retain(|id,_|returned.contains(id));
            result_object.result_count = result_object.results.len();
        }

        result_object.original_query = original_query;
        result_object.query = query_string.clone();

        result_object
    }
}

/// Non-recursive binary search of non-consecutive u64 values in a slice of bytes
#[inline(never)]
pub(crate) fn binary_search(
    byte_array: &[u8],
    len: usize,
    key_hash: u64,
    key_head_size: usize,
) -> i64 {
    let mut left = 0i64;
    let mut right = len as i64 - 1;
    while left <= right {
        let mid = (left + right) / 2;

        let pivot = read_u64(byte_array, mid as usize * key_head_size);
        match pivot.cmp(&key_hash) {
            cmp::Ordering::Equal => {
                return mid;
            }
            cmp::Ordering::Less => left = mid + 1,
            cmp::Ordering::Greater => right = mid - 1,
        }
    }
    -1
}

/// Decode posting_list_object and blocks on demand from mmap, instead keepping all posting_list_object and blocks for all keys in ram
#[inline(always)]
pub(crate) fn decode_posting_list_count(
    segment: &SegmentIndex,
    index: &Shard,
    key_hash1: u64,
    previous: bool,
) -> Option<u32> {
    let offset = if previous { 1 } else { 0 };

    let mut posting_count_list = 0u32;
    let mut found = false;

    if segment.byte_array_blocks_pointer.len() <= offset {
        return None;
    }

    let block_id_last = segment.byte_array_blocks_pointer.len() - 1 - offset;
    for pointer in segment
        .byte_array_blocks_pointer
        .iter()
        .take(block_id_last + 1)
    {
        let key_count = pointer.2 as usize;
        let byte_array =
            &index.index_file_mmap[pointer.0 - (key_count * index.key_head_size)..pointer.0];
        let key_index = binary_search(byte_array, key_count, key_hash1, index.key_head_size);

        if key_index >= 0 {
            found = true;
            let key_address = key_index as usize * index.key_head_size;
            let posting_count = read_u16(byte_array, key_address + 8);
            posting_count_list += posting_count as u32 + 1;
        }
    }

    if found {
        Some(posting_count_list)
    } else {
        None
    }
}

#[inline(always)]
pub(crate) fn decode_posting_list_counts(
    segment: &SegmentIndex,
    index: &Shard,
    key_hash1: u64,
) -> Option<(u32, u32, u32, u32)> {
    let mut posting_count_list = 0u32;
    let mut posting_count_ngram_1_compressed = 0;
    let mut posting_count_ngram_2_compressed = 0;
    let mut posting_count_ngram_3_compressed = 0;
    let mut posting_count_ngram_1 = 0;
    let mut posting_count_ngram_2 = 0;
    let mut posting_count_ngram_3 = 0;
    let mut found = false;

    let read_flag = key_hash1 & 0b111 > 0;

    if segment.byte_array_blocks_pointer.is_empty() {
        return None;
    }

    for pointer in segment.byte_array_blocks_pointer.iter() {
        let key_count = pointer.2 as usize;

        let byte_array =
            &index.index_file_mmap[pointer.0 - (key_count * index.key_head_size)..pointer.0];
        let key_index = binary_search(byte_array, key_count, key_hash1, index.key_head_size);

        if key_index >= 0 {
            found = true;
            let key_address = key_index as usize * index.key_head_size;
            let posting_count = read_u16(byte_array, key_address + 8);

            match index.key_head_size {
                20 => {}
                22 => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                    }
                }
                _ => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                        posting_count_ngram_3_compressed = read_u8(byte_array, key_address + 16);
                    }
                }
            }
            posting_count_list += posting_count as u32 + 1;
        }
    }

    if found {
        match index.key_head_size {
            20 => {}
            22 => {
                if read_flag {
                    posting_count_ngram_1 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                    posting_count_ngram_2 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                }
            }
            _ => {
                if read_flag {
                    posting_count_ngram_1 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                    posting_count_ngram_2 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                    posting_count_ngram_3 =
                        DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_3_compressed as usize];
                }
            }
        }

        Some((
            posting_count_list,
            posting_count_ngram_1,
            posting_count_ngram_2,
            posting_count_ngram_3,
        ))
    } else {
        None
    }
}

/// Decode posting_list_object and blocks on demand from mmap, instead keepping all posting_list_object and blocks for all keys in ram
#[inline(always)]
pub(crate) fn decode_posting_list_object(
    segment: &SegmentIndex,
    shard: &Shard,
    key_hash1: u64,
    calculate_score: bool,
) -> Option<PostingListObjectIndex> {
    let mut posting_count_list = 0u32;
    let mut max_list_score = 0.0;
    let mut blocks_owned: Vec<BlockObjectIndex> = Vec::new();
    let mut posting_count_ngram_1_compressed = 0;
    let mut posting_count_ngram_2_compressed = 0;
    let mut posting_count_ngram_3_compressed = 0;
    let mut posting_count_ngram_1 = 0;
    let mut posting_count_ngram_2 = 0;
    let mut posting_count_ngram_3 = 0;

    let read_flag = key_hash1 & 0b111 > 0;

    for (block_id, pointer) in segment.byte_array_blocks_pointer.iter().enumerate() {
        let key_count = pointer.2 as usize;
        let byte_array =
            &shard.index_file_mmap[pointer.0 - (key_count * shard.key_head_size)..pointer.0];
        let key_index = binary_search(byte_array, key_count, key_hash1, shard.key_head_size);

        if key_index >= 0 {
            let key_address = key_index as usize * shard.key_head_size;
            let posting_count = read_u16(byte_array, key_address + 8);

            let max_docid = read_u16(byte_array, key_address + 10);
            let max_p_docid = read_u16(byte_array, key_address + 12);
            match shard.key_head_size {
                20 => {}
                22 => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                    }
                }
                _ => {
                    if read_flag {
                        posting_count_ngram_1_compressed = read_u8(byte_array, key_address + 14);
                        posting_count_ngram_2_compressed = read_u8(byte_array, key_address + 15);
                        posting_count_ngram_3_compressed = read_u8(byte_array, key_address + 16);
                    }
                }
            }

            let pointer_pivot_p_docid = read_u16(byte_array, key_address + shard.key_head_size - 6);
            let compression_type_pointer =
                read_u32(byte_array, key_address + shard.key_head_size - 4);

            posting_count_list += posting_count as u32 + 1;

            let block_object_index = BlockObjectIndex {
                max_block_score: 0.0,
                block_id: block_id as u32,
                posting_count,
                max_docid,
                max_p_docid,
                pointer_pivot_p_docid,
                compression_type_pointer,
            };
            blocks_owned.push(block_object_index);
        }
    }

    if !blocks_owned.is_empty() {
        if calculate_score {
            match shard.key_head_size {
                20 => {}
                22 => {
                    if read_flag {
                        posting_count_ngram_1 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                        posting_count_ngram_2 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                    }
                }
                _ => {
                    if read_flag {
                        posting_count_ngram_1 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_1_compressed as usize];
                        posting_count_ngram_2 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_2_compressed as usize];
                        posting_count_ngram_3 =
                            DOCUMENT_LENGTH_COMPRESSION[posting_count_ngram_3_compressed as usize];
                    }
                }
            }

            let ngram_type =
                FromPrimitive::from_u64(key_hash1 & 0b111).unwrap_or(NgramType::SingleTerm);

            for block in blocks_owned.iter_mut() {
                block.max_block_score = get_max_score(
                    shard,
                    segment,
                    posting_count_ngram_1,
                    posting_count_ngram_2,
                    posting_count_ngram_3,
                    posting_count_list,
                    block.block_id as usize,
                    block.max_docid as usize,
                    block.max_p_docid as usize,
                    block.pointer_pivot_p_docid as usize,
                    block.compression_type_pointer,
                    &ngram_type,
                );

                if block.max_block_score > max_list_score {
                    max_list_score = block.max_block_score
                }
            }
        }

        let posting_list_object_index = PostingListObjectIndex {
            posting_count: posting_count_list,
            posting_count_ngram_1,
            posting_count_ngram_2,
            posting_count_ngram_3,
            max_list_score,
            blocks: blocks_owned,
            ..Default::default()
        };

        Some(posting_list_object_index)
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(async_fn_in_trait)]
pub(crate) trait SearchLexicalShard {
    async fn search_lexical_shard(
        &self,
        query_string: String,
        query_type_default: QueryType,
        enable_empty_query: bool,
        offset: usize,
        length: usize,
        result_type: ResultType,
        include_uncommitted: bool,
        field_filter: Vec<String>,

        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    ) -> ResultObject;
}

impl SearchLexicalShard for ShardArc {
    async fn search_lexical_shard(
        &self,
        query_string: String,
        query_type_default: QueryType,
        enable_empty_query: bool,
        offset: usize,
        length: usize,
        result_type: ResultType,
        include_uncommitted: bool,
        field_filter: Vec<String>,

        facet_filter: Vec<FacetFilter>,
        result_sort: Vec<ResultSort>,
    ) -> ResultObject {
        let mut result_object: ResultObject = Default::default();
        let shard_ref = self.read().await;
        let browse = enable_empty_query && query_string.is_empty();
        if !shard_ref.is_lexical_indexing && !browse {
            return result_object;
        }

        let mut query_type_mut = query_type_default;

        let mut result_type = result_type;
        if length == 0 && result_type != ResultType::Count {
            if result_type == ResultType::Topk {
                return result_object;
            }
            result_type = ResultType::Count;
        }

        if shard_ref.segments_index.is_empty() && !browse {
            return result_object;
        }
        let mut field_filter_set: AHashSet<u16> = AHashSet::new();
        for item in field_filter.iter() {
            match shard_ref.schema_map.get(item) {
                Some(value) => {
                    if value.index_lexical {
                        field_filter_set.insert(value.indexed_field_id as u16);
                    }
                }
                None => {
                    log::warn!("field not found: {}", item)
                }
            }
        }

        let mut result_sort_index: Vec<ResultSortIndex> = Vec::new();
        if !result_sort.is_empty() && result_type != ResultType::Count {
            for rs in result_sort.iter() {
                if rs.field == "_id" {
                    result_sort_index.push(ResultSortIndex {
                        idx: usize::MAX,
                        order: rs.order.clone(),
                    });
                    continue;
                }
                if rs.field == "_score" {
                    result_sort_index.push(ResultSortIndex {
                        idx: usize::MAX - 1,
                        order: rs.order.clone(),
                    });
                    continue;
                }

                if let Some(idx) = shard_ref.facets_map.get(&rs.field) {
                    result_sort_index.push(ResultSortIndex {
                        idx: *idx,
                        order: rs.order.clone(),
                    });
                }
            }
        }

        let heap_size = if result_type != ResultType::Count {
            cmp::min(offset + length, shard_ref.indexed_doc_count)
        } else {
            0
        };
        let mut search_result = SearchResult {
            topk_candidates: MinHeap::new(
                heap_size,
                &shard_ref,
                query_string.is_empty(),
                &result_sort_index,
            ),
        };

        let facet_filter_sparse =
            match crate::metadata_filter::compile_filters(&shard_ref, &facet_filter) {
                Ok(filters) => filters,
                Err(error) => {
                    return ResultObject {
                        error: Some(error),
                        ..Default::default()
                    };
                }
            };

        let result_count_arc = Arc::new(AtomicUsize::new(0));
        let result_count_uncommitted_arc = Arc::new(AtomicUsize::new(0));

        'search_blocks: {
            // Browsing needs only live IDs and metadata, including on vector-only
            // shards. Keep the regular candidate heap so stable Record ordering,
            // filters, deletion checks and pagination apply before truncation.
            if browse {
                search_iterator_shard(
                    &shard_ref,
                    result_type,
                    include_uncommitted,
                    &result_count_arc,
                    &mut search_result,
                    offset + length,
                    &facet_filter_sparse,
                )
                .await;
                break 'search_blocks;
            }
            let mut unique_terms: AHashMap<String, TermObject> = AHashMap::new();
            let mut non_unique_terms: Vec<NonUniqueTermObject> = Vec::new();
            let mut nonunique_terms_count = 0u32;
            tokenizer(
                &shard_ref,
                &query_string,
                &mut unique_terms,
                &mut non_unique_terms,
                shard_ref.meta.tokenizer,
                shard_ref.segment_number_mask1,
                &mut nonunique_terms_count,
                u16::MAX as u32,
                MAX_POSITIONS_PER_TERM,
                true,
                &mut query_type_mut,
                shard_ref.meta.ngram_indexing,
                0,
                1,
            )
            .await;

            if include_uncommitted && shard_ref.uncommitted {
                shard_ref.search_lexical_shard_uncommitted(
                    &unique_terms,
                    &non_unique_terms,
                    &mut query_type_mut,
                    &result_type,
                    &field_filter_set,
                    &facet_filter_sparse,
                    &mut search_result,
                    &result_count_uncommitted_arc,
                    offset + length,
                );
            }
            let mut query_list_map: AHashMap<u64, PostingListObjectQuery> = AHashMap::new();
            let mut query_list: Vec<PostingListObjectQuery>;

            let mut not_query_list_map: AHashMap<u64, PostingListObjectQuery> = AHashMap::new();
            let mut not_query_list: Vec<PostingListObjectQuery>;

            let mut non_unique_query_list: Vec<NonUniquePostingListObjectQuery> = Vec::new();
            let mut preceding_ngram_count = 0;

            let mut blocks_vec: Vec<Vec<BlockObjectIndex>> = Vec::new();

            let mut not_found_terms_hashset: AHashSet<u64> = AHashSet::new();

            for non_unique_term in non_unique_terms.iter() {
                let term = unique_terms.get(&non_unique_term.term).unwrap();
                let key0: u32 = term.key0;
                let key_hash: u64 = term.key_hash;
                let term_no_diacritics_umlaut_case = &non_unique_term.term;

                let mut idf = 0.0;
                let mut idf_ngram1 = 0.0;
                let mut idf_ngram2 = 0.0;
                let mut idf_ngram3 = 0.0;

                let mut term_index_unique = 0;
                if non_unique_term.op == QueryType::Not {
                    let query_list_map_len = not_query_list_map.len();
                    let not_query_list_option = not_query_list_map.get(&key_hash);
                    if not_query_list_option.is_none()
                        && !not_found_terms_hashset.contains(&key_hash)
                    {
                        let posting_count;
                        let max_list_score;
                        let blocks;
                        let blocks_len;
                        let found_plo = {
                            let posting_list_object_index_option = decode_posting_list_object(
                                &shard_ref.segments_index[key0 as usize],
                                &shard_ref,
                                key_hash,
                                false,
                            );
                            if let Some(plo) = posting_list_object_index_option {
                                posting_count = plo.posting_count;
                                max_list_score = plo.max_list_score;
                                blocks = &DUMMY_VEC;
                                blocks_len = plo.blocks.len();
                                blocks_vec.push(plo.blocks);
                                true
                            } else {
                                posting_count = 0;
                                max_list_score = 0.0;
                                blocks = &DUMMY_VEC;
                                blocks_len = 0;
                                false
                            }
                        };
                        if found_plo {
                            let value_new = PostingListObjectQuery {
                                posting_count,
                                max_list_score,
                                blocks,
                                blocks_index: blocks_vec.len(),
                                p_block_max: blocks_len as i32,
                                term: term_no_diacritics_umlaut_case.clone(),
                                key0,
                                term_index_unique: query_list_map_len,
                                idf,
                                idf_ngram1,
                                idf_ngram2,
                                idf_ngram3,
                                ngram_type: non_unique_term.ngram_type.clone(),
                                ..Default::default()
                            };
                            not_query_list_map.insert(key_hash, value_new);
                        } else {
                            not_found_terms_hashset.insert(key_hash);
                        }
                    }
                } else {
                    let query_list_map_len = query_list_map.len();
                    let mut found = true;
                    let query_list_option = query_list_map.get(&key_hash);
                    match query_list_option {
                        None => {
                            if !not_found_terms_hashset.contains(&key_hash) {
                                let posting_count;
                                let max_list_score;
                                let blocks;
                                let blocks_len;
                                let found_plo = {
                                    let posting_list_object_index_option =
                                        decode_posting_list_object(
                                            &shard_ref.segments_index[key0 as usize],
                                            &shard_ref,
                                            key_hash,
                                            true,
                                        );
                                    if let Some(plo) = posting_list_object_index_option {
                                        posting_count = plo.posting_count;
                                        max_list_score = plo.max_list_score;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = plo.blocks.len();
                                        blocks_vec.push(plo.blocks);
                                        true
                                    } else {
                                        posting_count = 0;
                                        max_list_score = 0.0;
                                        blocks = &DUMMY_VEC;
                                        blocks_len = 0;
                                        false
                                    }
                                };

                                if found_plo {
                                    if result_type != ResultType::Count {
                                        // Scoring sees all physical documents, as does N and the
                                        // realtime scorer. Keep traversal posting counts unchanged.
                                        let live_df = if shard_ref.uncommitted {
                                            shard_ref.segments_level0[key0 as usize].segment.get(&key_hash).map_or(0, |p| p.posting_count as u32)
                                        } else { 0 };
                                        let posting_count = shard_ref.scoring_df(key_hash, posting_count + live_df);
                                        let component_scoring = non_unique_term.ngram_type != NgramType::SingleTerm
                                            && shard_ref.meta.lexical_similarity == LexicalSimilarity::Bm25f;
                                        let component_df = |term: &str| if component_scoring { shard_ref.get_ngram_component_df(term) } else { 0 };
                                        let bigram = matches!(non_unique_term.ngram_type, NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF);
                                        let posting_count_ngram_1 = component_df(if bigram { &non_unique_term.term_ngram_1 } else { &non_unique_term.term_ngram_2 });
                                        let posting_count_ngram_2 = component_df(if bigram { &non_unique_term.term_ngram_0 } else { &non_unique_term.term_ngram_1 });
                                        let posting_count_ngram_3 = if bigram { 0 } else { component_df(&non_unique_term.term_ngram_0) };
                                        if non_unique_term.ngram_type == NgramType::SingleTerm
                                            || shard_ref.meta.lexical_similarity
                                                == LexicalSimilarity::Bm25fProximity
                                        {
                                            idf = (((shard_ref.scoring_count() as f32
                                                - posting_count as f32
                                                + 0.5)
                                                / (posting_count as f32 + 0.5))
                                                + 1.0)
                                                .ln();
                                        } else if non_unique_term.ngram_type == NgramType::NgramFF
                                            || non_unique_term.ngram_type == NgramType::NgramRF
                                            || non_unique_term.ngram_type == NgramType::NgramFR
                                        {
                                            idf_ngram1 = (((shard_ref.scoring_count() as f32
                                                - posting_count_ngram_1 as f32
                                                + 0.5)
                                                / (posting_count_ngram_1 as f32 + 0.5))
                                                + 1.0)
                                                .ln();

                                            idf_ngram2 = (((shard_ref.scoring_count() as f32
                                                - posting_count_ngram_2 as f32
                                                + 0.5)
                                                / (posting_count_ngram_2 as f32 + 0.5))
                                                + 1.0)
                                                .ln();
                                        } else {
                                            idf_ngram1 = (((shard_ref.scoring_count() as f32
                                                - posting_count_ngram_1 as f32
                                                + 0.5)
                                                / (posting_count_ngram_1 as f32 + 0.5))
                                                + 1.0)
                                                .ln();

                                            idf_ngram2 = (((shard_ref.scoring_count() as f32
                                                - posting_count_ngram_2 as f32
                                                + 0.5)
                                                / (posting_count_ngram_2 as f32 + 0.5))
                                                + 1.0)
                                                .ln();

                                            idf_ngram3 = (((shard_ref.scoring_count() as f32
                                                - posting_count_ngram_3 as f32
                                                + 0.5)
                                                / (posting_count_ngram_3 as f32 + 0.5))
                                                + 1.0)
                                                .ln();
                                        }
                                    }
                                    let value_new = PostingListObjectQuery {
                                        posting_count,
                                        max_list_score,
                                        blocks,
                                        blocks_index: blocks_vec.len(),
                                        p_block_max: blocks_len as i32,
                                        term: term_no_diacritics_umlaut_case.clone(),
                                        key0,
                                        term_index_unique: query_list_map_len,
                                        idf,
                                        idf_ngram1,
                                        idf_ngram2,
                                        idf_ngram3,
                                        ngram_type: non_unique_term.ngram_type.clone(),
                                        ..Default::default()
                                    };
                                    term_index_unique = value_new.term_index_unique;
                                    query_list_map.insert(key_hash, value_new);
                                } else {
                                    if non_unique_term.op == QueryType::Intersection
                                        || non_unique_term.op == QueryType::Phrase
                                    {
                                        break 'search_blocks;
                                    }
                                    not_found_terms_hashset.insert(key_hash);
                                    found = false;
                                }
                            }
                        }
                        Some(value) => {
                            term_index_unique = value.term_index_unique;
                        }
                    }

                    if found && non_unique_term.op == QueryType::Phrase {
                        let nu_plo = NonUniquePostingListObjectQuery {
                            term_index_unique,
                            term_index_nonunique: non_unique_query_list.len()
                                + preceding_ngram_count,
                            pos: 0,
                            p_pos: 0,
                            positions_pointer: 0,
                            positions_count: 0,
                            byte_array: &DUMMY_VEC_8,
                            field_vec: SmallVec::new(),
                            p_field: 0,
                            key0,
                            is_embedded: false,
                            embedded_positions: [0; 4],
                        };

                        match non_unique_term.ngram_type {
                            NgramType::SingleTerm => {}
                            NgramType::NgramFF | NgramType::NgramRF | NgramType::NgramFR => {
                                preceding_ngram_count += 1
                            }
                            _ => preceding_ngram_count += 2,
                        };

                        non_unique_query_list.push(nu_plo);
                    }
                }
                match term.ngram_type {
                    NgramType::SingleTerm => {}
                    NgramType::NgramFF | NgramType::NgramRF | NgramType::NgramFR => {
                        result_object
                            .query_terms
                            .push(term.term_ngram_1.to_string());
                        result_object
                            .query_terms
                            .push(term.term_ngram_0.to_string());
                    }
                    _ => {
                        result_object
                            .query_terms
                            .push(term.term_ngram_2.to_string());
                        result_object
                            .query_terms
                            .push(term.term_ngram_1.to_string());
                        result_object
                            .query_terms
                            .push(term.term_ngram_0.to_string());
                    }
                };
                {
                    result_object.query_terms.push(term.term.to_string());
                }
            }
            not_query_list = not_query_list_map.into_values().collect();
            query_list = query_list_map.into_values().collect();
            crate::add_result::prepare_scoring_order(&mut query_list);

            {
                for plo in query_list.iter_mut() {
                    plo.blocks = &blocks_vec[plo.blocks_index - 1]
                }
                for plo in not_query_list.iter_mut() {
                    plo.blocks = &blocks_vec[plo.blocks_index - 1]
                }
            }

            let query_list_len = query_list.len();
            let non_unique_query_list_len = non_unique_query_list.len();
            let mut matching_blocks: i32 = 0;
            let query_term_count = non_unique_terms.len();
            if query_list_len == 0 {
                break 'search_blocks;
            } else if query_list_len == 1 {
                if !(shard_ref.uncommitted && include_uncommitted)
                    && offset + length <= 1000
                    && not_query_list.is_empty()
                    && field_filter_set.is_empty()
                    && shard_ref.delete_hashset.is_empty()
                    && facet_filter_sparse.is_empty()
                    && result_sort_index.is_empty()
                    && let Some(stopword_result_object) = shard_ref
                        .frequentword_results
                        .get(&non_unique_terms[0].term)
                {
                    result_object.query = stopword_result_object.query.clone();
                    result_object
                        .query_terms
                        .clone_from(&stopword_result_object.query_terms);
                    result_object.result_count = stopword_result_object.result_count;
                    result_object.result_count_total = stopword_result_object.result_count_total;
                    if result_type != ResultType::Count {
                        result_object
                            .results
                            .clone_from(&stopword_result_object.results);
                        if offset > 0 {
                            result_object.results.drain(..offset);
                        }
                        if length < 1000 {
                            result_object.results.truncate(length);
                        }
                    }

                    return result_object;
                }

                single_blockid(
                    &shard_ref,
                    &mut non_unique_query_list,
                    &mut query_list,
                    &mut not_query_list,
                    &result_count_arc,
                    &mut search_result,
                    offset + length,
                    &result_type,
                    &field_filter_set,
                    &facet_filter_sparse,
                    &mut matching_blocks,
                )
                .await;
            } else if query_type_mut == QueryType::Union {
                if result_type == ResultType::Count && query_list_len != 2 {
                    union_blockid(
                        &shard_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                    )
                    .await;
                } else if SPEEDUP_FLAG
                    && query_list_len == 2
                    && facet_filter_sparse.is_empty()
                    && search_result.topk_candidates.result_sort.is_empty()
                {
                    union_docid_2(
                        &shard_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                        &mut matching_blocks,
                        query_term_count,
                    )
                    .await;
                } else if SPEEDUP_FLAG
                    && search_result.topk_candidates.result_sort.is_empty()
                    && query_list_len <= 10
                {
                    union_docid_3(
                        &shard_ref,
                        &mut non_unique_query_list,
                        &mut Vec::from([QueueObject {
                            query_list: query_list.clone(),
                            query_index: 0,
                            max_score: f32::MAX,
                        }]),
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                        &mut matching_blocks,
                        0,
                        query_term_count,
                        0,
                    )
                    .await;
                } else {
                    union_blockid(
                        &shard_ref,
                        &mut non_unique_query_list,
                        &mut query_list,
                        &mut not_query_list,
                        &result_count_arc,
                        &mut search_result,
                        offset + length,
                        &result_type,
                        &field_filter_set,
                        &facet_filter_sparse,
                    )
                    .await;
                }
            } else {
                intersection_blockid(
                    &shard_ref,
                    &mut non_unique_query_list,
                    &mut query_list,
                    &mut not_query_list,
                    &result_count_arc,
                    &mut search_result,
                    offset + length,
                    &result_type,
                    &field_filter_set,
                    &facet_filter_sparse,
                    &mut matching_blocks,
                    query_type_mut == QueryType::Phrase && non_unique_query_list_len >= 2,
                    query_term_count,
                )
                .await;
            }
        }

        result_object.result_count = search_result.topk_candidates.current_heap_size;

        if search_result.topk_candidates.current_heap_size > offset {
            result_object.results = mem::take(&mut search_result.topk_candidates._elements);

            if search_result.topk_candidates.current_heap_size < offset + length {
                result_object
                    .results
                    .truncate(search_result.topk_candidates.current_heap_size);
            }
            if result_sort.is_empty() {
                if shard_ref.active.is_some() {
                    result_object.results.sort_by(|a, b| shard_ref.rank(b.score, b.doc_id, a.score, a.doc_id));
                } else if query_string.is_empty() {
                    result_object
                        .results
                        .sort_by_key(|b| cmp::Reverse(b.doc_id));
                } else {
                    result_object
                        .results
                        .sort_by(|a, b| shard_ref.rank(b.score, b.doc_id, a.score, a.doc_id));
                }
            } else {
                result_object
                    .results
                    .sort_by(|a, b| search_result.topk_candidates.result_ordering_shard(*b, *a));
            }

            if offset > 0 {
                result_object.results.drain(..offset);
            }
        }

        result_object.result_count_total = result_count_uncommitted_arc.load(Ordering::Relaxed)
            + result_count_arc.load(Ordering::Relaxed);

        result_object
    }
}

/// Validate metadata filter field names, schema types and range bounds without executing a query.
pub async fn validate_metadata_filters(
    index: &IndexArc,
    filters: &[FacetFilter],
) -> std::result::Result<(), String> {
    crate::metadata_filter::validate_filters(&index.read().await.schema_map, filters)
}
