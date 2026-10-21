//! HTTP contracts, including an optional vector-search probe budget.
use crate::api_schema;
use seekpower::{
    index::{Document, FieldType, SchemaField, StopwordType, TokenizerType, TextNormalization},
    search::{FacetFilter, QueryType, ResultSort, ResultType, SearchMode},
    vector::{Inference, Precision, Quantization},
    vector_similarity::{AnnMode, VectorSimilarity},
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use utoipa::ToSchema;

/// Initial HTTP ANN budget; calibrated on the fixed MIRACL Chinese train subset.
pub const DEFAULT_NPROBE: usize = 200;

/// Search text, vectors, or both. Empty input browses documents. Pending writes are visible.
#[derive(Deserialize, Serialize, Clone, Debug, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchRequestObject {
    /// UTC millisecond event/ingestion ranges and business-valid point.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub temporal: Option<seekpower::record::TemporalFilter>,
    /// Optional weighted RRF for text plus vector queries; ranks start at zero.
    #[serde(default, deserialize_with = "present_value", skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<api_schema::RrfOptions>, nullable = false)]
    pub rrf: Option<seekpower::search::RrfOptions>,
    /// Text query; supports +, - and quoted phrases. Omit for vector search or browsing.
    #[serde(default, rename = "query")]
    pub query_string: String,
    /// External embedding. A vector alone selects Vector; nonempty text plus a vector selects Hybrid.
    #[serde(default)]
    pub query_vector: Option<serde_json::Value>,
    #[serde(default)]
    /// Number of matching results to skip.
    pub offset: usize,
    /// Number of results. Lexical/browse queries accept zero for count-only; vector/hybrid require a positive window.
    #[serde(default = "length_api")]
    #[schema(default = 10, minimum = 0)]
    pub length: usize,
    /// Include the total reported by the retrieval mode. False returns count_total:null.
    #[serde(default = "default_true")]
    #[schema(default = true)]
    pub count: bool,
    /// Match all or any unqualified query terms. Explicit query operators take precedence.
    #[serde(default)]
    #[schema(default = "and")]
    /// Match all (and) or any (or) query terms.
    pub operator: QueryOperator,
    /// Search only these indexed fields; empty means all indexed fields.
    #[serde(default)]
    #[serde(rename = "search_fields")]
    pub field_filter: Vec<String>,
    /// Return only these stored fields; empty means all stored fields.
    #[serde(default)]
    pub fields: Vec<String>,
    /// Metadata predicates, ANDed together. Fields must have filterable=true at index creation.
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::FacetFilter>)]
    #[serde(rename = "filter")]
    pub facet_filter: Vec<FacetFilter>,
    /// Sort by filterable fields or _score; empty means relevance order.
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::ResultSort>)]
    #[serde(rename = "sort")]
    pub result_sort: Vec<ResultSort>,
    /// Optional vector score/distance threshold, interpreted using the index metric.
    #[serde(default)]
    pub similarity_threshold: Option<f32>,
    /// Scan all clusters using the stored vector representation. Omit for ANN.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(nullable = false)]
    pub all: Option<bool>,
    /// Positive probe budget per shard per level; capped at that level's cluster count.
    /// Overrides the server default. Not a global candidate count or recall guarantee.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<usize>, minimum = 1, nullable = false)]
    pub nprobe: Option<NonZeroUsize>,
}

/// Boolean matching for unqualified text terms.
#[derive(Default, Deserialize, Serialize, Clone, Copy, Debug, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum QueryOperator {
    #[default]
    /// Match all terms.
    And,
    /// Match any term.
    Or,
}

impl QueryOperator {
    /// Resolve Boolean matching to the engine query type.
    pub fn query_type(self) -> QueryType {
        match self {
            QueryOperator::And => QueryType::Intersection,
            QueryOperator::Or => QueryType::Union,
        }
    }
}

impl SearchRequestObject {
    /// Resolve the public operator to the core query type.
    pub fn query_type(&self) -> QueryType {
        self.operator.query_type()
    }
    /// Resolve the retrieval mode from the supplied inputs.
    pub fn search_mode(&self, default_nprobe: NonZeroUsize) -> Result<SearchMode, String> {
        if self.query_vector.is_none() && (self.all.is_some() || self.nprobe.is_some()) {
            return Err("all and nprobe require query_vector".into());
        }
        if self.all == Some(true) && self.nprobe.is_some() {
            return Err("all=true cannot be combined with nprobe".into());
        }
        if let Some(rrf) = &self.rrf {
            rrf.validate()?;
            if self.query_vector.is_none() || self.query_string.trim().is_empty() {
                return Err("rrf requires both query text and query_vector".into());
            }
        }
        let ann_mode = if self.all == Some(true) {
            AnnMode::All
        } else {
            AnnMode::Nprobe(self.nprobe.unwrap_or(default_nprobe).get())
        };
        Ok(if self.query_vector.is_none() {
            SearchMode::Lexical
        } else if self.query_string.trim().is_empty() {
            SearchMode::Vector {
                similarity_threshold: self.similarity_threshold,
                ann_mode,
            }
        } else if let Some(rrf) = self.rrf {
            SearchMode::WeightedHybrid {
                similarity_threshold: self.similarity_threshold,
                ann_mode,
                rrf,
            }
        } else {
            SearchMode::Hybrid {
                similarity_threshold: self.similarity_threshold,
                ann_mode,
            }
        })
    }
    /// Resolve document and count collection requirements.
    pub fn result_type(&self) -> ResultType {
        if !self.count {
            ResultType::Topk
        } else if self.length == 0 {
            ResultType::Count
        } else {
            ResultType::TopkCount
        }
    }
}
// Missing fields use serde(default); explicit null is rejected like other wrong types.
fn present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

fn length_api() -> usize {
    10
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
/// Search result object
pub struct SearchResultObject {
    /// Time taken to execute the search query in nanoseconds
    pub time: u128,
    /// Search query string
    pub original_query: String,
    /// Search query string, unchanged from the caller input
    pub query: String,
    /// Offset of the returned search results
    pub offset: usize,
    /// Number of requested search results
    pub length: usize,
    /// Number of returned search results matching the query
    pub count: usize,
    /// Total matches reported by the selected retrieval mode; null when count=false.
    #[schema(required = true)]
    pub count_total: Option<usize>,
    /// Vector of lexical search query terms.
    pub query_terms: Vec<String>,
    #[schema(value_type=Vec<std::collections::HashMap<String, serde_json::Value>>)]
    /// Vector of search result documents
    pub results: Vec<Document>,
}

/// Create an index. Compression, scoring, clustering and word N-grams are managed internally.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateIndexRequest {
    /// At least seven-day change retention and hard admission budgets.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub change_feed: seekpower::record::FeedConfig,
    /// Name displayed in index listings.
    pub index_name: String,
    /// Explicit field definitions; names must be nonempty and unique.
    #[schema(min_items = 1)]
    pub schema: Vec<FieldRequest>,
    /// Keep the existing Unicode tokenizer by default. Use UnicodeAlphanumericZH for Chinese segmentation.
    #[serde(default = "tokenizer_type_api")]
    #[schema(value_type = api_schema::TokenizerType)]
    pub tokenizer: TokenizerType,
    /// Immutable rule shared by document and query analysis; default None.
    #[serde(default)]
    #[schema(value_type = api_schema::TextNormalization)]
    pub text_normalization: TextNormalization,
    /// Immutable length denominator policy; Shared is the default.
    #[serde(default)]
    #[schema(value_type = api_schema::FieldLengthNormalization)]
    pub field_length_normalization: seekpower::index::FieldLengthNormalization,
    /// Optional stopword removal; default None preserves all words.
    #[serde(default)]
    #[schema(value_type = api_schema::StopwordType)]
    pub stop_words: StopwordType,
    /// Required when a field has index_vector=true. Embeddings are provided by the caller.
    #[serde(default)]
    pub vector: Option<VectorConfig>,
}

/// Public field settings; internal field IDs and encoding hints are not accepted.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct FieldRequest {
    /// Unique, nonempty field name.
    pub field: String,
    #[schema(value_type = api_schema::FieldType)]
    /// Value type used for indexing and validation.
    pub field_type: FieldType,
    /// Default true for ordinary fields, false for vector fields.
    #[serde(default)]
    pub store: Option<bool>,
    /// Default true for Text fields, false for other types and vector fields.
    #[serde(default)]
    pub index_lexical: Option<bool>,
    #[serde(default)]
    /// Index externally supplied embeddings in a Json or Binary field.
    pub index_vector: bool,
    /// Enable metadata filtering and sorting. Default false avoids unnecessary storage.
    #[serde(default)]
    pub filterable: bool,
    /// Optional field weight for lexical relevance.
    #[serde(default = "default_boost")]
    #[schema(default = 1)]
    pub boost: f32,
}
impl From<FieldRequest> for SchemaField {
    fn from(field: FieldRequest) -> Self {
        let lexical = !field.index_vector && matches!(field.field_type, FieldType::Text);
        Self::new(
            field.field,
            field.store.unwrap_or(!field.index_vector),
            field.index_lexical.unwrap_or(lexical),
            field.index_vector,
            field.field_type,
            field.filterable,
            false,
            field.boost,
        )
    }
}

impl From<&SchemaField> for FieldRequest {
    fn from(field: &SchemaField) -> Self {
        Self {
            field: field.field.clone(),
            field_type: field.field_type.clone(),
            store: Some(field.store),
            index_lexical: Some(field.index_lexical),
            index_vector: field.index_vector,
            filterable: field.facet,
            boost: field.boost,
        }
    }
}

/// Vector space shared by the index. Defaults preserve floating-point embeddings without quantization.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    #[schema(minimum = 1)]
    /// Number of values in each embedding.
    pub dimensions: usize,
    #[serde(default = "cosine")]
    #[schema(value_type = api_schema::VectorSimilarity, default = "Cosine")]
    /// Distance metric expected by the embedding model.
    pub metric: VectorSimilarity,
    /// Advanced: stored embedding precision; normally omit.
    #[serde(default = "float_precision")]
    #[schema(value_type = api_schema::Precision, default = "F32")]
    pub precision: Precision,
    /// Advanced: optional lossy quantization; normally omit.
    #[serde(default)]
    #[schema(value_type = api_schema::Quantization, default = "None")]
    pub quantization: Quantization,
}
impl From<VectorConfig> for Inference {
    fn from(vector: VectorConfig) -> Self {
        Self::External {
            dimensions: vector.dimensions,
            precision: vector.precision,
            quantization: vector.quantization,
            similarity: vector.metric,
        }
    }
}
fn cosine() -> VectorSimilarity {
    VectorSimilarity::Cosine
}
fn float_precision() -> Precision {
    Precision::F32
}
fn default_boost() -> f32 {
    1.0
}
fn tokenizer_type_api() -> TokenizerType {
    TokenizerType::UnicodeAlphanumeric
}

/// Iterate live document IDs; optionally include stored documents.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct GetIteratorRequest {
    #[serde(default)]
    /// Start ID; omitted starts at the beginning or end according to take.
    pub document_id: Option<u64>,
    #[serde(default)]
    /// Number of live IDs to skip.
    pub skip: usize,
    /// Positive for forward traversal, negative for backward. Defaults to 10.
    #[serde(default = "iterator_take")]
    #[schema(default = 10)]
    pub take: isize,
    #[serde(default = "default_true")]
    #[schema(default = true)]
    /// Return stored fields as well as IDs.
    pub include_document: bool,
    #[serde(default)]
    /// Empty means all stored fields.
    pub fields: Vec<String>,
}
fn iterator_take() -> isize {
    10
}

/// URL query parameters for a stored document read.
#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct GetDocumentRequest {
    /// Comma-separated stored field names; omit to return all stored fields.
    #[serde(default)]
    pub fields: String,
}

/// One complete replacement. Updates delete the old document and allocate a new ID.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DocumentReplacement {
    /// Existing internal document ID.
    pub id: u64,
    /// Full replacement document, not a partial patch.
    #[schema(value_type = std::collections::HashMap<String, serde_json::Value>)]
    pub document: Document,
}

/// One replacement or a batch of replacements.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(untagged, try_from = "serde_json::Value")]
pub enum UpdateDocuments {
    /// One complete replacement.
    Single(DocumentReplacement),
    /// Several complete replacements.
    Batch(Vec<DocumentReplacement>),
}
impl TryFrom<serde_json::Value> for UpdateDocuments {
    type Error = serde_json::Error;
    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Object(_) => serde_json::from_value(value).map(Self::Single),
            serde_json::Value::Array(ref items)
                if items.iter().all(serde_json::Value::is_object) =>
            {
                serde_json::from_value(value).map(Self::Batch)
            }
            _ => Err(serde::de::Error::custom(
                "expected a replacement object or an array of replacement objects",
            )),
        }
    }
}
impl UpdateDocuments {
    /// Convert complete replacements to the engine batch format.
    pub fn into_documents(self) -> Vec<(u64, Document)> {
        let documents = match self {
            Self::Single(d) => vec![d],
            Self::Batch(d) => d,
        };
        documents.into_iter().map(|d| (d.id, d.document)).collect()
    }
}

/// Delete all documents matching nonempty text and optional metadata predicates.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteQueryRequest {
    /// Nonempty text query; all matching documents are deleted.
    pub query: String,
    #[serde(default)]
    #[schema(default = "and")]
    /// Match all (and) or any (or) query terms.
    pub operator: QueryOperator,
    #[serde(default)]
    /// Limit text matching to these fields; empty searches all.
    pub search_fields: Vec<String>,
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::FacetFilter>)]
    /// Metadata predicates combined with AND.
    pub filter: Vec<FacetFilter>,
}

/// Select exactly one deletion form. Full removal requires all=true.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(untagged, try_from = "serde_json::Map<String, serde_json::Value>")]
pub enum DeleteDocuments {
    /// Delete the specified internal IDs.
    Ids {
        /// IDs to delete.
        ids: Vec<u64>,
    },
    /// Delete every text and metadata match.
    Query(DeleteQueryRequest),
    /// Explicitly remove every document.
    All {
        /// Must be true.
        all: bool,
    },
}

impl TryFrom<serde_json::Map<String, serde_json::Value>> for DeleteDocuments {
    type Error = serde_json::Error;
    fn try_from(
        mut fields: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Self, Self::Error> {
        if fields.len() == 1 {
            if let Some(ids) = fields.remove("ids") {
                return serde_json::from_value(ids).map(|ids| Self::Ids { ids });
            }
            if let Some(all) = fields.remove("all") {
                return serde_json::from_value(all).map(|all| Self::All { all });
            }
        }
        serde_json::from_value(serde_json::Value::Object(fields)).map(Self::Query)
    }
}

/// All HTTP errors use this JSON shape; status codes determine success or failure.
#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    /// Stable error category.
    pub code: &'static str,
    /// Human-readable diagnostic.
    pub message: String,
}

/// Public index metadata. Field definitions use the same names as creation.
#[derive(Debug, Serialize, ToSchema)]
pub struct IndexResponseObject {
    /// Persisted pre-tokenization rule; cannot be overridden per query.
    #[schema(value_type = api_schema::TextNormalization)]
    pub text_normalization: TextNormalization,
    /// Immutable length denominator policy; Shared is the default.
    #[serde(default)]
    #[schema(value_type = api_schema::FieldLengthNormalization)]
    pub field_length_normalization: seekpower::index::FieldLengthNormalization,
    /// Index ID within the authenticated key scope.
    pub id: u64,
    /// Display name.
    pub name: String,
    /// Resolved public field settings, ordered by name.
    pub schema: Vec<FieldRequest>,
    /// Allocated document count, including deleted IDs.
    pub indexed_doc_count: usize,
    /// Committed document count.
    pub committed_doc_count: usize,
    /// Running server version.
    pub version: String,
}

#[cfg(test)]
mod rrf_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn http_rrf_contract() {
        let default_probe = NonZeroUsize::new(200).unwrap();
        for value in [json!({"rrf":{}}),json!({"query_vector":[1,0,0],"rrf":{}}),
            json!({"query":"alpha","query_vector":[1,0,0],"rrf":{"constant":0}}),
            json!({"query":"alpha","query_vector":[1,0,0],"rrf":{"keyword_candidates":0}})] {
            let request: SearchRequestObject = serde_json::from_value(value).unwrap();
            assert!(request.search_mode(default_probe).is_err());
        }
        for value in [json!({"rrf":null}),json!({"rrf":{"constant":null}}),json!({"rrf":{"bogus":1}})] {
            assert!(serde_json::from_value::<SearchRequestObject>(value).is_err());
        }
        let request: SearchRequestObject = serde_json::from_value(json!({"query":"alpha","query_vector":[1,0,0],"rrf":{"constant":5,"keyword_weight":0.5,"keyword_candidates":20,"vector_candidates":30}})).unwrap();
        assert!(matches!(request.search_mode(default_probe).unwrap(),SearchMode::WeightedHybrid { rrf, .. } if rrf.constant == 5.0 && rrf.keyword_candidates == Some(20) && rrf.vector_candidates == Some(30)));
        let request: SearchRequestObject = serde_json::from_value(json!({"query":"alpha","query_vector":[1,0,0]})).unwrap();
        assert!(matches!(request.search_mode(default_probe).unwrap(),SearchMode::Hybrid { .. }));
    }
}
