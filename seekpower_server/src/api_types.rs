//! HTTP contracts. Internal tuning stays in the engine; callers describe data and queries.
use crate::api_schema;
use seekpower::{
    index::{Document, FieldType, SchemaField, StopwordType, TokenizerType},
    search::{FacetFilter, QueryType, ResultSort, ResultType, SearchMode},
    vector::{Inference, Precision, Quantization},
    vector_similarity::{AnnMode, VectorSimilarity},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::ToSchema;

/// Search text, vectors, or both. Empty input browses documents. Pending writes are visible.
#[derive(Deserialize, Serialize, Clone, Debug, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchRequestObject {
    /// Text query; supports +, - and quoted phrases. Omit for vector search or browsing.
    #[serde(default, rename = "query")]
    pub query_string: String,
    /// External embedding. A vector alone selects Vector; nonempty text plus a vector selects Hybrid.
    #[serde(default)]
    pub query_vector: Option<Value>,
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
    pub operator: QueryOperator,
    /// Search only these indexed fields; empty means all indexed fields.
    #[serde(default)]
    pub field_filter: Vec<String>,
    /// Return only these stored fields; empty means all stored fields.
    #[serde(default)]
    pub fields: Vec<String>,
    /// Metadata predicates, ANDed together. Fields must have filterable=true at index creation.
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::FacetFilter>)]
    pub facet_filter: Vec<FacetFilter>,
    /// Sort by filterable fields or _score; empty means relevance order.
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::ResultSort>)]
    pub result_sort: Vec<ResultSort>,
    /// Optional vector score/distance threshold, interpreted using the index metric.
    #[serde(default)]
    pub similarity_threshold: Option<f32>,
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

impl SearchRequestObject {
    /// Resolve the public operator to the core query type.
    pub fn query_type(&self) -> QueryType {
        match self.operator {
            QueryOperator::And => QueryType::Intersection,
            QueryOperator::Or => QueryType::Union,
        }
    }
    /// Resolve the retrieval mode from the supplied inputs.
    pub fn search_mode(&self) -> SearchMode {
        if self.query_vector.is_none() {
            SearchMode::Lexical
        } else if self.query_string.trim().is_empty() {
            SearchMode::Vector {
                similarity_threshold: self.similarity_threshold,
                ann_mode: AnnMode::All,
            }
        } else {
            SearchMode::Hybrid {
                similarity_threshold: self.similarity_threshold,
                ann_mode: AnnMode::All,
            }
        }
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
    #[schema(value_type=Vec<HashMap<String, serde_json::Value>>)]
    /// Vector of search result documents
    pub results: Vec<Document>,
}

/// Create an index. Compression, scoring, clustering and word N-grams are managed internally.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateIndexRequest {
    /// Name displayed in index listings.
    pub index_name: String,
    /// Explicit field definitions; names must be nonempty and unique.
    #[schema(min_items = 1)]
    pub schema: Vec<FieldRequest>,
    /// Keep the existing Unicode tokenizer by default. Use UnicodeAlphanumericZH for Chinese segmentation.
    #[serde(default = "tokenizer_type_api")]
    #[schema(value_type = api_schema::TokenizerType)]
    pub tokenizer: TokenizerType,
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

/// Projection for a stored document read.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct GetDocumentRequest {
    /// Empty means all stored fields.
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
/// Index response object
pub struct IndexResponseObject {
    /// Index ID
    pub id: u64,
    /// Index name
    #[schema(example = "demo_index")]
    pub name: String,
    #[schema(example = json!({
        "title":{
            "field":"title",
            "store":true,
            "index_lexical":true,
            "field_type":"Text",
            "boost":10.0,
            "field_id":0
        },
        "body":{
            "field":"body",
            "store":true,
            "index_lexical":true,
            "field_type":"Text",
            "field_id":1
        },
        "url":{
           "field":"url",
           "store":true,
           "index_lexical":false,
           "field_type":"Text",
           "field_id":2
        },
        "date":{
           "field":"date",
           "store":true,
           "index_lexical":false,
           "field_type":"Timestamp",
           "facet":true,
           "field_id":3
        }
     }))]
    /// Schema definition for the index
    #[schema(value_type = HashMap<String, api_schema::SchemaField>)]
    pub schema: HashMap<String, SchemaField>,
    /// Number of indexed documents
    pub indexed_doc_count: usize,
    /// Number of committed documents
    pub committed_doc_count: usize,
    /// Number of operations: index, update, delete, queries
    pub operations_count: u64,
    /// Number of queries (informational)
    pub query_count: u64,
    /// Seekpower version the index was created with
    #[schema(example = "0.11.1")]
    pub version: String,
}
