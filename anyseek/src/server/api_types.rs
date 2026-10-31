//! HTTP contracts, including an optional vector-search probe budget.
use super::api_schema;
use crate::{
    index::{FieldType, SchemaField, StopwordType, TokenizerType, TextNormalization},
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
    /// Opt-in one-hop plus lexical/vector fusion, with explicit candidate windows.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub relation_fusion: Option<crate::record::RelationFusion>,
    /// UTC millisecond event/ingestion ranges and business-valid point.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub temporal: Option<crate::record::TemporalFilter>,
    /// Optional weighted RRF for text plus vector queries; ranks start at zero.
    #[serde(default, deserialize_with = "present_value", skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<api_schema::RrfOptions>, nullable = false)]
    pub rrf: Option<crate::search::RrfOptions>,
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
    /// Select indexed fields for the active search mode; unknown or inapplicable names are rejected.
    /// Omitted/empty means all fields. Hybrid accepts lexical and/or vector names;
    /// each channel uses its selected fields, or all its fields if none are selected.
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
    /// Sort by filterable fields, _id or _score; invalid fields are rejected. Empty means relevance order.
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
        if self.relation_fusion.is_some() && self.rrf.is_some() {return Err("specify RRF once in relation_fusion".into());}
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
        } else {
            SearchMode::Hybrid {
                similarity_threshold: self.similarity_threshold,
                ann_mode,
                rrf: self.rrf.unwrap_or_default(),
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

/// Create an index. Compression, scoring, clustering and word N-grams are managed internally.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateIndexRequest {
    /// Name displayed in index listings.
    pub index_name: String,
    /// Explicit field definitions; names must be nonempty and unique.
    #[schema(min_items = 1)]
    pub schema: Vec<FieldRequest>,
    /// Default is JiebaMixedV1 with zh, otherwise TantivySimpleV1; explicit English uses TantivySimpleV1.
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
    pub field_length_normalization: crate::index::FieldLengthNormalization,
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
    TokenizerType::default()
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
        assert!(matches!(request.search_mode(default_probe).unwrap(),SearchMode::Hybrid { rrf, .. } if rrf.constant == 5.0 && rrf.keyword_candidates == Some(20) && rrf.vector_candidates == Some(30)));
        let request: SearchRequestObject = serde_json::from_value(json!({"query":"alpha","query_vector":[1,0,0]})).unwrap();
        assert!(matches!(request.search_mode(default_probe).unwrap(),SearchMode::Hybrid { rrf, .. } if rrf == crate::search::RrfOptions::default()));
    }
}
