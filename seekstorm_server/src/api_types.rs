//! HTTP request/response contracts. Core operation types remain in seekstorm.
use crate::api_schema;
use seekstorm::{
    index::{
        Clustering, Document, DocumentCompression, FrequentwordType, LexicalSimilarity, NgramSet,
        SchemaField, StemmerType, StopwordType, Synonym, TokenizerType,
    },
    search::{FacetFilter, QueryType, ResultSort, ResultType, SearchMode},
    vector::Inference,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::ToSchema;

/// Search request object
#[derive(Deserialize, Serialize, Clone, Debug, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchRequestObject {
    /// Query string, search operators + - "" are recognized.
    #[serde(rename = "query")]
    pub query_string: String,
    /// External query vector, required for Vector and Hybrid search. No model inference is performed.
    #[serde(default)]
    pub query_vector: Option<Value>,
    #[serde(default)]
    #[schema(required = false, default = false, example = false)]
    /// Enable empty queries to browse all documents with pagination, filtering and sorting.
    /// Otherwise an empty query string returns no results.
    /// Typical use cases include index browsing, index export, conversion, analytics, audits, and inspection.
    pub enable_empty_query: bool,
    #[serde(default)]
    #[schema(required = false, minimum = 0, default = 0, example = 0)]
    /// Offset of search results to return.
    pub offset: usize,
    /// Number of search results to return.
    #[serde(default = "length_api")]
    #[schema(required = false, minimum = 1, default = 10, example = 10)]
    pub length: usize,
    #[serde(default)]
    /// Specify the type of search result.
    #[schema(value_type = api_schema::ResultType)]
    pub result_type: ResultType,
    /// True realtime search: include indexed, but uncommitted documents into search results.
    #[serde(default)]
    pub realtime: bool,
    /// Specify field names where to search at querytime, whereas SchemaField.indexed is set at indextime. If empty then all indexed fields are searched.
    #[schema(required = false, example = json!(["title"]))]
    #[serde(default)]
    pub field_filter: Vec<String>,
    /// Specify names of fields to return in the search results, where SchemaField.store is set at indextime. If empty then all stored fields are returned.
    #[serde(default)]
    pub fields: Vec<String>,

    /// Metadata filters apply to Lexical, Vector and Hybrid search and require facet:true fields.
    /// Predicates are ANDed; values within a string/set predicate are ORed. Ranges are start-inclusive, end-exclusive.
    /// Vector candidates are filtered before scoring and Top-K; ANN still scans only the configured clusters.
    /// Unknown fields, type mismatches and invalid range bounds return an explicit error.
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::FacetFilter>)]
    pub facet_filter: Vec<FacetFilter>,
    /// Sort field and order:
    /// Search results are sorted by the specified facet field, either in ascending or descending order.
    /// If no sort field is specified, then the search results are sorted by rank in descending order per default.
    /// Multiple sort fields are combined by a "sort by, then sort by"-method ("tie-breaking"-algorithm).
    /// The results are sorted by the first field, and only for those results where the first field value is identical (tie) the results are sub-sorted by the second field,
    /// until the n-th field value is either not equal or the last field is reached.
    /// A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th search criterium.
    /// Sort is only enabled on facet fields that are defined in schema at create_index!
    /// Examples:
    /// - result_sort = vec![ResultSort {field: "price".into(), order: SortOrder::Descending},ResultSort {field: "language".into(), order: SortOrder::Ascending}];
    #[schema(required = false, example = json!([{"field": "date", "order": "Ascending" }]))]
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::ResultSort>)]
    pub result_sort: Vec<ResultSort>,
    /// Specify default query type: (default=Intersection). This can be overwritten by search operator within the query string (+-"").
    #[schema(required = false, example = QueryType::Intersection)]
    #[serde(default = "query_type_api")]
    #[schema(value_type = api_schema::QueryType)]
    pub query_type_default: QueryType,
    /// Specify search mode: (default=Lexical).
    #[schema(required = false, example = SearchMode::Lexical)]
    #[serde(default = "search_mode_api")]
    #[schema(value_type = api_schema::SearchMode)]
    pub search_mode: SearchMode,
}

fn search_mode_api() -> SearchMode {
    SearchMode::Lexical
}

fn query_type_api() -> QueryType {
    QueryType::Intersection
}

fn length_api() -> usize {
    10
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
    /// Total number of search results matching the query
    pub count_total: usize,
    /// Vector of lexical search query terms.
    pub query_terms: Vec<String>,
    #[schema(value_type=Vec<HashMap<String, serde_json::Value>>)]
    /// Vector of search result documents
    pub results: Vec<Document>,
}

/// Create index request object
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateIndexRequest {
    /// Index name, used for informational purposes only.
    #[schema(example = "demo_index")]
    pub index_name: String,
    #[schema(required = true, example = json!([
    {"field":"title","field_type":"Text","store":true,"index_lexical":true,"boost":10.0},
    {"field":"body","field_type":"Text","store":true,"index_lexical":true,"longest":true},
    {"field":"url","field_type":"Text","store":true,"index_lexical":false},
    {"field":"date","field_type":"Timestamp","store":true,"index_lexical":false,"facet":true}]))]
    /// Schema definition for the index: field name, field type, and indexing options. The schema defines how documents are indexed and searched. It specifies the fields that are indexed, stored, and used for faceting, as well as the field types and their properties. It also defines whether lexical, hybrid, or vector search is enabled for each field.
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::SchemaField>)]
    pub schema: Vec<SchemaField>,
    /// Specify similarity measure for the index: (default=Bm25fProximity). The similarity function is used to calculate the relevance score of search results for a given search query. The choice of similarity function can affect search performance and relevance, depending on the characteristics of the text being indexed and the search queries being executed.
    #[serde(default = "similarity_type_api")]
    #[schema(value_type = api_schema::LexicalSimilarity)]
    pub similarity: LexicalSimilarity,
    /// Specify tokenizer type for the index: (default=UnicodeAlphanumeric). The tokenizer is used to split text into tokens for indexing and searching. The choice of tokenizer can affect search performance and relevance, depending on the language and characteristics of the text being indexed.
    #[serde(default = "tokenizer_type_api")]
    #[schema(value_type = api_schema::TokenizerType)]
    pub tokenizer: TokenizerType,
    #[serde(default)]
    /// Specify stemmer
    #[schema(value_type = api_schema::StemmerType)]
    pub stemmer: StemmerType,
    /// Specify stop words for the index. Stop words are not indexed and not searched for. This can be used to reduce index size and improve search performance by excluding high-frequency, low-information terms from the index.
    #[serde(default)]
    #[schema(value_type = api_schema::StopwordType)]
    pub stop_words: StopwordType,
    /// Specify frequent words for the index. Frequent words are used to optimize search performance for high-frequency terms.
    #[serde(default)]
    #[schema(value_type = api_schema::FrequentwordType)]
    pub frequent_words: FrequentwordType,
    /// Specify n-gram indexing for the index. N-gram indexing can improve search performance for certain types of queries.
    /// The n-gram set is defined as a bitwise combination of the following values:
    /// - NgramSet::SingleTerm = 0b00000000,,
    /// - NgramSet::NgramFF = 0b00000001, (Ngram frequent frequent)
    /// - NgramSet::NgramFR = 0b00000010, (Ngram frequent rare)
    /// - NgramSet::NgramRF = 0b00000011, (Ngram rare frequent)
    /// - NgramSet::NgramFFF = 0b00000100, (Ngram frequent frequent frequent)
    /// - NgramSet::NgramRFF = 0b00000101, (Ngram rare frequent frequent)
    /// - NgramSet::NgramFFR = 0b00000110, (Ngram frequent frequent rare)
    /// - NgramSet::NgramFRF = 0b00000111, (Ngram frequent rare frequent)
    ///
    /// For example, to enable both NgramFF and NgramFFF, set ngram_indexing to 5 (1 | 4).
    /// Note: enabling n-gram indexing (ngram_indexing>0) will increase index size and indexing time, but improves search performance of phrase queries with frequent terms.
    #[serde(default = "ngram_indexing_api")]
    pub ngram_indexing: u8,
    /// Enable document compression for the index. This can reduce the index size on disk and in memory, but may increase indexing and search latency. Default: Snappy compression.
    #[serde(default = "document_compression_api")]
    #[schema(value_type = api_schema::DocumentCompression)]
    pub document_compression: DocumentCompression,
    /// Specify synonyms for the index. Synonyms are used to expand search queries with additional terms that have the same or similar meaning, improving recall and search relevance. The multiway option specifies whether the synonym relationship is multiway (if true, all terms in the synonym set are considered synonyms of each other) or one-way (if false, only the first term in the synonym set is considered the main term, and the other terms are considered synonyms of the main term).
    #[schema(required = false, example = json!([{"terms":["berry","lingonberry","blueberry","gooseberry"],"multiway":false}]))]
    #[serde(default)]
    #[schema(value_type = Vec<api_schema::Synonym>)]
    pub synonyms: Vec<Synonym>,
    #[serde(default)]
    /// Enable clustering for vector search.
    #[schema(value_type = api_schema::Clustering)]
    pub clustering: Clustering,
    /// Configure external vector dimensions, precision, quantization and distance metric.
    #[serde(default)]
    #[schema(value_type = api_schema::Inference)]
    pub inference: Inference,
}

fn similarity_type_api() -> LexicalSimilarity {
    LexicalSimilarity::Bm25fProximity
}

fn tokenizer_type_api() -> TokenizerType {
    TokenizerType::UnicodeAlphanumeric
}

fn ngram_indexing_api() -> u8 {
    NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8
}

fn document_compression_api() -> DocumentCompression {
    DocumentCompression::Snappy
}

/// Specifies which document ID to return
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct GetIteratorRequest {
    /// base document ID to start the iteration from
    /// Use None to start from the beginning (take>0) or the end (take<0) of the index
    /// In JSON use null for None
    #[serde(default)]
    pub document_id: Option<u64>,
    /// the number of document IDs to skip
    #[serde(default)]
    pub skip: usize,
    /// the number of document IDs to return
    /// take>0: take next t document IDs, take<0: take previous t document IDs
    #[serde(default = "default_1usize")]
    pub take: isize,
    /// if true, also deleted document IDs are included in the result
    #[serde(default)]
    pub include_deleted: bool,
    /// if true, the documents are also retrieved along with their document IDs
    #[serde(default)]
    pub include_document: bool,
    /// which fields to return (if include_document is true, if empty then return all stored fields)
    #[serde(default)]
    pub fields: Vec<String>,
}

fn default_1usize() -> isize {
    1
}

/// Specifies which document and which field to return
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct GetDocumentRequest {
    /// which fields to return
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
    /// SeekStorm version the index was created with
    #[schema(example = "0.11.1")]
    pub version: String,
}
