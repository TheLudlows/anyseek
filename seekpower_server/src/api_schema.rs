//! Service-only OpenAPI descriptions for core values.
//! Requests still deserialize directly into core types: these adapters are not conversions.
//! Remote Serialize derives check public field/variant layouts against the core at compile time.
//! SchemaField is a documentation view because its internal field IDs are private.
#![allow(dead_code, missing_docs)]
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::Value;
use std::ops::Range;
use utoipa::ToSchema;

fn default_false() -> bool {
    false
}
fn default_1() -> f32 {
    1.0
}
fn is_default_bool(value: &bool) -> bool {
    !*value
}
fn is_default_f32(value: &f32) -> bool {
    *value == 1.0
}

///Specifies in which cluster to search for ANN results.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::vector_similarity::AnnMode")]
pub enum AnnMode {
    /// Search in all clusters (default)
    All,
    /// Search only in the clusters with the highest similarity scores to the query vector.
    /// The number of clusters to search is specified by the n-probe parameter.
    /// You cannot directly set a specific, guaranteed recall number (e.g., "always give me 95% recall@10"). There is no one-fits-all, there is no automatism.
    /// Instead, you manually tune parameters that control the tradeoff between query latency and accuracy.
    /// Because recall depends heavily on the structure of your specific data (distribution, dimensionality, and clustering) and queries,
    /// there is always a trial-and-error (benchmarking) phase required to determine the right settings for your data.
    Nprobe(usize),
    /// Search only in clusters with similarity scores to the query vector above the specified threshold.
    /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
    /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
    Similaritythreshold(f32),
    /// Search only in the clusters with the highest similarity scores to the query vector, but only if their similarity scores are above the specified threshold, and up to the number of clusters specified by the n-probe parameter.
    /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
    /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
    NprobeSimilaritythreshold(usize, f32),
}

/// Clustering defines the clustering behavior for approximate nearest neighbor (ANN) search: None, Auto, Fixed(usize).
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::index::Clustering")]
pub enum Clustering {
    /// Exhaustive vector search, no clustering/ANN.
    None,
    /// The number of clusters is automatically determined depending on the number of vectors per level and shard.
    Auto,
    /// Set the number of clusters to a fixed value per level and shard.
    Fixed(usize),
}

/// Compression type for document store
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::index::DocumentCompression")]
pub enum DocumentCompression {
    /// No compression (fastest, largest size)
    None,
    /// Snappy document compression (default).
    Snappy,
}

/// FacetFilter:
/// either numerical range facet filter (range start/end) or
/// string facet filter (vector of strings) at least one (boolean OR) must match.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::search::FacetFilter")]
pub enum FacetFilter {
    /// U8 range filter
    U8 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU8)]
        filter: Range<u8>,
    },
    /// U16 range filter
    U16 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU16)]
        filter: Range<u16>,
    },
    /// U32 range filter
    U32 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU32)]
        filter: Range<u32>,
    },
    /// U64 range filter
    U64 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeU64)]
        filter: Range<u64>,
    },
    /// I8 range filter
    I8 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI8)]
        filter: Range<i8>,
    },
    /// I16 range filter
    I16 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI16)]
        filter: Range<i16>,
    },
    /// I32 range filter
    I32 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI32)]
        filter: Range<i32>,
    },
    /// I64 range filter
    I64 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI64)]
        filter: Range<i64>,
    },
    /// Timestamp range filter, Unix timestamp: the number of seconds since 1 January 1970
    Timestamp {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeI64)]
        filter: Range<i64>,
    },
    /// F32 range filter
    F32 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeF32)]
        filter: Range<f32>,
    },
    /// F64 range filter
    F64 {
        /// field name
        field: String,
        /// filter: range start, range end
        #[schema(value_type=RangeF64)]
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

/// FieldType defines the type of a field in the document: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, point, string, stringset, text.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::index::FieldType")]
pub enum FieldType {
    /// Unsigned 8-bit integer
    U8,
    /// Unsigned 16-bit integer
    U16,
    /// Unsigned 32-bit integer
    U32,
    /// Unsigned 64-bit integer
    U64,
    /// Signed 8-bit integer
    I8,
    /// Signed 16-bit integer
    I16,
    /// Signed 32-bit integer
    I32,
    /// Signed 64-bit integer
    I64,
    /// Timestamp is identical to I64, but to be used for Unix timestamps <https://en.wikipedia.org/wiki/Unix_time>.
    /// The reason for a separate FieldType is to enable the UI to interpret I64 as timestamp without using the field name as indicator.
    /// For date facets and filtering.
    Timestamp,
    /// Floating point 32-bit
    F32,
    /// Floating point 64-bit
    F64,
    /// Boolean
    Bool,
    /// String16
    /// allows a maximum cardinality of 65_535 (16 bit) distinct values, is space-saving.
    /// For filtering and sorting with low cardinality fields, e.g. boolean fields, enum fields, country, language, status, category, tags, etc.
    String16,
    /// String32
    /// allows a maximum cardinality of 4_294_967_295 (32 bit) distinct values
    /// For filtering and sorting with low cardinality fields, e.g. boolean fields, enum fields, country, language, status, category, tags, etc.
    String32,
    /// StringSet16 is a set of strings, e.g. tags, categories, keywords, authors, genres, etc.
    /// allows a maximum cardinality of 65_535 (16 bit) distinct values, is space-saving.
    /// For filtering and sorting with low cardinality fields, e.g. boolean fields, enum fields, country, language, status, category, tags, etc.
    StringSet16,
    /// StringSet32 is a set of strings, e.g. tags, categories, keywords, authors, genres, etc.
    /// allows a maximum cardinality of 4_294_967_295 (32 bit) distinct values
    /// For filtering and sorting with low cardinality fields, e.g. boolean fields, enum fields, country, language, status, category, tags, etc.
    StringSet32,
    /// Text is a text field, that will be tokenized by the selected Tokenizer into string tokens.
    Text,
    /// Hierarchical JSON Object, that will be tokenized by the selected Tokenizer into string tokens.
    /// The text is extracted from all levels of the JSON object and combined into a single text string (values only, not keys).
    /// In that respect, it is similar to combined fields in other search engines.
    /// Despite being indexed as a single text string, the hierarchical structure of an JSON object is preserved in the stored document, so that it can be retrieved in the search results.
    Json,
    /// Binary encodes and stores binary data in base64 format. This field type will not be tokenized and indexed.
    /// For embedding binary data, e.g. images, audio, video, pdf, … in JSON or CSV documents. A self-contained alternative to storing URLs to external resources.
    /// Using the [Data URI scheme](https://en.wikipedia.org/wiki/Data_URI_scheme) in JavaScript you can create an Image object and put the base64 as its src, including the data:image... part like this:
    ///```javascript
    /// var image = new Image();
    /// image.src = 'data:image/png;base64,iVBORw0K...';
    /// document.body.appendChild(image);
    ///```
    Binary,
}

/// FrequentwordType defines the frequentword behavior: None, English, German, French, Spanish, Custom.
/// Adjacent frequent terms are combined to bi-grams, both in index and query: for shorter posting lists and faster phrase queries (only for bi-grams of frequent terms).
/// The lists of stop_words and frequent_words should not overlap.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::index::FrequentwordType")]
pub enum FrequentwordType {
    /// No frequent words
    None,
    /// English frequent words
    English,
    /// German frequent words
    German,
    /// French frequent words
    French,
    /// Spanish frequent words
    Spanish,
    /// Custom frequent words
    Custom {
        ///List of frequent terms, max. 256 terms.
        terms: Vec<String>,
    },
}

/// Vector configuration. Embeddings and document segmentation are supplied by callers.
/// The serialized `External` configuration is retained for existing external-vector indexes.
#[derive(ToSchema, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(remote = "seekpower::vector::Inference")]
pub enum Inference {
    /// External inference
    External {
        /// Number of dimensions for the embeddings.
        dimensions: usize,
        /// Data type for embeddings.
        #[schema(value_type = Precision)]
        precision: seekpower::vector::Precision,
        /// Quantization method for embeddings.
        #[schema(value_type = Quantization)]
        quantization: seekpower::vector::Quantization,
        /// Similarity metric to use for comparing embeddings, e.g. cosine similarity or euclidean distance.
        /// This should be the same similarity metric that was used during the training of the embedding model.
        #[schema(value_type = VectorSimilarity)]
        similarity: seekpower::vector_similarity::VectorSimilarity,
    },
    /// No inference
    None,
}

/// Iterator
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::iterator::IteratorResult")]
pub struct IteratorResult {
    /// number of actually skipped documents
    pub skip: usize,
    /// document IDs, and optionally the documents themselves
    #[schema(value_type = Vec<IteratorResultItem>)]
    pub results: Vec<seekpower::iterator::IteratorResultItem>,
}

/// Iterator result
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::iterator::IteratorResultItem")]
pub struct IteratorResultItem {
    /// document ID
    pub doc_id: u64,
    /// document
    pub doc: Option<IndexMap<String, Value>>,
}

/// Similarity type defines the scoring and ranking of the search results:
/// - Bm25f: considers documents composed from several fields, with different field lengths and importance
/// - Bm25fProximity: considers term proximity, e.g. for implicit phrase search with improved relevancy
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::index::LexicalSimilarity")]
pub enum LexicalSimilarity {
    /// Bm25f considers documents composed from several fields, with different field lengths and importance
    Bm25f = 0,
    /// Bm25fProximity considers term proximity, e.g. for implicit phrase search with improved relevancy
    Bm25fProximity = 1,
}

/// Vector precision
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::vector::Precision")]
pub enum Precision {
    /// No vector embedding, e.g. for lexical fields or fields without indexing.
    None = 0,
    /// 32-bit floating point vector embedding
    F32 = 1,
    /// 8-bit integer vector embedding
    I8 = 4,
}

/// Quantization method for embeddings.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::vector::Quantization")]
pub enum Quantization {
    /// Affine Scalar Quantization (SQ) f32 to i8 (8 bit per dimension).
    /// Affine quantization (or asymmetric quantization) maps high-precision floating-point numbers (e.g., FP32) to lower-precision integers (e.g., INT8)
    /// by linearly transforming them using a scaling factor and a zero-point offset.
    ScalarQuantizationI8,
    /// TurboQuant Quantization (TQ) f32 to i8 (8 bit per dimension).
    /// This implementation uses Fast Walsh-Hadamard Transform (FWHT) instead of Gram-Schmidt orthogonalization, and Quantized Johnson-Lindenstrauss (QJL) transformation.
    /// Random rotations and scalar quantization trade precision for a smaller representation.
    /// Measure recall and latency on the application's vectors and queries when choosing a method.
    TurboQuantI8,
    /// no quantization, keep f32
    None,
}

/// Specifies the default QueryType: The following query types are supported:
/// - **Union** (OR, disjunction),
/// - **Intersection** (AND, conjunction),
/// - **Phrase** (""),
/// - **Not** (-).
///
/// The default QueryType is superseded if the query parser detects that a different query type is specified within the query string (+ - "").
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::search::QueryType")]
pub enum QueryType {
    /// Union (OR, disjunction)
    Union = 0,
    /// Intersection (AND, conjunction)
    Intersection = 1,
    /// Phrase ("")
    Phrase = 2,
    /// Not (-)
    Not = 3,
}

/// F32 range filter
#[derive(ToSchema)]
pub struct RangeF32 {
    /// range start
    pub start: f32,
    /// range end
    pub end: f32,
}

/// F64 range filter
#[derive(ToSchema)]
pub struct RangeF64 {
    /// range start
    pub start: f64,
    /// range end
    pub end: f64,
}

/// I16 range filter
#[derive(ToSchema)]
pub struct RangeI16 {
    /// range start
    pub start: i16,
    /// range end
    pub end: i16,
}

/// I32 range filter
#[derive(ToSchema)]
pub struct RangeI32 {
    /// range start
    pub start: i32,
    /// range end
    pub end: i32,
}

/// I64 range filter
#[derive(ToSchema)]
pub struct RangeI64 {
    /// range start
    pub start: i64,
    /// range end
    pub end: i64,
}

/// I8 range filter
#[derive(ToSchema)]
pub struct RangeI8 {
    /// range start
    pub start: i8,
    /// range end
    pub end: i8,
}

/// U16 range filter
#[derive(ToSchema)]
pub struct RangeU16 {
    /// range start
    pub start: u16,
    /// range end
    pub end: u16,
}

/// U32 range filter
#[derive(ToSchema)]
pub struct RangeU32 {
    /// range start
    pub start: u32,
    /// range end
    pub end: u32,
}

/// U64 range filter
#[derive(ToSchema)]
pub struct RangeU64 {
    /// range start
    pub start: u64,
    /// range end
    pub end: u64,
}

/// U8 range filter
#[derive(ToSchema)]
pub struct RangeU8 {
    /// range start
    pub start: u8,
    /// range end
    pub end: u8,
}

/// Specifies the sort order for the search results.
#[derive(ToSchema, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(remote = "seekpower::search::ResultSort")]
pub struct ResultSort {
    /// name of the facet field to sort by
    pub field: String,
    /// Sort order: Ascending or Descending
    #[schema(value_type = SortOrder)]
    pub order: seekpower::search::SortOrder,
}

/// The following result types are supported:
/// - **Count** (count all results that match the query, but returning top-k results is not required)
/// - **Topk** (returns the top-k results per query, but counting all results that match the query is not required)
/// - **TopkCount** (returns the top-k results per query + count all results that match the query)
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::search::ResultType")]
pub enum ResultType {
    /// Count all results that match the query, without returning top-k results
    Count = 0,
    /// Return the top-k results per query, without counting all results that match the query
    Topk = 1,
    /// Return the top-k results per query and count all results that match the query
    TopkCount = 2,
}

/// Defines a field in index schema: field, stored, indexed , field_type, facet, boost.
#[derive(ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaField {
    /// unique name of a field
    pub field: String,
    /// only stored fields are returned in the search results
    pub store: bool,
    /// only indexed fields can be searched
    /// Even if index_lexical is true in the schema, the field in the actual document is optional. You can index documents without that field, but you cannot search for that field in those documents.
    pub index_lexical: bool,
    /// only indexed fields can be searched
    /// Vector fields are required and validated before the document is queued for indexing.
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub index_vector: bool,
    /// type of a field
    #[schema(value_type = FieldType)]
    pub field_type: seekpower::index::FieldType,
    /// Enable metadata filtering and sorting for string and numerical fields.
    /// both numerical and string fields can be indexed (indexed=true) and stored (stored=true) in the json document,
    /// but with field_facet=true they are additionally stored in a binary format, for fast filtering and sorting without docstore access (decompression, deserialization)
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub facet: bool,

    /// Indicate the longest field in schema.
    /// Otherwise the longest field will be automatically detected in first index_document.
    /// Setting/detecting the longest field ensures efficient index encoding.
    #[serde(skip_serializing_if = "is_default_bool")]
    #[serde(default = "default_false")]
    pub longest: bool,

    /// optional custom weight factor for Bm25 ranking
    #[serde(skip_serializing_if = "is_default_f32")]
    #[serde(default = "default_1")]
    pub boost: f32,

    #[serde(skip)]
    pub(crate) indexed_field_id: usize,
    #[serde(default)]
    #[schema(ignore)]
    pub(crate) field_id: usize,
}

/// Specifies the default QueryMode: The following query modes are supported:
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::search::SearchMode")]
pub enum SearchMode {
    /// Lexical search mode: Search results are retrieved based on exact matches of query terms with the indexed terms.
    Lexical,
    /// Vector search mode: Search results are retrieved based on the similarity of query vectors with the indexed vectors.
    Vector {
        /// Include only vectors with similarity scores above the specified threshold
        /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
        /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
        similarity_threshold: Option<f32>,
        /// Specifies in which clusters to search for ANN results.
        #[schema(value_type = AnnMode)]
        ann_mode: seekpower::vector_similarity::AnnMode,
    },
    /// Hybrid search mode: Search results are retrieved based on a combination of lexical and vector search.
    /// The relevance score of search results is calculated based on RRF (Reciprocal Rank Fusion) of the result positions in lexical and vector search.
    Hybrid {
        /// optional threshold to filter out low similarity scores
        /// For dot product similarity, the similarity threshold should be between 0.0 and 1.0, where higher values indicate higher similarity (identical=1.0).
        /// For Euclidean distance similarity, the similarity threshold should be between 0.0 and infinity, where lower values indicate higher similarity (identical=0.0).
        similarity_threshold: Option<f32>,
        /// Specifies in which clusters to search for ANN results.
        #[schema(value_type = AnnMode)]
        ann_mode: seekpower::vector_similarity::AnnMode,
    },
}

/// Specifies the sort order for the search results.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::search::SortOrder")]
pub enum SortOrder {
    /// Ascending sort order
    Ascending = 0,
    /// Descending sort order
    Descending = 1,
}

/// StopwordType defines the stopword behavior: None, English, German, French, Spanish, Custom.
/// Stopwords are removed, both from index and query: for compact index size and faster queries.
/// Stopword removal has drawbacks: “The Who”, “Take That”, “Let it be”, “To be or not to be”, "The The", "End of days", "What might have been" are all valid queries for bands, songs, movies, literature,
/// but become impossible when stopwords are removed.
/// The lists of stop_words and frequent_words should not overlap.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::index::StopwordType")]
pub enum StopwordType {
    /// No stopwords
    None,
    /// English stopwords
    English,
    /// German stopwords
    German,
    /// French stopwords
    French,
    /// Spanish stopwords
    Spanish,
    /// Custom stopwords
    Custom {
        ///List of stopwords.
        terms: Vec<String>,
    },
}

/// Defines tokenizer behavior:
/// AsciiAlphabetic
/// - Only ASCII alphabetic chars are recognized as token.
///
/// UnicodeAlphanumeric
/// - All Unicode alphanumeric chars are recognized as token.
/// - Allows '+' '-' '#' in middle or end of a token: c++, c#, block-max.
///
/// UnicodeAlphanumericFolded
/// - All Unicode alphanumeric chars are recognized as token.
/// - Allows '+' '-' '#' in middle or end of a token: c++, c#, block-max.
/// - Diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters are converted into its basic representation.
/// - Apostroph handling prevents that short term parts preceding or following the apostroph get indexed (e.g. "s" in "someone's").
/// - Tokenizing might be slower due to folding and apostroph processing.
///
/// UnicodeAlphanumericZH
/// - Implements Chinese word segmentation to segment continuous Chinese text into tokens for indexing and search.
/// - Supports mixed Latin and Chinese texts
/// - Requires feature #[cfg(feature = "zh")]
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::index::TokenizerType")]
pub enum TokenizerType {
    /// Only ASCII alphabetic chars are recognized as token.
    AsciiAlphabetic = 0,
    /// All Unicode alphanumeric chars are recognized as token.
    /// Allow '+' '-' '#' in middle or end of a token: c++, c#, block-max.
    UnicodeAlphanumeric = 1,
    /// All Unicode alphanumeric chars are recognized as token.
    /// Allows '+' '-' '#' in middle or end of a token: c++, c#, block-max.
    /// Diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters are converted into its basic representation.
    /// Apostroph handling prevents that short term parts preceding or following the apostroph get indexed (e.g. "s" in "someone's").
    /// Tokenizing might be slower due to folding and apostroph processing.
    UnicodeAlphanumericFolded = 2,
    /// Tokens are separated by whitespace.
    Whitespace = 3,
    /// Tokens are separated by whitespace. Token are converted to lowercase.
    WhitespaceLowercase = 4,
    /// Implements Chinese word segmentation to segment continuous Chinese text into tokens for indexing and search.
    /// Supports mixed Latin and Chinese texts
    /// Requires feature #[cfg(feature = "zh")]
    #[cfg(feature = "zh")]
    UnicodeAlphanumericZH = 5,
}

/// Similarity measure for comparing vector embeddings.
#[derive(ToSchema, Serialize)]
#[serde(remote = "seekpower::vector_similarity::VectorSimilarity")]
pub enum VectorSimilarity {
    /// Cosine Similarity
    /// Cosine similarity measures only the direction (angle) between vectors.
    /// At index time the vectors are normalized to a norm of 1.0, so that the cosine similarity can be computed efficiently as a dot product without needing to compute the norms at query time.
    /// The query vector is normalized to a norm of 1.0, so that the cosine similarity can be computed efficiently as a dot product without needing to compute the norms at query time.
    Cosine,
    /// Dot product (Inner Product),
    /// Dot product measures both the direction and the magnitude (length) of vectors.
    /// Use dot product, when using pre-normalized external embeddings, to prevent unnecessary normalization with cosine similarity.
    /// Dot product and cosine similarity produce the same ranking for L2-normalized vectors.
    Dot,
    /// Euclidean distance (L2)
    Euclidean,
}

/// Successful responses that retain the existing externally tagged Result encoding.
#[derive(ToSchema)]
pub struct WrappedResponse<T> {
    #[schema(rename = "Ok")]
    pub value: T,
}

/// One document or a batch of documents.
#[derive(ToSchema)]
#[serde(untagged)]
pub enum WriteDocuments {
    Single(std::collections::HashMap<String, Value>),
    Batch(Vec<std::collections::HashMap<String, Value>>),
}

/// One (document ID, replacement document) tuple or a batch of tuples.
#[derive(ToSchema)]
#[serde(untagged)]
pub enum UpdateDocuments {
    Single(DocumentReplacement),
    Batch(Vec<DocumentReplacement>),
}

/// JSON deletion forms; the raw clear command is documented separately as text/plain.
#[derive(ToSchema)]
#[serde(untagged)]
pub enum DeleteDocuments {
    Id(u64),
    Ids(Vec<u64>),
    Query(crate::api_types::SearchRequestObject),
}

#[derive(ToSchema)]
#[schema(min_items = 2, max_items = 2)]
pub struct DocumentReplacement(pub (u64, std::collections::HashMap<String, Value>));

#[derive(ToSchema)]
pub enum ClearCommand {
    #[schema(rename = "clear")]
    Clear,
}
