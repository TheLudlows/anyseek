//! Service-only OpenAPI descriptions for core values.
//! Shared core values use these descriptions; public request settings live in api_types.
//! Remote Serialize derives check public field/variant layouts against the core at compile time.
#![allow(dead_code, missing_docs)]
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::Value;
use std::ops::Range;
use utoipa::ToSchema;

/// FacetFilter:
/// either numerical range facet filter (range start/end) or
/// string facet filter (vector of strings) at least one (boolean OR) must match.
#[derive(ToSchema, Serialize)]
#[serde(remote = "crate::search::FacetFilter")]
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
#[serde(remote = "crate::index::FieldType")]
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

/// Iterator
#[derive(ToSchema, Serialize)]
#[serde(remote = "crate::iterator::IteratorResult")]
pub struct IteratorResult {
    /// number of actually skipped documents
    pub skip: usize,
    /// document IDs, and optionally the documents themselves
    #[schema(value_type = Vec<IteratorResultItem>)]
    pub results: Vec<crate::iterator::IteratorResultItem>,
}

/// Iterator result
#[derive(ToSchema, Serialize)]
#[serde(remote = "crate::iterator::IteratorResultItem")]
pub struct IteratorResultItem {
    /// document ID
    pub doc_id: u64,
    /// document
    pub doc: Option<IndexMap<String, Value>>,
}

/// Vector precision
#[derive(ToSchema, Serialize)]
#[serde(remote = "crate::vector::Precision")]
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
#[serde(remote = "crate::vector::Quantization")]
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
#[serde(remote = "crate::search::ResultSort")]
pub struct ResultSort {
    /// name of the facet field to sort by
    pub field: String,
    /// Sort order: Ascending or Descending
    #[schema(value_type = SortOrder)]
    pub order: crate::search::SortOrder,
}

/// Specifies the sort order for the search results.
#[derive(ToSchema, Serialize)]
#[serde(remote = "crate::search::SortOrder")]
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
#[serde(remote = "crate::index::StopwordType")]
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
#[serde(remote = "crate::index::TokenizerType")]
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
#[serde(remote = "crate::vector_similarity::VectorSimilarity")]
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

/// One document or a batch of documents.
#[derive(ToSchema)]
#[serde(untagged)]
pub enum WriteDocuments {
    Single(std::collections::HashMap<String, Value>),
    Batch(Vec<std::collections::HashMap<String, Value>>),
}

/// Versioned pre-tokenization rule. None preserves current behavior.
#[derive(Serialize, ToSchema)]
#[serde(remote = "crate::index::TextNormalization")]
pub enum TextNormalization {
    /// No extra normalization.
    None,
    /// OpenCC 1.1.9 phrase-aware t2s; no regional vocabulary conversion.
    OpenCcT2s1_1_9,
}

/// Immutable field length normalization policy.
#[derive(Serialize, ToSchema)]
#[serde(remote = "crate::index::FieldLengthNormalization")]
pub enum FieldLengthNormalization {
    /// Historical shared length average.
    Shared,
    /// Nonempty physical document average for each field.
    PerField,
}

/// Zero-based reciprocal rank fusion. Omitted fields preserve legacy defaults.
#[derive(ToSchema, Serialize)]
#[serde(remote = "crate::search::RrfOptions")]
pub struct RrfOptions {
    /// Positive finite smoothing constant; one-based k=60 is 61 here.
    #[schema(default = 0.6, exclusive_minimum = 0)]
    #[serde(default)]
    pub constant: f32,
    #[schema(default = 1, minimum = 0)]
    #[serde(default)]
    pub keyword_weight: f32,
    #[schema(default = 1, minimum = 0)]
    #[serde(default)]
    pub vector_weight: f32,
    /// Per-shard TopK budget and global fusion cap; omitted preserves legacy window.
    #[schema(minimum = 1)]
    pub keyword_candidates: Option<usize>,
    /// Per-shard TopK budget and global fusion cap; does not bound probes or scanned vectors.
    #[schema(minimum = 1)]
    pub vector_candidates: Option<usize>,
}
