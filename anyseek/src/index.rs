//! Public schema, analysis and vector configuration for RecordPartition.
//! Physical vector/metadata components are private and contain no lexical index.
use ahash::{AHashMap, AHashSet};
use indexmap::IndexMap;
use num_derive::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, sync::LazyLock};
use crate::{search::QueryType, vector::Inference};
pub(crate) use crate::component::{Index, IndexArc, Shard, ShardArc, create_index, open_index, IndexDocuments};
pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const ROARING_BLOCK_SIZE: usize = 65536;
pub(crate) const MAX_POSITIONS_PER_TERM: usize = 65536;
pub(crate) const MAX_QUERY_TERM_NUMBER: usize = 100;
/// A document is a flattened, single level of key-value pairs, where key is an arbitrary string, and value represents any valid JSON value.
pub type Document = IndexMap<String, Value>;

/// Legacy configuration tag retained in serialized index settings.
/// It does not select canonical Payload or native Tantivy package encoding.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum DocumentCompression {
    /// Retained `None` configuration tag.
    None,
    /// Retained `Snappy` configuration tag (default); no Snappy docstore is built.
    Snappy,
}

impl fmt::Display for DocumentCompression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DocumentCompression::None => write!(f, "None"),
            DocumentCompression::Snappy => write!(f, "Snappy"),
        }
    }
}

/// Committed index data is accessed through memory-mapped files.
/// Record publication, rather than an index commit, controls business visibility.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum AccessType {
    /// Memory-map committed index data; the OS manages resident pages.
    Mmap = 1,
}

/// Similarity type defines the scoring and ranking of the search results:
/// - Bm25f: considers documents composed from several fields, with different field lengths and importance
/// - Bm25fProximity: considers term proximity, e.g. for implicit phrase search with improved relevancy
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub enum LexicalSimilarity {
    /// Bm25f considers documents composed from several fields, with different field lengths and importance
    Bm25f = 0,
    /// Bm25fProximity considers term proximity, e.g. for implicit phrase search with improved relevancy
    #[default]
    Bm25fProximity = 1,
}

impl fmt::Display for LexicalSimilarity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LexicalSimilarity::Bm25f => write!(f, "Bm25f"),
            LexicalSimilarity::Bm25fProximity => write!(f, "Bm25fProximity"),
        }
    }
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
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Copy, Default)]
pub enum TokenizerType {
    /// Only ASCII alphabetic chars are recognized as token.
    #[default]
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

impl fmt::Display for TokenizerType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TokenizerType::AsciiAlphabetic => write!(f, "AsciiAlphabetic"),
            TokenizerType::UnicodeAlphanumeric => write!(f, "UnicodeAlphanumeric"),
            TokenizerType::UnicodeAlphanumericFolded => write!(f, "UnicodeAlphanumericFolded"),
            TokenizerType::Whitespace => write!(f, "Whitespace"),
            TokenizerType::WhitespaceLowercase => write!(f, "WhitespaceLowercase"),
            #[cfg(feature = "zh")]
            TokenizerType::UnicodeAlphanumericZH => write!(f, "UnicodeAlphanumericZH"),
        }
    }
}

/// FieldType defines the type of a field in the document: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, point, string, stringset, text.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
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
    #[default]
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

/// Defines a field in index schema: field, stored, indexed , field_type, facet, boost.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
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
    pub field_type: FieldType,
    /// Enable metadata filtering and sorting for string and numerical fields.
    /// both numerical and string fields can be indexed (indexed=true) and stored (stored=true) in the json document,
    /// but with field_facet=true they are additionally stored in a binary format, for fast filtering and sorting without reading and deserializing canonical Payload
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
    pub(crate) field_id: usize,
}

/// Defines a field in index schema: field, stored, indexed , field_type, facet, boost.
/// # Parameters
/// - field: unique name of a field
/// - store: only stored fields are returned in the search results
/// - index_lexical: index field into lexical index, only indexed fields can be searched.
/// - index_vector: index field into vector index, only indexed fields can be searched.
/// - field_type: type of a field: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, text, strings, JSON
/// - facet: enable metadata filtering and sorting by field values
/// - `longest`: This allows to annotate (manually set) the longest field in schema.
///   Otherwise the longest field will be automatically detected in first index_document.
///   Setting/detecting the longest field ensures efficient index encoding.
/// - boost: optional custom weight factor for Bm25 ranking
/// # Returns
/// - SchemaField
/// # Example
/// ```rust
/// use anyseek::index::{SchemaField, FieldType};
/// let schema_field = SchemaField::new("title".to_string(), true, true, false, FieldType::String16, false, false, 1.0);
/// ```
impl SchemaField {
    /// Creates a new SchemaField.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        field: String,
        store: bool,
        index_lexical: bool,
        index_vector: bool,
        field_type: FieldType,
        facet: bool,
        longest: bool,
        boost: f32,
    ) -> Self {
        SchemaField {
            field,
            store,
            index_lexical,
            index_vector,
            field_type,
            facet,
            longest,
            boost,

            indexed_field_id: 0,
            field_id: 0,
        }
    }
}

fn default_false() -> bool {
    false
}

fn is_default_bool(num: &bool) -> bool {
    !(*num)
}

fn default_1() -> f32 {
    1.0
}

fn is_default_f32(num: &f32) -> bool {
    *num == 1.0
}

/// StopwordType defines the stopword behavior: None, English, German, French, Spanish, Custom.
/// Stopwords are removed, both from index and query: for compact index size and faster queries.
/// Stopword removal has drawbacks: “The Who”, “Take That”, “Let it be”, “To be or not to be”, "The The", "End of days", "What might have been" are all valid queries for bands, songs, movies, literature,
/// but become impossible when stopwords are removed.
/// The lists of stop_words and frequent_words should not overlap.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub enum StopwordType {
    /// No stopwords
    #[default]
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

/// FrequentwordType defines the frequentword behavior: None, English, German, French, Spanish, Custom.
/// Adjacent frequent terms are combined to bi-grams, both in index and query: for shorter posting lists and faster phrase queries (only for bi-grams of frequent terms).
/// The lists of stop_words and frequent_words should not overlap.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub enum FrequentwordType {
    /// No frequent words
    None,
    /// English frequent words
    #[default]
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

/// Clustering defines the clustering behavior for approximate nearest neighbor (ANN) search: None, Auto, Fixed(usize).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Default)]
pub enum Clustering {
    /// Exhaustive vector search, no clustering/ANN.
    None,
    /// The number of clusters is automatically determined depending on the number of vectors per level and shard.
    #[default]
    Auto,
    /// Set the number of clusters to a fixed value per level and shard.
    Fixed(usize),
}

impl fmt::Display for Clustering {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Clustering::None => write!(f, "None"),
            Clustering::Auto => write!(f, "Auto"),
            Clustering::Fixed(value) => write!(f, "Fixed({})", value),
        }
    }
}

/// Length denominator selected when creating an index; immutable thereafter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum FieldLengthNormalization {
    /// Historical shared sum of field lengths divided by physical document count.
    #[default]
    Shared,
    /// Each field uses its decoded length sum divided by its nonempty document count.
    PerField,
}

/// Immutable pre-tokenization text normalization, shared by index and query.
/// Missing metadata defaults to None; an existing index is never auto-converted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum TextNormalization {
    /// Preserve the existing analysis behavior (default).
    #[default]
    None,
    /// Phrase-aware t2s using pinned OpenCC 1.1.9 tables, without regional rules.
    OpenCcT2s1_1_9,
}

/// Specifies LexicalSimilarity, TokenizerType and AccessType when creating an new index
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexMetaObject {
    /// unique index ID
    /// Only used in anyseek server, not by the anyseek library itself.
    /// In the anyseek server with REST API, the index ID is used to specify the index (within the API key) where you want to index and search.
    pub id: u64,
    /// index name: used informational purposes
    pub name: String,
    /// LexicalSimilarity defines the scoring and ranking of the search results: Bm25f or Bm25fProximity
    pub lexical_similarity: LexicalSimilarity,
    /// TokenizerType defines the tokenizer behavior: AsciiAlphabetic, UnicodeAlphanumeric, UnicodeAlphanumericFolded, UnicodeAlphanumericZH
    pub tokenizer: TokenizerType,
    /// Pre-tokenization rule for this index and every query. Immutable after creation.
    #[serde(default)]
    pub text_normalization: TextNormalization,
    /// Persisted length policy; Shared remains the conservative default.
    #[serde(default)]
    pub field_length_normalization: FieldLengthNormalization,

    /// StopwordType defines the stopword behavior: None, English, German, French, Spanish, Custom.
    /// Stopwords are removed, both from index and query: for compact index size and faster queries.
    /// Stopword removal has drawbacks: “The Who”, “Take That”, “Let it be”, “To be or not to be”, "The The", "End of days", "What might have been" are all valid queries for bands, songs, movies, literature,
    /// but become impossible when stopwords are removed.
    /// The lists of stop_words and frequent_words should not overlap.
    #[serde(default)]
    pub stop_words: StopwordType,
    /// FrequentwordType defines the frequentword behavior: None, English, German, French, Spanish, Custom.
    /// Adjacent frequent terms are combined to bi-grams, both in index and query: for shorter posting lists and faster phrase queries (only for bi-grams of frequent terms).
    /// The lists of stop_words and frequent_words should not overlap.
    #[serde(default)]
    pub frequent_words: FrequentwordType,
    /// N-gram indexing: n-grams are indexed in addition to single terms, for fast phrase search, at the cost of higher index size
    /// Preference valid both for index time and query time. Any change requires reindexing.
    /// bitwise OR flags:
    /// SingleTerm = 0b00000000, always enabled in addition to the other optional NgramSet below
    /// NgramFF    = 0b00000001, frequent frequent
    /// NgramFR    = 0b00000010, frequent rare
    /// NgramRF    = 0b00000100, rare frequent
    /// NgramFFF   = 0b00001000, frequent frequent frequent
    /// NgramRFF   = 0b00010000, rare frequent frequent
    /// NgramFFR   = 0b00100000, frequent frequent rare
    /// NgramFRF   = 0b01000000, frequent rare frequent
    /// When **minimum index size** is more important than phrase query latency, we recommend **Single Terms**:
    /// `NgramSet::SingleTerm as u8`
    /// For a **good balance of latency and index size** cost, we recommend **Single Terms + Frequent Bigrams + Frequent Trigrams** (default):
    /// `NgramSet::SingleTerm as u8 | NgramSet::NgramFF as u8 | NgramSet::NgramFFF`
    /// When **minimal phrase query latency** is more important than low index size, we recommend **Single Terms + Mixed Bigrams + Frequent Trigrams**:
    /// `NgramSet::SingleTerm as u8 | NgramSet::NgramFF as u8 | NgramSet::NgramFR as u8 | NgramSet::NgramRF | NgramSet::NgramFFF`
    #[serde(default = "ngram_indexing_default")]
    pub ngram_indexing: u8,

    /// Retained configuration tag; does not change current Payload/native encoding.
    #[serde(default = "doc_store_compression_default")]
    pub document_compression: DocumentCompression,

    /// AccessType defines where the index resides during search: Mmap
    pub access_type: AccessType,

    /// clustering defines the clustering behavior for approximate nearest neighbor (ANN) search: None, Auto, Fixed(usize).
    #[serde(default)]
    pub clustering: Clustering,

    /// External vector source: a Json array field or a Binary field with Base64 network-order bytes.
    #[serde(default)]
    pub inference: Inference,
}

fn ngram_indexing_default() -> u8 {
    NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8
}

fn doc_store_compression_default() -> DocumentCompression {
    DocumentCompression::Snappy
}

/// Metadata dictionary used by filtering and sorting. Value IDs retain insertion order.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct FacetField {
    /// Field name.
    pub name: String,
    /// String values or string sets, without occurrence counts.
    pub values: IndexMap<String, Vec<String>>,
    #[serde(skip)]
    pub(crate) offset: usize,
    #[serde(skip)]
    pub(crate) field_type: FieldType,
}

/// Get the version of the anyseek search library
pub fn version() -> &'static str {
    VERSION
}

/// N-gram indexing: n-grams are indexed in addition to single terms, for faster phrase search, at the cost of higher index size
/// Setting valid both for index time and query time. Any change requires reindexing.
/// bitwise OR flags:
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, FromPrimitive)]
pub enum NgramSet {
    /// no n-grams, only single terms are indexed
    SingleTerm = 0b00000000,
    /// Ngram frequent frequent
    NgramFF = 0b00000001,
    /// Ngram frequent rare
    NgramFR = 0b00000010,
    /// Ngram rare frequent
    NgramRF = 0b00000100,
    /// Ngram frequent frequent frequent
    NgramFFF = 0b00001000,
    /// Ngram rare frequent frequent
    NgramRFF = 0b00010000,
    /// Ngram frequent frequent rare
    NgramFFR = 0b00100000,
    /// Ngram frequent rare frequent
    NgramFRF = 0b01000000,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, FromPrimitive, Default)]
pub(crate) enum NgramType {
    /// no n-grams, only single terms are indexed
    #[default]
    SingleTerm = 0,
    /// Ngram frequent frequent
    NgramFF = 1,
    /// Ngram frequent rare
    NgramFR = 2,
    /// Ngram rare frequent
    NgramRF = 3,
    /// Ngram frequent frequent frequent
    NgramFFF = 4,
    /// Ngram rare frequent frequent
    NgramRFF = 5,
    /// Ngram frequent frequent rare
    NgramFFR = 6,
    /// Ngram frequent rare frequent
    NgramFRF = 7,
}

#[derive(Default, Debug, Deserialize, Serialize, Clone)]
pub(crate) struct TermObject {
    pub key_hash: u64,
    pub key0: u32,
    pub term: String,

    pub ngram_type: NgramType,

    pub term_ngram_2: String,
    pub term_ngram_1: String,
    pub term_ngram_0: String,
    pub field_vec_ngram1: Vec<(usize, u32)>,
    pub field_vec_ngram2: Vec<(usize, u32)>,
    pub field_vec_ngram3: Vec<(usize, u32)>,

    pub field_positions_vec: Vec<Vec<u16>>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub(crate) struct NonUniqueTermObject {
    pub term: String,
    pub ngram_type: NgramType,

    pub term_ngram_2: String,
    pub term_ngram_1: String,
    pub term_ngram_0: String,
    pub op: QueryType,
}

/// system endianess: true: the system is little endian, false: the system is big endian (network byte order).
pub static IS_SYSTEM_LE: LazyLock<bool> = LazyLock::new(|| u16::from_ne_bytes([1, 0]) == 1);

/// AVX2 support enabled
pub static IS_AVX2: LazyLock<bool> = LazyLock::new(|| {
    #[cfg(target_arch = "x86_64")]
    let is_avx2 = is_x86_feature_detected!("avx2");
    #[cfg(not(target_arch = "x86_64"))]
    let is_avx2 = false;
    is_avx2
});

/// NEON support enabled (always `true` on aarch64 — Advanced SIMD is a
/// mandatory part of ARMv8). The flag exists for parity with `IS_AVX2`
/// so the rest of the crate can branch on it the same way.
pub static IS_NEON: LazyLock<bool> = LazyLock::new(|| {
    #[cfg(target_arch = "aarch64")]
    let is_neon = std::arch::is_aarch64_feature_detected!("neon");
    #[cfg(not(target_arch = "aarch64"))]
    let is_neon = false;
    is_neon
});

/// Any SIMD backend (AVX2 / NEON) is available. Used by call sites to gate
/// the SIMD path; on archs without a backend this is always `false`.
pub static IS_SIMD: LazyLock<bool> = LazyLock::new(|| *IS_AVX2 || *IS_NEON);

/// Persistent term hash contract of index format 8.0. Changing it requires a format bump.
pub const TERM_HASH_ALGORITHM: &str = "xxh64-utf8-v1";

#[inline]
pub(crate) fn hash32(term_bytes: &[u8]) -> u32 {
    crate::term_hash::segment_hash(term_bytes)
}

#[inline]
pub(crate) fn hash64(term_bytes: &[u8]) -> u64 {
    crate::term_hash::term_key(term_bytes)
}

/// Build the exact persisted analysis dictionaries for both native and legacy callers.
pub(crate) fn analysis_dictionaries(
    meta: &IndexMetaObject,
) -> (AHashSet<String>, Vec<String>, AHashSet<u64>) {
    let stop_words: AHashSet<String> = match &meta.stop_words {
        StopwordType::None => AHashSet::new(),
        StopwordType::English => FREQUENT_EN.lines().map(|x| x.to_string()).collect(),
        StopwordType::German => FREQUENT_DE.lines().map(|x| x.to_string()).collect(),
        StopwordType::French => FREQUENT_FR.lines().map(|x| x.to_string()).collect(),
        StopwordType::Spanish => FREQUENT_ES.lines().map(|x| x.to_string()).collect(),
        StopwordType::Custom { terms } => terms.iter().map(|x| x.to_string()).collect(),
    };

    let frequent_words: Vec<String> = match &meta.frequent_words {
        FrequentwordType::None => Vec::new(),
        FrequentwordType::English => {
            let mut words: Vec<String> = FREQUENT_EN.lines().map(|x| x.to_string()).collect();
            words.sort_unstable();
            words
        }
        FrequentwordType::German => {
            let mut words: Vec<String> = FREQUENT_DE.lines().map(|x| x.to_string()).collect();
            words.sort_unstable();
            words
        }
        FrequentwordType::French => {
            let mut words: Vec<String> = FREQUENT_FR.lines().map(|x| x.to_string()).collect();
            words.sort_unstable();
            words
        }
        FrequentwordType::Spanish => {
            let mut words: Vec<String> = FREQUENT_ES.lines().map(|x| x.to_string()).collect();
            words.sort_unstable();
            words
        }
        FrequentwordType::Custom { terms } => {
            let mut words: Vec<String> = terms.iter().map(|x| x.to_string()).collect();
            words.sort_unstable();
            words
        }
    };

    // Metadata remains as supplied; the analysis dictionaries use the same
    // pinned normalization as document/query tokens, including after reopen.
    let (stop_words, frequent_words) =
        if meta.text_normalization == TextNormalization::OpenCcT2s1_1_9 {
            let stop_words = stop_words
                .into_iter()
                .map(|word| crate::opencc_t2s::convert(&word).into_owned())
                .collect();
            let mut frequent_words: Vec<String> = frequent_words
                .into_iter()
                .map(|word| crate::opencc_t2s::convert(&word).into_owned())
                .collect();
            frequent_words.sort_unstable();
            frequent_words.dedup();
            (stop_words, frequent_words)
        } else {
            (stop_words, frequent_words)
        };

    let frequent_hashset: AHashSet<u64> = frequent_words
        .iter()
        .map(|x| hash64(x.as_bytes()))
        .collect();
    (stop_words, frequent_words, frequent_hashset)
}

static FREQUENT_EN: &str = include_str!("../assets/dictionaries/frequent_en.txt");
static FREQUENT_DE: &str = include_str!("../assets/dictionaries/frequent_de.txt");
static FREQUENT_FR: &str = include_str!("../assets/dictionaries/frequent_fr.txt");
static FREQUENT_ES: &str = include_str!("../assets/dictionaries/frequent_es.txt");

pub(crate) const NUM_FREE_VALUES: u32 = 24;

/// Compress an u32 to a byte, preserving 4 significant bits.
/// used for compressing n-gram frequent_term positions_count and doc/field length
/// Ported from Lucene SmallFloat.java https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java
pub(crate) fn int_to_byte4(i: u32) -> u8 {
    if i < NUM_FREE_VALUES {
        i as u8
    } else {
        let ii = i - NUM_FREE_VALUES;
        let num_bits = 32 - ii.leading_zeros();
        if num_bits < 4 {
            (NUM_FREE_VALUES + ii) as u8
        } else {
            let shift = num_bits - 4;
            (NUM_FREE_VALUES + (((ii >> shift) & 0x07) | (shift + 1) << 3)) as u8
        }
    }
}

/// Decompress a byte that has been compressed with intToByte4(int), to an u32
/// used for pre-calculating DOCUMENT_LENGTH_COMPRESSION table. Decompressing n-gram frequent_term positions_count and doc/field length via table lookup.
/// Ported from Lucene SmallFloat.java https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java
pub(crate) const fn byte4_to_int(b: u8) -> u32 {
    if (b as u32) < NUM_FREE_VALUES {
        b as u32
    } else {
        let i = b as u32 - NUM_FREE_VALUES;
        let bits = i & 0x07;
        let shift = i >> 3;
        if shift == 0 {
            NUM_FREE_VALUES + bits
        } else {
            NUM_FREE_VALUES + ((bits | 0x08) << (shift - 1))
        }
    }
}

/// Pre-calculated DOCUMENT_LENGTH_COMPRESSION table for fast lookup.
pub(crate) const DOCUMENT_LENGTH_COMPRESSION: [u32; 256] = {
    let mut k2 = [0; 256];
    let mut i = 0usize;
    while i < 256 {
        k2[i] = byte4_to_int(i as u8);
        i += 1;
    }
    k2
};

pub(crate) fn object_values_to_string_vec_recursive(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => out.push(s.clone()),
        Value::Array(arr) => {
            for v in arr {
                object_values_to_string_vec_recursive(v, out);
            }
        }
        Value::Object(map) => {
            for v in map.values() {
                object_values_to_string_vec_recursive(v, out);
            }
        }
        _ => {}
    }
}
