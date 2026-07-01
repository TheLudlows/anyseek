/// Field definition in the index schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaField {
    pub field: String,
    pub store: bool,
    pub index_lexical: bool,
    pub index_vector: bool,
    pub field_type: FieldType,
    pub boost: f32,
}

impl SchemaField {
    pub fn text(name: &str) -> Self {
        Self {
            field: name.to_string(),
            store: true,
            index_lexical: true,
            index_vector: false,
            field_type: FieldType::Text,
            boost: 1.0,
        }
    }

    /// Create a vector field with specified dimensions.
    pub fn vector(name: &str, dimensions: u32) -> Self {
        Self {
            field: name.to_string(),
            store: false,
            index_lexical: false,
            index_vector: true,
            field_type: FieldType::Vector { dimensions },
            boost: 1.0,
        }
    }

    pub fn with_boost(mut self, boost: f32) -> Self {
        self.boost = boost;
        self
    }

    pub fn with_store(mut self, store: bool) -> Self {
        self.store = store;
        self
    }
}

/// Field type enumeration.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bool,
    Timestamp,
    Text,

    /// Vector field with specified dimensions.
    /// Uses f32 elements (can be extended to f64 in the future).
    Vector { dimensions: u32 },
}

/// Schema map: field_name -> SchemaField.
pub type SchemaMap = ahash::AHashMap<String, SchemaField>;

/// Index configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexConfig {
    // Tokenizer
    pub tokenizer: TokenizerType,
    pub stemmer_language: Option<String>,
    pub stop_words_preset: StopWordsPreset,
    pub stop_words: Option<Vec<String>>,
    pub lowercase: bool,

    // Vector
    pub vector_dimensions: u32,
    pub vector_similarity: Similarity,
    pub clustering: ClusteringConfig,
    pub quantization: QuantType,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            tokenizer: TokenizerType::Jieba,
            stemmer_language: None,
            stop_words_preset: StopWordsPreset::Auto,
            stop_words: None,
            lowercase: true,
            vector_dimensions: 768,
            vector_similarity: Similarity::Cosine,
            clustering: ClusteringConfig::Auto,
            quantization: QuantType::SqI8,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TokenizerType {
    Simple,
    Unicode,
    Jieba,
    Custom(String),
}

/// Preset stop-words source.
///
/// - `Auto` – pick a language based on the tokenizer type
///   (Jieba → Chinese, Simple/Unicode → English).
/// - `None` – no preset stop-words (the default before this field existed).
/// - `Language(code)` – use the `stop-words` crate's built-in list for the
///   given ISO 639-1 code (e.g. `"en"`, `"zh"`, `"ja"`, `"de"`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StopWordsPreset {
    Auto,
    None,
    Language(String),
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum Similarity {
    Dot,
    Cosine,
    Euclidean,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ClusteringConfig {
    Auto,
    Fixed(usize),
    None,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum QuantType {
    None,
    SqI8,
}

/// Index statistics (persisted in m:{path}:stats).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct IndexStats {
    pub doc_count: u64,
    pub vector_count: u64,
    pub total_doc_length: u64,
    pub avg_doc_length: f32,
}

// ---------------------------------------------------------------------------
// Storage key data-type tags (u8, used as 1-byte prefix in physical keys)
// ---------------------------------------------------------------------------

pub const TYPE_META: u8 = 0;  // schema, config, stats, fields
pub const TYPE_DOC: u8 = 1;   // document content
pub const TYPE_DICT: u8 = 2;  // term dictionary (term <-> term_id)
pub const TYPE_IDX: u8 = 3;   // inverted index (posting list)
pub const TYPE_VEC: u8 = 4;   // vector index (meta, medoid, cluster)
pub const TYPE_DELL: u8 = 5;  // deletion marker (tombstone)
pub const TYPE_LEN: u8 = 6;   // document length

/// Construct a key from multiple parts.
pub fn make_key(parts: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::new();
    for part in parts {
        key.extend_from_slice(part);
    }
    key
}

/// Validate an index name.
pub fn validate_name(name: &str) -> Result<(), String> {
    const NAME_MAX_LEN: usize = 128;

    if name.is_empty() {
        return Err("index name must not be empty".into());
    }
    if name.len() > NAME_MAX_LEN {
        return Err(format!("index name too long: {} > {} chars", name.len(), NAME_MAX_LEN));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err(format!("index name must match [a-zA-Z0-9_-]+: {:?}", name));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_field_text() {
        let field = SchemaField::text("title");
        assert_eq!(field.field, "title");
        assert!(field.store);
        assert!(field.index_lexical);
        assert!(!field.index_vector);
        assert!(matches!(field.field_type, FieldType::Text));
        assert_eq!(field.boost, 1.0);
    }

    #[test]
    fn test_schema_field_vector() {
        let field = SchemaField::vector("embedding", 768);
        assert_eq!(field.field, "embedding");
        assert!(!field.store);
        assert!(!field.index_lexical);
        assert!(field.index_vector);
        assert!(matches!(field.field_type, FieldType::Vector { dimensions: 768 }));
        assert_eq!(field.boost, 1.0);
    }

    #[test]
    fn test_schema_field_with_boost() {
        let field = SchemaField::text("title").with_boost(2.5);
        assert_eq!(field.boost, 2.5);
    }

    #[test]
    fn test_schema_field_with_store() {
        let field = SchemaField::vector("embedding", 384).with_store(true);
        assert!(field.store);
    }

    #[test]
    fn test_index_config_default() {
        let config = IndexConfig::default();
        assert!(matches!(config.tokenizer, TokenizerType::Jieba));
        assert!(config.stemmer_language.is_none());
        assert!(matches!(config.stop_words_preset, StopWordsPreset::Auto));
        assert!(config.stop_words.is_none());
        assert!(config.lowercase);
        assert_eq!(config.vector_dimensions, 768);
        assert!(matches!(config.vector_similarity, Similarity::Cosine));
        assert!(matches!(config.clustering, ClusteringConfig::Auto));
        assert!(matches!(config.quantization, QuantType::SqI8));
    }

    #[test]
    fn test_index_config_roundtrip_json() {
        let config = IndexConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: IndexConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.vector_dimensions, config.vector_dimensions);
        assert!(matches!(parsed.tokenizer, TokenizerType::Jieba));
    }

    #[test]
    fn test_index_stats_default() {
        let stats = IndexStats::default();
        assert_eq!(stats.doc_count, 0);
        assert_eq!(stats.vector_count, 0);
        assert_eq!(stats.total_doc_length, 0);
        assert_eq!(stats.avg_doc_length, 0.0);
    }

    #[test]
    fn test_schema_map_type() {
        let mut map: SchemaMap = ahash::AHashMap::new();
        map.insert("title".to_string(), SchemaField::text("title"));
        map.insert("embedding".to_string(), SchemaField::vector("embedding", 768));
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("title"));
        assert!(map.contains_key("embedding"));
    }

    #[test]
    fn test_make_key() {
        let key = make_key(&[b"medoid:", &42u32.to_le_bytes()]);
        assert!(key.starts_with(b"medoid:"));
        assert_eq!(&key[7..], &42u32.to_le_bytes());
    }

    #[test]
    fn test_validate_name_accepts_valid() {
        assert!(validate_name("blog").is_ok());
        assert!(validate_name("shop").is_ok());
        assert!(validate_name("a").is_ok());
        assert!(validate_name("foo_bar-baz123").is_ok());
        assert!(validate_name("__indexes__").is_ok());
        assert!(validate_name(&"a".repeat(128)).is_ok());
    }

    #[test]
    fn test_validate_name_rejects_empty() {
        assert!(validate_name("").is_err());
    }

    #[test]
    fn test_validate_name_rejects_too_long() {
        assert!(validate_name(&"a".repeat(129)).is_err());
    }

    #[test]
    fn test_validate_name_rejects_bad_chars() {
        assert!(validate_name("foo/bar").is_err());
        assert!(validate_name("foo:bar").is_err());
        assert!(validate_name("foo bar").is_err());
        assert!(validate_name("foo.bar").is_err());
        assert!(validate_name("foo@bar").is_err());
        assert!(validate_name("föo").is_err());
    }
}