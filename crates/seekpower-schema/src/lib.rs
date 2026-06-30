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

    pub fn vector(name: &str) -> Self {
        Self {
            field: name.to_string(),
            store: false,
            index_lexical: false,
            index_vector: true,
            field_type: FieldType::Vector,
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
    String16,
    String32,
    StringSet16,
    StringSet32,
    Vector,
}

/// Schema map: field_name -> SchemaField.
pub type SchemaMap = ahash::AHashMap<String, SchemaField>;

/// Index configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexConfig {
    // Tokenizer
    pub tokenizer: TokenizerType,
    pub stemmer_language: Option<String>,
    pub stop_words: Option<Vec<String>>,
    pub lowercase: bool,

    // Vector
    pub vector_dimensions: u32,
    pub vector_similarity: Similarity,
    pub clustering: ClusteringConfig,
    pub quantization: QuantType,

    // Compression
    pub doc_compression: DocCompression,

    // Cache
    pub posting_cache_size_mb: usize,
    pub cluster_cache_size_mb: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            tokenizer: TokenizerType::Unicode,
            stemmer_language: None,
            stop_words: None,
            lowercase: true,
            vector_dimensions: 768,
            vector_similarity: Similarity::Cosine,
            clustering: ClusteringConfig::Auto,
            quantization: QuantType::SqI8,
            doc_compression: DocCompression::Snappy,
            posting_cache_size_mb: 256,
            cluster_cache_size_mb: 512,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DocCompression {
    None,
    Snappy,
    Lz4,
    Zstd,
}

/// Index statistics (persisted in m:{path}:stats).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct IndexStats {
    pub doc_count: u64,
    pub vector_count: u64,
    pub total_doc_length: u64,
    pub avg_doc_length: f32,
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
        let field = SchemaField::vector("embedding");
        assert_eq!(field.field, "embedding");
        assert!(!field.store);
        assert!(!field.index_lexical);
        assert!(field.index_vector);
        assert!(matches!(field.field_type, FieldType::Vector));
        assert_eq!(field.boost, 1.0);
    }

    #[test]
    fn test_schema_field_with_boost() {
        let field = SchemaField::text("title").with_boost(2.5);
        assert_eq!(field.boost, 2.5);
    }

    #[test]
    fn test_schema_field_with_store() {
        let field = SchemaField::vector("embedding").with_store(true);
        assert!(field.store);
    }

    #[test]
    fn test_index_config_default() {
        let config = IndexConfig::default();
        assert!(matches!(config.tokenizer, TokenizerType::Unicode));
        assert!(config.stemmer_language.is_none());
        assert!(config.stop_words.is_none());
        assert!(config.lowercase);
        assert_eq!(config.vector_dimensions, 768);
        assert!(matches!(config.vector_similarity, Similarity::Cosine));
        assert!(matches!(config.clustering, ClusteringConfig::Auto));
        assert!(matches!(config.quantization, QuantType::SqI8));
        assert!(matches!(config.doc_compression, DocCompression::Snappy));
        assert_eq!(config.posting_cache_size_mb, 256);
        assert_eq!(config.cluster_cache_size_mb, 512);
    }

    #[test]
    fn test_index_config_roundtrip_json() {
        let config = IndexConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: IndexConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.vector_dimensions, config.vector_dimensions);
        assert!(matches!(parsed.tokenizer, TokenizerType::Unicode));
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
        map.insert("embedding".to_string(), SchemaField::vector("embedding"));
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("title"));
        assert!(map.contains_key("embedding"));
    }
}
