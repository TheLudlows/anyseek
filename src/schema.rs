use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldType {
    U8, U16, U32, U64,
    I8, I16, I32, I64,
    Timestamp,
    F32, F64,
    Bool,
    String16, String32,
    StringSet16, StringSet32,
    Text,
    Point,
    Vector,
}

impl FieldType {
    pub fn byte_size(&self) -> usize {
        match self {
            FieldType::U8 | FieldType::I8 | FieldType::Bool => 1,
            FieldType::U16 | FieldType::I16 | FieldType::String16 | FieldType::StringSet16 => 2,
            FieldType::U32 | FieldType::I32 | FieldType::F32 | FieldType::String32 | FieldType::StringSet32 => 4,
            FieldType::U64 | FieldType::I64 | FieldType::Timestamp | FieldType::F64 | FieldType::Point => 8,
            FieldType::Text | FieldType::Vector => 0,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, FieldType::U8 | FieldType::U16 | FieldType::U32 | FieldType::U64
            | FieldType::I8 | FieldType::I16 | FieldType::I32 | FieldType::I64
            | FieldType::Timestamp | FieldType::F32 | FieldType::F64)
    }

    pub fn is_text(&self) -> bool { matches!(self, FieldType::Text) }
    pub fn is_vector(&self) -> bool { matches!(self, FieldType::Vector) }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaField {
    pub field: String,
    pub store: bool,
    pub index_lexical: bool,
    pub index_vector: bool,
    pub field_type: FieldType,
    pub facet: bool,
    pub boost: f32,
}

impl SchemaField {
    pub fn text(field: &str) -> Self {
        Self {
            field: field.to_string(),
            store: true,
            index_lexical: true,
            index_vector: false,
            field_type: FieldType::Text,
            facet: false,
            boost: 1.0,
        }
    }

    pub fn vector(field: &str) -> Self {
        Self {
            field: field.to_string(),
            store: false,
            index_lexical: false,
            index_vector: true,
            field_type: FieldType::Vector,
            facet: false,
            boost: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenizerType {
    AsciiAlphabetic,
    UnicodeAlphanumeric,
    UnicodeAlphanumericFolded,
    UnicodeAlphanumericZH,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorSimilarity {
    Dot,
    Cosine,
    Euclidean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Quantization {
    None,
    ScalarQuantizationI8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Clustering {
    Auto,
    Fixed(usize),
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    None,
    Lz4,
    Snappy,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub tokenizer_type: TokenizerType,
    pub stemmer_language: Option<String>,
    pub vector_similarity: VectorSimilarity,
    pub vector_dimensions: usize,
    pub quantization: Quantization,
    pub compression: Compression,
    pub segment_number_bits: u8,
    pub clustering: Clustering,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            tokenizer_type: TokenizerType::UnicodeAlphanumericFolded,
            stemmer_language: None,
            vector_similarity: VectorSimilarity::Cosine,
            vector_dimensions: 256,
            quantization: Quantization::None,
            compression: Compression::Snappy,
            segment_number_bits: 4,
            clustering: Clustering::Auto,
        }
    }
}

pub type SchemaMap = indexmap::IndexMap<String, SchemaField>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type_byte_size() {
        assert_eq!(FieldType::U8.byte_size(), 1);
        assert_eq!(FieldType::U16.byte_size(), 2);
        assert_eq!(FieldType::U32.byte_size(), 4);
        assert_eq!(FieldType::U64.byte_size(), 8);
        assert_eq!(FieldType::Text.byte_size(), 0);
    }

    #[test]
    fn test_schema_field_text() {
        let f = SchemaField::text("title");
        assert!(f.index_lexical);
        assert!(!f.index_vector);
        assert_eq!(f.field_type, FieldType::Text);
    }

    #[test]
    fn test_index_config_default() {
        let c = IndexConfig::default();
        assert_eq!(c.segment_number_bits, 4);
        assert_eq!(c.vector_dimensions, 256);
    }

    #[test]
    fn test_schema_serialization() {
        let mut schema = SchemaMap::new();
        schema.insert("title".into(), SchemaField::text("title"));
        let json = serde_json::to_string(&schema).unwrap();
        let deserialized: SchemaMap = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, deserialized);
    }
}
