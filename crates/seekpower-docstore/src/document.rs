use seekpower_schema::FieldType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// 文档值类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DocumentValue {
    String(String),
    Integer(i64),
    F32(f32),
    F64(f64),
    Boolean(bool),
    Null,
    Array(Vec<DocumentValue>),
    Object(HashMap<String, DocumentValue>),
}

impl DocumentValue {
    /// 获取字符串值
    pub fn as_str(&self) -> Option<&str> {
        match self {
            DocumentValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// 获取整数值
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            DocumentValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// 获取 F32 值
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            DocumentValue::F32(f) => Some(*f),
            _ => None,
        }
    }

    /// 获取 F64 值
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            DocumentValue::F64(f) => Some(*f),
            _ => None,
        }
    }

    /// 获取布尔值
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            DocumentValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// 获取数组值
    pub fn as_array(&self) -> Option<&[DocumentValue]> {
        match self {
            DocumentValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// 获取对象值
    pub fn as_object(&self) -> Option<&HashMap<String, DocumentValue>> {
        match self {
            DocumentValue::Object(obj) => Some(obj),
            _ => None,
        }
    }

    /// 推断对应的 FieldType
    pub fn infer_type(&self) -> FieldType {
        match self {
            DocumentValue::String(s) => {
                if s.len() >= 100 {
                    FieldType::Text
                } else {
                    FieldType::Keyword
                }
            }
            DocumentValue::F32(_) => FieldType::F32,
            DocumentValue::F64(_) => FieldType::F64,
            DocumentValue::Integer(i) => {
                if *i >= 0 {
                    FieldType::U64
                } else {
                    FieldType::I64
                }
            }
            DocumentValue::Boolean(_) => FieldType::Bool,
            DocumentValue::Array(arr) => {
                // 空数组或混合类型数组 → Json
                if arr.is_empty() {
                    FieldType::Json
                } else if arr.iter().all(|v| matches!(v, DocumentValue::F32(_))) {
                    FieldType::Vector(arr.len() as u32)
                } else {
                    FieldType::Json
                }
            }
            DocumentValue::Object(_) => FieldType::Json,
            DocumentValue::Null => FieldType::Keyword,
        }
    }

    /// 计算值的字节大小
    pub fn size_bytes(&self) -> usize {
        match self {
            DocumentValue::String(s) => s.len(),
            DocumentValue::Integer(_) => std::mem::size_of::<i64>(),
            DocumentValue::F32(_) => std::mem::size_of::<f32>(),
            DocumentValue::F64(_) => std::mem::size_of::<f64>(),
            DocumentValue::Boolean(_) => 1,
            DocumentValue::Null => 1,
            DocumentValue::Array(arr) => arr.iter().map(|v| v.size_bytes()).sum(),
            DocumentValue::Object(obj) => obj.iter().map(|(k, v)| k.len() + v.size_bytes()).sum(),
        }
    }
}

/// 文档结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// 文档 ID
    pub doc_id: Option<u64>,
    /// 字段值
    pub fields: HashMap<String, DocumentValue>,
    /// 时间戳
    pub timestamp: Option<u64>,
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

impl Document {
    /// 创建新文档
    pub fn new() -> Self {
        Self {
            doc_id: None,
            fields: HashMap::new(),
            timestamp: None,
        }
    }

    /// 设置文档 ID
    pub fn with_doc_id(mut self, doc_id: u64) -> Self {
        self.doc_id = Some(doc_id);
        self
    }

    /// 设置时间戳
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// 插入字段
    pub fn insert(&mut self, key: String, value: DocumentValue) -> Option<DocumentValue> {
        self.fields.insert(key, value)
    }

    /// 获取字段
    pub fn get(&self, key: &str) -> Option<&DocumentValue> {
        self.fields.get(key)
    }

    /// 移除字段
    pub fn remove(&mut self, key: &str) -> Option<DocumentValue> {
        self.fields.remove(key)
    }

    /// 检查字段是否存在
    pub fn contains_key(&self, key: &str) -> bool {
        self.fields.contains_key(key)
    }

    /// 获取所有字段名
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }

    /// 计算文档总大小（字节）
    pub fn size_bytes(&self) -> usize {
        self.fields.iter()
            .map(|(k, v)| k.len() + v.size_bytes())
            .sum()
    }

    /// 获取字段数量
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// 从 JSON 反序列化
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// 序列化为 JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// 序列化为紧凑 JSON
    pub fn to_json_compact(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// 推断文档中所有字段的 FieldType
    pub fn infer_field_types(&self) -> HashMap<String, FieldType> {
        self.fields.iter()
            .map(|(k, v)| (k.clone(), v.infer_type()))
            .collect()
    }
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Document(doc_id={:?}, fields={})", self.doc_id, self.fields.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_creation() {
        let doc = Document::new();
        assert!(doc.is_empty());
        assert_eq!(doc.len(), 0);
        assert!(doc.doc_id.is_none());
    }

    #[test]
    fn test_document_with_id() {
        let doc = Document::new().with_doc_id(42);
        assert_eq!(doc.doc_id, Some(42));
    }

    #[test]
    fn test_document_insert_get() {
        let mut doc = Document::new();
        doc.insert("name".to_string(), DocumentValue::String("Alice".to_string()));

        assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("Alice"));
    }

    #[test]
    fn test_document_value_types() {
        let mut doc = Document::new();
        doc.insert("string".to_string(), DocumentValue::String("test".to_string()));
        doc.insert("integer".to_string(), DocumentValue::Integer(42));
        doc.insert("f32".to_string(), DocumentValue::F32(3.14_f32));
        doc.insert("f64".to_string(), DocumentValue::F64(6.28));
        doc.insert("boolean".to_string(), DocumentValue::Boolean(true));
        doc.insert("null".to_string(), DocumentValue::Null);

        assert_eq!(doc.get("string").and_then(|v| v.as_str()), Some("test"));
        assert_eq!(doc.get("integer").and_then(|v| v.as_i64()), Some(42));
        assert_eq!(doc.get("f32").and_then(|v| v.as_f32()), Some(3.14));
        assert_eq!(doc.get("f64").and_then(|v| v.as_f64()), Some(6.28));
        assert_eq!(doc.get("boolean").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(doc.get("null"), Some(&DocumentValue::Null));
    }

    #[test]
    fn test_document_size_calculation() {
        let mut doc = Document::new();
        doc.insert("name".to_string(), DocumentValue::String("Alice".to_string()));
        doc.insert("age".to_string(), DocumentValue::Integer(30));

        // "name".len() + "Alice".len() + "age".len() + size_of::<i64>()
        // 4 + 5 + 3 + 8 = 20
        assert_eq!(doc.size_bytes(), 20);
    }

    #[test]
    fn test_document_json_serialization() {
        let mut doc = Document::new()
            .with_doc_id(1)
            .with_timestamp(1234567890);

        doc.insert("title".to_string(), DocumentValue::String("Test".to_string()));
        doc.insert("f32_val".to_string(), DocumentValue::F32(1.5_f32));
        doc.insert("f64_val".to_string(), DocumentValue::F64(2.5));

        let json = doc.to_json().unwrap();
        let deserialized = Document::from_json(&json).unwrap();

        assert_eq!(deserialized.doc_id, Some(1));
        assert_eq!(deserialized.timestamp, Some(1234567890));
        assert_eq!(deserialized.get("title").and_then(|v| v.as_str()), Some("Test"));
        assert_eq!(deserialized.get("f32_val").and_then(|v| v.as_f32()), Some(1.5));
        assert_eq!(deserialized.get("f64_val").and_then(|v| v.as_f64()), Some(2.5));
    }

    #[test]
    fn test_infer_f32_type() {
        let value = DocumentValue::F32(3.14_f32);
        assert_eq!(value.infer_type(), FieldType::F32);
    }

    #[test]
    fn test_infer_vector_type() {
        let arr = vec![
            DocumentValue::F32(0.1),
            DocumentValue::F32(0.2),
            DocumentValue::F32(0.3),
        ];
        let value = DocumentValue::Array(arr);
        assert_eq!(value.infer_type(), FieldType::Vector(3));
    }

    #[test]
    fn test_infer_empty_array_as_json() {
        let arr: Vec<DocumentValue> = vec![];
        let value = DocumentValue::Array(arr);
        assert_eq!(value.infer_type(), FieldType::Json);
    }

    #[test]
    fn test_infer_mixed_array_as_json() {
        let arr = vec![
            DocumentValue::F32(0.1),
            DocumentValue::String("hello".to_string()),
        ];
        let value = DocumentValue::Array(arr);
        assert_eq!(value.infer_type(), FieldType::Json);
    }

    #[test]
    fn test_infer_positive_integer_as_u64() {
        let value = DocumentValue::Integer(42);
        assert_eq!(value.infer_type(), FieldType::U64);
    }

    #[test]
    fn test_infer_negative_integer_as_i64() {
        let value = DocumentValue::Integer(-42);
        assert_eq!(value.infer_type(), FieldType::I64);
    }

    #[test]
    fn test_infer_field_types() {
        let mut doc = Document::new();
        doc.insert("text_field".to_string(), DocumentValue::String("short".to_string()));
        doc.insert("f32_field".to_string(), DocumentValue::F32(1.5_f32));
        doc.insert("vector_field".to_string(), DocumentValue::Array(vec![
            DocumentValue::F32(0.1),
            DocumentValue::F32(0.2),
        ]));

        let types = doc.infer_field_types();
        assert_eq!(types.get("text_field"), Some(&FieldType::Keyword));
        assert_eq!(types.get("f32_field"), Some(&FieldType::F32));
        assert_eq!(types.get("vector_field"), Some(&FieldType::Vector(2)));
    }

    #[test]
    fn test_long_string_as_text() {
        let long_string = "a".repeat(100);
        let value = DocumentValue::String(long_string);
        assert_eq!(value.infer_type(), FieldType::Text);
    }

    #[test]
    fn test_short_string_as_keyword() {
        let short_string = "hello".to_string();
        let value = DocumentValue::String(short_string);
        assert_eq!(value.infer_type(), FieldType::Keyword);
    }
}