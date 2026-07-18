use serde::{Deserialize, Serialize};
use std::time::SystemTime;

use crate::FieldType;

/// 字段元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub name: String,
    pub r#type: FieldType,
    pub indexed: bool,
    pub stored: bool,
    pub first_seen_at: SystemTime,
    pub updated_at: SystemTime,
    pub sample_count: u64,
}

impl FieldMetadata {
    pub fn new(name: String, r#type: FieldType) -> Self {
        let now = SystemTime::now();
        let indexed = !matches!(r#type, FieldType::Json);
        Self {
            name,
            r#type,
            indexed,
            stored: true,
            first_seen_at: now,
            updated_at: now,
            sample_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_metadata_creation() {
        let meta = FieldMetadata::new("title".to_string(), FieldType::Keyword);
        assert_eq!(meta.name, "title");
        assert_eq!(meta.sample_count, 0);
        assert_eq!(meta.indexed, true);
    }

    #[test]
    fn test_field_metadata_json_indexed() {
        let meta = FieldMetadata::new("data".to_string(), FieldType::Json);
        assert_eq!(meta.indexed, false); // Json 不索引
    }

    #[test]
    fn test_field_metadata_serialization() {
        let now = SystemTime::UNIX_EPOCH;
        let meta = FieldMetadata {
            name: "title".to_string(),
            r#type: FieldType::Keyword,
            indexed: true,
            stored: true,
            first_seen_at: now,
            updated_at: now,
            sample_count: 100,
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("title"));
        assert!(json.contains("Keyword"));
        assert!(json.contains("sample_count"));
    }
}
