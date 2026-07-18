use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

use crate::FieldMetadata;

/// Schema 定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub version: u64,
    pub fields: HashMap<String, FieldMetadata>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

impl Schema {
    pub fn new() -> Self {
        let now = SystemTime::now();
        Self {
            version: 0,
            fields: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn add_field(&mut self, metadata: FieldMetadata) {
        self.version += 1;
        self.updated_at = SystemTime::now();
        self.fields.insert(metadata.name.clone(), metadata);
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldMetadata> {
        self.fields.get(name)
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldType;

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new();
        assert_eq!(schema.version, 0);
        assert!(schema.fields.is_empty());
    }

    #[test]
    fn test_schema_add_field() {
        let mut schema = Schema::new();
        let meta = FieldMetadata::new("title".to_string(), FieldType::Keyword);
        schema.add_field(meta);
        assert_eq!(schema.fields.len(), 1);
        assert!(schema.fields.contains_key("title"));
    }

    #[test]
    fn test_schema_serialization() {
        let mut schema = Schema::new();
        let meta = FieldMetadata::new("title".to_string(), FieldType::Keyword);
        schema.add_field(meta);
        let json = serde_json::to_string(&schema).unwrap();
        assert!(json.contains("fields"));
        assert!(json.contains("version"));
    }
}
