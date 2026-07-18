use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use bytes::Bytes;

use crate::{Schema, FieldType, SchemaError, FieldMetadata};
use seekpower_storage::ObjectStore;

/// Schema 更新结果
#[derive(Debug, Clone)]
pub enum SchemaUpdate {
    NoChange,
    FieldAdded { field_name: String, field_type: FieldType },
    TypeEvolved { field_name: String, old_type: FieldType, new_type: FieldType },
}

/// Schema 管理器
pub struct SchemaManager {
    storage: Arc<dyn ObjectStore>,
    index_prefix: String,
    schema: Arc<RwLock<Schema>>,
    save_trigger: Arc<Mutex<SaveTrigger>>,
}

impl SchemaManager {
    pub fn new(storage: Arc<dyn ObjectStore>, index_prefix: String) -> Self {
        Self {
            storage,
            index_prefix,
            schema: Arc::new(RwLock::new(Schema::new())),
            save_trigger: Arc::new(Mutex::new(SaveTrigger::new(std::time::Duration::from_secs(1)))),
        }
    }

    pub async fn get_schema(&self) -> Schema {
        self.schema.read().await.clone()
    }

    pub async fn initialize(&self) -> Result<(), SchemaError> {
        let path = format!("{}/schema.json", self.index_prefix);
        match self.storage.get(&path).await {
            Ok(data) => {
                let loaded: Schema = serde_json::from_slice(&data)
                    .map_err(|e| SchemaError::DeserializationError(e.to_string()))?;
                *self.schema.write().await = loaded;
                Ok(())
            }
            Err(seekpower_error::StoreError::NotFound(_)) => {
                Ok(())
            }
            Err(e) => Err(SchemaError::from(e)),
        }
    }

    pub fn check_vector_dimension(
        &self,
        field_type: &FieldType,
        new_dim: u32,
        field_name: &str,
    ) -> Result<(), SchemaError> {
        if let FieldType::Vector(existing_dim) = field_type {
            if *existing_dim != new_dim {
                return Err(SchemaError::VectorDimensionConflict {
                    field_name: field_name.to_string(),
                    existing_dim: *existing_dim,
                    new_dim,
                });
            }
        }
        Ok(())
    }

    pub async fn process_document(&self, doc: &serde_json::Value) -> Result<SchemaUpdate, SchemaError> {
        let mut updates = Vec::new();
        let mut schema = self.schema.write().await;

        if let serde_json::Value::Object(fields) = doc {
            for (field_name, field_value) in fields {
                let inferred_type = match FieldType::infer(field_value) {
                    Some(t) => t,
                    None => continue,
                };

                if let Some(existing) = schema.get_field(field_name) {
                    if let (FieldType::Vector(existing_dim), FieldType::Vector(new_dim)) =
                        (&existing.r#type, &inferred_type)
                    {
                        if *existing_dim != *new_dim {
                            return Err(SchemaError::VectorDimensionConflict {
                                field_name: field_name.clone(),
                                existing_dim: *existing_dim,
                                new_dim: *new_dim,
                            });
                        }
                    }

                    if existing.r#type != inferred_type {
                        let converged = existing.r#type.converge(&inferred_type);
                        if converged != existing.r#type {
                            let old_type = existing.r#type.clone();
                            let new_type = converged.clone();

                            let mut new_metadata = existing.clone();
                            new_metadata.r#type = converged;
                            new_metadata.updated_at = std::time::SystemTime::now();
                            new_metadata.sample_count += 1;

                            schema.add_field(new_metadata);
                            updates.push(SchemaUpdate::TypeEvolved {
                                field_name: field_name.clone(),
                                old_type,
                                new_type,
                            });
                        }
                    } else {
                        let mut new_metadata = existing.clone();
                        new_metadata.sample_count += 1;
                        schema.fields.insert(field_name.clone(), new_metadata);
                    }
                } else {
                    let metadata = FieldMetadata::new(field_name.clone(), inferred_type.clone());
                    schema.add_field(metadata);
                    updates.push(SchemaUpdate::FieldAdded {
                        field_name: field_name.clone(),
                        field_type: inferred_type,
                    });
                }
            }
        }

        if !updates.is_empty() {
            let schema_clone = schema.clone();
            self.save_trigger.lock().await.mark_dirty(schema_clone);
        }

        Ok(updates.into_iter().next().unwrap_or(SchemaUpdate::NoChange))
    }

    pub async fn flush(&self) -> Result<(), SchemaError> {
        if let Some(schema) = self.save_trigger.lock().await.take_pending() {
            let path = format!("{}/schema.json", self.index_prefix);
            let json = serde_json::to_vec(&schema)
                .map_err(|e| SchemaError::SerializationError(e.to_string()))?;

            self.storage
                .put(&path, Bytes::from(json))
                .await
                .map_err(SchemaError::from)?;

            self.schema.write().await.version = schema.version;
        }
        Ok(())
    }
}

/// 保存触发器
#[derive(Debug)]
pub struct SaveTrigger {
    pending: bool,
    last_save_time: std::time::Instant,
    save_interval: std::time::Duration,
    pending_schema: Option<Schema>,
}

impl SaveTrigger {
    pub fn new(save_interval: std::time::Duration) -> Self {
        Self {
            pending: false,
            last_save_time: std::time::Instant::now(),
            save_interval,
            pending_schema: None,
        }
    }

    pub fn mark_dirty(&mut self, schema: Schema) {
        self.pending = true;
        self.pending_schema = Some(schema);
    }

    pub fn should_save(&self) -> bool {
        self.pending && self.last_save_time.elapsed() >= self.save_interval
    }

    pub fn take_pending(&mut self) -> Option<Schema> {
        if self.should_save() {
            self.pending = false;
            self.last_save_time = std::time::Instant::now();
            self.pending_schema.take()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_schema_update_equality() {
        let update = SchemaUpdate::FieldAdded {
            field_name: "title".to_string(),
            field_type: FieldType::Keyword,
        };
        match update {
            SchemaUpdate::FieldAdded { field_name, .. } => {
                assert_eq!(field_name, "title");
            }
            _ => panic!("Unexpected update type"),
        }
    }

    #[test]
    fn test_check_vector_dimension_same() {
        let storage = Arc::new(NullStore::new());
        let manager = SchemaManager::new(storage, "test".to_string());
        let result = manager.check_vector_dimension(&FieldType::Vector(3), 3, "embedding");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_vector_dimension_different() {
        let storage = Arc::new(NullStore::new());
        let manager = SchemaManager::new(storage, "test".to_string());
        let result = manager.check_vector_dimension(&FieldType::Vector(3), 4, "embedding");
        assert!(result.is_err());
        assert!(matches!(result, Err(SchemaError::VectorDimensionConflict { .. })));
    }

    mod save_trigger_tests {
        use super::*;

        #[test]
        fn test_save_trigger_initial_state() {
            let mut trigger = SaveTrigger::new(std::time::Duration::from_millis(100));
            assert!(!trigger.should_save());
            assert!(trigger.take_pending().is_none());
        }

        #[test]
        fn test_save_trigger_mark_and_wait() {
            let mut trigger = SaveTrigger::new(std::time::Duration::from_millis(10));
            let schema = Schema::new();
            trigger.mark_dirty(schema);
            assert!(!trigger.should_save());
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert!(trigger.should_save());
        }
    }

    #[tokio::test]
    async fn test_process_document_new_fields() {
        let storage = Arc::new(NullStore::new());
        let manager = SchemaManager::new(storage, "test".to_string());
        manager.initialize().await.unwrap();

        let doc = json!({
            "title": "Hello",
            "count": 42,
            "active": true
        });

        let update = manager.process_document(&doc).await.unwrap();
        match update {
            SchemaUpdate::FieldAdded { field_name, .. } => {
                assert!(["title", "count", "active"].contains(&field_name.as_str()));
            }
            _ => panic!("Expected FieldAdded"),
        }

        let schema = manager.get_schema().await;
        assert_eq!(schema.fields.len(), 3);
    }

    #[tokio::test]
    async fn test_process_document_vector_conflict() {
        let storage = Arc::new(NullStore::new());
        let manager = SchemaManager::new(storage, "test".to_string());
        manager.initialize().await.unwrap();

        let doc1 = json!({ "embedding": [0.1, 0.2, 0.3] });
        manager.process_document(&doc1).await.unwrap();

        let doc2 = json!({ "embedding": [0.1, 0.2, 0.3, 0.4] });
        let result = manager.process_document(&doc2).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(SchemaError::VectorDimensionConflict { .. })));
    }

    #[tokio::test]
    async fn test_process_document_type_evolution() {
        let storage = Arc::new(NullStore::new());
        let manager = SchemaManager::new(storage, "test".to_string());
        manager.initialize().await.unwrap();

        let doc1 = json!({ "value": 42 });
        manager.process_document(&doc1).await.unwrap();

        let doc2 = json!({ "value": 3.14 });
        let update = manager.process_document(&doc2).await.unwrap();
        match update {
            SchemaUpdate::TypeEvolved { old_type, new_type, .. } => {
                assert_eq!(old_type, FieldType::U64);
                assert_eq!(new_type, FieldType::F64);
            }
            _ => panic!("Expected TypeEvolved"),
        }
    }

    #[tokio::test]
    async fn test_flush_persists_schema() {
        let storage = Arc::new(TestStore::new());
        let manager = SchemaManager::new(storage.clone(), "test".to_string());
        manager.initialize().await.unwrap();

        let doc = json!({ "title": "Hello" });
        manager.process_document(&doc).await.unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100)); // 等待 save interval
        manager.flush().await.unwrap();

        let saved = storage.get_data().await;
        assert!(saved.is_some());
        let binding = saved.unwrap();
        let saved_str = std::str::from_utf8(&binding).unwrap();
        assert!(saved_str.contains("title"));
    }
}

struct TestStore {
    data: Arc<Mutex<Option<bytes::Bytes>>>,
}

impl TestStore {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_data(&self) -> Option<bytes::Bytes> {
        self.data.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl ObjectStore for TestStore {
    async fn get(&self, location: &str) -> Result<Bytes, seekpower_error::StoreError> {
        match self.data.lock().await.as_ref() {
            Some(data) => Ok(data.clone()),
            None => Err(seekpower_error::StoreError::NotFound(location.to_string())),
        }
    }

    async fn put(&self, _location: &str, value: Bytes) -> Result<(), seekpower_error::StoreError> {
        *self.data.lock().await = Some(value);
        Ok(())
    }

    async fn range(&self, location: &str, _offset: u64, _len: Option<u64>) -> Result<Bytes, seekpower_error::StoreError> {
        match self.data.lock().await.as_ref() {
            Some(data) => Ok(data.clone()),
            None => Err(seekpower_error::StoreError::NotFound(location.to_string())),
        }
    }

    async fn delete(&self, _location: &str) -> Result<(), seekpower_error::StoreError> {
        *self.data.lock().await = None;
        Ok(())
    }

    async fn list(&self, _prefix: Option<&str>) -> Result<Vec<String>, seekpower_error::StoreError> {
        Ok(vec![])
    }

    async fn put_if_absent(&self, _location: &str, value: Bytes) -> Result<bool, seekpower_error::StoreError> {
        let mut data = self.data.lock().await;
        if data.is_none() {
            *data = Some(value);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn put_if_match(&self, location: &str, _bytes: Bytes, _etag: &str) -> Result<String, seekpower_error::StoreError> {
        Err(seekpower_error::StoreError::NotFound(location.to_string()))
    }

    async fn metadata(&self, location: &str) -> Result<seekpower_error::ObjectMetadata, seekpower_error::StoreError> {
        Err(seekpower_error::StoreError::NotFound(location.to_string()))
    }
}

struct NullStore;

impl NullStore {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl ObjectStore for NullStore {
    async fn get(&self, location: &str) -> Result<Bytes, seekpower_error::StoreError> {
        Err(seekpower_error::StoreError::NotFound(location.to_string()))
    }

    async fn put(&self, _location: &str, _bytes: Bytes) -> Result<(), seekpower_error::StoreError> {
        Ok(())
    }

    async fn range(&self, location: &str, _offset: u64, _len: Option<u64>) -> Result<Bytes, seekpower_error::StoreError> {
        Err(seekpower_error::StoreError::NotFound(location.to_string()))
    }

    async fn delete(&self, _location: &str) -> Result<(), seekpower_error::StoreError> {
        Ok(())
    }

    async fn list(&self, _prefix: Option<&str>) -> Result<Vec<String>, seekpower_error::StoreError> {
        Ok(vec![])
    }

    async fn put_if_absent(&self, _location: &str, _bytes: Bytes) -> Result<bool, seekpower_error::StoreError> {
        Ok(true)
    }

    async fn put_if_match(&self, location: &str, _bytes: Bytes, _etag: &str) -> Result<String, seekpower_error::StoreError> {
        Err(seekpower_error::StoreError::NotFound(location.to_string()))
    }

    async fn metadata(&self, location: &str) -> Result<seekpower_error::ObjectMetadata, seekpower_error::StoreError> {
        Err(seekpower_error::StoreError::NotFound(location.to_string()))
    }
}