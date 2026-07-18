use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use bytes::Bytes;

use crate::{Schema, FieldType, SchemaError};
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
            assert!(!trigger.should_save()); // 时间未到
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert!(trigger.should_save());
        }
    }
}

// 简单的测试用存储实现
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
