use ahash::AHashSet;
use seekpower_error::{Result, SeekPowerError};
use seekpower_schema::{IndexConfig, IndexStats, SchemaField, SchemaMap};
use seekpower_storage::{KvStore, ReadCache};
use seekpower_vector::IvfIndex;

use crate::index::Index;
use crate::lexical_index::LexicalIndex;
use crate::search::QueryEngine;

pub async fn create_index(
    shared_db: &KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let db = shared_db.for_index(path);
    create_index_with_db(db, path, config, schema_fields).await
}

pub async fn create_index_with_db(
    db: KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let mut schema: SchemaMap = SchemaMap::new();
    let mut field_names = Vec::new();
    for field in &schema_fields {
        schema.insert(field.field.clone(), field.clone());
        field_names.push(field.field.clone());
    }

    db.put_serialized(b"m:schema", &schema).await?;
    db.put_serialized(b"m:config", &config).await?;
    db.put_serialized(b"m:stats", &IndexStats::default()).await?;
    db.put_serialized(b"m:fields", &field_names).await?;

    let cache = ReadCache::new(config.posting_cache_size_mb, config.cluster_cache_size_mb);
    let vector_index = IvfIndex::new(db.clone(), config.clone());
    let lexical_index = LexicalIndex::new(
        db.clone(), config.clone(), schema.clone(), field_names.clone(), cache, AHashSet::new(),
    );

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        field_names,
        config,
        stats: tokio::sync::RwLock::new(IndexStats::default()),
        next_doc_id: std::sync::atomic::AtomicU64::new(0),
        lexical_index,
        vector_index,
        query_engine: QueryEngine::new(),
        delete_set: tokio::sync::RwLock::new(AHashSet::new()),
    })
}

pub async fn open_index(shared_db: &KvStore, path: &str) -> Result<Index> {
    let db = shared_db.for_index(path);
    open_index_with_db(db, path).await
}

pub async fn open_index_with_db(db: KvStore, path: &str) -> Result<Index> {
    let schema: SchemaMap = db
        .get_deserialized(b"m:schema")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("schema not found for index '{}'", path)))?;

    let config: IndexConfig = db
        .get_deserialized(b"m:config")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("config not found for index '{}'", path)))?;

    let stats: IndexStats = db
        .get_deserialized(b"m:stats")
        .await?
        .unwrap_or_default();

    let field_names: Vec<String> = db
        .get_deserialized(b"m:fields")
        .await?
        .unwrap_or_else(|| schema.keys().cloned().collect());

    let mut delete_set = AHashSet::new();
    let dell_entries = db.scan_prefix(b"l:").await?;
    for (key_bytes, _) in dell_entries {
        let suffix = &key_bytes[2..];
        if suffix.len() == 8 {
            let bytes: [u8; 8] = suffix.try_into().unwrap();
            delete_set.insert(u64::from_le_bytes(bytes));
        }
    }

    let cache = ReadCache::new(config.posting_cache_size_mb, config.cluster_cache_size_mb);
    let vector_index = IvfIndex::new(db.clone(), config.clone());
    let lexical_index = LexicalIndex::new(
        db.clone(), config.clone(), schema.clone(), field_names.clone(), cache, delete_set.clone(),
    );
    let next_doc_id = stats.doc_count;

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        field_names,
        config,
        stats: tokio::sync::RwLock::new(stats),
        next_doc_id: std::sync::atomic::AtomicU64::new(next_doc_id),
        lexical_index,
        vector_index,
        query_engine: QueryEngine::new(),
        delete_set: tokio::sync::RwLock::new(delete_set),
    })
}

pub async fn close_index(index: &Index) -> Result<()> {
    index.db.close().await
}

pub async fn delete_index(shared_db: &KvStore, path: &str) -> Result<()> {
    seekpower_storage::validate_path(path)?;
    let view = shared_db.for_index(path);
    let entries = view.scan_all().await?;
    for (key, _) in entries {
        view.delete(&key).await?;
    }
    shared_db
        .for_index("__indexes__")
        .delete(path.as_bytes())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use seekpower_schema::SchemaField;
    use std::sync::Arc;
    use std::collections::HashMap;

    fn test_schema() -> Vec<SchemaField> {
        vec![
            SchemaField::text("title"),
            SchemaField::text("body").with_boost(0.5).with_store(true),
            SchemaField::vector("embedding"),
        ]
    }

    fn test_config() -> IndexConfig {
        IndexConfig { vector_dimensions: 4, ..IndexConfig::default() }
    }

    #[tokio::test]
    async fn test_create_and_close_index() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-create-close", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();
        assert_eq!(idx.schema.len(), 3);
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_two_indexes_share_db() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-two-idx", store).await.unwrap();

        let a = create_index(&shared_db, "a", test_config(), test_schema()).await.unwrap();
        let b = create_index(&shared_db, "b", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello"));
        crate::document::index_document(&a, doc.clone()).await.unwrap();
        crate::document::index_document(&b, doc).await.unwrap();
        crate::commit::commit(&a).await.unwrap();
        crate::commit::commit(&b).await.unwrap();

        let raw_a = shared_db.for_index("a").scan_prefix(b"m:").await.unwrap();
        let raw_b = shared_db.for_index("b").scan_prefix(b"m:").await.unwrap();
        assert!(!raw_a.is_empty());
        assert!(!raw_b.is_empty());

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_index_removes_all_keys() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-delete-idx", store).await.unwrap();
        let idx = create_index(&shared_db, "doomed", test_config(), test_schema()).await.unwrap();
        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("x"));
        crate::document::index_document(&idx, doc).await.unwrap();
        crate::commit::commit(&idx).await.unwrap();

        assert!(!shared_db.for_index("doomed").scan_all().await.unwrap().is_empty());
        delete_index(&shared_db, "doomed").await.unwrap();
        assert!(shared_db.for_index("doomed").scan_all().await.unwrap().is_empty());
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_index_fails_for_missing() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-missing", store).await.unwrap();
        let res = open_index(&shared_db, "nope").await;
        assert!(res.is_err());
        match res {
            Err(SeekPowerError::IndexNotFound(_)) => {}
            Err(_) => panic!("expected IndexNotFound"),
            Ok(_) => panic!("expected error"),
        }
        shared_db.close().await.unwrap();
    }
}
