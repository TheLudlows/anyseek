use seekpower_error::{Result, SeekPowerError};
use seekpower_ranking::{ResultType, SearchResult};
use seekpower_tokenizer::TokenizerRegistry;
use seekpower_vector::VectorIndex;

use crate::index::Index;

pub use seekpower_search::*;

pub async fn search(index: &Index, request: SearchRequest) -> Result<SearchResponse> {
    let delete_set = index.lexical_index.delete_set().await;
    let delete_std: std::collections::HashSet<u64> = delete_set.iter().copied().collect();

    let results: Vec<SearchResult> = match &request.search_mode {
        SearchMode::Lexical => {
            let query_string = request.query_string.as_ref()
                .ok_or_else(|| SeekPowerError::InvalidQuery("query_string required for lexical search".into()))?;

            let registry = TokenizerRegistry::global();
            let registry_guard = registry.read().unwrap();
            let terms = seekpower_tokenizer::tokenize_query(query_string, &index.config, &registry_guard);

            if terms.is_empty() {
                return Ok(SearchResponse { query: query_string.clone(), result_count: 0, results: vec![] });
            }

            let k = request.length.max(1).min(1000) + request.offset;
            let stats = index.stats.read().await.clone();
            index.lexical_index.search(&terms, request.query_type, &stats, k).await?
        }

        SearchMode::Vector { ann_mode } => {
            let query_vector = request.query_vector.as_ref()
                .ok_or_else(|| SeekPowerError::InvalidQuery("query_vector required for vector search".into()))?;
            index.vector_index.search(query_vector, request.length.max(10), ann_mode, &delete_std).await
                .into_iter()
                .map(|r| SearchResult { doc_id: r.doc_id, score: r.score, result_type: ResultType::Vector })
                .collect()
        }

        SearchMode::Hybrid { ann_mode, w_lexical, w_vector } => {
            let mut lexical_results: Vec<SearchResult> = Vec::new();
            let mut vector_results: Vec<SearchResult> = Vec::new();

            if let Some(ref query_string) = request.query_string {
                let registry = TokenizerRegistry::global();
                let registry_guard = registry.read().unwrap();
                let terms = seekpower_tokenizer::tokenize_query(query_string, &index.config, &registry_guard);

                if !terms.is_empty() {
                    let k = request.length.max(1).min(1000) + request.offset;
                    let stats = index.stats.read().await.clone();
                    lexical_results = index.lexical_index.search(&terms, request.query_type, &stats, k).await?;
                }
            }

            if let Some(ref query_vector) = request.query_vector {
                vector_results = index.vector_index.search(query_vector, request.length.max(10), ann_mode, &delete_std).await
                    .into_iter()
                    .map(|r| SearchResult { doc_id: r.doc_id, score: r.score, result_type: ResultType::Vector })
                    .collect();
            }

            let fusion = seekpower_search::RrfFusion { k: 60, w_lexical: *w_lexical, w_vector: *w_vector };
            fusion.fuse(lexical_results, vector_results)
        }
    };

    let result_count = results.len();
    let offset = request.offset.min(results.len());
    let length = request.length.min(results.len() - offset);
    let results_slice = &results[offset..][..length];

    Ok(SearchResponse {
        query: request.query_string.unwrap_or_default(),
        result_count,
        results: results_slice.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use crate::open_index;
use crate::create_index;
use super::*;
    use object_store::memory::InMemory;
    use seekpower_schema::{IndexConfig, SchemaField, TokenizerType};
    use seekpower_storage::KvStore;
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
    async fn test_search_empty_index() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-search", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let request = SearchRequest {
            query_string: Some("hello".to_string()),
            search_mode: SearchMode::Lexical,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 0);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lexical_search_union() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-lexical-union", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), serde_json::json!("rust programming"));
        crate::document::index_document(&idx, doc1).await.unwrap();

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), serde_json::json!("python programming"));
        crate::document::index_document(&idx, doc2).await.unwrap();

        let mut doc3 = HashMap::new();
        doc3.insert("title".to_string(), serde_json::json!("web development"));
        crate::document::index_document(&idx, doc3).await.unwrap();

        crate::commit::commit(&idx).await.unwrap();

        let request = SearchRequest {
            query_string: Some("programming".to_string()),
            search_mode: SearchMode::Lexical,
            query_type: QueryType::Union,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 2, "Expected 2 results for 'programming'");
        assert!(response.results.iter().all(|r| r.score > 0.0));

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lexical_search_intersection() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-lexical-intersection", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), serde_json::json!("rust programming language"));
        crate::document::index_document(&idx, doc1).await.unwrap();

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), serde_json::json!("python programming"));
        crate::document::index_document(&idx, doc2).await.unwrap();

        crate::commit::commit(&idx).await.unwrap();

        let request = SearchRequest {
            query_string: Some("rust language".to_string()),
            search_mode: SearchMode::Lexical,
            query_type: QueryType::Intersection,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 1, "Expected 1 result for 'rust language' AND");
        assert_eq!(response.results[0].doc_id, 0);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lexical_search_phrase() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-lexical-phrase", store).await.unwrap();
        let config = IndexConfig {
            vector_dimensions: 4,
            tokenizer: TokenizerType::Unicode,
            ..IndexConfig::default()
        };
        let idx = create_index(&shared_db, "test", config, test_schema()).await.unwrap();

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), serde_json::json!("the quick brown fox"));
        crate::document::index_document(&idx, doc1).await.unwrap();

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), serde_json::json!("quick red fox"));
        crate::document::index_document(&idx, doc2).await.unwrap();

        crate::commit::commit(&idx).await.unwrap();

        let request = SearchRequest {
            query_string: Some("quick brown".to_string()),
            search_mode: SearchMode::Lexical,
            query_type: QueryType::Phrase,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 1, "Expected 1 result for phrase 'quick brown'");
        assert_eq!(response.results[0].doc_id, 0);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lexical_search_with_field_boost() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-lexical-boost", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), serde_json::json!("rust"));
        doc1.insert("body".to_string(), serde_json::json!("other content"));
        crate::document::index_document(&idx, doc1).await.unwrap();

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), serde_json::json!("other"));
        doc2.insert("body".to_string(), serde_json::json!("rust content"));
        crate::document::index_document(&idx, doc2).await.unwrap();

        crate::commit::commit(&idx).await.unwrap();

        let request = SearchRequest {
            query_string: Some("rust".to_string()),
            search_mode: SearchMode::Lexical,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 2);
        assert!(response.results[0].doc_id == 0, "doc1 should rank higher due to title boost");
        assert!(response.results[0].score > response.results[1].score);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lexical_search_uncommitted_buffer() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-lexical-buffer", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("uncommitted document"));
        crate::document::index_document(&idx, doc).await.unwrap();

        let request = SearchRequest {
            query_string: Some("uncommitted".to_string()),
            search_mode: SearchMode::Lexical,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 1, "Should find uncommitted document in buffer");

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lexical_search_delete_excludes() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-lexical-delete", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), serde_json::json!("rust programming"));
        crate::document::index_document(&idx, doc1).await.unwrap();

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), serde_json::json!("rust systems"));
        let doc2_id = crate::document::index_document(&idx, doc2).await.unwrap();

        crate::commit::commit(&idx).await.unwrap();

        crate::document::delete_document(&idx, doc2_id).await.unwrap();

        let request = SearchRequest {
            query_string: Some("rust".to_string()),
            search_mode: SearchMode::Lexical,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 1, "Deleted document should be excluded");
        assert_eq!(response.results[0].doc_id, 0);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lexical_search_multiple_fields() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-lexical-multifield", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), serde_json::json!("rust"));
        doc1.insert("body".to_string(), serde_json::json!("rust programming"));
        crate::document::index_document(&idx, doc1).await.unwrap();

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), serde_json::json!("python"));
        doc2.insert("body".to_string(), serde_json::json!("rust programming"));
        crate::document::index_document(&idx, doc2).await.unwrap();

        crate::commit::commit(&idx).await.unwrap();

        let request = SearchRequest {
            query_string: Some("rust".to_string()),
            search_mode: SearchMode::Lexical,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 2);
        assert_eq!(response.results[0].doc_id, 0, "doc1 should rank higher (term in both fields)");

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_index_preserves_search() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-open-search", store).await.unwrap();

        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("persistent data"));
        crate::document::index_document(&idx, doc).await.unwrap();
        crate::commit::commit(&idx).await.unwrap();

        let idx2 = open_index(&shared_db, "test").await.unwrap();

        let request = SearchRequest {
            query_string: Some("persistent".to_string()),
            search_mode: SearchMode::Lexical,
            ..Default::default()
        };

        let response = search(&idx2, request).await.unwrap();
        assert_eq!(response.result_count, 1, "Should find document after reopen");

        shared_db.close().await.unwrap();
    }
}
