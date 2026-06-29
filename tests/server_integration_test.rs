// Integration tests for the HTTP server module
use std::sync::Arc;
use tokio::sync::RwLock;

use storm_db::server::{SharedIndex, create_app};
use storm_db::server::handler::{
    CreateIndexRequest, OpenIndexRequest, IndexDocumentRequest,
    SearchRequestHttp,
};
use storm_db::server::router::ApiDoc;
use storm_db::index::Index;
use storm_db::schema::{IndexConfig, SchemaField, FieldType};
use object_store::memory::InMemory;

/// Helper: build a SharedIndex with an in-memory index for testing.
async fn build_test_shared_index() -> SharedIndex {
    let schema = vec![
        SchemaField::text("title"),
        SchemaField::text("body"),
    ];
    let config = IndexConfig::default();
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    let index = storm_db::index::create_index("/test-server", object_store, config, schema)
        .await
        .expect("Failed to create test index");

    Arc::new(RwLock::new(Some(index)))
}

#[tokio::test]
async fn test_create_app_does_not_panic() {
    let state: SharedIndex = Arc::new(RwLock::new(None));
    let app = create_app(state);
    // App created successfully — routes are registered.
    drop(app);
}

#[tokio::test]
async fn test_openapi_doc_generates() {
    let doc = ApiDoc::openapi();
    assert!(!doc.is_empty());
}

#[tokio::test]
async fn test_search_on_empty_index_returns_empty_results() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    // Use axum's test client to send a search request
    let client = axum::test::TestClient::new(app);

    let req = SearchRequestHttp {
        query_string: Some("hello world".to_string()),
        query_vector: None,
        query_type: Some("union".to_string()),
        search_mode: Some("lexical".to_string()),
        offset: Some(0),
        length: Some(10),
    };

    let response = client
        .post("/search")
        .json(&req)
        .send()
        .await;

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await;
    assert_eq!(body["result_count"], 0);
    assert!(body["results"].is_array());
    assert_eq!(body["results"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_index_document_returns_doc_id() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let client = axum::test::TestClient::new(app);

    let req = IndexDocumentRequest {
        document: [
            ("title".to_string(), serde_json::json!("Test Document")),
            ("body".to_string(), serde_json::json!("Some content here")),
        ]
        .into_iter()
        .collect(),
    };

    let response = client
        .post("/document")
        .json(&req)
        .send()
        .await;

    assert_eq!(response.status(), 201);
    let body: serde_json::Value = response.json().await;
    assert!(body.get("doc_id").is_some());
}

#[tokio::test]
async fn test_index_documents_batch() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let client = axum::test::TestClient::new(app);

    let req = serde_json::json!({
        "documents": [
            {"title": "Doc One", "body": "First"},
            {"title": "Doc Two", "body": "Second"},
            {"title": "Doc Three", "body": "Third"},
        ]
    });

    let response = client
        .post("/documents")
        .json(&req)
        .send()
        .await;

    assert_eq!(response.status(), 201);
    let body: serde_json::Value = response.json().await;
    assert!(body.get("doc_ids").is_some());
    assert_eq!(body["doc_ids"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_search_after_index_and_commit() {
    let state = build_test_shared_index().await;
    let app = create_app(state.clone());

    let client = axum::test::TestClient::new(app);

    // Index a document
    let doc_req = IndexDocumentRequest {
        document: [
            ("title".to_string(), serde_json::json!("Rust Programming")),
            ("body".to_string(), serde_json::json!("Rust is a systems programming language")),
        ]
        .into_iter()
        .collect(),
    };
    let resp = client.post("/document").json(&doc_req).send().await;
    assert_eq!(resp.status(), 201);

    // Commit
    let resp = client.post("/commit").send().await;
    assert_eq!(resp.status(), 200);

    // Search for "rust"
    let search_req = SearchRequestHttp {
        query_string: Some("rust".to_string()),
        query_vector: None,
        query_type: Some("union".to_string()),
        search_mode: Some("lexical".to_string()),
        offset: Some(0),
        length: Some(10),
    };

    let resp = client.post("/search").json(&search_req).send().await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await;
    assert!(body["result_count"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn test_get_document_not_found_before_commit() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let client = axum::test::TestClient::new(app);

    // Try to get a document that doesn't exist
    let response = client.get("/document/999").send().await;
    // Should be 404 or 400 depending on whether document exists in store
    assert!(response.status() == 404 || response.status() == 500);
}

#[tokio::test]
async fn test_search_without_index_returns_bad_request() {
    let state: SharedIndex = Arc::new(RwLock::new(None));
    let app = create_app(state);

    let client = axum::test::TestClient::new(app);

    let req = SearchRequestHttp {
        query_string: Some("test".to_string()),
        query_vector: None,
        query_type: None,
        search_mode: None,
        offset: None,
        length: None,
    };

    let response = client
        .post("/search")
        .json(&req)
        .send()
        .await;

    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn test_delete_index() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let client = axum::test::TestClient::new(app);

    let response = client.delete("/index/some-path").send().await;
    assert_eq!(response.status(), 200);
}
