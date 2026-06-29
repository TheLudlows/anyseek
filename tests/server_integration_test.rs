// Integration tests for the HTTP server module
use std::sync::Arc;
use tokio::sync::RwLock;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use storm_db::server::{SharedIndex, create_app};
use storm_db::server::handler::{
    CreateIndexRequest, OpenIndexRequest, IndexDocumentRequest,
    SearchRequestHttp,
};
use object_store::memory::InMemory;

/// Helper: build a SharedIndex with an in-memory index for testing.
async fn build_test_shared_index() -> SharedIndex {
    let schema = vec![
        storm_db::schema::SchemaField::text("title"),
        storm_db::schema::SchemaField::text("body"),
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
async fn test_search_on_empty_index_returns_empty_results() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let req = SearchRequestHttp {
        query_string: Some("hello world".to_string()),
        query_vector: None,
        query_type: Some("union".to_string()),
        search_mode: Some("lexical".to_string()),
        offset: Some(0),
        length: Some(10),
    };

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/search")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["result_count"], 0);
    assert!(body["results"].is_array());
    assert_eq!(body["results"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_index_document_returns_doc_id() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let req = IndexDocumentRequest {
        document: [
            ("title".to_string(), serde_json::json!("Test Document")),
            ("body".to_string(), serde_json::json!("Some content here")),
        ]
        .into_iter()
        .collect(),
    };

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/document")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body.get("doc_id").is_some());
}

#[tokio::test]
async fn test_index_documents_batch() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let req = serde_json::json!({
        "documents": [
            {"title": "Doc One", "body": "First"},
            {"title": "Doc Two", "body": "Second"},
            {"title": "Doc Three", "body": "Third"},
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/documents")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body.get("doc_ids").is_some());
    assert_eq!(body["doc_ids"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_search_after_index_and_commit() {
    let state = build_test_shared_index().await;
    let app = create_app(state.clone());

    // Index a document
    let doc_req = IndexDocumentRequest {
        document: [
            ("title".to_string(), serde_json::json!("Rust Programming")),
            ("body".to_string(), serde_json::json!("Rust is a systems programming language")),
        ]
        .into_iter()
        .collect(),
    };
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/document")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&doc_req).unwrap()))
                .unwrap()
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Commit
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/commit")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Search for "rust"
    let search_req = SearchRequestHttp {
        query_string: Some("rust".to_string()),
        query_vector: None,
        query_type: Some("union".to_string()),
        search_mode: Some("lexical".to_string()),
        offset: Some(0),
        length: Some(10),
    };

    let resp = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/search")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&search_req).unwrap()))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body["result_count"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn test_get_document_not_found_before_commit() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    // Try to get a document that doesn't exist
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/document/999")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();

    // Should be 404 or 500 depending on whether document exists in store
    assert!(response.status() == StatusCode::NOT_FOUND || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_search_without_index_returns_bad_request() {
    let state: SharedIndex = Arc::new(RwLock::new(None));
    let app = create_app(state);

    let req = SearchRequestHttp {
        query_string: Some("test".to_string()),
        query_vector: None,
        query_type: None,
        search_mode: None,
        offset: None,
        length: None,
    };

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/search")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_delete_index() {
    let state = build_test_shared_index().await;
    let app = create_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/index/some-path")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}