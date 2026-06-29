use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StormDbError;
use crate::search::SearchResponse;
use crate::ranking::ResultSource;
use crate::schema::IndexConfig;
use crate::server::SharedIndex;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateIndexRequest {
    pub path: String,
    pub config: IndexConfig,
    pub schema: Vec<crate::schema::SchemaField>,
}

#[derive(Deserialize)]
pub struct OpenIndexRequest {
    pub path: String,
}

#[derive(Deserialize)]
pub struct IndexDocumentRequest {
    pub document: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
pub struct IndexDocumentsRequest {
    pub documents: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
pub struct IndexDocumentResponse {
    pub doc_id: u64,
}

#[derive(Serialize)]
pub struct IndexDocumentsResponse {
    pub doc_ids: Vec<u64>,
}

#[derive(Deserialize)]
pub struct SearchRequestHttp {
    pub query_string: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub query_type: Option<String>,  // "union", "intersection", "phrase"
    pub search_mode: Option<String>, // "lexical", "vector", "hybrid"
    pub offset: Option<usize>,
    pub length: Option<usize>,
}

#[derive(Serialize)]
pub struct SearchResponseHttp {
    pub query: String,
    pub result_count: usize,
    pub results: Vec<SearchResultHttp>,
}

#[derive(Serialize)]
pub struct SearchResultHttp {
    pub doc_id: u64,
    pub score: f32,
    pub source: String,
    pub document: Option<HashMap<String, serde_json::Value>>,
}

// ---------------------------------------------------------------------------
// Handler functions
// ---------------------------------------------------------------------------

/// Create a new index.
///
/// POST /index/create
#[utoipa::path(
    post,
    path = "/index/create",
    request_body = CreateIndexRequest,
    tag = "Index",
    responses(
        (status = 201, description = "Index created successfully"),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn create_index(
    State(state): State<SharedIndex>,
    Json(req): Json<CreateIndexRequest>,
) -> Result<(StatusCode, &'static str), (StatusCode, String)> {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    let index = crate::index::create_index(&req.path, object_store, req.config, req.schema)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut guard = state.write().await;
    *guard = Some(index);

    Ok((StatusCode::CREATED, "Index created"))
}

/// Open an existing index.
///
/// POST /index/open
#[utoipa::path(
    post,
    path = "/index/open",
    request_body = OpenIndexRequest,
    tag = "Index",
    responses(
        (status = 200, description = "Index opened successfully"),
        (status = 404, description = "Index not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn open_index(
    State(state): State<SharedIndex>,
    Json(req): Json<OpenIndexRequest>,
) -> Result<(StatusCode, &'static str), (StatusCode, String)> {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    let index = crate::index::open_index(&req.path, object_store)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let mut guard = state.write().await;
    *guard = Some(index);

    Ok((StatusCode::OK, "Index opened"))
}

/// Close and delete an index.
///
/// DELETE /index/{path}
#[utoipa::path(
    delete,
    path = "/index/{path}",
    tag = "Index",
    responses(
        (status = 200, description = "Index deleted successfully"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_index(
    State(state): State<SharedIndex>,
    Path(_path): Path<String>,
) -> Result<(StatusCode, &'static str), (StatusCode, String)> {
    let mut guard = state.write().await;
    if let Some(index) = guard.take() {
        crate::index::close_index(&index)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    Ok((StatusCode::OK, "Index deleted"))
}

/// Index a single document.
///
/// POST /document
#[utoipa::path(
    post,
    path = "/document",
    request_body = IndexDocumentRequest,
    tag = "Document",
    responses(
        (status = 201, description = "Document indexed", body = IndexDocumentResponse),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn index_document(
    State(state): State<SharedIndex>,
    Json(req): Json<IndexDocumentRequest>,
) -> Result<Json<IndexDocumentResponse>, (StatusCode, String)> {
    let guard = state.read().await;
    let index = guard
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No index open".to_string()))?;

    let doc_id = crate::document::index_document(index, req.document)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(IndexDocumentResponse { doc_id }))
}

/// Batch index multiple documents.
///
/// POST /documents
#[utoipa::path(
    post,
    path = "/documents",
    request_body = IndexDocumentsRequest,
    tag = "Document",
    responses(
        (status = 201, description = "Documents indexed", body = IndexDocumentsResponse),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn index_documents(
    State(state): State<SharedIndex>,
    Json(req): Json<IndexDocumentsRequest>,
) -> Result<Json<IndexDocumentsResponse>, (StatusCode, String)> {
    let guard = state.read().await;
    let index = guard
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No index open".to_string()))?;

    let doc_ids = crate::document::index_documents(index, req.documents)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(IndexDocumentsResponse { doc_ids }))
}

/// Delete a document.
///
/// DELETE /document/{doc_id}
#[utoipa::path(
    delete,
    path = "/document/{doc_id}",
    tag = "Document",
    responses(
        (status = 200, description = "Document deleted"),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_document(
    State(state): State<SharedIndex>,
    Path(doc_id): Path<u64>,
) -> Result<(StatusCode, &'static str), (StatusCode, String)> {
    let guard = state.read().await;
    let index = guard
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No index open".to_string()))?;

    crate::document::delete_document(index, doc_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, "Document deleted"))
}

/// Get a document by ID.
///
/// GET /document/{doc_id}
#[utoipa::path(
    get,
    path = "/document/{doc_id}",
    tag = "Document",
    responses(
        (status = 200, description = "Document retrieved", body = HashMap<String, serde_json::Value>),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Document not found"),
    )
)]
pub async fn get_document(
    State(state): State<SharedIndex>,
    Path(doc_id): Path<u64>,
) -> Result<Json<HashMap<String, serde_json::Value>>, (StatusCode, String)> {
    let guard = state.read().await;
    let index = guard
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No index open".to_string()))?;

    let document = crate::document::get_document(index, doc_id)
        .await
        .map_err(|e| match e {
            StormDbError::DocumentNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;

    Ok(Json(document))
}

/// Commit pending writes to the index.
///
/// POST /commit
pub async fn commit(
    State(state): State<SharedIndex>,
) -> Result<(StatusCode, &'static str), (StatusCode, String)> {
    let guard = state.read().await;
    let index = guard
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No index open".to_string()))?;

    crate::index::commit(index)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, "Committed"))
}

/// Execute a search query.
///
/// POST /search
#[utoipa::path(
    post,
    path = "/search",
    request_body = SearchRequestHttp,
    tag = "Search",
    responses(
        (status = 200, description = "Search results", body = SearchResponseHttp),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn search(
    State(state): State<SharedIndex>,
    Json(req): Json<SearchRequestHttp>,
) -> Result<Json<SearchResponseHttp>, (StatusCode, String)> {
    let guard = state.read().await;
    let index = guard
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No index open".to_string()))?;

    // Convert HTTP request to internal SearchRequest
    let search_request = crate::search::SearchRequest {
        query_string: req.query_string.clone(),
        query_vector: req.query_vector.clone(),
        query_type: match req.query_type.as_deref() {
            Some("intersection") => crate::search::QueryType::Intersection,
            Some("phrase") => crate::search::QueryType::Phrase,
            _ => crate::search::QueryType::Union,
        },
        search_mode: match req.search_mode.as_deref() {
            Some("vector") => crate::search::SearchMode::Vector {
                ann_mode: crate::search::AnnMode::All,
                similarity_threshold: 0.0,
            },
            Some("hybrid") => crate::search::SearchMode::Hybrid {
                ann_mode: crate::search::AnnMode::All,
                similarity_threshold: 0.0,
            },
            _ => crate::search::SearchMode::Lexical,
        },
        offset: req.offset.unwrap_or(0),
        length: req.length.unwrap_or(10),
        result_type: crate::search::ResultType::Topk,
        include_uncommitted: false,
        field_filter: None,
    };

    let response = crate::index::search(index, search_request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Convert internal SearchResponse to HTTP response
    let http_response = to_search_response_http(response);

    Ok(Json(http_response))
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn to_search_response_http(response: SearchResponse) -> SearchResponseHttp {
    SearchResponseHttp {
        query: response.query,
        result_count: response.result_count,
        results: response
            .results
            .into_iter()
            .map(to_search_result_http)
            .collect(),
    }
}

fn to_search_result_http(result: crate::ranking::SearchResult) -> SearchResultHttp {
    let source = match result.source {
        ResultSource::Lexical => "lexical".to_string(),
        ResultSource::Vector => "vector".to_string(),
        ResultSource::Hybrid => "hybrid".to_string(),
    };
    SearchResultHttp {
        doc_id: result.doc_id,
        score: result.score,
        source,
        document: None, // Document retrieval is a separate GET /document/{doc_id} call
    }
}
