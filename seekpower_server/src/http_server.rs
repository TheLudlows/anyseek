use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path, Query, Request, State},
    http::{Method, StatusCode, header, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose};
use rand::{TryRng, rngs::SysRng};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, sync::RwLock};
use utoipa::{OpenApi, openapi::OpenApiVersion};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    MASTER_KEY_SECRET,
    api_types::{
        CreateIndexRequest, DeleteDocuments, ErrorResponse, GetDocumentRequest, GetIteratorRequest,
        IndexResponseObject, SearchRequestObject, SearchResultObject, UpdateDocuments,
    },
    keys::{
        ApikeyObject, CreateApikeyRequest, DeleteApikeyRequest, calculate_hash, create_apikey_api,
        delete_apikey_api, get_apikey_hash,
    },
    operations::*,
};
use seekpower::{
    INDEX_RUNTIME,
    index::{Document, IndexArc},
};

#[derive(Clone)]
struct AppState {
    index_path: PathBuf,
    keys: Arc<RwLock<HashMap<u128, ApikeyObject>>>,
    force_shard_number: Option<usize>,
}

pub(crate) enum ApiError {
    Unauthorized,
    NotFound(&'static str),
    BadRequest(String),
    Internal,
    MethodNotAllowed,
}

impl From<String> for ApiError {
    fn from(error: String) -> Self {
        Self::BadRequest(error)
    }
}

impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        log::error!("HTTP storage operation failed: {error}");
        Self::Internal
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "apikey invalid or missing".into(),
            ),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message.into()),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal server error".into(),
            ),
            Self::MethodNotAllowed => (
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "method not allowed".into(),
            ),
        };
        (status, Json(ErrorResponse { code, message })).into_response()
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

fn api_key(parts: &Parts) -> Result<&str, ApiError> {
    parts
        .headers
        .get("apikey")
        .and_then(|value| value.to_str().ok())
        .ok_or(ApiError::Unauthorized)
}

struct ApiKey(u128);
impl FromRequestParts<AppState> for ApiKey {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        get_apikey_hash(api_key(parts)?.to_owned(), &state.keys)
            .await
            .map(Self)
            .ok_or(ApiError::Unauthorized)
    }
}

struct MasterKey;
impl FromRequestParts<AppState> for MasterKey {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, _: &AppState) -> Result<Self, ApiError> {
        let expected =
            general_purpose::STANDARD.encode(Sha256::digest(MASTER_KEY_SECRET.as_bytes()));
        if api_key(parts)? == expected {
            Ok(Self)
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}

// Resolve ownership once and release the scope lock before executing index work.
struct ScopedIndex(IndexArc);
impl FromRequestParts<AppState> for ScopedIndex {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let ApiKey(key) = ApiKey::from_request_parts(parts, state).await?;
        let Path(params) = Path::<HashMap<String, String>>::from_request_parts(parts, state)
            .await
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        let id = params["index_id"]
            .parse::<u64>()
            .map_err(|_| ApiError::BadRequest("index_id invalid or missing".into()))?;
        let keys = state.keys.read().await;
        let scope = keys.get(&key).ok_or(ApiError::Unauthorized)?;
        scope
            .index_list
            .get(&id)
            .cloned()
            .map(Self)
            .ok_or(ApiError::NotFound("index does not exist"))
    }
}

// The existing API accepts JSON regardless of Content-Type and reports parse errors as 400.
struct JsonBody<T>(T);
fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(bytes).map_err(|error| ApiError::BadRequest(error.to_string()))
}
impl<T: DeserializeOwned + Send> FromRequest<AppState> for JsonBody<T> {
    type Rejection = ApiError;
    async fn from_request(request: Request, state: &AppState) -> Result<Self, ApiError> {
        let bytes = Bytes::from_request(request, state)
            .await
            .map_err(|_| ApiError::BadRequest("failed to read request body".into()))?;
        parse_json(&bytes).map(Self)
    }
}

// Normalize paths before routing and apply CORS and POST-only query semantics.
async fn protocol(mut request: Request, next: Next) -> Response {
    let path = request.uri().path().trim_end_matches('/');
    let post_only = path.starts_with("/api/v1/index/")
        && (path.ends_with("/query") || path.ends_with("/iterator"));
    let mut response = if request.method() == Method::OPTIONS {
        (
            StatusCode::NO_CONTENT,
            [
                (
                    header::ACCESS_CONTROL_ALLOW_METHODS,
                    if post_only {
                        "POST, OPTIONS"
                    } else {
                        "GET, HEAD, POST, PATCH, DELETE, OPTIONS"
                    },
                ),
                (header::ACCESS_CONTROL_ALLOW_HEADERS, "content-type, apikey"),
            ],
        )
            .into_response()
    } else if post_only && request.method() != Method::POST {
        let mut response = ApiError::MethodNotAllowed.into_response();
        response
            .headers_mut()
            .insert(header::ALLOW, "POST, OPTIONS".parse().unwrap());
        response
    } else if post_only && request.uri().query().is_some_and(|query| !query.is_empty()) {
        ApiError::BadRequest(
            "put all query/iterator parameters in the JSON request body, not the URL".into(),
        )
        .into_response()
    } else {
        // Accept the existing trailing-slash spelling without redirecting a request body.
        if !path.is_empty() && path != request.uri().path() {
            let mut uri = request.uri().clone().into_parts();
            let query = request
                .uri()
                .query()
                .map(|query| format!("?{query}"))
                .unwrap_or_default();
            uri.path_and_query = Some(format!("{path}{query}").parse().unwrap());
            *request.uri_mut() = axum::http::Uri::from_parts(uri).unwrap();
        }
        next.run(request).await
    };
    response
        .headers_mut()
        .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
    response
}

/// Returns the running server version.
#[utoipa::path(
    get, path = "/api/v1/live", tag = "Info",
    responses(
        (status = 200, description = "Success", body = String, content_type = "application/json")
    ),
    operation_id = "live"
)]
async fn live() -> Json<String> {
    Json(live_api())
}

/// Creates a random API key. Requires the master API key and an empty JSON object. URL parameters are rejected.
#[utoipa::path(
    post, path = "/api/v1/apikey", tag = "API Key",
    params(
        ("apikey" = String, Header, description = "Master API key")
    ),
    request_body(content = inline(CreateApikeyRequest), example = json!({})),
    responses(
        (status = 200, description = "Success", body = String, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 500, description = "Internal server error", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "create_api_key"
)]
async fn create_key(
    State(state): State<AppState>,
    _: MasterKey,
    uri: axum::http::Uri,
    JsonBody(_): JsonBody<CreateApikeyRequest>,
) -> ApiResult<String> {
    if uri.query().is_some_and(|query| !query.is_empty()) {
        return Err(ApiError::BadRequest(
            "API key creation accepts only an empty JSON object".into(),
        ));
    }
    let mut key = [0u8; 32];
    SysRng
        .try_fill_bytes(&mut key)
        .map_err(|_| ApiError::Internal)?;
    create_apikey_api(&state.index_path, &key, &mut *state.keys.write().await)?;
    Ok(Json(general_purpose::STANDARD.encode(key)))
}

/// Lists indexes in the authenticated key scope.
#[utoipa::path(
    get, path = "/api/v1/apikey", tag = "API Key",
    params(
        ("apikey" = String, Header, description = "API key")
    ),
    responses(
        (status = 200, description = "Success", body = Vec<IndexResponseObject>, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "get_api_key_info"
)]
async fn list_indexes(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
) -> ApiResult<Vec<IndexResponseObject>> {
    let keys = state.keys.read().await;
    let scope = keys.get(&key).ok_or(ApiError::Unauthorized)?;
    Ok(Json(get_apikey_indices_info_api(&scope.index_list).await?))
}

/// Revokes a key and deletes all its indexes. Requires the master API key. Returns the remaining key count.
#[utoipa::path(
    delete, path = "/api/v1/apikey", tag = "API Key",
    params(
        ("apikey" = String, Header, description = "Master API key")
    ),
    request_body(content = DeleteApikeyRequest, example = json!({"apikey_base64":"KEY_TO_REVOKE"})),
    responses(
        (status = 500, description = "Internal server error", body = ErrorResponse, content_type = "application/json"),
        (status = 200, description = "Success", body = u64, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "delete_api_key"
)]
async fn delete_key(
    State(state): State<AppState>,
    _: MasterKey,
    JsonBody(request): JsonBody<DeleteApikeyRequest>,
) -> ApiResult<u64> {
    let key = general_purpose::STANDARD
        .decode(request.apikey_base64)
        .map_err(|_| ApiError::BadRequest("apikey_base64 is invalid".into()))?;
    let count = delete_apikey_api(
        &state.index_path,
        &mut *state.keys.write().await,
        calculate_hash(&key) as u128,
    )?
    .ok_or(ApiError::NotFound("apikey does not exist"))?;
    Ok(Json(count))
}

/// Creates an index in the authenticated key scope and returns its ID. Vector fields require dimensions; embeddings are supplied by callers.
#[utoipa::path(
    post, path = "/api/v1/index", tag = "Index",
    params(
        ("apikey" = String, Header, description = "API key")
    ),
    request_body(content = CreateIndexRequest,
        examples(
            ("Lexical" = (value = json!({"index_name":"documents","schema":[{"field":"body","field_type":"Text"}]}))),
            ("Hybrid" = (value = json!({"index_name":"hybrid","schema":[{"field":"body","field_type":"Text"},{"field":"vector","field_type":"Json","index_vector":true}],"vector":{"dimensions":3}}))),
            ("Vector" = (value = json!({"index_name":"vectors","schema":[{"field":"vector","field_type":"Json","index_vector":true}],"vector":{"dimensions":3}})))
        )),
    responses(
        (status = 500, description = "Internal server error", body = ErrorResponse, content_type = "application/json"),
        (status = 200, description = "Success", body = u64, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "create_index"
)]
async fn create_index(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
    JsonBody(request): JsonBody<CreateIndexRequest>,
) -> ApiResult<u64> {
    let mut keys = state.keys.write().await;
    let scope = keys.get_mut(&key).ok_or(ApiError::Unauthorized)?;
    let id = create_index_api(&state.index_path, request, state.force_shard_number, scope).await?;
    Ok(Json(id))
}

/// Returns index metadata, schema and document counts.
#[utoipa::path(
    get, path = "/api/v1/index/{index_id}", tag = "Index",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    responses(
        (status = 200, description = "Success", body = IndexResponseObject, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "get_index_info"
)]
async fn get_index(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
) -> ApiResult<IndexResponseObject> {
    let Path(index_id) = path.map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let index_id = index_id
        .parse()
        .map_err(|_| ApiError::BadRequest("index_id invalid or missing".into()))?;
    let keys = state.keys.read().await;
    let scope = keys.get(&key).ok_or(ApiError::Unauthorized)?;
    Ok(Json(
        get_index_info_api(index_id, &scope.index_list)
            .await
            .map_err(|_| ApiError::NotFound("index does not exist"))?,
    ))
}

/// Commits pending writes to disk and returns the indexed document count. Searches include pending writes; explicit commit persists them. Automatic commit also occurs every 64K documents per shard and on graceful shutdown.
#[utoipa::path(
    patch, path = "/api/v1/index/{index_id}", tag = "Index",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    responses(
        (status = 200, description = "Success", body = u64, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json"),
        (status = 500, description = "Internal server error", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "commit_index"
)]
async fn commit_index(ScopedIndex(index): ScopedIndex) -> ApiResult<u64> {
    let count = INDEX_RUNTIME
        .spawn(async move { commit_index_api(&index).await })
        .await
        .map_err(|_| ApiError::Internal)??;
    Ok(Json(count))
}

/// Deletes an index and returns the remaining index count in this key scope.
#[utoipa::path(
    delete, path = "/api/v1/index/{index_id}", tag = "Index",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    responses(
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 200, description = "Success", body = usize, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "delete_index"
)]
async fn delete_index(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
) -> ApiResult<usize> {
    let Path(index_id) = path.map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let index_id = index_id
        .parse()
        .map_err(|_| ApiError::BadRequest("index_id invalid or missing".into()))?;
    let mut keys = state.keys.write().await;
    let scope = keys.get_mut(&key).ok_or(ApiError::Unauthorized)?;
    delete_index_api(index_id, &mut scope.index_list)
        .await
        .map_err(|_| ApiError::NotFound("index does not exist"))?;
    Ok(Json(scope.index_list.len()))
}

/// Iterates document IDs and optionally stored documents. IDs may have gaps. Positive take traverses forward; negative take traverses backward. For the next page use the last returned ID and skip=1. Parameters must be in the JSON body.
#[utoipa::path(
    post, path = "/api/v1/index/{index_id}/iterator", tag = "Iterator",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    request_body(content = GetIteratorRequest, example = json!({"document_id":null,"skip":0,"take":10,"include_document":true})),
    responses(
        (status = 200, description = "Success", body = crate::api_schema::IteratorResult, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json"),
        (status = 405, description = "Use POST for query and iteration", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "document_iterator_post"
)]
async fn iterate(
    ScopedIndex(index): ScopedIndex,
    JsonBody(request): JsonBody<GetIteratorRequest>,
) -> ApiResult<seekpower::iterator::IteratorResult> {
    Ok(Json(
        get_iterator_api_post(
            &index,
            request.document_id,
            request.skip,
            request.take,
            false,
            request.include_document,
            request.fields,
        )
        .await,
    ))
}

/// Writes one document or a batch and returns the indexed document count as a JSON number. Vector fields must contain external embeddings matching the index dimensions. Commit to persist pending writes.
#[utoipa::path(
    post, path = "/api/v1/index/{index_id}/doc", tag = "Document",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    request_body(content = crate::api_schema::WriteDocuments,
        examples(
            ("Single" = (value = json!({"body":"hello"}))),
            ("Batch" = (value = json!([{"body":"hello"},{"body":"world"}]))),
            ("External vector" = (value = json!({"body":"hello","vector":[1.0,0.0,0.0]})))
        )),
    responses(
        (status = 200, description = "Success", body = usize, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json"),
        (status = 500, description = "Internal server error", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "index_document"
)]
async fn write_documents(
    ScopedIndex(index): ScopedIndex,
    bytes: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> ApiResult<usize> {
    let bytes = bytes.map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let count = INDEX_RUNTIME
        .spawn(async move {
            let documents = if bytes.trim_ascii_start().starts_with(b"[") {
                parse_json(&bytes)?
            } else {
                vec![parse_json(&bytes)?]
            };
            index_documents_api(&index, documents)
                .await
                .map_err(ApiError::from)
        })
        .await
        .map_err(|_| ApiError::Internal)??;
    Ok(Json(count))
}

/// Replaces one document or a batch by ID. Each replacement is a complete delete-and-insert with a new document ID, not a partial patch. Returns the indexed document count as a JSON number.
#[utoipa::path(
    patch, path = "/api/v1/index/{index_id}/doc", tag = "Document",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    request_body(content = UpdateDocuments,
        examples(
            ("Single" = (value = json!({"id":0,"document":{"body":"updated"}}))),
            ("Batch" = (value = json!([{"id":0,"document":{"body":"updated"}},{"id":1,"document":{"body":"also updated"}}])))
        )),
    responses(
        (status = 200, description = "Success", body = u64, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "update_document"
)]
async fn update_documents(
    ScopedIndex(index): ScopedIndex,
    JsonBody(request): JsonBody<UpdateDocuments>,
) -> ApiResult<u64> {
    Ok(Json(
        update_documents_api(&index, request.into_documents()).await?,
    ))
}

/// Reads a stored document. Omit fields to return all stored fields, or use ?fields=body,title. Request bodies are rejected. Obtain valid IDs through search or iteration.
#[utoipa::path(
    get, path = "/api/v1/index/{index_id}/doc/{document_id}", tag = "Document",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID"),
        ("document_id" = u64, Path, description = "Document ID"),
        GetDocumentRequest
    ),
    responses(
        (status = 200, description = "Success", body = std::collections::HashMap<String, serde_json::Value>, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "get_document"
)]
async fn get_document(
    ScopedIndex(index): ScopedIndex,
    Path((_, document_id)): Path<(String, String)>,
    query: Result<Query<GetDocumentRequest>, axum::extract::rejection::QueryRejection>,
    bytes: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> ApiResult<Document> {
    let document_id = document_id
        .parse()
        .map_err(|_| ApiError::BadRequest("doc_id invalid or missing".into()))?;
    let bytes = bytes.map_err(|e| ApiError::BadRequest(e.to_string()))?;
    if !bytes.is_empty() {
        return Err(ApiError::BadRequest(
            "document reads accept fields in the URL, not a request body".into(),
        ));
    }
    let Query(request) = query.map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let fields = request
        .fields
        .split(',')
        .filter(|f| !f.is_empty())
        .map(str::to_owned)
        .collect();

    Ok(Json(
        get_document_api(&index, document_id, fields)
            .await
            .ok_or(ApiError::NotFound("document not found"))?,
    ))
}

/// Deletes a numeric document ID and returns the allocated document count as a JSON number. Deletion is immediately visible.
#[utoipa::path(
    delete, path = "/api/v1/index/{index_id}/doc/{document_id}", tag = "Document",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID"),
        ("document_id" = u64, Path, description = "Document ID")
    ),
    responses(
        (status = 200, description = "Success", body = u64, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "delete_document_by_parameter"
)]
async fn delete_document(
    ScopedIndex(index): ScopedIndex,
    Path((_, document_id)): Path<(String, String)>,
    bytes: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> ApiResult<u64> {
    let bytes = bytes.map_err(|e| ApiError::BadRequest(e.to_string()))?;
    if !bytes.is_empty() {
        return Err(ApiError::BadRequest(
            "delete by ID does not accept a request body".into(),
        ));
    }
    let id = document_id
        .parse()
        .map_err(|_| ApiError::BadRequest("document_id must be an unsigned integer".into()))?;
    Ok(Json(delete_document_api(&index, id).await?))
}

/// Deletes IDs, all matches of a nonempty text query, or all documents with all=true. Returns the allocated document count (zero after all=true). Deletion is immediately visible; storage space is not reclaimed.
#[utoipa::path(
    delete, path = "/api/v1/index/{index_id}/doc", tag = "Document",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    request_body(content = DeleteDocuments, examples(
        ("IDs" = (value = json!({"ids":[0,1]}))),
        ("Query" = (value = json!({"query":"hello"}))),
        ("All" = (value = json!({"all":true})))
    )),
    responses(
        (status = 200, description = "Success", body = u64, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "delete_document_by_request_object"
)]
async fn delete_documents(
    ScopedIndex(index): ScopedIndex,
    JsonBody(request): JsonBody<DeleteDocuments>,
) -> ApiResult<u64> {
    let count = match request {
        DeleteDocuments::Ids { ids } => delete_documents_by_object_api(&index, ids).await?,
        DeleteDocuments::Query(query) => delete_documents_by_query_api(&index, query).await?,
        DeleteDocuments::All { all: true } => clear_index_api(&index).await?,
        DeleteDocuments::All { all: false } => {
            return Err(ApiError::BadRequest(
                "all must be true to remove all documents".into(),
            ));
        }
    };
    Ok(Json(count))
}

/// Searches text, vectors, or both. Mode is inferred from the supplied inputs. Pending writes are visible; empty input browses documents. Parameters must be in the JSON body.
#[utoipa::path(
    post, path = "/api/v1/index/{index_id}/query", tag = "Query",
    params(
        ("apikey" = String, Header, description = "API key"),
        ("index_id" = u64, Path, description = "Index ID")
    ),
    request_body(content = SearchRequestObject,
        examples(
            ("Lexical" = (value = json!({"query":"hello"}))),
            ("Vector" = (value = json!({"query_vector":[1.0,0.0,0.0]}))),
            ("Hybrid" = (value = json!({"query":"hello","query_vector":[1.0,0.0,0.0]})))
        )),
    responses(
        (status = 200, description = "Success", body = SearchResultObject, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ErrorResponse, content_type = "application/json"),
        (status = 401, description = "API key missing or invalid", body = ErrorResponse, content_type = "application/json"),
        (status = 404, description = "Resource not found in this key scope", body = ErrorResponse, content_type = "application/json"),
        (status = 405, description = "Use POST for query and iteration", body = ErrorResponse, content_type = "application/json")
    ),
    operation_id = "query_index_post"
)]
async fn query_index(
    ScopedIndex(index): ScopedIndex,
    JsonBody(request): JsonBody<SearchRequestObject>,
) -> ApiResult<SearchResultObject> {
    Ok(Json(query_index_api(&index, request).await?))
}

#[derive(OpenApi, Default)]
#[openapi(tags(
    (name="Info", description="Return info about the server"),
    (name="API Key", description="Create and delete API keys"),
    (name="Index", description="Create and delete indices"),
    (name="Iterator", description="Iterate through document IDs and documents"),
    (name="Document", description="Index, update, get and delete documents"),
    (name="Query", description="Query an index"),
)
)]
#[openapi(info(
    title = "Seekpower REST API documentation",
    contact(name = "Seekpower", url = "https://github.com/TheLudlows/seekpower")
))]
#[openapi(servers((url = "http://127.0.0.1:8080", description = "Local Seekpower server")))]
struct ApiDoc;

fn api_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(live))
        .routes(routes!(create_key, list_indexes, delete_key))
        .routes(routes!(create_index))
        .routes(routes!(get_index, commit_index, delete_index))
        .routes(routes!(iterate))
        .routes(routes!(write_documents, update_documents, delete_documents))
        .routes(routes!(get_document, delete_document))
        .routes(routes!(query_index))
}

async fn not_found() -> ApiError {
    ApiError::NotFound("route not found")
}

async fn method_not_allowed() -> ApiError {
    ApiError::MethodNotAllowed
}

fn router(state: AppState) -> Router {
    let (api, _) = api_router().split_for_parts();
    let mut app = api;
    for (path, content_type, bytes) in [
        (
            "/",
            "text/html; charset=utf-8",
            include_bytes!("../web/index.html").as_slice(),
        ),
        (
            "/css/flexboxgrid.min.css",
            "text/css",
            include_bytes!("../web/css/flexboxgrid.min.css").as_slice(),
        ),
        (
            "/css/master.css",
            "text/css",
            include_bytes!("../web/css/master.css").as_slice(),
        ),
        (
            "/js/master.js",
            "text/javascript",
            include_bytes!("../web/js/master.js").as_slice(),
        ),
        (
            "/js/setup.js",
            "text/javascript",
            include_bytes!("../web/js/setup.js").as_slice(),
        ),
        (
            "/js/documents.js",
            "text/javascript",
            include_bytes!("../web/js/documents.js").as_slice(),
        ),
        (
            "/js/jquery-3.7.1.min.js",
            "text/javascript",
            include_bytes!("../web/js/jquery-3.7.1.min.js").as_slice(),
        ),
        (
            "/svg/logo.svg",
            "image/svg+xml",
            include_bytes!("../web/svg/logo.svg").as_slice(),
        ),
    ] {
        app = app.route(
            path,
            get(move || async move { ([(header::CONTENT_TYPE, content_type)], bytes) }),
        );
    }
    let app = app
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::disable())
        .with_state(state);
    // Wrap the router so normalization happens before route matching.
    Router::new()
        .fallback_service(app)
        .layer(middleware::from_fn(protocol))
}

pub(crate) async fn http_server(
    index_path: &FsPath,
    keys: Arc<RwLock<HashMap<u128, ApikeyObject>>>,
    local_ip: &String,
    local_port: &u16,
    force_shard_number: &Option<usize>,
) {
    let address =
        std::net::SocketAddr::new(local_ip.parse().expect("validated local_ip"), *local_port);
    let listener = TcpListener::bind(address).await.unwrap_or_else(|error| {
        log::error!("Starting the server at {address} failed: {error}");
        std::process::exit(1)
    });
    let state = AppState {
        index_path: index_path.to_path_buf(),
        keys,
        force_shard_number: *force_shard_number,
    };
    if let Err(error) = axum::serve(listener, router(state))
        .with_graceful_shutdown(crate::server::shutdown_signal())
        .await
    {
        log::error!("HTTP server failed: {error}");
    }
}

pub(crate) fn generate_openapi() {
    let (_, mut openapi) = api_router().split_for_parts();
    openapi.openapi = OpenApiVersion::Version32;

    println!("{}", openapi.to_pretty_json().unwrap());

    let mut path = std::env::current_exe().unwrap();
    path.pop();
    let path_json = path.join("openapi.json");
    let path_yml = path.join("openapi.yml");

    serde_json::to_writer_pretty(&std::fs::File::create(path_json.clone()).unwrap(), &openapi)
        .unwrap();
    std::fs::write(path_yml.clone(), openapi.to_yaml().unwrap()).unwrap();

    println!(
        "OpenAPI documents generated: {} {}",
        path_json.to_string_lossy(),
        path_yml.to_string_lossy()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn resolve<'a>(doc: &'a Value, schema: &'a Value) -> &'a Value {
        match schema["$ref"].as_str() {
            Some(reference) => doc.pointer(reference.strip_prefix('#').unwrap()).unwrap(),
            None => schema,
        }
    }

    #[test]
    fn openapi_describes_uniform_json_contracts() {
        let (_, openapi) = api_router().split_for_parts();
        let doc = serde_json::to_value(openapi).unwrap();
        let paths = doc["paths"].as_object().unwrap();
        assert_eq!(
            paths
                .values()
                .map(|p| p.as_object().unwrap().len())
                .sum::<usize>(),
            15
        );
        for operations in paths.values() {
            for operation in operations.as_object().unwrap().values() {
                for (status, response) in operation["responses"].as_object().unwrap() {
                    let content = &response["content"];
                    assert_eq!(content.as_object().unwrap().len(), 1);
                    let schema = resolve(&doc, &content["application/json"]["schema"]);
                    assert!(!schema.is_null());
                    if status != "200" {
                        assert_eq!(schema["required"], json!(["code", "message"]));
                    }
                    assert!(schema["properties"].get("Ok").is_none());
                }
            }
        }
        let schemas = &doc["components"]["schemas"];
        assert!(schemas.get("SchemaField").is_none());
        assert_eq!(
            schemas["DocumentReplacement"]["required"],
            json!(["id", "document"])
        );
        let document = &paths["/api/v1/index/{index_id}/doc/{document_id}"];
        assert!(document["get"].get("requestBody").is_none());
        assert!(document["delete"].get("requestBody").is_none());
        assert!(
            document["get"]["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p["name"] == "fields" && p["in"] == "query")
        );
        for path in ["/api/v1/index/{index_id}/doc"] {
            for method in ["patch", "delete"] {
                for example in
                    paths[path][method]["requestBody"]["content"]["application/json"]["examples"]
                        .as_object()
                        .unwrap()
                        .values()
                {
                    let value = example["value"].clone();
                    if method == "patch" {
                        serde_json::from_value::<UpdateDocuments>(value).unwrap();
                    } else {
                        serde_json::from_value::<DeleteDocuments>(value).unwrap();
                    }
                }
            }
        }
    }
    #[test]
    fn openapi_describes_only_public_settings_and_valid_examples() {
        let (_, openapi) = api_router().split_for_parts();
        let doc = serde_json::to_value(openapi).unwrap();
        let schemas = &doc["components"]["schemas"];
        for (name, removed) in [
            (
                "CreateIndexRequest",
                &[
                    "similarity",
                    "frequent_words",
                    "ngram_indexing",
                    "clustering",
                    "document_compression",
                    "inference",
                ][..],
            ),
            (
                "SearchRequestObject",
                &[
                    "search_mode",
                    "realtime",
                    "enable_empty_query",
                    "result_type",
                    "query_type_default",
                ][..],
            ),
            (
                "FieldRequest",
                &["longest", "facet", "field_id", "indexed_field_id"][..],
            ),
            ("GetIteratorRequest", &["include_deleted"][..]),
        ] {
            for field in removed {
                assert!(
                    schemas[name]["properties"].get(field).is_none(),
                    "{name}.{field}"
                );
            }
        }
        assert_eq!(
            schemas["FieldRequest"]["required"],
            json!(["field", "field_type"])
        );
        assert_eq!(schemas["VectorConfig"]["required"], json!(["dimensions"]));
        assert_eq!(
            schemas["GetIteratorRequest"]["properties"]["take"]["default"],
            10
        );
        assert_eq!(
            schemas["GetIteratorRequest"]["properties"]["include_document"]["default"],
            true
        );
        assert_eq!(
            schemas["SearchRequestObject"]["properties"]["count"]["default"],
            true
        );
        for (path, creation) in [
            ("/api/v1/index", true),
            ("/api/v1/index/{index_id}/query", false),
        ] {
            let examples = doc["paths"][path]["post"]["requestBody"]["content"]["application/json"]
                ["examples"]
                .as_object()
                .unwrap();
            for example in examples.values() {
                if creation {
                    serde_json::from_value::<CreateIndexRequest>(example["value"].clone()).unwrap();
                } else {
                    serde_json::from_value::<SearchRequestObject>(example["value"].clone())
                        .unwrap();
                }
            }
        }
    }
}
