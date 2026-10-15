use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path, Request, State},
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
    api_endpoints::*,
    api_types::{
        CreateIndexRequest, GetDocumentRequest, GetIteratorRequest, IndexResponseObject,
        SearchRequestObject, SearchResultObject,
    },
    multi_tenancy::{
        ApikeyObject, CreateApikeyRequest, DeleteApikeyRequest, calculate_hash, get_apikey_hash,
    },
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

enum ApiError {
    Unauthorized,
    NotFound(&'static str),
    BadRequest(String),
    Internal,
}

impl From<String> for ApiError {
    fn from(error: String) -> Self {
        Self::BadRequest(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "apikey invalid or missing").into_response()
            }
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message).into_response(),
            Self::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, format!("bad request:{message}")).into_response()
            }
            Self::Internal => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response()
            }
        }
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
        let id = params["index_id"].parse::<u64>().map_err(|_| {
            if parts.uri.path().trim_end_matches('/').ends_with("/query") {
                ApiError::NotFound("index does not exist")
            } else {
                ApiError::BadRequest("index_id invalid or missing".into())
            }
        })?;
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

// Preserve the existing OPTIONS, POST-only and URL-parameter contracts.
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
                        "POST, GET, OPTIONS"
                    },
                ),
                (header::ACCESS_CONTROL_ALLOW_HEADERS, "content-type, apikey"),
            ],
        )
            .into_response()
    } else if post_only && request.method() != Method::POST {
        (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, "POST, OPTIONS")],
            "use POST with a JSON request body for search and document iteration",
        )
            .into_response()
    } else if post_only && request.uri().query().is_some_and(|query| !query.is_empty()) {
        ApiError::BadRequest(
            "put all query/iterator parameters in the JSON request body, not the URL".into(),
        )
        .into_response()
    } else if request.method() == Method::HEAD {
        not_implemented().await.into_response()
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
    if response.status() == StatusCode::NOT_IMPLEMENTED {
        response.headers_mut().remove(header::ALLOW);
    }
    response
        .headers_mut()
        .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
    response
}

/// Live
///
/// Returns a live message with the Seekpower server version.
#[utoipa::path(
    tag = "Info",
    get,
    path = "/api/v1/live",
    responses(
        (status = 200, description = "Seekpower server is live", body = String),
    ),
    operation_id = "live",
)]
async fn live() -> Json<String> {
    Json(live_api())
}

/// Create API Key
///
/// Creates an API key and returns the Base64 encoded API key.
/// Expects the Base64 encoded master API key in the header.
/// Use the **master API key displayed** in the server console at startup.
///
/// WARNING: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.
#[utoipa::path(
    tag = "API Key",
    post,
    path = "/api/v1/apikey",
    params(
        ("apikey" = String, Header, description = "YOUR_MASTER_API_KEY",example="BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="),
    ),
    request_body = inline(CreateApikeyRequest),
    responses(
        (status = 200, description = "API key created, returns Base64 encoded API key", body = String),
        (status = BAD_REQUEST, description = "Invalid or retired API key creation options"),
        (status = UNAUTHORIZED, description = "master_apikey invalid"),
        (status = UNAUTHORIZED, description = "master_apikey missing")
    ),
    operation_id = "create_api_key",
)]
async fn create_key(
    State(state): State<AppState>,
    _: MasterKey,
    uri: axum::http::Uri,
    JsonBody(_): JsonBody<CreateApikeyRequest>,
) -> Result<String, ApiError> {
    if uri.query().is_some_and(|query| !query.is_empty()) {
        return Err(ApiError::BadRequest(
            "API key creation accepts only an empty JSON object".into(),
        ));
    }
    let mut key = [0u8; 32];
    SysRng
        .try_fill_bytes(&mut key)
        .map_err(|_| ApiError::Internal)?;
    create_apikey_api(&state.index_path, &key, &mut *state.keys.write().await);
    Ok(general_purpose::STANDARD.encode(key))
}

/// Get API Key Info
///
/// Get info about all indices associated with the specified API key
#[utoipa::path(
    get,
    tag = "API Key",
    path = "/api/v1/apikey",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ),
    responses(
        (
            status = 200, description = "Indices found, returns a list of index info",
            body = Vec<IndexResponseObject>,
        ),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index ID or API key missing"),
        (status = UNAUTHORIZED, description = "API key does not exists"),
    ),
    operation_id = "get_api_key_info",
)]
async fn list_indexes(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
) -> ApiResult<Result<Vec<IndexResponseObject>, String>> {
    let keys = state.keys.read().await;
    let scope = keys.get(&key).ok_or(ApiError::Unauthorized)?;
    Ok(Json(get_apikey_indices_info_api(&scope.index_list).await))
}

/// Delete API Key
///
/// Deletes an API key and returns the number of remaining API keys.
/// Expects the Base64 encoded master API key in the header.
/// WARNING: This will delete all indices and documents associated with the API key.
#[utoipa::path(
    delete,
    tag = "API Key",
    path = "/api/v1/apikey",
    params(
        ("apikey" = String, Header, description = "YOUR_MASTER_API_KEY",example="BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="),
    ),
    responses(
        (status = 200, description = "API key deleted, returns number of remaining API keys", body = u64),
        (status = UNAUTHORIZED, description = "master_apikey invalid"),
        (status = UNAUTHORIZED, description = "master_apikey missing")
    ),
    operation_id = "delete_api_key",
)]
async fn delete_key(
    State(state): State<AppState>,
    _: MasterKey,
    JsonBody(request): JsonBody<DeleteApikeyRequest>,
) -> ApiResult<u64> {
    let key = general_purpose::STANDARD
        .decode(request.apikey_base64)
        .map_err(|_| ApiError::Unauthorized)?;
    let count = delete_apikey_api(
        &state.index_path,
        &mut *state.keys.write().await,
        calculate_hash(&key) as u128,
    )
    .map_err(|_| ApiError::NotFound("apikey does not exist"))?;
    Ok(Json(count))
}

/// Create Index
///
/// Create an index within the directory associated with the specified API key and return the index_id.
#[utoipa::path(
    post,
    tag = "Index",
    path = "/api/v1/index",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ),

    request_body(
        content = CreateIndexRequest,
        examples(
            ("Example: Lexical index" = (value = json!({
				"schema":[{
					"field": "title",
					"field_type": "Text",
					"store": true,
					"index_lexical": true,
					"boost":10.0
				},
					{
					"field": "body",
					"field_type": "Text",
					"store": true,
					"index_lexical": true,
					"longest": true
				},
				{
					"field": "url",
					"field_type": "String32",
					"store": true,
					"index_lexical": false
				}],
				"index_name": "wikipedia",
				"similarity": "Bm25fProximity",
				"tokenizer": "UnicodeAlphanumeric"
			}))),
			("Example: Hybrid index" = (value = json!({
				"schema":[{
					"field": "title",
					"field_type": "Text",
					"store": true,
					"index_lexical": true,
					"index_vector": false
				},
				{
					"field": "body",
					"field_type": "Text",
					"store": true,
					"index_lexical": true,
					"index_vector": false
				},
					{
					"field": "vector",
					"field_type": "Json",
					"store": true,
					"index_lexical": false,
					"index_vector": true
				}],
				"index_name": "wikipedia",
				"similarity": "Bm25fProximity",
				"tokenizer": "UnicodeAlphanumeric",
				"clustering": "Auto",
				"inference": {"External": { "dimensions": 3, "precision": "F32", "similarity": "Cosine", "quantization": "None" }}
			}))),
            ("Example: Vector index" = (value = json!({
				"schema":[
				{
					"field":"vector",
					"field_type":"Json",
					"store":false,
					"index_lexical":false,
					"index_vector":true
				},
				{
					"field":"index",
					"field_type":"Text",
					"store":true,
					"index_lexical":false,
					"index_vector":false
				}],
				"index_name": "vectors",
				"clustering": "Auto",
				"inference": {
					"External": { "dimensions": 128, "precision": "F32", "quantization": "ScalarQuantizationI8", "similarity": "Euclidean" }
				}
			})))
        )
    ),

    responses(
        (status = OK, description = "Index created, returns the index_id", body = u64),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "API key does not exists"),
        (status = UNAUTHORIZED, description = "API key is missing"),
        (status = UNAUTHORIZED, description = "API key does not exists")
    ),
    operation_id = "create_index",
)]
async fn create_index(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
    JsonBody(request): JsonBody<CreateIndexRequest>,
) -> ApiResult<u64> {
    let mut keys = state.keys.write().await;
    let scope = keys.get_mut(&key).ok_or(ApiError::Unauthorized)?;
    let id = create_index_api(
        &state.index_path,
        request.index_name,
        request.schema,
        request.similarity,
        request.tokenizer,
        request.stop_words,
        request.frequent_words,
        request.ngram_indexing,
        request.document_compression,
        state.force_shard_number,
        scope,
        true,
        request.clustering,
        request.inference,
    )
    .await?;
    Ok(Json(id))
}

/// Get Index Info
///
/// Get index Info from index with index_id
#[utoipa::path(
    get,
    tag = "Index",
    path = "/api/v1/index/{index_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    responses(
        (
            status = 200, description = "Index found, returns the index info",
            body = IndexResponseObject,
        ),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    ),
    operation_id = "get_index_info",
)]
async fn get_index(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
    Path(index_id): Path<String>,
) -> ApiResult<IndexResponseObject> {
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

/// Commit Index
///
/// Commit moves indexed documents from the intermediate uncompressed data structure (array lists/HashMap, queryable by realtime search) in RAM
/// to the final compressed data structure (roaring bitmap) on Mmap or disk -
/// which is persistent, more compact, with lower query latency and allows search with realtime=false.
/// Commit is invoked automatically each time 64K documents are newly indexed **per shard** as well as on close_index (e.g. server quit).
/// There is no way to prevent this automatic commit by not manually invoking it.
/// But commit can also be invoked manually at any time at any number of newly indexed documents.
/// commit is a **hard commit** for persistence on disk. A **soft commit** for searchability
/// is invoked implicitly with every index_doc,
/// i.e. the document can immediately searched and included in the search results
/// if it matches the query AND the query parameter realtime=true is enabled.
/// **Use commit with caution, as it is an expensive operation**.
/// **Usually, there is no need to invoke it manually**, as it is invoked automatically every 64k documents **per shard** and when the index is closed with close_index.
/// Before terminating the program, always call close_index (commit), otherwise all documents indexed since last (manual or automatic) commit are lost.
/// There are only 2 reasons that justify a manual commit:
/// 1. if you want to search newly indexed documents without using realtime=true for search performance reasons or
/// 2. if after indexing new documents there won't be more documents indexed (for some time),
///    so there won't be (soon) a commit invoked automatically at the next 64k threshold **per shard** or close_index,
///    but you still need immediate persistence guarantees on disk to protect against data loss in the event of a crash.
#[utoipa::path(
    patch,
    tag = "Index",
    path = "/api/v1/index/{index_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    responses(
        (status = 200, description = "Index committed, returns the number of committed documents", body = u64),
        (status = BAD_REQUEST, description = "Index id invalid or missing"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    ),
    operation_id = "commit_index",
)]
async fn commit_index(ScopedIndex(index): ScopedIndex) -> ApiResult<u64> {
    let count = INDEX_RUNTIME
        .spawn(async move { commit_index_api(&index).await })
        .await
        .map_err(|_| ApiError::Internal)??;
    Ok(Json(count))
}

/// Delete Index
///
/// Delete an index within the directory associated with the specified API key and return the number of remaining indices.
#[utoipa::path(
    delete,
    tag = "Index",
    path = "/api/v1/index/{index_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    responses(
        (status = 200, description = "Index deleted, returns the number of indices", body = u64),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = NOT_FOUND, description = "Index_id does not exists"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    ),
    operation_id = "delete_index",
)]
async fn delete_index(
    State(state): State<AppState>,
    ApiKey(key): ApiKey,
    Path(index_id): Path<String>,
) -> ApiResult<usize> {
    let index_id = index_id
        .parse()
        .map_err(|_| ApiError::NotFound("index does not exist"))?;
    let mut keys = state.keys.write().await;
    let scope = keys.get_mut(&key).ok_or(ApiError::Unauthorized)?;
    delete_index_api(index_id, &mut scope.index_list)
        .await
        .map_err(|_| ApiError::NotFound("index does not exist"))?;
    Ok(Json(scope.index_list.len()))
}

/// Document iterator
///
/// Document iterator accepts a JSON request body via POST.
/// The document iterator allows to iterate over all document IDs and documents in the entire index, forward or backward.
/// It enables efficient sequential access to every document, even in very large indexes, without running a search.
/// Paging through the index works without collecting document IDs to Min-heap in size-limited RAM first.
/// The iterator guarantees that only valid document IDs are returned, even though document IDs are not strictly continuous.
/// Document IDs can also be fetched in batches, reducing round trips and significantly improving performance, especially when using the REST API.
/// Typical use cases include index export, conversion, analytics, audits, and inspection.
/// Explanation of "eventually continuous" docid:
/// In Seekpower, document IDs become continuous over time. In a multi-sharded index, each shard maintains its own document ID space.
/// Because documents are distributed across shards in a non-deterministic, load-dependent way, shard-local document IDs advance at different rates.
/// When these are mapped to global document IDs, temporary gaps can appear.
/// As a result, simply iterating from 0 to the total document count may encounter invalid IDs near the end.
/// The Document Iterator abstracts this complexity and reliably returns only valid document IDs.
/// # Parameters
/// - docid=None, take>0: **skip first s document IDs**, then **take next t document IDs** of an index.
/// - docid=None, take<0: **skip last s document IDs**, then **take previous t document IDs** of an index.
/// - docid=Some, take>0: **skip next s document IDs**, then **take next t document IDs** of an index, relative to a given document ID, with end-of-index indicator.
/// - docid=Some, take<0: **skip previous s document IDs**, then **take previous t document IDs**, relative to a given document ID, with start-of-index indicator.
/// - take=0: does not make sense, that defies the purpose of get_iterator.
/// - The sign of take indicates the direction of iteration: positive take for forward iteration, negative take for backward iteration.
/// - The skip parameter is always positive, indicating the number of document IDs to skip before taking document IDs. The skip direction is determined by the sign of take too.
/// - include_document: if true, the documents are also retrieved along with their document IDs.
/// Next page:     take last  docid from previous result set, skip=1, take=+page_size
/// Previous page: take first docid from previous result set, skip=1, take=-page_size
/// Returns an IteratorResult, consisting of the number of actually skipped document IDs, and a list of taken document IDs and documents, ordered in traversal direction (ascending for positive take, descending for negative take).
/// Detect end/begin of index during iteration: if returned vec.len() < requested take || if returned skip <requested skip
#[utoipa::path(
    post,
    tag = "Iterator",
    path = "/api/v1/index/{index_id}/iterator",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    request_body(content = GetIteratorRequest, example=json!({
        "document_id": null,
        "skip": 0,
        "take": -1,
    })),
    responses(
        (status = 200, description = "Document ID found, returning an IteratorResult", body = crate::api_schema::IteratorResult),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    ),
    operation_id = "document_iterator_post"
)]
async fn iterate(
    ScopedIndex(index): ScopedIndex,
    bytes: Bytes,
) -> ApiResult<seekpower::iterator::IteratorResult> {
    if bytes.is_empty() {
        return Err(ApiError::BadRequest("Request body is empty".into()));
    }
    let request: GetIteratorRequest = parse_json(&bytes)?;
    Ok(Json(
        get_iterator_api_post(
            &index,
            request.document_id,
            request.skip,
            request.take,
            request.include_deleted,
            request.include_document,
            request.fields,
        )
        .await,
    ))
}

/// Index Document(s)
///
/// Index a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
/// Index documents enables true real-time search (as opposed to near realtime.search):
/// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
/// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed **per shard** or on close_index for persistence.
#[utoipa::path(
    post,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),

    request_body(
        content = HashMap<String, Value>, description = "JSON document or array of JSON documents, each consisting of key-value pairs",content_type = "application/json",
        examples(
    ("Example: Single Lexical/Hybrid document" = (value = json!(  {
        "title": "title1 test",
        "body": "body1",
        "url": "url1"
		} ))),
	("Example: Multiple Lexical/Hybrid documents" = (value = json!(  [
{
    "title":"title2",
    "body":"body2 test",
    "url":"url2"
},
{
    "title":"title3 test",
    "body":"body3 test",
    "url":"url3"
}
]   ))),
    ("Example: Multiple Vector documents" = (value = json!(  [
    {"vector":[0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010, 0.011, 0.012, 0.013, 0.014, 0.015, 0.016, 0.017, 0.018, 0.019, 0.020, 0.021, 0.022, 0.023, 0.024, 0.025, 0.026, 0.027, 0.028, 0.029, 0.030, 0.031, 0.032, 0.033, 0.034, 0.035, 0.036, 0.037, 0.038, 0.039, 0.040, 0.041, 0.042, 0.043, 0.044, 0.045, 0.046, 0.047, 0.048, 0.049, 0.050, 0.051, 0.052, 0.053, 0.054, 0.055, 0.056, 0.057, 0.058, 0.059, 0.060, 0.061, 0.062, 0.063, 0.064, 0.065, 0.066, 0.067, 0.068, 0.069, 0.070, 0.071, 0.072, 0.073, 0.074, 0.075, 0.076, 0.077, 0.078, 0.079, 0.080, 0.081, 0.082, 0.083, 0.084, 0.085, 0.086, 0.087, 0.088, 0.089, 0.090, 0.091, 0.092, 0.093, 0.094, 0.095, 0.096, 0.097, 0.098, 0.099, 0.100, 0.101, 0.102, 0.103, 0.104, 0.105, 0.106, 0.107, 0.108, 0.109, 0.110, 0.111, 0.112, 0.113, 0.114, 0.115, 0.116, 0.117, 0.118, 0.119, 0.120, 0.121, 0.122, 0.123, 0.124, 0.125, 0.126, 0.127, 0.128],"index":"0"},
    {"vector":[0.129, 0.130, 0.131, 0.132, 0.133, 0.134, 0.135, 0.136, 0.137, 0.138, 0.139, 0.140, 0.141, 0.142, 0.143, 0.144, 0.145, 0.146, 0.147, 0.148, 0.149, 0.150, 0.151, 0.152, 0.153, 0.154, 0.155, 0.156, 0.157, 0.158, 0.159, 0.160, 0.161, 0.162, 0.163, 0.164, 0.165, 0.166, 0.167, 0.168, 0.169, 0.170, 0.171, 0.172, 0.173, 0.174, 0.175, 0.176, 0.177, 0.178, 0.179, 0.180, 0.181, 0.182, 0.183, 0.184, 0.185, 0.186, 0.187, 0.188, 0.189, 0.190, 0.191, 0.192, 0.193, 0.194, 0.195, 0.196, 0.197, 0.198, 0.199, 0.200, 0.201, 0.202, 0.203, 0.204, 0.205, 0.206, 0.207, 0.208, 0.209, 0.210, 0.211, 0.212, 0.213, 0.214, 0.215, 0.216, 0.217, 0.218, 0.219, 0.220, 0.221, 0.222, 0.223, 0.224, 0.225, 0.226, 0.227, 0.228, 0.229, 0.230, 0.231, 0.232, 0.233, 0.234, 0.235, 0.236, 0.237, 0.238, 0.239, 0.240, 0.241, 0.242, 0.243, 0.244, 0.245, 0.246, 0.247, 0.248, 0.249, 0.250, 0.251, 0.252, 0.253, 0.254, 0.255, 0.256],"index":"1"},
    {"vector":[0.257, 0.258, 0.259, 0.260, 0.261, 0.262, 0.263, 0.264, 0.265, 0.266, 0.267, 0.268, 0.269, 0.270, 0.271, 0.272, 0.273, 0.274, 0.275, 0.276, 0.277, 0.278, 0.279, 0.280, 0.281, 0.282, 0.283, 0.284, 0.285, 0.286, 0.287, 0.288, 0.289, 0.290, 0.291, 0.292, 0.293, 0.294, 0.295, 0.296, 0.297, 0.298, 0.299, 0.300, 0.301, 0.302, 0.303, 0.304, 0.305, 0.306, 0.307, 0.308, 0.309, 0.310, 0.311, 0.312, 0.313, 0.314, 0.315, 0.316, 0.317, 0.318, 0.319, 0.320, 0.321, 0.322, 0.323, 0.324, 0.325, 0.326, 0.327, 0.328, 0.329, 0.330, 0.331, 0.332, 0.333, 0.334, 0.335, 0.336, 0.337, 0.338, 0.339, 0.340, 0.341, 0.342, 0.343, 0.344, 0.345, 0.346, 0.347, 0.348, 0.349, 0.350, 0.351, 0.352, 0.353, 0.354, 0.355, 0.356, 0.357, 0.358, 0.359, 0.360, 0.361, 0.362, 0.363, 0.364, 0.365, 0.366, 0.367, 0.368, 0.369, 0.370, 0.371, 0.372, 0.373, 0.374, 0.375, 0.376, 0.377, 0.378, 0.379, 0.380, 0.381, 0.382, 0.383, 0.384],"index":"2"}
]   )))
        )
    ),
    responses(
        (status = 200, description = "Document indexed, returns the number of indexed documents", body = usize),
        (status = BAD_REQUEST, description = "Document object invalid"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    ),
    operation_id = "index_document",
)]
async fn write_documents(
    ScopedIndex(index): ScopedIndex,
    bytes: Bytes,
) -> ApiResult<Result<usize, String>> {
    let count = INDEX_RUNTIME
        .spawn(async move {
            if bytes.trim_ascii_start().starts_with(b"[") {
                index_documents_api(&index, parse_json(&bytes)?)
                    .await
                    .map_err(ApiError::from)
            } else {
                index_document_api(&index, parse_json(&bytes)?)
                    .await
                    .map_err(ApiError::from)
            }
        })
        .await
        .map_err(|_| ApiError::Internal)??;
    Ok(Json(Ok(count)))
}

/// Update Document(s)
///
/// Update a JSON document or an array of JSON documents (bulk), each consisting of arbitrary key-value pairs to the index with the specified apikey and index_id, and return the number of indexed docs.
/// Update document is a combination of delete_document and index_document.
/// All current limitations of delete_document apply.
/// Update documents enables true real-time search (as opposed to near realtime.search):
/// When in query_index the parameter `realtime` is set to `true` then indexed, but uncommitted documents are immediately included in the search results, without requiring a commit or refresh.
/// Therefore a explicit commit_index is almost never required, as it is invoked automatically after 64k documents are indexed **per shard** or on close_index for persistence.
#[utoipa::path(
    patch,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    request_body(content = (u64, HashMap<String, Value>), description = "Tuple of (doc_id, JSON document) or array of tuples (doc_id, JSON documents), each JSON document consisting of arbitrary key-value pairs", content_type = "application/json", example=json!([0,{"title":"title1 test","body":"body1","url":"url1"}])),
    responses(
        (status = 200, description = "Document indexed, returns the number of indexed documents", body = usize),
        (status = BAD_REQUEST, description = "Document object invalid"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing")
    ),
    operation_id = "update_document",
)]
async fn update_documents(
    ScopedIndex(index): ScopedIndex,
    JsonBody(value): JsonBody<serde_json::Value>,
) -> ApiResult<Result<u64, String>> {
    let count = if value.get(0).is_some_and(serde_json::Value::is_array) {
        let documents = serde_json::from_value(value)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        update_documents_api(&index, documents).await?
    } else {
        let document = serde_json::from_value(value)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        update_document_api(&index, document).await?
    };
    Ok(Json(Ok(count)))
}

/// Get Document
///
/// Get document from index with index_id
/// ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
#[utoipa::path(
    get,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc/{document_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
        ("document_id" = u64, Path, description = "document id"),
    ),
    request_body(content = GetDocumentRequest, example=json!({
        "fields": ["title", "body"]
    })),
    responses(
        (status = 200, description = "Document found, returns the JSON document consisting of arbitrary key-value pairs", body = HashMap<String, Value>),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "doc_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "Document id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    ),
    operation_id = "get_document",
)]
async fn get_document(
    ScopedIndex(index): ScopedIndex,
    Path((_, document_id)): Path<(String, String)>,
    uri: axum::http::Uri,
    bytes: Bytes,
) -> ApiResult<Document> {
    let document_id = document_id
        .parse()
        .map_err(|_| ApiError::BadRequest("doc_id invalid or missing".into()))?;
    if uri.query().is_some_and(|query| !query.is_empty()) {
        return Err(ApiError::BadRequest(
            "document reads accept fields in the JSON body, not URL parameters".into(),
        ));
    }
    let request = if bytes.is_empty() {
        GetDocumentRequest { fields: Vec::new() }
    } else {
        parse_json(&bytes)?
    };
    Ok(Json(
        get_document_api(&index, document_id, request)
            .await
            .ok_or(ApiError::NotFound("document not found"))?,
    ))
}

/// Delete Document by document ID
///
/// Delete document by document_id from index with index_id
/// ⚠️ Use search or get_iterator first to obtain a valid doc_id. Document IDs are not guaranteed to be continuous and gapless!
/// Immediately effective, independent of commit.
/// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
/// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
/// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
/// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
/// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
#[utoipa::path(
    delete,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc/{document_id}",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
        ("document_id" = u64, Path, description = "document id"),
    ),
    responses(
        (status = 200, description = "Document deleted, returns indexed documents count", body = usize),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "doc_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "Document id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    ),
    operation_id = "delete_document_by_parameter"
)]
async fn delete_document(
    ScopedIndex(index): ScopedIndex,
    Path((_, document_id)): Path<(String, String)>,
    bytes: Bytes,
) -> ApiResult<Result<u64, String>> {
    match document_id.parse() {
        Ok(id) => Ok(Json(delete_document_by_parameter_api(&index, id).await)),
        Err(_) => delete_documents(ScopedIndex(index), bytes).await,
    }
}

/// Delete Document(s) by Request Object
///
/// Delete document by document_id, by array of document_id (bulk), by query (SearchRequestObject) from index with index_id, or clear all documents from index.
/// Immediately effective, independent of commit.
/// Index space used by deleted documents is not reclaimed (until compaction is implemented), but result_count_total is updated.
/// By manually deleting the delete.bin file the deleted documents can be recovered (until compaction).
/// Deleted documents impact performance, especially but not limited to counting (Count, TopKCount). They also increase the size of the index (until compaction is implemented).
/// For minimal query latency delete index and reindexing documents is preferred over deleting documents (until compaction is implemented).
/// BM25 scores are not updated (until compaction is implemented), but the impact is minimal.
/// Document ID can by obtained by search. When deleting by query (SearchRequestObject), it is advised to perform a dry run search first, to see which documents will be deleted.
#[utoipa::path(
    delete,
    tag = "Document",
    path = "/api/v1/index/{index_id}/doc",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),
    request_body(content = SearchRequestObject, description = "Specifies the document(s) to delete by different request objects\n- 'clear' : delete all documents in index (clear index)\n- u64 : delete single doc ID\n- [u64] : delete array of doc ID \n- SearchRequestObject : delete documents by query", content_type = "application/json", example=json!({
        "query":"test",
        "offset":0,
        "length":10,
        "realtime": false,
        "field_filter": ["title", "body"]
    })),

    responses(
        (status = 200, description = "Document deleted, returns indexed documents count", body = usize),
        (status = BAD_REQUEST, description = "index_id invalid or missing"),
        (status = BAD_REQUEST, description = "doc_id invalid or missing"),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "Document id does not exist"),
        (status = NOT_FOUND, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
    ),
    operation_id = "delete_document_by_request_object"
)]
async fn delete_documents(
    ScopedIndex(index): ScopedIndex,
    bytes: Bytes,
) -> ApiResult<Result<u64, String>> {
    let result = if bytes == b"clear"[..] {
        clear_index_api(&index).await
    } else if let Ok(query) = serde_json::from_slice::<SearchRequestObject>(&bytes) {
        Ok(delete_documents_by_query_api(&index, query).await?)
    } else if bytes.trim_ascii_start().starts_with(b"[") {
        delete_documents_by_object_api(&index, parse_json(&bytes)?).await
    } else {
        delete_document_by_object_api(&index, parse_json(&bytes)?).await
    };
    Ok(Json(result))
}

/// Query Index
///
/// Query results from index with index_id
/// The following parameters are supported:
/// - Result type
/// - Result sorting
/// - Realtime search
/// - Field filter
/// - Fields to include in search results
/// - Facet filter: filter facets by field and value
/// - Result sort: sort results by field and direction
/// - Query type default: default query type, if not specified in query
#[utoipa::path(
    post,
    tag = "Query",
    path = "/api/v1/index/{index_id}/query",
    params(
        ("apikey" = String, Header, description = "YOUR_SECRET_API_KEY",example="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ("index_id" = u64, Path, description = "index id"),
    ),

    request_body(
        content = SearchRequestObject,
        examples(
            ("Example: Lexical search" = (value = json!(  {
        "query": "detroit",
        "query_vector": null,
        "enable_empty_query": false,
        "offset": 0,
        "length": 10,
        "result_type": "TopkCount",
        "realtime": true,
        "field_filter": ["title"],
        "fields": [],
        "query_type_default": "Intersection",
        "search_mode": "Lexical"
    }   ))),
	("Example: Hybrid search" = (value = json!({
        "query": "example", "query_vector": [1.0, 0.0, 0.0],
        "length": 10, "result_type": "TopkCount", "realtime": true,
        "search_mode": {"Hybrid": {"similarity_threshold": null, "ann_mode": "All"}}
    }))),
("Example: Vector search" = (value = json!(  {
    "query":"",
    "query_vector": [
        0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010, 0.011, 0.012, 0.013,
        0.014, 0.015, 0.016, 0.017, 0.018, 0.019, 0.020, 0.021, 0.022, 0.023, 0.024, 0.025, 0.026,
        0.027, 0.028, 0.029, 0.030, 0.031, 0.032, 0.033, 0.034, 0.035, 0.036, 0.037, 0.038, 0.039,
        0.040, 0.041, 0.042, 0.043, 0.044, 0.045, 0.046, 0.047, 0.048, 0.049, 0.050, 0.051, 0.052,
        0.053, 0.054, 0.055, 0.056, 0.057, 0.058, 0.059, 0.060, 0.061, 0.062, 0.063, 0.064, 0.065,
        0.066, 0.067, 0.068, 0.069, 0.070, 0.071, 0.072, 0.073, 0.074, 0.075, 0.076, 0.077, 0.078,
        0.079, 0.080, 0.081, 0.082, 0.083, 0.084, 0.085, 0.086, 0.087, 0.088, 0.089, 0.090, 0.091,
        0.092, 0.093, 0.094, 0.095, 0.096, 0.097, 0.098, 0.099, 0.100, 0.101, 0.102, 0.103, 0.104,
        0.105, 0.106, 0.107, 0.108, 0.109, 0.110, 0.111, 0.112, 0.113, 0.114, 0.115, 0.116, 0.117,
        0.118, 0.119, 0.120, 0.121, 0.122, 0.123, 0.124, 0.125, 0.126, 0.127, 0.128
    ],
    "offset":0,
    "length":10,
    "result_type": "Topk",
    "realtime": false,
    "search_mode": {
        "Vector":{
            "similarity_threshold": 0.7,
            "ann_mode": {"Nprobe":55}
        }
    }

}  )))
        )
    ),
    responses(
        (status = 200, description = "Results found, returns the SearchResultObject", body = SearchResultObject),
        (status = BAD_REQUEST, description = "Request object incorrect"),
        (status = NOT_FOUND, description = "Index id does not exist"),
        (status = NOT_FOUND, description = "API key does not exist"),
        (status = UNAUTHORIZED, description = "api_key does not exists"),
        (status = UNAUTHORIZED, description = "api_key missing"),
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
#[openapi(servers((url = "http://127.0.0.1", description = "Local Seekpower server")))]
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

async fn not_implemented() -> impl IntoResponse {
    (StatusCode::NOT_IMPLEMENTED, "not implemented")
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
        .fallback(not_implemented)
        .method_not_allowed_fallback(not_implemented)
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
