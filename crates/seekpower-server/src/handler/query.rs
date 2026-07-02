use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use seekpower_core::get_document;
use seekpower_error::SeekPowerError;
use seekpower_search::{SearchRequest, SearchResponse};

use crate::model::*;
use crate::error::*;
use crate::state::{get_index, SharedState};

pub async fn handle_get_document(
    State(state): State<SharedState>,
    Query(query): Query<IndexQuery>,
    Path(doc_id): Path<u64>,
) -> impl IntoResponse {
    let index_name = match query.index.as_ref() {
        Some(n) => n,
        None => return map_error(SeekPowerError::InvalidQuery("missing index query parameter".into())).into_response(),
    };
    let index = match get_index(&state, index_name) {
        Ok(i) => i,
        Err(e) => return map_error(e).into_response(),
    };

    let res = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SeekPowerError::StorageError(e.to_string()))?;
        rt.block_on(get_document(&index, doc_id))
    })
    .await;

    match res {
        Ok(Ok(Some(doc))) => (
            StatusCode::OK,
            Json(GetDocumentResponse { doc_id, document: doc }),
        )
            .into_response(),
        Ok(Ok(None)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("document {doc_id} not found"),
            }),
        )
            .into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}

pub async fn handle_search(
    State(state): State<SharedState>,
    Query(query): Query<IndexQuery>,
    Json(req): Json<SearchRequestHttp>,
) -> impl IntoResponse {
    let parsed: SearchRequest = match req.into_search_request() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };

    let index_name = match query.index.as_ref() {
        Some(n) => n,
        None => return map_error(SeekPowerError::InvalidQuery("missing index query parameter".into())).into_response(),
    };

    let index = match get_index(&state, index_name) {
        Ok(i) => i,
        Err(e) => return map_error(e).into_response(),
    };

    let res = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SeekPowerError::StorageError(e.to_string()))?;
        rt.block_on(seekpower_core::search(&index, parsed))
    })
    .await;

    match res {
        Ok(Ok(resp)) => search_response_to_json(resp).into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}

fn search_response_to_json(resp: SearchResponse) -> (StatusCode, Json<serde_json::Value>) {
    let results: Vec<serde_json::Value> = resp
        .results
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "doc_id": r.doc_id,
                "score": r.score,
                "result_type": format!("{:?}", r.result_type),
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "query": resp.query,
            "result_count": resp.result_count,
            "results": results,
        })),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use crate::router::create_app;
    use crate::state::SharedState;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    async fn empty_shared_index() -> SharedState {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = seekpower_storage::KvStore::open("test", store).await.unwrap();
        Arc::new(crate::state::State::new(db))
    }

    #[tokio::test]
    async fn search_on_empty_state_returns_bad_request() {
        let app = create_app(empty_shared_index().await);

        let req = SearchRequestHttp {
            query_string: Some("hello".into()),
            query_vector: None,
            query_type: Some("union".into()),
            search_mode: Some("lexical".into()),
            offset: Some(0),
            length: Some(10),
            ann_mode: None,
            w_lexical: None,
            w_vector: None,
        };
        let body = serde_json::to_vec(&req).expect("serialize");

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/search")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_document_without_index_returns_404() {
        let app = create_app(empty_shared_index().await);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/document/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
