use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use seekpower_core::{commit, delete_document, index_document};
use seekpower_error::SeekPowerError;

use crate::model::*;
use crate::error::*;
use crate::state::{get_index, SharedState};

pub async fn handle_index_document(
    State(state): State<SharedState>,
    Query(query): Query<IndexQuery>,
    Json(req): Json<IndexDocumentRequest>,
) -> impl IntoResponse {
    let index_name = &req.index_name;

    let index = match get_index(&state, index_name) {
        Ok(i) => i,
        Err(e) => return map_error(e).into_response(),
    };

    let document = req.document;
    let res = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SeekPowerError::StorageError(e.to_string()))?;
        rt.block_on(index_document(&index, document))
    })
    .await;

    match res {
        Ok(Ok(doc_id)) => (
            StatusCode::CREATED,
            Json(IndexDocumentResponse { doc_id }),
        )
            .into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}

pub async fn handle_index_documents(
    State(state): State<SharedState>,
    Query(query): Query<IndexQuery>,
    Json(req): Json<IndexDocumentsRequest>,
) -> impl IntoResponse {
    let index_name = &req.index_name;

    let documents = req.documents;
    let index = match get_index(&state, index_name) {
        Ok(i) => i,
        Err(e) => return map_error(e).into_response(),
    };

    let res = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SeekPowerError::StorageError(e.to_string()))?;
        let mut ids = Vec::with_capacity(documents.len());
        for doc in documents {
            let id = rt.block_on(index_document(&index, doc))?;
            ids.push(id);
        }
        Ok::<_, SeekPowerError>(ids)
    })
    .await;

    match res {
        Ok(Ok(doc_ids)) => (
            StatusCode::CREATED,
            Json(IndexDocumentsResponse { doc_ids }),
        )
            .into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}

pub async fn handle_commit(State(state): State<SharedState>, Query(query): Query<IndexQuery>) -> impl IntoResponse {
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
        rt.block_on(commit(&index))
    })
    .await;

    match res {
        Ok(Ok(())) => (
            StatusCode::OK,
            Json(serde_json::json!({ "committed": true })),
        )
            .into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}

pub async fn handle_delete_document(
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
        rt.block_on(delete_document(&index, doc_id))
    })
    .await;

    match res {
        Ok(Ok(())) => (
            StatusCode::OK,
            Json(serde_json::json!({ "deleted": true, "doc_id": doc_id })),
        )
            .into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}
