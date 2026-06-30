use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use seekpower_core::{create_index, create_index_with_db, open_index, open_index_with_db};
use seekpower_error::SeekPowerError;
use seekpower_schema::SchemaField;

use crate::model::*;
use crate::error::*;
use crate::state::{SharedState};

pub async fn handle_create_index(
    State(state): State<SharedState>,
    Json(req): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    let store = match build_object_store(&req.object_store_kind, &req.name) {
        Ok(s) => s,
        Err(e) => return map_error(e).into_response(),
    };
    let schema: Vec<SchemaField> = req.schema.into_iter().map(|s| s.into_schema_field()).collect();
    let config = req.config.unwrap_or_default();
    let field_count = schema.len();
    let name = req.name.clone();

    // Create a KvStore view for this index
    let db = state.db.for_index(&name);

    let res = create_index_with_db(db, &name, config, schema).await;

    match res {
        Ok(idx) => {
            {
                let mut guard = match state.indexes.lock() {
                    Ok(g) => g,
                    Err(e) => return map_io_err(e).into_response(),
                };
                guard.insert(name.clone(), Arc::new(idx));
            }
            (
                StatusCode::CREATED,
                Json(CreateIndexResponse {
                    name,
                    field_count,
                }),
            )
                .into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

pub async fn handle_open_index(
    State(state): State<SharedState>,
    Json(req): Json<OpenIndexRequest>,
) -> impl IntoResponse {
    let name = req.name.clone();
    let db = state.db.for_index(&name);

    let res = open_index_with_db(db, &name).await;

    match res {
        Ok(idx) => {
            {
                let mut guard = match state.indexes.lock() {
                    Ok(g) => g,
                    Err(e) => return map_io_err(e).into_response(),
                };
                guard.insert(name.clone(), Arc::new(idx));
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({ "name": name })),
            )
                .into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

pub async fn handle_delete_index(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    // Remove from loaded indexes
    {
        let mut guard = match state.indexes.lock() {
            Ok(g) => g,
            Err(e) => return map_io_err(e).into_response(),
        };
        if guard.remove(&name).is_none() {
            return map_error(SeekPowerError::IndexNotFound(format!("index '{}' not loaded", name))).into_response();
        }
    }

    // Delete the index data
    match seekpower_core::delete_index(&state.db, &name).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "deleted": true, "name": name })),
        )
            .into_response(),
        Err(e) => map_error(e).into_response(),
    }
}
