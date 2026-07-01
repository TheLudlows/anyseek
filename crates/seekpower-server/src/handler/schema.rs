use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use seekpower_core::{create_index_with_id, open_index_with_id, delete_index_with_id};
use seekpower_error::SeekPowerError;
use seekpower_schema::SchemaField;

use crate::model::*;
use crate::error::*;
use crate::state::{SharedState};

pub async fn handle_create_index(
    State(state): State<SharedState>,
    Json(req): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    match build_object_store(&req.object_store_kind, &req.name) {
        Ok(_) => {}
        Err(e) => return map_error(e).into_response(),
    };
    let schema: Vec<SchemaField> = req.schema.into_iter().map(|s| s.into_schema_field()).collect();
    let config = req.config.unwrap_or_default();
    let field_count = schema.len();
    let name = req.name.clone();

    // Allocate a globally unique index_id
    let index_id = state.next_index_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let res = create_index_with_id(state.db.clone(), index_id, &name, config, schema).await;

    match res {
        Ok(idx) => {
            // Register in registry: store the allocated index_id (u32 BE)
            let registry_key = format!("__indexes__:{}", name);
            if let Err(e) = state.db.put(registry_key.as_bytes(), &index_id.to_be_bytes()).await {
                return map_error(e).into_response();
            }

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

    // Read index_id from registry
    let registry_key = format!("__indexes__:{}", name);
    let id_bytes = match state.db.get(registry_key.as_bytes()).await {
        Ok(Some(v)) => v,
        Ok(None) => return map_error(SeekPowerError::IndexNotFound(format!("Index '{}' not found in registry", name))).into_response(),
        Err(e) => return map_error(e).into_response(),
    };
    let id_bytes: [u8; 4] = match id_bytes.try_into() {
        Ok(b) => b,
        Err(_) => return map_error(SeekPowerError::StorageError(format!("Invalid index_id for '{}'", name))).into_response(),
    };
    let index_id = u32::from_be_bytes(id_bytes);

    let res = open_index_with_id(state.db.clone(), index_id, &name).await;

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
    // Read index_id from registry first
    let registry_key = format!("__indexes__:{}", name);
    let id_bytes = match state.db.get(registry_key.as_bytes()).await {
        Ok(Some(v)) => v,
        Ok(None) => return map_error(SeekPowerError::IndexNotFound(format!("Index '{}' not found in registry", name))).into_response(),
        Err(e) => return map_error(e).into_response(),
    };
    let id_bytes: [u8; 4] = match id_bytes.try_into() {
        Ok(b) => b,
        Err(_) => return map_error(SeekPowerError::StorageError(format!("Invalid index_id for '{}'", name))).into_response(),
    };
    let index_id = u32::from_be_bytes(id_bytes);

    // Remove from loaded indexes
    {
        let mut guard = match state.indexes.lock() {
            Ok(g) => g,
            Err(e) => return map_io_err(e).into_response(),
        };
        guard.remove(&name);
    }

    // Delete the index data for the allocated index_id
    match delete_index_with_id(&state.db, index_id).await {
        Ok(()) => {
            // Remove from registry
            let _ = state.db.delete(registry_key.as_bytes()).await;
            (
                StatusCode::OK,
                Json(serde_json::json!({ "deleted": true, "name": name })),
            )
                .into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}
