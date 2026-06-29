use axum::{Router, Json};
use axum::routing::{delete, get, post};
use axum::extract::State;

use crate::server::handler::{
    create_index, open_index, delete_index,
    index_document, index_documents,
    delete_document, get_document,
    search,
};
use crate::server::SharedIndex;

/// Create the Axum router with all API endpoints.
pub fn create_router(state: SharedIndex) -> Router {
    Router::new()
        // Index lifecycle
        .route("/index/create", post(create_index))
        .route("/index/open", post(open_index))
        .route("/index/{path}", delete(delete_index))
        // Document operations
        .route("/document", post(index_document))
        .route("/documents", post(index_documents))
        .route("/document/{doc_id}", delete(delete_document))
        .route("/document/{doc_id}", get(get_document))
        // Commit - using GET temporarily for axum 0.8 compatibility
        .route("/commit", get(commit_route))
        // Search
        .route("/search", post(search))
        .with_state(state)
}

/// Inline commit handler for /commit endpoint
async fn commit_route(
    State(state): State<SharedIndex>,
) -> Json<serde_json::Value> {
    let guard = state.read().await;
    let result = if let Some(index) = guard.as_ref() {
        match crate::index::commit(index).await {
            Ok(_) => (200u16, "Committed".to_string()),
            Err(e) => (500u16, e.to_string()),
        }
    } else {
        (400u16, "No index open".to_string())
    };
    Json(serde_json::json!({"status": result.0, "message": result.1}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[test]
    fn test_create_router_does_not_panic() {
        let state: SharedIndex = Arc::new(RwLock::new(None));
        let _router = create_router(state);
        // Router created successfully — routes are registered.
    }
}