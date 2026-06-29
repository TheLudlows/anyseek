use axum::Router;
use axum::routing::{delete, get, post};

use crate::server::handler::{
    create_index, open_index, delete_index,
    index_document, index_documents,
    delete_document, get_document,
    commit, search,
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
        // Commit
        .route("/commit", post(commit))
        // Search
        .route("/search", post(search))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::SharedIndex;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[test]
    fn test_create_router_does_not_panic() {
        let state: SharedIndex = Arc::new(RwLock::new(None));
        let _router = create_router(state);
        // Router created successfully — routes are registered.
    }
}
