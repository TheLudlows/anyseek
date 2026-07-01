use axum::routing::{delete, get, post};
use axum::Router;

use crate::handler::query::*;
use crate::handler::schema::*;
use crate::handler::write::*;
use crate::state::SharedState;

pub fn create_app(state: SharedState) -> Router {
    Router::new()
        .route("/index/create", post(handle_create_index))
        .route("/index/{path}", delete(handle_delete_index))
        .route("/document", post(handle_index_document))
        .route("/documents", post(handle_index_documents))
        .route("/document/{id}", get(handle_get_document).delete(handle_delete_document))
        .route("/commit", post(handle_commit))
        .route("/search", post(handle_search))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
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
    async fn create_app_does_not_panic() {
        let app = create_app(empty_shared_index().await);
        drop(app);
    }

    #[tokio::test]
    async fn delete_index_when_none_returns_not_found() {
        let app = create_app(empty_shared_index().await);

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/index/anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn commit_without_index_returns_404() {
        let app = create_app(empty_shared_index().await);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/commit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
