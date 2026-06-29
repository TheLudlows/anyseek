pub mod router;
pub mod handler;

use axum::Router;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::index::Index;

pub type SharedIndex = Arc<RwLock<Option<Index>>>;

pub fn create_app(state: SharedIndex) -> Router {
    router::create_router(state)
}

pub async fn run_server(addr: &str, state: SharedIndex) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_app(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
