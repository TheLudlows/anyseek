use axum::http::StatusCode;
use axum::response::Json;
use seekpower_error::SeekPowerError;

use crate::model::ErrorResponse;

pub fn map_error(err: SeekPowerError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match &err {
        SeekPowerError::IndexNotFound(_) => StatusCode::NOT_FOUND,
        SeekPowerError::InvalidQuery(_) | SeekPowerError::SchemaError(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ErrorResponse {
            error: err.to_string(),
        }),
    )
}

pub fn map_join_err(err: tokio::task::JoinError) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: format!("blocking task failed: {err}"),
        }),
    )
}

pub fn map_io_err(err: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: err.to_string(),
        }),
    )
}
