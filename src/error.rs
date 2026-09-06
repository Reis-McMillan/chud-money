//! Application error type mapped onto HTTP responses.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::kalshi::client::KalshiError;
use crate::model::ModelError;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("{0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Kalshi(#[from] KalshiError),
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, body) = match &self {
            AppError::BadRequest(m) => (StatusCode::BAD_REQUEST, json!({ "error": m })),
            AppError::NotFound(m) => (StatusCode::NOT_FOUND, json!({ "error": m })),
            AppError::Conflict(m) => (StatusCode::CONFLICT, json!({ "error": m })),
            AppError::Model(ModelError::Validation(errors)) => (
                StatusCode::BAD_REQUEST,
                json!({ "error": "document failed schema validation", "details": errors }),
            ),
            AppError::Model(e) => (StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": e.to_string() })),
            AppError::Kalshi(e) => (StatusCode::BAD_GATEWAY, json!({ "error": e.to_string() })),
            AppError::Postgres(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": format!("questdb query failed: {e}") }),
            ),
            AppError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": format!("{e:#}") })),
        };
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        }
        (status, Json(body)).into_response()
    }
}
