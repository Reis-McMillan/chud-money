//! Application error type mapped onto HTTP responses.

use axum::Json;
use axum::http::header::WWW_AUTHENTICATE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use verys_rs_client::Error as VerysError;

use crate::kalshi::client::KalshiError;
use crate::model::ModelError;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("{0}")]
    BadRequest(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("too many requests: {0}")]
    TooManyRequests(String),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Kalshi(#[from] KalshiError),
    #[error(transparent)]
    Verys(#[from] VerysError),
    #[error("invalid access token: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, body) = match &self {
            AppError::BadRequest(m) => (StatusCode::BAD_REQUEST, json!({ "error": m })),
            AppError::Unauthorized(m) => (StatusCode::UNAUTHORIZED, json!({ "error": m })),
            AppError::Forbidden(m) => (StatusCode::FORBIDDEN, json!({ "error": m })),
            AppError::NotFound(m) => (StatusCode::NOT_FOUND, json!({ "error": m })),
            AppError::Conflict(m) => (StatusCode::CONFLICT, json!({ "error": m })),
            AppError::TooManyRequests(m) => (StatusCode::TOO_MANY_REQUESTS, json!({ "error": m })),
            AppError::Model(ModelError::Validation(errors)) => (
                StatusCode::BAD_REQUEST,
                json!({ "error": "document failed schema validation", "details": errors }),
            ),
            AppError::Model(e) => (StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": e.to_string() })),
            AppError::Kalshi(e) => (StatusCode::BAD_GATEWAY, json!({ "error": e.to_string() })),
            // Only the JWKS fetch goes through Verys now, so any failure is
            // Verys being unreachable or misconfigured.
            AppError::Verys(e) => (StatusCode::BAD_GATEWAY, json!({ "error": format!("verys: {e}") })),
            AppError::Jwt(e) => (StatusCode::UNAUTHORIZED, json!({ "error": format!("invalid access token: {e}") })),
            AppError::Postgres(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": format!("questdb query failed: {e}") }),
            ),
            AppError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": format!("{e:#}") })),
        };
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        }
        let mut response = (status, Json(body)).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}
