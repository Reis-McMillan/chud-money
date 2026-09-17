//! `GET /auth/me` — who the bearer token belongs to. Login itself happens in
//! the SPA against Verys; see `middleware::authenticated` for how the
//! exchanged token is verified.

use axum::Json;

use crate::middleware::authenticated::AuthUser;

pub async fn me(user: AuthUser) -> Json<AuthUser> {
    Json(user)
}
