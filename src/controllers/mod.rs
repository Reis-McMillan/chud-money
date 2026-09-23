//! HTTP + websocket controllers. Routes:
//!
//! - `GET  /`                    list markets (+ feed status)
//! - `GET  /healthz`             liveness/readiness probe (touches no dependency)
//! - `GET  /auth/me`             🔒 the caller's identity
//! - `POST /add`                 🔒 create a market and start its feed
//! - `POST /ingest`              🔒 start a historical backfill job (`kind`: `index` or `contracts`)
//! - `GET  /ingest/{job_id}`     backfill job status
//! - `GET  /{tag}`               market document + feed status + QuestDB summary
//! - `DELETE /{tag}`             🔒 stop the feed and delete the document
//! - `GET  /ws/{tag}/ticker`     🔒 proxy of the CF Benchmarks 5Hz index stream
//! - `GET  /ws/{tag}/orderbook`  🔒 proxy of Kalshi orderbook snapshot/delta frames
//!
//! 🔒 routes go through `middleware::authenticated` and need a bearer token.
//! Websocket routes take it as `?access_token=` instead, since a browser
//! cannot set headers on an upgrade request.

pub mod auth;
pub mod ingest;
pub mod markets;
pub mod ws;

use axum::routing::{delete, get, post};
use axum::{Router, middleware};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::middleware::authenticated::authenticated;
use crate::state::AppState;

/// Probe target for Kubernetes. Deliberately touches neither Mongo nor
/// QuestDB: startup already fails fast when they are down, and a transient
/// outage after that should not get the pod killed and its feeds restarted.
async fn healthz() -> &'static str {
    "ok"
}

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/auth/me", get(auth::me))
        .route("/add", post(markets::add))
        .route("/ingest", post(ingest::start))
        .route("/{tag}", delete(markets::remove))
        .route("/ws/{tag}/ticker", get(ws::ticker))
        .route("/ws/{tag}/orderbook", get(ws::orderbook))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticated));

    Router::new()
        .route("/", get(markets::list))
        .route("/healthz", get(healthz))
        .route("/ingest/{job_id}", get(ingest::status))
        .route("/{tag}", get(markets::show))
        .merge(protected)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::extract::Request;
    use axum::http::{Method, StatusCode};
    use axum::middleware::Next;
    use axum::response::{IntoResponse, Response};
    use axum::routing::{delete, get, post};
    use axum::{Router, middleware};
    use tower::ServiceExt;

    async fn ok() -> &'static str {
        "ok"
    }

    async fn deny(_req: Request, _next: Next) -> Response {
        StatusCode::UNAUTHORIZED.into_response()
    }

    /// Same shape as `router`: a guarded `DELETE /{tag}` merged into a router
    /// that already has `GET /{tag}`. axum panics at construction if it
    /// cannot merge the two method routers, so building it is the test. The
    /// websocket route shows the guard runs before any upgrade handling.
    #[tokio::test]
    async fn protected_methods_merge_into_public_paths() {
        let protected = Router::new()
            .route("/add", post(ok))
            .route("/{tag}", delete(ok))
            .route("/ws/{tag}/ticker", get(ok))
            .route_layer(middleware::from_fn(deny));
        let app: Router = Router::new().route("/", get(ok)).route("/{tag}", get(ok)).merge(protected);

        let call = |method: Method, uri: &'static str| {
            let app = app.clone();
            async move {
                app.oneshot(Request::builder().method(method).uri(uri).body(Body::empty()).unwrap())
                    .await
                    .unwrap()
                    .status()
            }
        };
        assert_eq!(call(Method::GET, "/btc-15m").await, StatusCode::OK);
        assert_eq!(call(Method::DELETE, "/btc-15m").await, StatusCode::UNAUTHORIZED);
        assert_eq!(call(Method::POST, "/add").await, StatusCode::UNAUTHORIZED);
        assert_eq!(call(Method::GET, "/ws/btc-15m/ticker").await, StatusCode::UNAUTHORIZED);
        assert_eq!(call(Method::GET, "/").await, StatusCode::OK);
    }

    /// `/healthz` sits next to the `/{tag}` capture; axum prefers the static
    /// match, so the probe must never turn into a market lookup.
    #[tokio::test]
    async fn healthz_is_not_captured_by_tag_route() {
        let app: Router = Router::new().route("/healthz", get(super::healthz)).route("/{tag}", get(ok));
        let res = app
            .oneshot(Request::builder().method(Method::GET).uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 16).await.unwrap();
        assert_eq!(&body[..], b"ok");
    }
}
