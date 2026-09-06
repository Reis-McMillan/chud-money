//! HTTP + websocket controllers. Routes:
//!
//! - `GET  /`                    list markets (+ feed status)
//! - `POST /add`                 create a market and start its feed
//! - `POST /ingest`              start a historical backfill job
//! - `GET  /ingest/{job_id}`     backfill job status
//! - `GET  /{tag}`               market document + feed status + QuestDB summary
//! - `DELETE /{tag}`             stop the feed and delete the document
//! - `GET  /ws/{tag}/ticker`     proxy of the CF Benchmarks 5Hz index stream
//! - `GET  /ws/{tag}/orderbook`  proxy of Kalshi orderbook snapshot/delta frames

pub mod ingest;
pub mod markets;
pub mod ws;

use axum::Router;
use axum::routing::{get, post};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(markets::list))
        .route("/add", post(markets::add))
        .route("/ingest", post(ingest::start))
        .route("/ingest/{job_id}", get(ingest::status))
        .route("/ws/{tag}/ticker", get(ws::ticker))
        .route("/ws/{tag}/orderbook", get(ws::orderbook))
        .route("/{tag}", get(markets::show).delete(markets::remove))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
