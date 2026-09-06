//! chud-money: axum backend for a Kalshi trading dashboard.
//!
//! Stores tracked markets in MongoDB, streams each market's CF Benchmarks
//! index and Kalshi orderbooks over a per-market feed task, writes index
//! values to QuestDB, and exposes REST + websocket proxy endpoints.
//!
//! Configuration is read from the environment (see `.env.example`).

mod config;
mod controllers;
mod db;
mod error;
mod feeds;
mod kalshi;
mod model;
mod state;

use std::sync::Arc;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::db::mongo::Mongo;
use crate::db::questdb::Questdb;
use crate::kalshi::auth::Auth;
use crate::kalshi::client::KalshiClient;
use crate::model::Model;
use crate::model::market::Market;
use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = Config::from_env()?;

    let pem = std::fs::read_to_string(&config.kalshi_key_path)
        .with_context(|| format!("reading private key at {}", config.kalshi_key_path))?;
    let auth = Auth::new(config.kalshi_key_id.clone(), &pem)?;
    let kalshi = Arc::new(KalshiClient::new(config.kalshi_env.endpoints(), auth));
    tracing::info!(env = config.kalshi_env.as_str(), key = kalshi.key_id(), "kalshi client ready");

    let mongo = Mongo::connect(&config.mongo_uri, &config.mongo_db).await?;
    mongo.ensure_indexes(&[Market::spec()]).await?;

    let questdb = Questdb::connect(&config.questdb_ilp_conf, config.questdb_pg_conninfo.clone()).await?;

    let state = AppState {
        config: Arc::new(config),
        mongo,
        questdb,
        kalshi,
        feeds: Default::default(),
        ingest_jobs: Default::default(),
    };

    let markets = Market::all(&state.mongo).await?;
    for market in markets {
        tracing::info!(tag = %market.tag, series = %market.series_ticker, "starting feed");
        state.feeds.spawn(&state, market).await;
    }

    let bind = state.config.bind_addr.clone();
    let listener = tokio::net::TcpListener::bind(&bind).await.with_context(|| format!("binding {bind}"))?;
    tracing::info!(%bind, "listening");
    axum::serve(listener, controllers::router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    Ok(())
}
