//! Market CRUD-ish endpoints.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use mongodb::bson::doc;
use serde::Serialize;
use serde_json::{Value, json};

use crate::db::questdb::{CandleSummary, Table, TableSummary};
use crate::error::AppError;
use crate::feeds::FeedStatus;
use crate::model::Model;
use crate::model::market::{AddMarket, Market};
use crate::state::AppState;

#[derive(Serialize)]
pub struct MarketView {
    #[serde(flatten)]
    pub market: Market,
    pub feed: Option<FeedStatus>,
}

/// `GET /` — every market document with its live feed status.
pub async fn list(State(state): State<AppState>) -> Result<Json<Vec<MarketView>>, AppError> {
    let mut out = Vec::new();
    for market in Market::all(&state.mongo).await? {
        let feed = state.feeds.status(&market.tag).await;
        out.push(MarketView { market, feed });
    }
    Ok(Json(out))
}

/// `POST /add` — validate, store, and start streaming a new market.
pub async fn add(
    State(state): State<AppState>,
    Json(input): Json<AddMarket>,
) -> Result<(StatusCode, Json<MarketView>), AppError> {
    if let Some(env) = input.kalshi_env
        && env != state.config.kalshi_env
    {
        return Err(AppError::BadRequest(format!(
            "this server is configured for kalshi env '{}'",
            state.config.kalshi_env.as_str()
        )));
    }
    let market = Market::from_add(input, &state.config);

    if Market::find(&state.mongo, doc! { "tag": &market.tag }).await?.is_some() {
        return Err(AppError::Conflict(format!("market '{}' already exists", market.tag)));
    }
    market.upsert(&state.mongo).await?;
    tracing::info!(tag = %market.tag, series = %market.series_ticker, index = %market.index_id, "market added");

    state.feeds.spawn(&state, market.clone()).await;
    let feed = state.feeds.status(&market.tag).await;
    Ok((StatusCode::CREATED, Json(MarketView { market, feed })))
}

#[derive(Serialize)]
pub struct MarketDetail {
    pub market: Market,
    pub feed: Option<FeedStatus>,
    pub questdb: QuestdbSummary,
}

#[derive(Serialize)]
pub struct QuestdbSummary {
    pub live: TableSummary,
    pub hist: TableSummary,
    pub contracts: CandleSummary,
}

/// `GET /{tag}` — the document plus what QuestDB holds for its index and contracts.
pub async fn show(
    Path(tag): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<MarketDetail>, AppError> {
    let market = Market::find(&state.mongo, doc! { "tag": &tag })
        .await?
        .ok_or_else(|| AppError::NotFound(format!("market '{tag}'")))?;
    let feed = state.feeds.status(&tag).await;
    let (live, hist, contracts) = tokio::try_join!(
        state.questdb.summary(Table::Live, &market.index_id),
        state.questdb.summary(Table::Hist, &market.index_id),
        state.questdb.candle_summary(&market.series_ticker),
    )?;
    Ok(Json(MarketDetail { market, feed, questdb: QuestdbSummary { live, hist, contracts } }))
}

/// `DELETE /{tag}` — stop the feed and remove the document. QuestDB rows are kept.
pub async fn remove(Path(tag): Path<String>, State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    let stopped = state.feeds.stop(&tag).await;
    let deleted = Market::delete(&state.mongo, doc! { "tag": &tag }).await?;
    if !deleted && !stopped {
        return Err(AppError::NotFound(format!("market '{tag}'")));
    }
    tracing::info!(%tag, deleted, stopped, "market removed");
    Ok(Json(json!({ "tag": tag, "deleted": deleted, "feed_stopped": stopped })))
}
