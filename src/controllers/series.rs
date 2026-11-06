//! One price series of one market, downsampled for charting.
//!
//! `GET /{tag}/series/{table}?start=&end=&step=` returns the last value of
//! each `step`-second bucket in `[start, end)` from QuestDB's `SAMPLE BY`.
//! `table` is a table with a price column (`index-live`, `coinbase-ticker`,
//! `perp-ticker` or their full names). `end` defaults to now and `start` to
//! `DEFAULT_SPAN` before `end`; `step` is whole seconds and defaults to the
//! smallest that keeps the reply within `MAX_POINTS`.
//!
//! ```json
//! { "table": "index_values_live", "key": "BRTI", "step_s": 5,
//!   "start": "…Z", "end": "…Z", "points": [[1790208000000, 111234.56], …] }
//! ```
//!
//! `points` are `[bucket start in epoch ms, value]`, oldest first; empty
//! buckets are left out. A query holds one of the caller's data-stream slots
//! (see `controllers::data`) while it runs.

use axum::Json;
use axum::extract::{Path, Query, State};
use chrono::Utc;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

use super::data::{market_key, parse_range};
use crate::db::questdb::{DataTable, ts_literal_us};
use crate::error::AppError;
use crate::middleware::authenticated::AuthUser;
use crate::model::Model;
use crate::model::market::Market;
use crate::state::AppState;

const SECOND_US: i64 = 1_000_000;
/// `start` when only `end` (or nothing) is given.
const DEFAULT_SPAN: i64 = 15 * 60 * SECOND_US;
/// Longest range one request may cover. The UI shows up to 7 days; the
/// extra day leaves room for clock skew and a view that straddles midnight.
const MAX_SPAN: i64 = 8 * 24 * 3_600 * SECOND_US;
/// Most buckets one request may ask for.
pub const MAX_POINTS: i64 = 5_000;

#[derive(Debug, Default, Deserialize)]
pub struct SeriesQuery {
    #[serde(default)]
    pub start: Option<String>,
    #[serde(default)]
    pub end: Option<String>,
    #[serde(default)]
    pub step: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SeriesResponse {
    pub table: &'static str,
    pub key: String,
    pub step_s: i64,
    pub start: String,
    pub end: String,
    pub points: Vec<(i64, f64)>,
}

/// `GET /{tag}/series/{table}`
pub async fn get(
    Path((tag, table)): Path<(String, String)>,
    Query(q): Query<SeriesQuery>,
    State(state): State<AppState>,
    user: AuthUser,
) -> Result<Json<SeriesResponse>, AppError> {
    let table = DataTable::parse(&table)
        .filter(|t| t.value_column().is_some())
        .ok_or_else(|| {
            let known = DataTable::ALL
                .into_iter()
                .filter(|t| t.value_column().is_some())
                .map(|t| format!("{} ({})", t.name(), t.alias()))
                .collect::<Vec<_>>()
                .join(", ");
            AppError::NotFound(format!("series '{table}'; known series: {known}"))
        })?;
    let (start_us, end_us, step_s) = resolve(&q, Utc::now().timestamp_micros())?;
    let market = Market::find(&state.mongo, doc! { "tag": &tag })
        .await?
        .ok_or_else(|| AppError::NotFound(format!("market '{tag}'")))?;
    let key = market_key(&market, table)?;
    let _slot = state.data_streams.acquire(&user.id)?;
    let points = state.questdb.sample_series(table, &key, start_us, end_us, step_s).await?;
    Ok(Json(SeriesResponse {
        table: table.name(),
        key,
        step_s,
        start: ts_literal_us(start_us),
        end: ts_literal_us(end_us),
        points,
    }))
}

/// `(start_us, end_us, step_s)` from the query, with defaults filled in and
/// the span and bucket count capped. `now_us` is the default `end`.
fn resolve(q: &SeriesQuery, now_us: i64) -> Result<(i64, i64, i64), AppError> {
    let (start, end) = parse_range(q.start.as_deref(), q.end.as_deref())?;
    let end_us = end.unwrap_or(now_us);
    let start_us = start.unwrap_or(end_us - DEFAULT_SPAN);
    if start_us >= end_us {
        return Err(AppError::BadRequest("start must be before end".into()));
    }
    let span = end_us - start_us;
    if span > MAX_SPAN {
        return Err(AppError::BadRequest(format!("range may span at most {} days", MAX_SPAN / (86_400 * SECOND_US))));
    }
    let min_step = (span + MAX_POINTS * SECOND_US - 1) / (MAX_POINTS * SECOND_US);
    let step_s = match q.step {
        None => min_step.max(1),
        Some(s) if s < 1 => return Err(AppError::BadRequest("step must be at least 1 second".into())),
        Some(s) if s < min_step => {
            return Err(AppError::BadRequest(format!(
                "step {s}s gives more than {MAX_POINTS} points over this range; use at least {min_step}s"
            )));
        }
        Some(s) => s,
    };
    Ok((start_us, end_us, step_s))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_790_208_000_000_000;

    fn q(start: Option<&str>, end: Option<&str>, step: Option<i64>) -> SeriesQuery {
        SeriesQuery { start: start.map(Into::into), end: end.map(Into::into), step }
    }

    #[test]
    fn defaults_to_the_last_fifteen_minutes() {
        let (s, e, step) = resolve(&q(None, None, None), NOW).unwrap();
        assert_eq!((e, e - s, step), (NOW, DEFAULT_SPAN, 1));
    }

    #[test]
    fn explicit_range_and_step() {
        let (s, e, step) = resolve(&q(Some("2026-09-24T00:00:00Z"), Some("2026-09-24T01:00:00Z"), Some(10)), NOW).unwrap();
        assert_eq!((s, e, step), (1_790_208_000_000_000, 1_790_211_600_000_000, 10));
    }

    #[test]
    fn caps_span_and_points() {
        // Seven days at 10 minutes is ~1000 points: fine.
        let week = q(Some("2026-09-17T00:00:00Z"), Some("2026-09-24T00:00:00Z"), Some(600));
        assert!(resolve(&week, NOW).is_ok());
        // …but at 1 second it is 604,800.
        let fine = q(Some("2026-09-17T00:00:00Z"), Some("2026-09-24T00:00:00Z"), Some(1));
        assert!(matches!(resolve(&fine, NOW), Err(AppError::BadRequest(m)) if m.contains("121s")));
        // Without a step, the smallest allowed one is picked.
        let auto = q(Some("2026-09-17T00:00:00Z"), Some("2026-09-24T00:00:00Z"), None);
        assert_eq!(resolve(&auto, NOW).unwrap().2, 121);

        let long = q(Some("2026-09-01T00:00:00Z"), Some("2026-09-24T00:00:00Z"), Some(3_600));
        assert!(matches!(resolve(&long, NOW), Err(AppError::BadRequest(m)) if m.contains("8 days")));
        assert!(matches!(resolve(&q(None, None, Some(0)), NOW), Err(AppError::BadRequest(_))));
        // A start after the default end.
        assert!(resolve(&q(Some("2027-01-01T00:00:00Z"), None, None), NOW).is_err());
    }
}
