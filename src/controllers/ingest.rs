//! Historical backfill of CF Benchmarks index values through Kalshi's REST
//! pass-through into `index_values_hist`.
//!
//! Each pass-through call costs 50 rate-limit tokens (roughly 4 requests per
//! second on the basic tier), so a backfill runs as a throttled background
//! job and the endpoint returns 202 with a job id to poll.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::db::questdb::{IndexRow, Table};
use crate::error::AppError;
use crate::kalshi::client::KalshiError;
use crate::model::Model;
use crate::model::market::Market;
use crate::state::AppState;

const REQUEST_GAP: Duration = Duration::from_millis(300);
const RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(5);
const MAX_RATE_LIMIT_RETRIES: u32 = 5;

pub type IngestJobs = Arc<RwLock<HashMap<String, IngestJob>>>;

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub tag: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Upstream granularity/window, e.g. `1s`, `1m`, `1h`, `1d`.
    #[serde(default = "default_timespan")]
    pub timespan: String,
}

fn default_timespan() -> String {
    "1m".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestJob {
    pub job_id: String,
    pub tag: String,
    pub index_id: String,
    pub status: JobStatus,
    pub timespan: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub cursor_ms: i64,
    pub requests: u64,
    pub rows: u64,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct IngestAccepted {
    pub job_id: String,
    pub tag: String,
    pub index_id: String,
    pub cursor_ms: i64,
    pub end_ms: i64,
}

/// Parse `<n><unit>` where unit is one of `ms`, `s`, `m`, `h`, `d`.
pub fn timespan_ms(s: &str) -> Option<i64> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (n, unit) = s.split_at(split);
    let n: i64 = n.parse().ok()?;
    let mult = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    (n > 0).then_some(n * mult)
}

/// `POST /ingest` — start a backfill job for a market's index.
pub async fn start(
    State(state): State<AppState>,
    Json(req): Json<IngestRequest>,
) -> Result<(StatusCode, Json<IngestAccepted>), AppError> {
    let span_ms = timespan_ms(&req.timespan)
        .ok_or_else(|| AppError::BadRequest(format!("invalid timespan '{}'", req.timespan)))?;
    if req.end <= req.start {
        return Err(AppError::BadRequest("end must be after start".into()));
    }
    let market = Market::find(&state.mongo, doc! { "tag": &req.tag })
        .await?
        .ok_or_else(|| AppError::NotFound(format!("market '{}'", req.tag)))?;

    let start_ms = req.start.timestamp_millis();
    let cursor_ms = start_ms - start_ms.rem_euclid(span_ms);
    let job = IngestJob {
        job_id: uuid::Uuid::new_v4().to_string(),
        tag: market.tag.clone(),
        index_id: market.index_id.clone(),
        status: JobStatus::Running,
        timespan: req.timespan.clone(),
        start_ms,
        end_ms: req.end.timestamp_millis(),
        cursor_ms,
        requests: 0,
        rows: 0,
        error: None,
        started_at: Utc::now(),
        finished_at: None,
    };

    {
        let mut jobs = state.ingest_jobs.write().await;
        if let Some(running) = jobs.values().find(|j| j.tag == job.tag && j.status == JobStatus::Running) {
            return Err(AppError::Conflict(format!(
                "ingest job {} is already running for '{}'",
                running.job_id, job.tag
            )));
        }
        jobs.insert(job.job_id.clone(), job.clone());
    }

    let accepted = IngestAccepted {
        job_id: job.job_id.clone(),
        tag: job.tag.clone(),
        index_id: job.index_id.clone(),
        cursor_ms,
        end_ms: job.end_ms,
    };
    tokio::spawn(run_job(state.clone(), job, span_ms));
    Ok((StatusCode::ACCEPTED, Json(accepted)))
}

/// `GET /ingest/{job_id}`
pub async fn status(
    Path(job_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<IngestJob>, AppError> {
    state
        .ingest_jobs
        .read()
        .await
        .get(&job_id)
        .cloned()
        .map(Json)
        .ok_or_else(|| AppError::NotFound(format!("ingest job '{job_id}'")))
}

async fn run_job(state: AppState, mut job: IngestJob, span_ms: i64) {
    let job_id = job.job_id.clone();
    let outcome = ingest_loop(&state, &mut job, span_ms).await;
    job.finished_at = Some(Utc::now());
    match outcome {
        Ok(()) => {
            job.status = JobStatus::Done;
            tracing::info!(%job_id, tag = %job.tag, requests = job.requests, rows = job.rows, "ingest done");
        }
        Err(e) => {
            job.status = JobStatus::Failed;
            job.error = Some(format!("{e:#}"));
            tracing::error!(%job_id, tag = %job.tag, error = format!("{e:#}"), "ingest failed");
        }
    }
    state.ingest_jobs.write().await.insert(job_id, job);
}

async fn ingest_loop(state: &AppState, job: &mut IngestJob, span_ms: i64) -> anyhow::Result<()> {
    let mut rate_limit_hits = 0u32;
    let mut logged_sample = false;

    while job.cursor_ms < job.end_ms {
        let result = state.kalshi.cf_history_values(&job.index_id, job.cursor_ms, &job.timespan).await;
        let (values, raw) = match result {
            Ok(ok) => ok,
            Err(KalshiError::RateLimited) => {
                rate_limit_hits += 1;
                anyhow::ensure!(
                    rate_limit_hits <= MAX_RATE_LIMIT_RETRIES,
                    "rate limited {rate_limit_hits} times in a row at cursor {}",
                    job.cursor_ms
                );
                tracing::warn!(job_id = %job.job_id, cursor = job.cursor_ms, "rate limited; backing off");
                tokio::time::sleep(RATE_LIMIT_BACKOFF).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        rate_limit_hits = 0;
        job.requests += 1;

        if !logged_sample {
            logged_sample = true;
            let sample = raw.to_string();
            let sample = &sample[..sample.len().min(600)];
            tracing::info!(job_id = %job.job_id, parsed = values.len(), %sample, "first pass-through payload");
        }

        let mut last_time = None;
        for v in values.iter().filter(|v| v.time_ms >= job.start_ms && v.time_ms < job.end_ms) {
            let row = IndexRow {
                table: Table::Hist,
                index_id: job.index_id.clone(),
                value: v.value,
                ts_ms: v.time_ms,
                received_at_ms: None,
                source: "rest_history",
            };
            state.questdb.ilp.send(row).await.map_err(|_| anyhow::anyhow!("ilp writer gone"))?;
            job.rows += 1;
            last_time = Some(v.time_ms);
        }

        // Advance at least one window; further if the upstream returned more.
        job.cursor_ms = (job.cursor_ms + span_ms).max(last_time.map(|t| t + 1).unwrap_or(0));
        state.ingest_jobs.write().await.insert(job.job_id.clone(), job.clone());

        tokio::time::sleep(REQUEST_GAP).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::timespan_ms;

    #[test]
    fn parses_timespans() {
        assert_eq!(timespan_ms("200ms"), Some(200));
        assert_eq!(timespan_ms("1s"), Some(1_000));
        assert_eq!(timespan_ms("15m"), Some(900_000));
        assert_eq!(timespan_ms("1h"), Some(3_600_000));
        assert_eq!(timespan_ms("1d"), Some(86_400_000));
        assert_eq!(timespan_ms("0m"), None);
        assert_eq!(timespan_ms("1w"), None);
        assert_eq!(timespan_ms("m"), None);
    }
}
