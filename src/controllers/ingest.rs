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
use chrono::{DateTime, Datelike, NaiveDate, Utc};
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

/// Every request uses the smallest window the pass-through allows, which is
/// also where CF Benchmarks returns its finest resolution.
const INGEST_TIMESPAN: Timespan = Timespan::Hour;

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub tag: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// One CF Benchmarks history window (`HOUR`, `DAY`, `MONTH` or `YEAR`, the
/// only values the pass-through accepts). The upstream API requires
/// `timestamp` to be the start of a period, so cursors are floored to calendar
/// boundaries in UTC and advanced one period at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // only HOUR is requested today; the others stay tested and available
pub enum Timespan {
    Hour,
    Day,
    Month,
    Year,
}

impl Timespan {
    #[cfg(test)]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "HOUR" => Some(Self::Hour),
            "DAY" => Some(Self::Day),
            "MONTH" => Some(Self::Month),
            "YEAR" => Some(Self::Year),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hour => "HOUR",
            Self::Day => "DAY",
            Self::Month => "MONTH",
            Self::Year => "YEAR",
        }
    }

    /// Start of the period containing `ms`.
    pub fn floor(self, ms: i64) -> i64 {
        match self {
            Self::Hour => ms - ms.rem_euclid(3_600_000),
            Self::Day => ms - ms.rem_euclid(86_400_000),
            Self::Month => {
                let d = DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default().date_naive();
                ymd_ms(d.year(), d.month())
            }
            Self::Year => {
                let d = DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default().date_naive();
                ymd_ms(d.year(), 1)
            }
        }
    }

    /// Start of the period after the one starting at `period_start_ms`.
    pub fn next(self, period_start_ms: i64) -> i64 {
        match self {
            Self::Hour => period_start_ms + 3_600_000,
            Self::Day => period_start_ms + 86_400_000,
            Self::Month => {
                let d = DateTime::<Utc>::from_timestamp_millis(period_start_ms).unwrap_or_default().date_naive();
                if d.month() == 12 { ymd_ms(d.year() + 1, 1) } else { ymd_ms(d.year(), d.month() + 1) }
            }
            Self::Year => {
                let d = DateTime::<Utc>::from_timestamp_millis(period_start_ms).unwrap_or_default().date_naive();
                ymd_ms(d.year() + 1, 1)
            }
        }
    }
}

/// Midnight UTC on the first of the month, in ms.
fn ymd_ms(year: i32, month: u32) -> i64 {
    NaiveDate::from_ymd_opt(year, month, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp_millis())
        .unwrap_or(0)
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

/// `POST /ingest` — start a backfill job for a market's index.
pub async fn start(
    State(state): State<AppState>,
    Json(req): Json<IngestRequest>,
) -> Result<(StatusCode, Json<IngestAccepted>), AppError> {
    let timespan = INGEST_TIMESPAN;
    if req.end <= req.start {
        return Err(AppError::BadRequest("end must be after start".into()));
    }
    let market = Market::find(&state.mongo, doc! { "tag": &req.tag })
        .await?
        .ok_or_else(|| AppError::NotFound(format!("market '{}'", req.tag)))?;

    let start_ms = req.start.timestamp_millis();
    let cursor_ms = timespan.floor(start_ms);
    let job = IngestJob {
        job_id: uuid::Uuid::new_v4().to_string(),
        tag: market.tag.clone(),
        index_id: market.index_id.clone(),
        status: JobStatus::Running,
        timespan: timespan.as_str().to_string(),
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
    tokio::spawn(run_job(state.clone(), job, timespan));
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

async fn run_job(state: AppState, mut job: IngestJob, timespan: Timespan) {
    let job_id = job.job_id.clone();
    let outcome = ingest_loop(&state, &mut job, timespan).await;
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

async fn ingest_loop(state: &AppState, job: &mut IngestJob, timespan: Timespan) -> anyhow::Result<()> {
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
        }

        // Always step to the next aligned period; the upstream rejects
        // unaligned timestamps and QuestDB dedups on (ts, index_id).
        job.cursor_ms = timespan.next(job.cursor_ms);
        state.ingest_jobs.write().await.insert(job.job_id.clone(), job.clone());

        tokio::time::sleep(REQUEST_GAP).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Timespan;

    #[test]
    fn parses_timespans() {
        assert_eq!(Timespan::parse("HOUR"), Some(Timespan::Hour));
        assert_eq!(Timespan::parse(" day "), Some(Timespan::Day));
        assert_eq!(Timespan::parse("Month"), Some(Timespan::Month));
        assert_eq!(Timespan::parse("year"), Some(Timespan::Year));
        assert_eq!(Timespan::parse("1m"), None);
        assert_eq!(Timespan::parse("WEEK"), None);
        assert_eq!(Timespan::parse(""), None);
    }

    #[test]
    fn aligns_and_advances_periods() {
        // 2026-09-06T23:36:46.855Z
        let ms = 1_788_737_806_855;
        assert_eq!(Timespan::Hour.floor(ms), 1_788_735_600_000); // 23:00Z
        assert_eq!(Timespan::Hour.next(Timespan::Hour.floor(ms)), 1_788_739_200_000);
        assert_eq!(Timespan::Day.floor(ms), 1_788_652_800_000); // 2026-09-06T00:00Z
        assert_eq!(Timespan::Month.floor(ms), 1_788_220_800_000); // 2026-09-01
        assert_eq!(Timespan::Month.next(Timespan::Month.floor(ms)), 1_790_812_800_000); // 2026-10-01
        assert_eq!(Timespan::Year.floor(ms), 1_767_225_600_000); // 2026-01-01
        assert_eq!(Timespan::Year.next(Timespan::Year.floor(ms)), 1_798_761_600_000); // 2027-01-01
        // December rolls the year.
        let dec = Timespan::Month.floor(1_798_000_000_000); // 2026-12-...
        assert_eq!(Timespan::Month.next(dec), 1_798_761_600_000);
    }
}
