//! Historical backfill for a market, of either `kind`:
//!
//! - `index`: the CF Benchmarks index values the series settles on, through
//!   Kalshi's REST pass-through into `index_values_hist`.
//! - `contracts`: the prices the contracts themselves traded at, as 1-minute
//!   candlesticks (yes bid/ask and trade OHLC, volume, open interest) of
//!   every market of the series that was open during the range, into
//!   `contract_candles_hist`.
//!
//! Before asking Kalshi for anything a job checks what QuestDB already holds
//! and skips the windows / markets that are complete there, so
//! resubmitting a failed or overlapping request only fetches what is missing
//! (`force` turns this off). Transient upstream failures are retried with
//! backoff for up to `RETRY_WINDOW` before the job fails.
//!
//! Each pass-through call costs 50 rate-limit tokens (roughly 4 requests per
//! second on the basic tier) and a contracts backfill makes one request per
//! market, so a backfill runs as a throttled background job and the endpoint
//! returns 202 with a job id to poll.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::db::questdb::{CandleRow, IndexRow, Table};
use crate::error::AppError;
use crate::kalshi::client::{KalshiError, SeriesMarket, Tier};
use crate::model::Model;
use crate::model::market::Market;
use crate::state::AppState;

const REQUEST_GAP: Duration = Duration::from_millis(300);
/// Market and candlestick calls cost the default 10 tokens, not 50.
const CONTRACT_REQUEST_GAP: Duration = Duration::from_millis(100);
/// Finest candlestick period Kalshi offers (the others are 60 and 1440).
const CANDLE_PERIOD_MINUTES: u32 = 1;
const RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(5);
/// Backoff after a network failure or upstream 5xx, doubling up to the max.
const RETRY_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// How long one upstream call may keep failing before the job gives up.
const RETRY_WINDOW: Duration = Duration::from_secs(5 * 60);
/// CF Benchmarks publishes its real-time indices once per second.
const INDEX_ROWS_PER_SECOND: i64 = 1;
/// Share of the expected rows a window needs to count as already ingested.
/// Full hours upstream are never below 96%; a dropped write loses far more.
const INDEX_COMPLETE_PERCENT: i64 = 90;

pub type IngestJobs = Arc<RwLock<HashMap<String, IngestJob>>>;

/// Every request uses the smallest window the pass-through allows, which is
/// also where CF Benchmarks returns its finest resolution.
const INGEST_TIMESPAN: Timespan = Timespan::Hour;

/// What a job backfills; see the module docs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IngestKind {
    #[default]
    Index,
    Contracts,
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub tag: String,
    #[serde(default)]
    pub kind: IngestKind,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Fetch everything again, even what QuestDB already holds.
    #[serde(default)]
    pub force: bool,
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

    /// The same period as a QuestDB `SAMPLE BY` interval.
    pub fn sample_by(self) -> &'static str {
        match self {
            Self::Hour => "1h",
            Self::Day => "1d",
            Self::Month => "1M",
            Self::Year => "1y",
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
    pub kind: IngestKind,
    pub index_id: String,
    pub series_ticker: String,
    pub status: JobStatus,
    /// Window of one upstream request: a CF Benchmarks timespan for `index`,
    /// the candlestick period for `contracts`.
    pub timespan: String,
    pub start_ms: i64,
    pub end_ms: i64,
    /// Everything before this has been ingested. A `contracts` job first
    /// lists the markets in range, so it stays at the start until then.
    pub cursor_ms: i64,
    /// `contracts` only: markets found in range, and how many are done.
    pub markets_total: u64,
    pub markets_done: u64,
    pub force: bool,
    pub requests: u64,
    /// Windows (`index`) or markets (`contracts`) not requested because
    /// QuestDB already held them.
    pub skipped: u64,
    pub retries: u64,
    /// Why the call in flight is being retried; cleared once it succeeds.
    pub retry_error: Option<String>,
    pub rows: u64,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct IngestAccepted {
    pub job_id: String,
    pub tag: String,
    pub kind: IngestKind,
    pub index_id: String,
    pub series_ticker: String,
    pub cursor_ms: i64,
    pub end_ms: i64,
}

/// `POST /ingest` — start a backfill job for a market's index or contracts.
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
    let (cursor_ms, timespan_label) = match req.kind {
        IngestKind::Index => (timespan.floor(start_ms), timespan.as_str().to_string()),
        IngestKind::Contracts => (start_ms, format!("{CANDLE_PERIOD_MINUTES}m")),
    };
    let job = IngestJob {
        job_id: uuid::Uuid::new_v4().to_string(),
        tag: market.tag.clone(),
        kind: req.kind,
        index_id: market.index_id.clone(),
        series_ticker: market.series_ticker.clone(),
        status: JobStatus::Running,
        timespan: timespan_label,
        start_ms,
        end_ms: req.end.timestamp_millis(),
        cursor_ms,
        markets_total: 0,
        markets_done: 0,
        force: req.force,
        skipped: 0,
        retries: 0,
        retry_error: None,
        requests: 0,
        rows: 0,
        error: None,
        started_at: Utc::now(),
        finished_at: None,
    };

    {
        let mut jobs = state.ingest_jobs.write().await;
        let clash = |j: &&IngestJob| j.tag == job.tag && j.kind == job.kind && j.status == JobStatus::Running;
        if let Some(running) = jobs.values().find(clash) {
            return Err(AppError::Conflict(format!(
                "{:?} ingest job {} is already running for '{}'",
                job.kind, running.job_id, job.tag
            )));
        }
        jobs.insert(job.job_id.clone(), job.clone());
    }

    let accepted = IngestAccepted {
        job_id: job.job_id.clone(),
        tag: job.tag.clone(),
        kind: job.kind,
        index_id: job.index_id.clone(),
        series_ticker: job.series_ticker.clone(),
        cursor_ms,
        end_ms: job.end_ms,
    };
    tokio::spawn(run_job(state.clone(), job, timespan));
    Ok((StatusCode::ACCEPTED, Json(accepted)))
}

/// `GET /ingest/{job_id}`
pub async fn status(Path(job_id): Path<String>, State(state): State<AppState>) -> Result<Json<IngestJob>, AppError> {
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
    let outcome = match job.kind {
        IngestKind::Index => ingest_loop(&state, &mut job, timespan).await,
        IngestKind::Contracts => contracts_loop(&state, &mut job).await,
    };
    job.finished_at = Some(Utc::now());
    match outcome {
        Ok(()) => {
            job.status = JobStatus::Done;
            tracing::info!(
                %job_id,
                tag = %job.tag,
                kind = ?job.kind,
                requests = job.requests,
                skipped = job.skipped,
                retries = job.retries,
                rows = job.rows,
                "ingest done"
            );
        }
        Err(e) => {
            job.status = JobStatus::Failed;
            job.error = Some(format!("{e:#}"));
            tracing::error!(%job_id, tag = %job.tag, kind = ?job.kind, error = format!("{e:#}"), "ingest failed");
        }
    }
    state.ingest_jobs.write().await.insert(job_id, job);
}

async fn publish(state: &AppState, job: &IngestJob) {
    state.ingest_jobs.write().await.insert(job.job_id.clone(), job.clone());
}

/// What `throttled` needs to know about a failed upstream call.
trait UpstreamError: std::error::Error + Send + Sync + 'static {
    fn is_transient(&self) -> bool;
    fn is_rate_limited(&self) -> bool;
}

impl UpstreamError for KalshiError {
    fn is_transient(&self) -> bool {
        KalshiError::is_transient(self)
    }
    fn is_rate_limited(&self) -> bool {
        matches!(self, KalshiError::RateLimited)
    }
}

/// Runs one upstream call for `job` and counts it once it gets through.
/// Transient failures (see `UpstreamError::is_transient`) are retried with
/// backoff until the call has been failing for `RETRY_WINDOW`.
async fn throttled<T, E, F>(state: &AppState, job: &mut IngestJob, mut call: impl FnMut() -> F) -> anyhow::Result<T>
where
    E: UpstreamError,
    F: Future<Output = Result<T, E>>,
{
    let first_attempt = Instant::now();
    let mut backoff = RETRY_BACKOFF_MIN;
    let mut attempts = 0u32;
    loop {
        let error = match call().await {
            Err(e) if e.is_transient() => e,
            result => {
                job.requests += 1;
                job.retry_error = None;
                return Ok(result?);
            }
        };
        attempts += 1;
        let delay = if error.is_rate_limited() { RATE_LIMIT_BACKOFF } else { backoff };
        if first_attempt.elapsed() + delay > RETRY_WINDOW {
            return Err(anyhow::Error::new(error).context(format!(
                "gave up after {attempts} attempts over {}s at cursor {}",
                first_attempt.elapsed().as_secs(),
                job.cursor_ms
            )));
        }
        tracing::warn!(job_id = %job.job_id, cursor = job.cursor_ms, attempts, ?delay, %error, "upstream call failed; retrying");
        job.retries += 1;
        job.retry_error = Some(error.to_string());
        publish(state, job).await;
        tokio::time::sleep(delay).await;
        backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
    }
}

/// Whether QuestDB's `rows` cover the part of the window starting at
/// `window_ms` that lies inside `[start_ms, end_ms)`. A window still in
/// progress at `now_ms` never does.
fn index_window_complete(rows: i64, window_ms: i64, next_ms: i64, start_ms: i64, end_ms: i64, now_ms: i64) -> bool {
    let seconds = (next_ms.min(end_ms) - window_ms.max(start_ms)) / 1_000;
    next_ms <= now_ms && seconds > 0 && rows * 100 >= seconds * INDEX_ROWS_PER_SECOND * INDEX_COMPLETE_PERCENT
}

async fn ingest_loop(state: &AppState, job: &mut IngestJob, timespan: Timespan) -> anyhow::Result<()> {
    let mut logged_sample = false;
    let existing = if job.force {
        HashMap::new()
    } else {
        let counts = state.questdb.index_counts(&job.index_id, job.start_ms, job.end_ms, timespan.sample_by()).await;
        counts.unwrap_or_else(|e| {
            tracing::warn!(job_id = %job.job_id, error = %e, "cannot read existing rows; fetching everything");
            HashMap::new()
        })
    };

    while job.cursor_ms < job.end_ms {
        let next_ms = timespan.next(job.cursor_ms);
        let rows = existing.get(&job.cursor_ms).copied().unwrap_or(0);
        if index_window_complete(rows, job.cursor_ms, next_ms, job.start_ms, job.end_ms, Utc::now().timestamp_millis())
        {
            job.skipped += 1;
            job.cursor_ms = next_ms;
            publish(state, job).await;
            continue;
        }

        let (index_id, cursor_ms, span) = (job.index_id.clone(), job.cursor_ms, job.timespan.clone());
        let (values, raw) =
            throttled(state, job, || state.kalshi.cf_history_values(&index_id, cursor_ms, &span)).await?;

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
            state.questdb.ilp.send(row.into()).await.map_err(|_| anyhow::anyhow!("ilp writer gone"))?;
            job.rows += 1;
        }

        // Always step to the next aligned period; the upstream rejects
        // unaligned timestamps and QuestDB dedups on (ts, index_id).
        job.cursor_ms = next_ms;
        publish(state, job).await;

        tokio::time::sleep(REQUEST_GAP).await;
    }
    Ok(())
}

/// Whether a market was open at any point of `[start_ms, end_ms)`.
fn overlaps(market: &SeriesMarket, start_ms: i64, end_ms: i64) -> bool {
    market.close_ms > start_ms && market.open_ms < end_ms
}

/// Whether QuestDB's `candles` cover a market over `[from_ms, to_ms]`: Kalshi
/// reports one candle per period, trades or not. A market still trading at
/// `now_ms` never does.
fn candles_complete(candles: i64, from_ms: i64, to_ms: i64, now_ms: i64) -> bool {
    let expected = (to_ms - from_ms) / (CANDLE_PERIOD_MINUTES as i64 * 60_000);
    to_ms <= now_ms && expected > 0 && candles >= expected
}

/// Every market of the job's series that was open during its range, oldest
/// first. Markets settled before Kalshi's cutoff are only listed by the
/// historical endpoint and the rest only by the live one, so a range that
/// straddles the cutoff reads both.
async fn markets_in_range(state: &AppState, job: &mut IngestJob) -> anyhow::Result<Vec<SeriesMarket>> {
    let series = job.series_ticker.clone();
    let (start_ms, end_ms) = (job.start_ms, job.end_ms);
    let cutoff_ms = throttled(state, job, || state.kalshi.historical_cutoff()).await?.timestamp_millis();

    let mut tiers = Vec::new();
    if start_ms < cutoff_ms {
        tiers.push(Tier::Historical);
    }
    if end_ms > cutoff_ms {
        tiers.push(Tier::Live);
    }

    let mut markets: Vec<SeriesMarket> = Vec::new();
    for tier in tiers {
        let mut cursor: Option<String> = None;
        loop {
            let (page, next) =
                throttled(state, job, || state.kalshi.series_markets_page(&series, tier, start_ms, cursor.as_deref()))
                    .await?;
            // Pages run newest close first and the historical listing cannot
            // be filtered by time, so stop once a page reaches past the start.
            let past_start = page.last().is_none_or(|m| m.close_ms <= start_ms);
            markets.extend(page.into_iter().filter(|m| overlaps(m, start_ms, end_ms)));
            cursor = next;
            if cursor.is_none() || past_start {
                break;
            }
            tokio::time::sleep(CONTRACT_REQUEST_GAP).await;
        }
    }
    markets.sort_by(|a, b| (a.close_ms, &a.ticker).cmp(&(b.close_ms, &b.ticker)));
    markets.dedup_by(|a, b| a.ticker == b.ticker);
    Ok(markets)
}

async fn contracts_loop(state: &AppState, job: &mut IngestJob) -> anyhow::Result<()> {
    let markets = markets_in_range(state, job).await?;
    job.markets_total = markets.len() as u64;
    tracing::info!(job_id = %job.job_id, series = %job.series_ticker, markets = markets.len(), "markets in range");
    publish(state, job).await;

    let existing = if job.force {
        HashMap::new()
    } else {
        state.questdb.candle_counts(&job.series_ticker, job.start_ms, job.end_ms).await.unwrap_or_else(|e| {
            tracing::warn!(job_id = %job.job_id, error = %e, "cannot read existing candles; fetching everything");
            HashMap::new()
        })
    };

    let series = job.series_ticker.clone();
    for market in markets {
        let from_ms = market.open_ms.max(job.start_ms);
        let to_ms = market.close_ms.min(job.end_ms);
        let candles = existing.get(&market.ticker).copied().unwrap_or(0);
        if candles_complete(candles, from_ms, to_ms, Utc::now().timestamp_millis()) {
            job.skipped += 1;
            job.markets_done += 1;
            job.cursor_ms = job.cursor_ms.max(to_ms);
            publish(state, job).await;
            continue;
        }
        let fetch = |tier: Tier| {
            state.kalshi.market_candlesticks(&series, &market.ticker, tier, from_ms, to_ms, CANDLE_PERIOD_MINUTES)
        };
        let candles = match throttled(state, job, || fetch(market.tier)).await {
            Ok(candles) => candles,
            // Kalshi archives markets some time after the cutoff moves, so one
            // that settled near it can still sit in the tier that did not list it.
            Err(e) if matches!(e.downcast_ref(), Some(KalshiError::Status { status: 404, .. })) => {
                throttled(state, job, || fetch(market.tier.other())).await?
            }
            Err(e) => return Err(e),
        };

        for candle in candles {
            let row = CandleRow {
                series_ticker: series.clone(),
                ticker: market.ticker.clone(),
                floor_strike: market.floor_strike,
                yes_bid: candle.yes_bid,
                yes_ask: candle.yes_ask,
                price: candle.price,
                price_mean: candle.price_mean,
                volume: candle.volume,
                open_interest: candle.open_interest,
                ts_ms: candle.end_ms,
                source: "rest_candlesticks",
            };
            state.questdb.ilp.send(row.into()).await.map_err(|_| anyhow::anyhow!("ilp writer gone"))?;
            job.rows += 1;
        }

        job.markets_done += 1;
        job.cursor_ms = job.cursor_ms.max(to_ms);
        publish(state, job).await;

        tokio::time::sleep(CONTRACT_REQUEST_GAP).await;
    }
    job.cursor_ms = job.end_ms;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{IngestKind, IngestRequest, Timespan, candles_complete, index_window_complete, overlaps};
    use crate::kalshi::client::{SeriesMarket, Tier};

    #[test]
    fn kind_defaults_to_index() {
        let body = r#"{"tag":"btc-15m","start":"2026-09-01T00:00:00Z","end":"2026-09-02T00:00:00Z"}"#;
        let req: IngestRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.kind, IngestKind::Index);
        let body = body.replace("{", r#"{"kind":"contracts","#);
        let req: IngestRequest = serde_json::from_str(&body).unwrap();
        assert_eq!(req.kind, IngestKind::Contracts);
        let body = body.replace("contracts", "coinbase");
        assert!(serde_json::from_str::<IngestRequest>(&body).is_err(), "coinbase candles are no longer a backfill");
    }

    #[test]
    fn complete_index_windows_are_skipped() {
        const HOUR: i64 = 3_600_000;
        let now = 100 * HOUR;
        // Upstream gaps leave full hours a little short of 3600 rows.
        assert!(index_window_complete(3_600, 0, HOUR, 0, 10 * HOUR, now));
        assert!(index_window_complete(3_459, 0, HOUR, 0, 10 * HOUR, now));
        assert!(!index_window_complete(1_800, 0, HOUR, 0, 10 * HOUR, now));
        assert!(!index_window_complete(0, 0, HOUR, 0, 10 * HOUR, now));
        // Only the part of the window inside the range is expected.
        assert!(index_window_complete(372, 0, HOUR, HOUR - 372_000, 10 * HOUR, now));
        assert!(index_window_complete(600, HOUR, 2 * HOUR, 0, HOUR + 600_000, now));
        // The hour in progress keeps growing.
        assert!(!index_window_complete(3_600, 0, HOUR, 0, 10 * HOUR, HOUR - 1));
    }

    #[test]
    fn complete_markets_are_skipped() {
        const MINUTE: i64 = 60_000;
        let now = 1_000 * MINUTE;
        assert!(candles_complete(15, 0, 15 * MINUTE, now));
        assert!(!candles_complete(14, 0, 15 * MINUTE, now));
        assert!(!candles_complete(0, 0, 15 * MINUTE, now));
        // Clipped by the range: only the minutes inside it are expected.
        assert!(candles_complete(5, 10 * MINUTE, 15 * MINUTE, now));
        // Still trading.
        assert!(!candles_complete(15, 0, 15 * MINUTE, 15 * MINUTE - 1));
    }

    #[test]
    fn market_overlap_is_half_open() {
        let market = |open_ms, close_ms| SeriesMarket {
            ticker: "T".into(),
            open_ms,
            close_ms,
            floor_strike: None,
            tier: Tier::Live,
        };
        assert!(overlaps(&market(0, 900), 100, 200)); // spans the range
        assert!(overlaps(&market(150, 900), 100, 200)); // opens inside it
        assert!(!overlaps(&market(0, 100), 100, 200)); // closed as it starts
        assert!(!overlaps(&market(200, 900), 100, 200)); // opens as it ends
    }

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
