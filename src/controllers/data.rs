//! Bulk export of one QuestDB table for one market as Server-Sent Events.
//!
//! `GET /{tag}/data/{table}?start=&end=` streams every row the table holds
//! for the market's key (`index_id`, `series_ticker` or `coinbase_product`,
//! depending on the table) with `ts` in `[start, end)`, oldest first. `table`
//! is a QuestDB table name or its alias (`index-live`, `index-hist`,
//! `candles`, `ticker`, `book`, `coinbase-ticker`, `coinbase-book`); `start`
//! and `end` are RFC3339 and default to the table's first and last row.
//!
//! Events, each with a JSON body:
//!
//! - `meta`: table, key, effective range and `columns`, the column names;
//!   sent first.
//! - `rows`: a JSON array of rows, each a JSON array of values in `columns`
//!   order (text and timestamps as strings, numbers as numbers, NULL as
//!   null). `id` = the `ts` of the batch's last row. Batches hold up to
//!   `BATCH_ROWS` rows or about `BATCH_BYTES` of JSON, whichever fills
//!   first: one SSE event is one HTTP chunk and one write, so per-row events
//!   cap throughput at a few thousand rows a second.
//! - `done`: row count and elapsed time; the stream ends after it. Clients
//!   must `close()` here, otherwise `EventSource` reconnects (cheaply, with
//!   `Last-Event-ID`, but forever).
//! - `error`: QuestDB failed mid-stream; carries the rows sent so far and
//!   `resume_from`, the last `ts` delivered. Rows read before the failure
//!   are flushed in a final `rows` event first. The stream ends after it.
//!
//! A `Last-Event-ID` header (which `EventSource` sends on reconnect) replaces
//! `start`. Resuming is at-least-once: every row sharing that microsecond is
//! sent again, so clients dedupe on their side or pass an explicit `start`.
//!
//! The export is not a consistent snapshot: `end` defaults to the last row
//! at request time, and a late out-of-order write into a window already
//! streamed is missed. The book tables run to tens of millions of rows, so
//! at most `DATA_STREAMS` exports run at once (429 beyond that) and each is
//! pulled from QuestDB one bounded window at a time; see
//! `Questdb::stream_data`.

use std::convert::Infallible;
use std::pin::Pin;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, KeepAliveStream, Sse};
use chrono::DateTime;
use futures_util::{Stream, StreamExt, stream};
use mongodb::bson::doc;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::OwnedSemaphorePermit;

use crate::db::questdb::{DataRow, DataStream, DataTable, KeySource};
use crate::error::AppError;
use crate::middleware::authenticated::AuthUser;
use crate::model::Model;
use crate::model::market::Market;
use crate::state::AppState;

/// Concurrent exports allowed process-wide. Each pins a PGWire connection and
/// a stream of scans on a two-worker, CPU-bound QuestDB.
pub const DATA_STREAMS: usize = 2;

/// Reconnect delay asked of `EventSource`, so a client that ignores `done`
/// does not hammer the endpoint.
const RETRY: Duration = Duration::from_secs(10);

/// Rows per `rows` event, and the JSON size at which a batch is flushed
/// early. The row cap bounds what a client parses per event; the byte cap
/// keeps wide rows (candles are 20 columns) from making events of several
/// hundred KB.
pub const BATCH_ROWS: usize = 1_000;
pub const BATCH_BYTES: usize = 64 * 1024;

/// When a `rows` batch is flushed: at `rows` rows, or once its JSON reaches
/// `bytes`. Tests use small values; `stream` uses `BATCH_ROWS`/`BATCH_BYTES`.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub rows: usize,
    pub bytes: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        BatchLimits { rows: BATCH_ROWS, bytes: BATCH_BYTES }
    }
}

pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>>;
pub type SseResponse = Sse<KeepAliveStream<EventStream>>;

#[derive(Debug, Default, Deserialize)]
pub struct RangeQuery {
    #[serde(default)]
    pub start: Option<String>,
    #[serde(default)]
    pub end: Option<String>,
}

/// `GET /{tag}/data/{table}`
pub async fn stream(
    Path((tag, table)): Path<(String, String)>,
    Query(range): Query<RangeQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
    user: AuthUser,
) -> Result<SseResponse, AppError> {
    let market = Market::find(&state.mongo, doc! { "tag": &tag })
        .await?
        .ok_or_else(|| AppError::NotFound(format!("market '{tag}'")))?;
    let table = DataTable::parse(&table).ok_or_else(|| {
        let known = DataTable::ALL.map(|t| format!("{} ({})", t.name(), t.alias())).join(", ");
        AppError::NotFound(format!("table '{table}'; known tables: {known}"))
    })?;
    let key = match table.key_source() {
        KeySource::IndexId => market.index_id.clone(),
        KeySource::SeriesTicker => market.series_ticker.clone(),
        KeySource::CoinbaseProduct => market
            .coinbase_product
            .clone()
            .ok_or_else(|| AppError::BadRequest(format!("market '{tag}' has no coinbase_product")))?,
    };
    let (mut start, end) = parse_range(range.start.as_deref(), range.end.as_deref())?;
    if let Some(id) = headers.get("last-event-id").and_then(|v| v.to_str().ok()).and_then(parse_ts) {
        start = Some(id);
    }
    let permit = state
        .data_streams
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::TooManyRequests(format!("at most {DATA_STREAMS} data streams may run at once")))?;

    // Clip the request to what the table holds so an open-ended range does
    // not walk empty windows back to 1970 or forward past the last row.
    let range = match state.questdb.data_bounds(table).await? {
        Some((min_us, max_us)) => {
            let start_us = start.map_or(min_us, |s| s.max(min_us));
            let end_us = end.map_or(max_us + 1, |e| e.min(max_us + 1));
            (start_us < end_us).then_some((start_us, end_us))
        }
        None => None,
    };
    let Some((start_us, end_us)) = range else {
        let meta = meta_event(table, &key, None);
        let done = done_event(0, None, Duration::ZERO);
        return Ok(sse(Box::pin(stream::iter([Ok(meta), Ok(done)]))));
    };
    tracing::info!(%tag, table = table.name(), user = %user.id, start_us, end_us, "data stream opened");
    let rows = state.questdb.stream_data(table, key.clone(), start_us, end_us).await?;
    Ok(sse(Box::pin(events(table, key, (start_us, end_us), rows, permit, BatchLimits::default()))))
}

fn sse(events: EventStream) -> SseResponse {
    Sse::new(events).keep_alive(KeepAlive::default())
}

/// `meta`, the rows in `rows` batches, then `done` or `error`. Holds the
/// concurrency permit for as long as the client keeps reading.
fn events(
    table: DataTable,
    key: String,
    range: (i64, i64),
    rows: DataStream,
    permit: OwnedSemaphorePermit,
    limits: BatchLimits,
) -> impl Stream<Item = Result<Event, Infallible>> + Send {
    async_stream::stream! {
        let _permit = permit;
        let started = Instant::now();
        yield Ok(meta_event(table, &key, Some(range)));
        let mut rows = std::pin::pin!(rows);
        let mut batch = Batch::new(limits);
        // Rows delivered in flushed batches, and the `ts` of the last one:
        // what `error` reports, so a client resuming from it misses nothing.
        let mut sent: u64 = 0;
        let mut last_ts = None;
        while let Some(row) = rows.next().await {
            match row {
                Ok(row) => {
                    if let Some(event) = batch.push(row) {
                        sent += event.rows;
                        last_ts = Some(event.last_ts.clone());
                        yield Ok(rows_event(event));
                    }
                }
                Err(e) => {
                    if let Some(event) = batch.flush() {
                        sent += event.rows;
                        last_ts = Some(event.last_ts.clone());
                        yield Ok(rows_event(event));
                    }
                    tracing::warn!(table = table.name(), sent, error = %e, "data stream failed");
                    yield Ok(error_event(&e.to_string(), sent, last_ts));
                    return;
                }
            }
        }
        if let Some(event) = batch.flush() {
            sent += event.rows;
            yield Ok(rows_event(event));
        }
        yield Ok(done_event(sent, Some(range), started.elapsed()));
    }
}

/// Rows accumulated for one `rows` event: `json` is the array text so far,
/// open (no closing `]`) until flushed.
struct Batch {
    limits: BatchLimits,
    json: String,
    rows: u64,
    last_ts: String,
}

/// One flushed batch: the complete JSON array and what it holds.
struct RowsBatch {
    json: String,
    rows: u64,
    last_ts: String,
}

impl Batch {
    fn new(limits: BatchLimits) -> Self {
        Batch { limits, json: Self::open(limits), rows: 0, last_ts: String::new() }
    }

    /// An empty, open array, sized so a full batch does not regrow.
    fn open(limits: BatchLimits) -> String {
        let mut json = String::with_capacity(limits.bytes.min(BATCH_BYTES) + 256);
        json.push('[');
        json
    }

    /// Adds `row`; returns the batch when that filled it.
    fn push(&mut self, row: DataRow) -> Option<RowsBatch> {
        if self.rows > 0 {
            self.json.push(',');
        }
        self.json.push_str(&row.json);
        self.rows += 1;
        self.last_ts = row.ts;
        (self.rows as usize >= self.limits.rows || self.json.len() >= self.limits.bytes).then(|| self.take())
    }

    /// The batch so far, if it holds anything.
    fn flush(&mut self) -> Option<RowsBatch> {
        (self.rows > 0).then(|| self.take())
    }

    fn take(&mut self) -> RowsBatch {
        let mut json = std::mem::replace(&mut self.json, Self::open(self.limits));
        json.push(']');
        RowsBatch { json, rows: std::mem::take(&mut self.rows), last_ts: std::mem::take(&mut self.last_ts) }
    }
}

fn meta_event(table: DataTable, key: &str, range: Option<(i64, i64)>) -> Event {
    let columns: Vec<&str> = table.columns().iter().map(|c| c.name).collect();
    let body = json!({
        "table": table.name(),
        "key_column": table.key_column(),
        "key": key,
        "start": range.map(|(s, _)| ts_string(s)),
        "end": range.map(|(_, e)| ts_string(e)),
        "columns": columns,
    });
    json_event("meta", body).retry(RETRY)
}

fn rows_event(batch: RowsBatch) -> Event {
    text_event("rows", batch.json).id(batch.last_ts)
}

fn done_event(rows: u64, range: Option<(i64, i64)>, elapsed: Duration) -> Event {
    json_event(
        "done",
        json!({
            "rows": rows,
            "start": range.map(|(s, _)| ts_string(s)),
            "end": range.map(|(_, e)| ts_string(e)),
            "elapsed_ms": elapsed.as_millis() as u64,
        }),
    )
}

fn error_event(error: &str, rows: u64, resume_from: Option<String>) -> Event {
    json_event(
        "error",
        json!({ "error": format!("questdb query failed: {error}"), "rows": rows, "resume_from": resume_from }),
    )
}

fn json_event(name: &str, body: Value) -> Event {
    text_event(name, body.to_string())
}

/// `data` must be compact JSON: it has no newlines outside escaped strings,
/// so the event is a single `data:` line.
fn text_event(name: &str, data: String) -> Event {
    Event::default().event(name).data(data)
}

fn ts_string(us: i64) -> String {
    DateTime::from_timestamp_micros(us).unwrap_or_default().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// An RFC3339 timestamp as microseconds since the epoch.
fn parse_ts(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s.trim()).ok().map(|d| d.timestamp_micros())
}

/// `start`/`end` as microseconds; either may be absent. Rejects non-RFC3339
/// values and an empty or inverted range.
fn parse_range(start: Option<&str>, end: Option<&str>) -> Result<(Option<i64>, Option<i64>), AppError> {
    let parse = |name: &str, value: Option<&str>| {
        value
            .map(|s| parse_ts(s).ok_or_else(|| AppError::BadRequest(format!("{name} must be an RFC3339 timestamp"))))
            .transpose()
    };
    let (start, end) = (parse("start", start)?, parse("end", end)?);
    if let (Some(s), Some(e)) = (start, end)
        && s >= e
    {
        return Err(AppError::BadRequest("start must be before end".into()));
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn range_parsing() {
        assert_eq!(parse_range(None, None).unwrap(), (None, None));
        let (s, e) = parse_range(Some("2026-09-24T00:00:00Z"), Some("2026-09-24T01:00:00.5+00:00")).unwrap();
        assert_eq!(s, Some(1_790_208_000_000_000));
        assert_eq!(e, Some(1_790_211_600_500_000));
        // Offsets are honoured: 02:00 at -02:00 is 04:00Z.
        assert_eq!(parse_range(Some("2026-09-24T02:00:00-02:00"), None).unwrap().0, Some(1_790_222_400_000_000));
        assert!(matches!(parse_range(Some("yesterday"), None), Err(AppError::BadRequest(m)) if m.contains("start")));
        assert!(matches!(parse_range(None, Some("2026-09-24")), Err(AppError::BadRequest(m)) if m.contains("end")));
        let same = Some("2026-09-24T00:00:00Z");
        assert!(matches!(parse_range(same, same), Err(AppError::BadRequest(_))));
        // What QuestDB prints (and so what comes back in Last-Event-ID) round-trips.
        assert_eq!(parse_ts("2026-09-24T00:00:00.000001Z"), Some(1_790_208_000_000_001));
    }

    /// One SSE frame split into its fields, with `data:` parsed as JSON so
    /// key order (a serde_json feature choice) does not matter.
    fn frame(text: &str) -> (Vec<(&str, &str)>, Value) {
        let mut fields = Vec::new();
        let mut data = None;
        for line in text.lines() {
            let (name, value) = line.split_once(": ").unwrap();
            if name == "data" { data = Some(serde_json::from_str(value).unwrap()) } else { fields.push((name, value)) }
        }
        (fields, data.unwrap())
    }

    fn row(n: u64) -> DataRow {
        DataRow { ts: format!("2026-09-24T00:00:00.{n:06}Z"), json: format!(r#"["KXBTC15M",{n}]"#) }
    }

    /// Every event the given rows produce, with `data` parsed, as
    /// `(fields, data)`.
    async fn run(
        rows: Vec<Result<DataRow, tokio_postgres::Error>>,
        limits: BatchLimits,
    ) -> Vec<(Vec<(String, String)>, Value)> {
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let events =
            events(DataTable::ContractBookLive, "K".into(), (0, 1), Box::pin(stream::iter(rows)), permit, limits);
        let res = Sse::new(events).into_response();
        let body = String::from_utf8(to_bytes(res.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap();
        body.strip_suffix("\n\n")
            .unwrap()
            .split("\n\n")
            .map(|f| {
                let (fields, data) = frame(f);
                (fields.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), data)
            })
            .collect()
    }

    /// A stream error that `events` can be handed: tokio-postgres exposes no
    /// constructor, so provoke one by connecting to nothing.
    async fn pg_error() -> tokio_postgres::Error {
        match tokio_postgres::connect("host=127.0.0.1 port=1 user=x connect_timeout=1", tokio_postgres::NoTls).await {
            Err(e) => e,
            Ok(_) => panic!("port 1 is not a postgres server"),
        }
    }

    /// Batches close at the row limit or the byte limit, carry the last
    /// row's `ts` as `id`, and the tail is flushed before `done`.
    #[tokio::test]
    async fn batches_by_rows_and_bytes() {
        let rows = (1..=25).map(|n| Ok(row(n))).collect();
        let events = run(rows, BatchLimits { rows: 10, bytes: usize::MAX }).await;
        assert_eq!(events.len(), 5, "{events:?}");
        assert_eq!(events[0].0[0].1, "meta");
        for (i, (fields, data)) in events[1..4].iter().enumerate() {
            let last = (10 * (i as u64 + 1)).min(25);
            let expected_id = format!("2026-09-24T00:00:00.{last:06}Z");
            assert_eq!(*fields, vec![("event".to_string(), "rows".to_string()), ("id".to_string(), expected_id)]);
            let rows = data.as_array().unwrap();
            assert_eq!(rows.len(), if i < 2 { 10 } else { 5 });
            assert_eq!(rows[0], json!(["KXBTC15M", last - rows.len() as u64 + 1]));
            assert_eq!(rows[rows.len() - 1], json!(["KXBTC15M", last]));
        }
        assert_eq!(events[4].0, vec![("event".to_string(), "done".to_string())]);
        assert_eq!(events[4].1["rows"], json!(25));

        // A row is ["KXBTC15M",n]: 16 bytes for n < 10, so 40 bytes fills at the third row.
        let rows = (1..=7).map(|n| Ok(row(n))).collect();
        let events = run(rows, BatchLimits { rows: usize::MAX, bytes: 40 }).await;
        let sizes: Vec<usize> = events[1..events.len() - 1].iter().map(|(_, d)| d.as_array().unwrap().len()).collect();
        assert_eq!(sizes, vec![3, 3, 1]);
        assert_eq!(events.last().unwrap().1["rows"], json!(7));
    }

    /// Rows read before a failure go out in a final `rows` event, and
    /// `error` counts them and points `resume_from` at the last one.
    #[tokio::test]
    async fn error_flushes_partial_batch() {
        let mut rows: Vec<Result<DataRow, _>> = (1..=7).map(|n| Ok(row(n))).collect();
        rows.push(Err(pg_error().await));
        let events = run(rows, BatchLimits { rows: 5, bytes: usize::MAX }).await;
        let kinds: Vec<&str> = events.iter().map(|(f, _)| f[0].1.as_str()).collect();
        assert_eq!(kinds, vec!["meta", "rows", "rows", "error"]);
        assert_eq!(events[2].1.as_array().unwrap().len(), 2);
        assert_eq!(events[2].0[1].1, "2026-09-24T00:00:00.000007Z");
        assert_eq!(events[3].1["rows"], json!(7));
        assert_eq!(events[3].1["resume_from"], json!("2026-09-24T00:00:00.000007Z"));

        // No rows at all: just `error`, with nothing to resume from.
        let events = run(vec![Err(pg_error().await)], BatchLimits::default()).await;
        let kinds: Vec<&str> = events.iter().map(|(f, _)| f[0].1.as_str()).collect();
        assert_eq!(kinds, vec!["meta", "error"]);
        assert_eq!(events[1].1["rows"], json!(0));
        assert_eq!(events[1].1["resume_from"], Value::Null);
    }

    /// The wire text of one event of each kind.
    #[tokio::test]
    async fn event_encoding() {
        let ts = "2026-09-24T00:00:00.000001Z";
        let mut batch = Batch::new(BatchLimits::default());
        assert!(batch.push(DataRow { ts: "2026-09-24T00:00:00.000000Z".into(), json: r#"[0.41,2]"#.into() }).is_none());
        assert!(batch.push(DataRow { ts: ts.into(), json: r#"[0.42,3]"#.into() }).is_none());
        let events = vec![
            Ok::<_, Infallible>(meta_event(DataTable::ContractBookLive, "KXBTC15M", Some((0, 1_000_000)))),
            Ok(rows_event(batch.flush().unwrap())),
            Ok(error_event("boom", 1, Some(ts.into()))),
            Ok(done_event(1, None, Duration::from_millis(7))),
        ];
        let res = Sse::new(stream::iter(events)).into_response();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "text/event-stream");
        let body = String::from_utf8(to_bytes(res.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap();
        let frames: Vec<&str> = body.strip_suffix("\n\n").unwrap().split("\n\n").collect();
        assert_eq!(frames.len(), 4, "{body}");

        let (fields, data) = frame(frames[0]);
        assert_eq!(fields, vec![("event", "meta"), ("retry", "10000")]);
        let columns =
            ["ticker", "series_ticker", "side", "kind", "source", "price", "size", "seq", "received_at", "ts"];
        assert_eq!(
            data,
            json!({
                "table": "contract_book_live", "key_column": "series_ticker", "key": "KXBTC15M",
                "start": "1970-01-01T00:00:00.000000Z", "end": "1970-01-01T00:00:01.000000Z", "columns": columns,
            })
        );

        let (fields, data) = frame(frames[1]);
        assert_eq!(fields, vec![("event", "rows"), ("id", ts)]);
        assert_eq!(data, json!([[0.41, 2], [0.42, 3]]));

        let (fields, data) = frame(frames[2]);
        assert_eq!(fields, vec![("event", "error")]);
        assert_eq!(data, json!({ "error": "questdb query failed: boom", "rows": 1, "resume_from": ts }));

        let (fields, data) = frame(frames[3]);
        assert_eq!(fields, vec![("event", "done")]);
        assert_eq!(data, json!({ "rows": 1, "start": null, "end": null, "elapsed_ms": 7 }));
    }
}
