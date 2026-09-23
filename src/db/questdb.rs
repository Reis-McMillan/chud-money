//! QuestDB: ILP/HTTP ingestion on a dedicated blocking thread, PGWire queries.
//!
//! Two tables with an identical column set hold index values from different
//! sources: `index_values_live` (5Hz websocket) and `index_values_hist`
//! (REST pass-through backfill). Both are DEDUP'd on `(ts, index_id)` so
//! re-ingesting a window or overlapping after a reconnect is idempotent.
//!
//! `contract_candles_hist` holds the prices the contracts themselves traded
//! at: one row per market ticker per candlestick period, DEDUP'd on
//! `(ts, ticker)`. `coinbase_candles_hist` holds Coinbase spot candles, one
//! row per product per minute, DEDUP'd on `(ts, product)`.

use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use questdb::ingress::{Buffer, Sender, TimestampMicros, TimestampNanos};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// Rows queued for the writer before producers see backpressure.
const ILP_QUEUE: usize = 50_000;
/// Rows drained per flush when the queue is busy (bulk ingest).
const ILP_BATCH: usize = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Table {
    Live,
    Hist,
}

impl Table {
    pub fn name(self) -> &'static str {
        match self {
            Table::Live => "index_values_live",
            Table::Hist => "index_values_hist",
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexRow {
    pub table: Table,
    pub index_id: String,
    pub value: f64,
    /// Designated timestamp: the upstream publication time.
    pub ts_ms: i64,
    /// When Kalshi received it (websocket only).
    pub received_at_ms: Option<i64>,
    pub source: &'static str,
}

pub const CANDLES_TABLE: &str = "contract_candles_hist";

/// Open/high/low/close of one quantity over a candlestick period, in dollars.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Ohlc {
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
}

/// One candlestick of one market (e.g. `KXBTC15M-26SEP181300-00`).
#[derive(Debug, Clone)]
pub struct CandleRow {
    pub series_ticker: String,
    pub ticker: String,
    /// Settlement target of the market, repeated on every row so candles can
    /// be compared against the index without a second lookup.
    pub floor_strike: Option<f64>,
    pub yes_bid: Ohlc,
    pub yes_ask: Ohlc,
    /// Trade prices; all `None` for a period without trades.
    pub price: Ohlc,
    /// Volume-weighted average trade price.
    pub price_mean: Option<f64>,
    pub volume: Option<f64>,
    pub open_interest: Option<f64>,
    /// Designated timestamp: the *end* of the period, as Kalshi reports it.
    pub ts_ms: i64,
    pub source: &'static str,
}

pub const COINBASE_TABLE: &str = "coinbase_candles_hist";

/// One Coinbase spot candle of one product (e.g. `BTC-USD`).
#[derive(Debug, Clone)]
pub struct CoinbaseRow {
    pub product: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// In base currency.
    pub volume: f64,
    /// Designated timestamp: the *start* of the period, as Coinbase reports
    /// it (Kalshi's candles use the end).
    pub ts_ms: i64,
    pub source: &'static str,
}

/// Anything the ILP writer can persist.
#[derive(Debug, Clone)]
pub enum Row {
    Index(IndexRow),
    Coinbase(CoinbaseRow),
    Candle(Box<CandleRow>),
}

impl From<IndexRow> for Row {
    fn from(row: IndexRow) -> Self {
        Row::Index(row)
    }
}

impl From<CoinbaseRow> for Row {
    fn from(row: CoinbaseRow) -> Self {
        Row::Coinbase(row)
    }
}

impl From<CandleRow> for Row {
    fn from(row: CandleRow) -> Self {
        Row::Candle(Box::new(row))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CandleSummary {
    pub table: &'static str,
    pub rows: i64,
    pub markets: i64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoinbaseSummary {
    pub table: &'static str,
    pub product: String,
    pub rows: i64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
    pub last_close: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableSummary {
    pub table: &'static str,
    pub rows: i64,
    pub rows_last_hour: i64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
    pub last_value: Option<f64>,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
}

#[derive(Clone)]
pub struct Questdb {
    pub ilp: mpsc::Sender<Row>,
    pg_conninfo: String,
}

impl Questdb {
    /// Creates the tables over PGWire first (so ILP never auto-creates them
    /// without DEDUP), then starts the ILP writer thread.
    pub async fn connect(ilp_conf: &str, pg_conninfo: String) -> Result<Self> {
        let (tx, rx) = mpsc::channel(ILP_QUEUE);
        let db = Self { ilp: tx, pg_conninfo };
        db.ensure_tables().await?;
        start_ilp_writer(ilp_conf.to_string(), rx)?;
        Ok(db)
    }

    async fn pg(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(&self.pg_conninfo, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "questdb pg connection closed");
            }
        });
        Ok(client)
    }

    pub async fn ensure_tables(&self) -> Result<()> {
        let client = self.pg().await.context("connecting to questdb pgwire")?;
        for table in [Table::Live, Table::Hist] {
            let ddl = format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                    index_id SYMBOL CAPACITY 256 CACHE INDEX, \
                    source SYMBOL, \
                    value DOUBLE, \
                    received_at TIMESTAMP, \
                    ts TIMESTAMP\
                 ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, index_id);",
                table.name()
            );
            client.batch_execute(&ddl).await.with_context(|| format!("creating {}", table.name()))?;
            tracing::info!(table = table.name(), "ensured questdb table");
        }
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {CANDLES_TABLE} (\
                ticker SYMBOL CAPACITY 65536 CACHE INDEX, \
                series_ticker SYMBOL CAPACITY 256 CACHE, \
                source SYMBOL, \
                floor_strike DOUBLE, \
                yes_bid_open DOUBLE, yes_bid_high DOUBLE, yes_bid_low DOUBLE, yes_bid_close DOUBLE, \
                yes_ask_open DOUBLE, yes_ask_high DOUBLE, yes_ask_low DOUBLE, yes_ask_close DOUBLE, \
                price_open DOUBLE, price_high DOUBLE, price_low DOUBLE, price_close DOUBLE, \
                price_mean DOUBLE, \
                volume DOUBLE, \
                open_interest DOUBLE, \
                ts TIMESTAMP\
             ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, ticker);"
        );
        client.batch_execute(&ddl).await.with_context(|| format!("creating {CANDLES_TABLE}"))?;
        tracing::info!(table = CANDLES_TABLE, "ensured questdb table");
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {COINBASE_TABLE} (\
                product SYMBOL CAPACITY 256 CACHE INDEX, \
                source SYMBOL, \
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, \
                volume DOUBLE, \
                ts TIMESTAMP\
             ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, product);"
        );
        client.batch_execute(&ddl).await.with_context(|| format!("creating {COINBASE_TABLE}"))?;
        tracing::info!(table = COINBASE_TABLE, "ensured questdb table");
        Ok(())
    }

    /// Row counts, time span, and value stats for one index in one table.
    ///
    /// Uses the simple query protocol with the index id inlined: QuestDB's
    /// PGWire has known issues binding parameters, and `index_id` is
    /// schema-validated to `[A-Z0-9_]+` so inlining is safe (quotes are
    /// escaped regardless).
    pub async fn summary(&self, table: Table, index_id: &str) -> Result<TableSummary, tokio_postgres::Error> {
        let client = self.pg().await?;
        let id = index_id.replace('\'', "''");
        let sql = format!(
            "SELECT count() AS rows, \
                    cast(min(ts) AS string) AS first_ts, \
                    cast(max(ts) AS string) AS last_ts, \
                    last(value) AS last_value, \
                    min(value) AS min_value, \
                    max(value) AS max_value, \
                    sum(CASE WHEN ts > dateadd('h', -1, now()) THEN 1 ELSE 0 END) AS rows_last_hour \
             FROM {} WHERE index_id = '{id}'",
            table.name()
        );
        let row = client
            .simple_query(&sql)
            .await?
            .into_iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            });

        let col = |i: usize| -> Option<String> { row.as_ref().and_then(|r| r.get(i)).map(str::to_string) };
        let num = |i: usize| -> Option<f64> { col(i).and_then(|s| s.parse().ok()) };
        Ok(TableSummary {
            table: table.name(),
            rows: col(0).and_then(|s| s.parse().ok()).unwrap_or(0),
            first_ts: col(1),
            last_ts: col(2),
            last_value: num(3),
            min_value: num(4),
            max_value: num(5),
            rows_last_hour: col(6).and_then(|s| s.parse::<f64>().ok()).map(|v| v as i64).unwrap_or(0),
        })
    }

    /// Rows `index_values_hist` already holds for one index within
    /// `[start_ms, end_ms)`, counted per calendar-aligned `sample_by` bucket
    /// (a QuestDB interval such as `1h`) and keyed by bucket start in ms.
    /// See `summary` for why the id is inlined.
    pub async fn index_counts(
        &self,
        index_id: &str,
        start_ms: i64,
        end_ms: i64,
        sample_by: &str,
    ) -> Result<HashMap<i64, i64>, tokio_postgres::Error> {
        let client = self.pg().await?;
        let id = index_id.replace('\'', "''");
        let sql = format!(
            "SELECT cast(ts AS long) AS bucket_us, count() AS rows \
             FROM {} WHERE index_id = '{id}' AND ts >= '{}' AND ts < '{}' \
             SAMPLE BY {sample_by} ALIGN TO CALENDAR",
            Table::Hist.name(),
            ts_literal(start_ms),
            ts_literal(end_ms),
        );
        let counts = client.simple_query(&sql).await?.into_iter().filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((r.get(0)?.parse::<i64>().ok()? / 1_000, r.get(1)?.parse().ok()?)),
            _ => None,
        });
        Ok(counts.collect())
    }

    /// Candles `coinbase_candles_hist` already holds for one product within
    /// `[start_ms, end_ms)`, per calendar hour, keyed by hour start in ms.
    /// `product` is pattern-validated; see `summary` for why it is inlined.
    pub async fn coinbase_counts(
        &self,
        product: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<i64, i64>, tokio_postgres::Error> {
        let client = self.pg().await?;
        let product = product.replace('\'', "''");
        let sql = format!(
            "SELECT cast(ts AS long) AS bucket_us, count() AS rows \
             FROM {COINBASE_TABLE} WHERE product = '{product}' AND ts >= '{}' AND ts < '{}' \
             SAMPLE BY 1h ALIGN TO CALENDAR",
            ts_literal(start_ms),
            ts_literal(end_ms),
        );
        let counts = client.simple_query(&sql).await?.into_iter().filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((r.get(0)?.parse::<i64>().ok()? / 1_000, r.get(1)?.parse().ok()?)),
            _ => None,
        });
        Ok(counts.collect())
    }

    /// Row count, time span and latest close of one product's candles.
    pub async fn coinbase_summary(&self, product: &str) -> Result<CoinbaseSummary, tokio_postgres::Error> {
        let client = self.pg().await?;
        let escaped = product.replace('\'', "''");
        let sql = format!(
            "SELECT count() AS rows, \
                    cast(min(ts) AS string) AS first_ts, \
                    cast(max(ts) AS string) AS last_ts, \
                    last(close) AS last_close \
             FROM {COINBASE_TABLE} WHERE product = '{escaped}'"
        );
        let row = client
            .simple_query(&sql)
            .await?
            .into_iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            });
        let col = |i: usize| -> Option<String> { row.as_ref().and_then(|r| r.get(i)).map(str::to_string) };
        Ok(CoinbaseSummary {
            table: COINBASE_TABLE,
            product: product.to_string(),
            rows: col(0).and_then(|s| s.parse().ok()).unwrap_or(0),
            first_ts: col(1),
            last_ts: col(2),
            last_close: col(3).and_then(|s| s.parse().ok()),
        })
    }

    /// Candles `contract_candles_hist` already holds per market ticker of one
    /// series, for periods ending within `[start_ms, end_ms]`.
    pub async fn candle_counts(
        &self,
        series_ticker: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<String, i64>, tokio_postgres::Error> {
        let client = self.pg().await?;
        let series = series_ticker.replace('\'', "''");
        let sql = format!(
            "SELECT ticker, count() AS rows \
             FROM {CANDLES_TABLE} WHERE series_ticker = '{series}' AND ts >= '{}' AND ts <= '{}'",
            ts_literal(start_ms),
            ts_literal(end_ms),
        );
        let counts = client.simple_query(&sql).await?.into_iter().filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((r.get(0)?.to_string(), r.get(1)?.parse().ok()?)),
            _ => None,
        });
        Ok(counts.collect())
    }

    /// Row count, distinct markets and time span of one series' candles.
    /// `series_ticker` is schema-validated to `[A-Z0-9]+`; see `summary` for
    /// why it is inlined.
    pub async fn candle_summary(&self, series_ticker: &str) -> Result<CandleSummary, tokio_postgres::Error> {
        let client = self.pg().await?;
        let series = series_ticker.replace('\'', "''");
        let sql = format!(
            "SELECT count() AS rows, \
                    count_distinct(ticker) AS markets, \
                    cast(min(ts) AS string) AS first_ts, \
                    cast(max(ts) AS string) AS last_ts \
             FROM {CANDLES_TABLE} WHERE series_ticker = '{series}'"
        );
        let row = client
            .simple_query(&sql)
            .await?
            .into_iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            });
        let col = |i: usize| -> Option<String> { row.as_ref().and_then(|r| r.get(i)).map(str::to_string) };
        Ok(CandleSummary {
            table: CANDLES_TABLE,
            rows: col(0).and_then(|s| s.parse().ok()).unwrap_or(0),
            markets: col(1).and_then(|s| s.parse().ok()).unwrap_or(0),
            first_ts: col(2),
            last_ts: col(3),
        })
    }
}

/// `ms` as a timestamp literal QuestDB compares against `ts`.
fn ts_literal(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default().to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn push(buf: &mut Buffer, row: &Row) -> questdb::Result<()> {
    match row {
        Row::Index(row) => push_index(buf, row),
        Row::Candle(row) => push_candle(buf, row),
        Row::Coinbase(row) => push_coinbase(buf, row),
    }
}

fn push_coinbase(buf: &mut Buffer, row: &CoinbaseRow) -> questdb::Result<()> {
    buf.table(COINBASE_TABLE)?
        .symbol("product", row.product.as_str())?
        .symbol("source", row.source)?
        .column_f64("open", row.open)?
        .column_f64("high", row.high)?
        .column_f64("low", row.low)?
        .column_f64("close", row.close)?
        .column_f64("volume", row.volume)?;
    buf.at(TimestampNanos::new(row.ts_ms * 1_000_000))
}

fn push_candle(buf: &mut Buffer, row: &CandleRow) -> questdb::Result<()> {
    buf.table(CANDLES_TABLE)?
        .symbol("ticker", row.ticker.as_str())?
        .symbol("series_ticker", row.series_ticker.as_str())?
        .symbol("source", row.source)?;
    // Absent values are left out so the column stays NULL.
    let mut columns = vec![
        ("floor_strike", row.floor_strike),
        ("price_mean", row.price_mean),
        ("volume", row.volume),
        ("open_interest", row.open_interest),
    ];
    for (names, ohlc) in [
        (["yes_bid_open", "yes_bid_high", "yes_bid_low", "yes_bid_close"], row.yes_bid),
        (["yes_ask_open", "yes_ask_high", "yes_ask_low", "yes_ask_close"], row.yes_ask),
        (["price_open", "price_high", "price_low", "price_close"], row.price),
    ] {
        columns.extend(names.into_iter().zip([ohlc.open, ohlc.high, ohlc.low, ohlc.close]));
    }
    for (name, value) in columns {
        if let Some(value) = value {
            buf.column_f64(name, value)?;
        }
    }
    buf.at(TimestampNanos::new(row.ts_ms * 1_000_000))
}

fn push_index(buf: &mut Buffer, row: &IndexRow) -> questdb::Result<()> {
    // Symbols must precede regular columns in a row.
    let b = buf
        .table(row.table.name())?
        .symbol("index_id", row.index_id.as_str())?
        .symbol("source", row.source)?
        .column_f64("value", row.value)?;
    if let Some(received) = row.received_at_ms {
        b.column_ts("received_at", TimestampMicros::new(received * 1_000))?;
    }
    buf.at(TimestampNanos::new(row.ts_ms * 1_000_000))
}

/// Owns the sync `questdb-rs` sender on its own OS thread. Blocks on the
/// first row, then drains whatever else is queued (up to `ILP_BATCH`) into
/// one flush, so 5Hz live traffic flushes promptly and bulk ingest batches.
fn start_ilp_writer(conf: String, mut rx: mpsc::Receiver<Row>) -> Result<()> {
    let mut sender = Sender::from_conf(&conf).context("QUESTDB_ILP_CONF")?;
    let mut buf = sender.new_buffer();
    std::thread::Builder::new()
        .name("questdb-ilp".into())
        .spawn(move || {
            while let Some(first) = rx.blocking_recv() {
                let mut batch = vec![first];
                while batch.len() < ILP_BATCH {
                    match rx.try_recv() {
                        Ok(row) => batch.push(row),
                        Err(_) => break,
                    }
                }
                for row in &batch {
                    if let Err(e) = push(&mut buf, row) {
                        tracing::error!(error = %e, ?row, "ilp row rejected");
                        buf.clear();
                    }
                }
                if buf.row_count() == 0 {
                    continue;
                }
                match sender.flush(&mut buf) {
                    Ok(()) => tracing::debug!(rows = batch.len(), "ilp flush"),
                    Err(e) => {
                        tracing::error!(error = %e, rows = batch.len(), "ilp flush failed; rows dropped");
                        buf.clear();
                    }
                }
            }
            tracing::info!("ilp writer exiting");
        })
        .context("spawning ilp writer thread")?;
    Ok(())
}
