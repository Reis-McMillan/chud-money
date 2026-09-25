//! QuestDB: ILP/HTTP ingestion on a dedicated blocking thread, PGWire queries.
//!
//! Two tables with an identical column set hold index values from different
//! sources: `index_values_live` (5Hz websocket) and `index_values_hist`
//! (REST pass-through backfill). Both are DEDUP'd on `(ts, index_id)` so
//! re-ingesting a window or overlapping after a reconnect is idempotent.
//!
//! `contract_candles_hist` holds the prices the contracts themselves traded
//! at: one row per market ticker per candlestick period, DEDUP'd on
//! `(ts, ticker)`.
//!
//! Four `*_live` tables hold what the websocket feeds stream:
//!
//! - `contract_ticker_live`: Kalshi's `ticker` channel, one row per market
//!   per change (last price, best yes bid/ask and sizes, volume, open
//!   interest).
//! - `contract_book_live`: Kalshi orderbook frames as an event log. A new
//!   subscription (session start, reconnect, a market added on rotation)
//!   writes the snapshot as one `kind = 'snapshot'` row per level; every
//!   `orderbook_delta` after that is one `kind = 'delta'` row whose `size` is
//!   the signed change. Replay in `seq` order (it restarts at each snapshot),
//!   not `ts`: a snapshot carries no upstream time so its `ts` is the receive
//!   time, which can trail the first deltas' Kalshi `ts_ms` by a few ms.
//! - `coinbase_ticker_live`: Coinbase Advanced Trade `ticker` events for the
//!   market's spot product.
//! - `coinbase_book_live`: Coinbase `level2` as an event log with the same
//!   snapshot-then-updates convention; `size` is the absolute resting size
//!   at the level (`0` removes it).
//!
//! `ts` is the upstream timestamp and `received_at` when this process (or,
//! for Coinbase level2, Coinbase's gateway) saw the frame. Prices are in
//! dollars and sizes in contracts / base currency.
//!
//! An earlier `coinbase_candles_hist` table (REST candle backfill) is no
//! longer written or created; drop it by hand if it is still around.
//!
//! Queries go over PGWire on a fresh connection per call. The only per-market
//! statistics exposed are row count and time span ([`Questdb::summaries`]):
//! filtered aggregates over the book tables are multi-second full scans, so
//! `feeds::summary` computes them on a timer and the HTTP layer only ever
//! reads its cache. Ingestion is the other side of the same CPU budget: the
//! writer thread coalesces up to `FLUSH_INTERVAL` of rows into one flush so
//! QuestDB sees a few large WAL commits per second instead of a hundred tiny
//! ones, each of which costs a dedup + O3 merge.

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::{Stream, TryStreamExt};
use questdb::ingress::{Buffer, Sender, TimestampMicros, TimestampNanos};
use serde::Serialize;
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// Rows queued for the writer before producers see backpressure.
const ILP_QUEUE: usize = 50_000;
/// Rows drained per flush when the queue is busy (bulk ingest).
const ILP_BATCH: usize = 5_000;
/// How long the writer keeps collecting after the first row before flushing.
/// Each flush is a WAL commit with a dedup + O3 merge, so a hundred 1-row
/// commits a second cost far more than two 5000-row ones; the rows only go
/// to storage, so the added delay is invisible to websocket subscribers.
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
/// Poll gap while waiting out `FLUSH_INTERVAL` on a blocking thread.
const DRAIN_POLL: Duration = Duration::from_millis(5);

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
pub const CONTRACT_TICKER_TABLE: &str = "contract_ticker_live";
pub const CONTRACT_BOOK_TABLE: &str = "contract_book_live";
pub const COINBASE_TICKER_TABLE: &str = "coinbase_ticker_live";
pub const COINBASE_BOOK_TABLE: &str = "coinbase_book_live";

/// A table this API can export row by row, and how its rows are keyed to a
/// market (see [`DataTable::key_source`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataTable {
    IndexValuesLive,
    IndexValuesHist,
    ContractCandlesHist,
    ContractTickerLive,
    ContractBookLive,
    CoinbaseTickerLive,
    CoinbaseBookLive,
}

/// Which `Market` field holds the value a table's rows are keyed by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    IndexId,
    SeriesTicker,
    CoinbaseProduct,
}

/// How a column's text value from the simple query protocol becomes JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColKind {
    Text,
    Double,
    Long,
    Timestamp,
}

#[derive(Debug, Clone, Copy)]
pub struct Column {
    pub name: &'static str,
    pub kind: ColKind,
}

const fn col(name: &'static str, kind: ColKind) -> Column {
    Column { name, kind }
}

// Column lists in DDL order (`ts` last); a test checks them against `ddl`.
const INDEX_COLUMNS: &[Column] = &[
    col("index_id", ColKind::Text),
    col("source", ColKind::Text),
    col("value", ColKind::Double),
    col("received_at", ColKind::Timestamp),
    col("ts", ColKind::Timestamp),
];
const CANDLE_COLUMNS: &[Column] = &[
    col("ticker", ColKind::Text),
    col("series_ticker", ColKind::Text),
    col("source", ColKind::Text),
    col("floor_strike", ColKind::Double),
    col("yes_bid_open", ColKind::Double),
    col("yes_bid_high", ColKind::Double),
    col("yes_bid_low", ColKind::Double),
    col("yes_bid_close", ColKind::Double),
    col("yes_ask_open", ColKind::Double),
    col("yes_ask_high", ColKind::Double),
    col("yes_ask_low", ColKind::Double),
    col("yes_ask_close", ColKind::Double),
    col("price_open", ColKind::Double),
    col("price_high", ColKind::Double),
    col("price_low", ColKind::Double),
    col("price_close", ColKind::Double),
    col("price_mean", ColKind::Double),
    col("volume", ColKind::Double),
    col("open_interest", ColKind::Double),
    col("ts", ColKind::Timestamp),
];
const CONTRACT_TICKER_COLUMNS: &[Column] = &[
    col("ticker", ColKind::Text),
    col("series_ticker", ColKind::Text),
    col("source", ColKind::Text),
    col("floor_strike", ColKind::Double),
    col("price", ColKind::Double),
    col("yes_bid", ColKind::Double),
    col("yes_ask", ColKind::Double),
    col("yes_bid_size", ColKind::Double),
    col("yes_ask_size", ColKind::Double),
    col("last_trade_size", ColKind::Double),
    col("volume", ColKind::Double),
    col("open_interest", ColKind::Double),
    col("dollar_volume", ColKind::Long),
    col("dollar_open_interest", ColKind::Long),
    col("received_at", ColKind::Timestamp),
    col("ts", ColKind::Timestamp),
];
const CONTRACT_BOOK_COLUMNS: &[Column] = &[
    col("ticker", ColKind::Text),
    col("series_ticker", ColKind::Text),
    col("side", ColKind::Text),
    col("kind", ColKind::Text),
    col("source", ColKind::Text),
    col("price", ColKind::Double),
    col("size", ColKind::Double),
    col("seq", ColKind::Long),
    col("received_at", ColKind::Timestamp),
    col("ts", ColKind::Timestamp),
];
const COINBASE_TICKER_COLUMNS: &[Column] = &[
    col("product", ColKind::Text),
    col("source", ColKind::Text),
    col("price", ColKind::Double),
    col("best_bid", ColKind::Double),
    col("best_ask", ColKind::Double),
    col("best_bid_qty", ColKind::Double),
    col("best_ask_qty", ColKind::Double),
    col("volume_24h", ColKind::Double),
    col("low_24h", ColKind::Double),
    col("high_24h", ColKind::Double),
    col("low_52w", ColKind::Double),
    col("high_52w", ColKind::Double),
    col("pct_chg_24h", ColKind::Double),
    col("sequence_num", ColKind::Long),
    col("received_at", ColKind::Timestamp),
    col("ts", ColKind::Timestamp),
];
const COINBASE_BOOK_COLUMNS: &[Column] = &[
    col("product", ColKind::Text),
    col("side", ColKind::Text),
    col("kind", ColKind::Text),
    col("source", ColKind::Text),
    col("price", ColKind::Double),
    col("size", ColKind::Double),
    col("sequence_num", ColKind::Long),
    col("received_at", ColKind::Timestamp),
    col("ts", ColKind::Timestamp),
];

impl DataTable {
    pub const ALL: [DataTable; 7] = [
        DataTable::IndexValuesLive,
        DataTable::IndexValuesHist,
        DataTable::ContractCandlesHist,
        DataTable::ContractTickerLive,
        DataTable::ContractBookLive,
        DataTable::CoinbaseTickerLive,
        DataTable::CoinbaseBookLive,
    ];

    pub fn name(self) -> &'static str {
        match self {
            DataTable::IndexValuesLive => Table::Live.name(),
            DataTable::IndexValuesHist => Table::Hist.name(),
            DataTable::ContractCandlesHist => CANDLES_TABLE,
            DataTable::ContractTickerLive => CONTRACT_TICKER_TABLE,
            DataTable::ContractBookLive => CONTRACT_BOOK_TABLE,
            DataTable::CoinbaseTickerLive => COINBASE_TICKER_TABLE,
            DataTable::CoinbaseBookLive => COINBASE_BOOK_TABLE,
        }
    }

    /// Short URL-friendly name accepted by [`DataTable::parse`].
    pub fn alias(self) -> &'static str {
        match self {
            DataTable::IndexValuesLive => "index-live",
            DataTable::IndexValuesHist => "index-hist",
            DataTable::ContractCandlesHist => "candles",
            DataTable::ContractTickerLive => "ticker",
            DataTable::ContractBookLive => "book",
            DataTable::CoinbaseTickerLive => "coinbase-ticker",
            DataTable::CoinbaseBookLive => "coinbase-book",
        }
    }

    /// The table name or alias, case-insensitively and with `-` and `_`
    /// interchangeable.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().to_ascii_lowercase().replace('-', "_");
        Self::ALL.into_iter().find(|t| t.name() == s || t.alias().replace('-', "_") == s)
    }

    pub fn key_source(self) -> KeySource {
        match self {
            DataTable::IndexValuesLive | DataTable::IndexValuesHist => KeySource::IndexId,
            DataTable::ContractCandlesHist | DataTable::ContractTickerLive | DataTable::ContractBookLive => {
                KeySource::SeriesTicker
            }
            DataTable::CoinbaseTickerLive | DataTable::CoinbaseBookLive => KeySource::CoinbaseProduct,
        }
    }

    /// The symbol column a market's rows are selected by.
    pub fn key_column(self) -> &'static str {
        match self.key_source() {
            KeySource::IndexId => "index_id",
            KeySource::SeriesTicker => "series_ticker",
            KeySource::CoinbaseProduct => "product",
        }
    }

    pub fn columns(self) -> &'static [Column] {
        match self {
            DataTable::IndexValuesLive | DataTable::IndexValuesHist => INDEX_COLUMNS,
            DataTable::ContractCandlesHist => CANDLE_COLUMNS,
            DataTable::ContractTickerLive => CONTRACT_TICKER_COLUMNS,
            DataTable::ContractBookLive => CONTRACT_BOOK_COLUMNS,
            DataTable::CoinbaseTickerLive => COINBASE_TICKER_COLUMNS,
            DataTable::CoinbaseBookLive => COINBASE_BOOK_COLUMNS,
        }
    }

    /// The `CREATE TABLE IF NOT EXISTS` statement `ensure_tables` runs.
    fn ddl(self) -> String {
        match self {
            DataTable::IndexValuesLive | DataTable::IndexValuesHist => format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                    index_id SYMBOL CAPACITY 256 CACHE INDEX, \
                    source SYMBOL, \
                    value DOUBLE, \
                    received_at TIMESTAMP, \
                    ts TIMESTAMP\
                 ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, index_id);",
                self.name()
            ),
            DataTable::ContractCandlesHist => format!(
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
            ),
            DataTable::ContractTickerLive => format!(
                "CREATE TABLE IF NOT EXISTS {CONTRACT_TICKER_TABLE} (\
                    ticker SYMBOL CAPACITY 65536 CACHE INDEX, \
                    series_ticker SYMBOL CAPACITY 256 CACHE, \
                    source SYMBOL, \
                    floor_strike DOUBLE, \
                    price DOUBLE, \
                    yes_bid DOUBLE, yes_ask DOUBLE, \
                    yes_bid_size DOUBLE, yes_ask_size DOUBLE, \
                    last_trade_size DOUBLE, \
                    volume DOUBLE, \
                    open_interest DOUBLE, \
                    dollar_volume LONG, \
                    dollar_open_interest LONG, \
                    received_at TIMESTAMP, \
                    ts TIMESTAMP\
                 ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, ticker);"
            ),
            DataTable::ContractBookLive => format!(
                "CREATE TABLE IF NOT EXISTS {CONTRACT_BOOK_TABLE} (\
                    ticker SYMBOL CAPACITY 65536 CACHE INDEX, \
                    series_ticker SYMBOL CAPACITY 256 CACHE, \
                    side SYMBOL, \
                    kind SYMBOL, \
                    source SYMBOL, \
                    price DOUBLE, \
                    size DOUBLE, \
                    seq LONG, \
                    received_at TIMESTAMP, \
                    ts TIMESTAMP\
                 ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, ticker, seq, side, price);"
            ),
            DataTable::CoinbaseTickerLive => format!(
                "CREATE TABLE IF NOT EXISTS {COINBASE_TICKER_TABLE} (\
                    product SYMBOL CAPACITY 256 CACHE INDEX, \
                    source SYMBOL, \
                    price DOUBLE, \
                    best_bid DOUBLE, best_ask DOUBLE, \
                    best_bid_qty DOUBLE, best_ask_qty DOUBLE, \
                    volume_24h DOUBLE, low_24h DOUBLE, high_24h DOUBLE, \
                    low_52w DOUBLE, high_52w DOUBLE, \
                    pct_chg_24h DOUBLE, \
                    sequence_num LONG, \
                    received_at TIMESTAMP, \
                    ts TIMESTAMP\
                 ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, product);"
            ),
            DataTable::CoinbaseBookLive => format!(
                "CREATE TABLE IF NOT EXISTS {COINBASE_BOOK_TABLE} (\
                    product SYMBOL CAPACITY 256 CACHE INDEX, \
                    side SYMBOL, \
                    kind SYMBOL, \
                    source SYMBOL, \
                    price DOUBLE, \
                    size DOUBLE, \
                    sequence_num LONG, \
                    received_at TIMESTAMP, \
                    ts TIMESTAMP\
                 ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, product, side, price);"
            ),
        }
    }
}

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

/// One Kalshi `ticker` frame of one market. Fields Kalshi left out stay NULL.
#[derive(Debug, Clone)]
pub struct ContractTickerRow {
    pub series_ticker: String,
    pub ticker: String,
    pub floor_strike: Option<f64>,
    /// Last trade price, in dollars.
    pub price: Option<f64>,
    pub yes_bid: Option<f64>,
    pub yes_ask: Option<f64>,
    pub yes_bid_size: Option<f64>,
    pub yes_ask_size: Option<f64>,
    pub last_trade_size: Option<f64>,
    pub volume: Option<f64>,
    pub open_interest: Option<f64>,
    pub dollar_volume: Option<i64>,
    pub dollar_open_interest: Option<i64>,
    /// Designated timestamp: Kalshi's `ts_ms`.
    pub ts_ms: i64,
    pub received_at_ms: i64,
    pub source: &'static str,
}

/// Whether a book row comes from a full snapshot or a change after one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookKind {
    Snapshot,
    /// Kalshi: `size` is the signed change at the level.
    Delta,
    /// Coinbase: `size` is the new absolute size at the level.
    Update,
}

impl BookKind {
    pub fn as_str(self) -> &'static str {
        match self {
            BookKind::Snapshot => "snapshot",
            BookKind::Delta => "delta",
            BookKind::Update => "update",
        }
    }
}

/// One level of a Kalshi orderbook snapshot, or one delta.
#[derive(Debug, Clone)]
pub struct ContractBookRow {
    pub series_ticker: String,
    pub ticker: String,
    /// `yes` or `no`: both sides are resting bids, see `kalshi::book`.
    pub side: &'static str,
    pub kind: BookKind,
    /// In dollars.
    pub price: f64,
    /// Resting contracts (snapshot) or the signed change (delta).
    pub size: f64,
    /// Kalshi's per-subscription sequence number of the frame.
    pub seq: u64,
    /// Designated timestamp: the frame's `ts_ms` when present.
    pub ts_ms: i64,
    pub received_at_ms: i64,
    pub source: &'static str,
}

/// One Coinbase `ticker` event of one product (e.g. `BTC-USD`).
#[derive(Debug, Clone)]
pub struct CoinbaseTickerRow {
    pub product: String,
    pub price: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_bid_qty: Option<f64>,
    pub best_ask_qty: Option<f64>,
    pub volume_24h: Option<f64>,
    pub low_24h: Option<f64>,
    pub high_24h: Option<f64>,
    pub low_52w: Option<f64>,
    pub high_52w: Option<f64>,
    pub pct_chg_24h: Option<f64>,
    /// Per-connection message counter.
    pub sequence_num: u64,
    /// Designated timestamp: Coinbase's send time, in ns.
    pub ts_ns: i64,
    pub received_at_ms: i64,
    pub source: &'static str,
}

/// One level of a Coinbase level2 snapshot, or one update.
#[derive(Debug, Clone)]
pub struct CoinbaseBookRow {
    pub product: String,
    /// `bid` or `offer`.
    pub side: &'static str,
    pub kind: BookKind,
    pub price: f64,
    /// Absolute size at the level; `0` means the level is gone.
    pub size: f64,
    pub sequence_num: u64,
    /// Designated timestamp: the update's `event_time`, in ns.
    pub ts_ns: i64,
    /// Coinbase's send time for the frame, in ns.
    pub received_at_ns: i64,
    pub source: &'static str,
}

/// Anything the ILP writer can persist.
#[derive(Debug, Clone)]
pub enum Row {
    Index(IndexRow),
    Candle(Box<CandleRow>),
    ContractTicker(Box<ContractTickerRow>),
    ContractBook(ContractBookRow),
    CoinbaseTicker(Box<CoinbaseTickerRow>),
    CoinbaseBook(CoinbaseBookRow),
}

impl From<IndexRow> for Row {
    fn from(row: IndexRow) -> Self {
        Row::Index(row)
    }
}

impl From<CandleRow> for Row {
    fn from(row: CandleRow) -> Self {
        Row::Candle(Box::new(row))
    }
}

impl From<ContractTickerRow> for Row {
    fn from(row: ContractTickerRow) -> Self {
        Row::ContractTicker(Box::new(row))
    }
}

impl From<ContractBookRow> for Row {
    fn from(row: ContractBookRow) -> Self {
        Row::ContractBook(row)
    }
}

impl From<CoinbaseTickerRow> for Row {
    fn from(row: CoinbaseTickerRow) -> Self {
        Row::CoinbaseTicker(Box::new(row))
    }
}

impl From<CoinbaseBookRow> for Row {
    fn from(row: CoinbaseBookRow) -> Self {
        Row::CoinbaseBook(row)
    }
}

/// Row count and time span of the rows one market key owns in one table.
///
/// Deliberately count + span only: `sum(CASE WHEN ts > dateadd(...))` and
/// `last/min/max(value)` forced a non-vectorized scan, and the book tables
/// hold tens of millions of rows.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TableSummary {
    pub table: &'static str,
    pub rows: i64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

/// One table plus the symbol column and value that select a market's rows.
#[derive(Debug, Clone, Copy)]
pub struct SummaryKey<'a> {
    pub table: &'static str,
    pub column: &'static str,
    pub key: &'a str,
}

/// The one statement behind [`Questdb::summaries`]. `count()` is the cost (a
/// filtered scan); `min`/`max` of the designated timestamp ride along in the
/// same pass, so one query beats three.
fn summary_sql(key: SummaryKey<'_>) -> String {
    let value = key.key.replace('\'', "''");
    format!(
        "SELECT count() AS rows, cast(min(ts) AS string) AS first_ts, cast(max(ts) AS string) AS last_ts \
         FROM {} WHERE {} = '{value}'",
        key.table, key.column
    )
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
        for table in DataTable::ALL {
            client.batch_execute(&table.ddl()).await.with_context(|| format!("creating {}", table.name()))?;
            tracing::info!(table = table.name(), "ensured questdb table");
        }
        Ok(())
    }

    /// Microseconds since the epoch of the first and last row of `table`, or
    /// `None` when it is empty. Unfiltered on purpose: without a WHERE these
    /// come from partition metadata in well under a second, while a key
    /// filter would make them a scan of the whole table.
    pub async fn data_bounds(&self, table: DataTable) -> Result<Option<(i64, i64)>, tokio_postgres::Error> {
        let client = self.pg().await?;
        let sql = format!("SELECT cast(min(ts) AS long), cast(max(ts) AS long) FROM {}", table.name());
        let row = client.simple_query(&sql).await?.into_iter().find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        });
        Ok(row.and_then(|r| Some((r.get(0)?.parse().ok()?, r.get(1)?.parse().ok()?))))
    }

    /// Every row of `table` for `key` with `ts` in `[start_us, end_us)`, in
    /// `ts` order, over one PGWire connection.
    ///
    /// The range is walked as consecutive half-open time windows, one bounded
    /// query each, sized so a window holds roughly `WINDOW_TARGET_ROWS`: no
    /// single query approaches QuestDB's 60 s `query.timeout.sec`, ties on
    /// `ts` (rife in the book tables) can neither drop nor duplicate rows the
    /// way `LIMIT` paging would, and a client that goes away costs at most
    /// one window. That last point matters because dropping the query stream
    /// does not cancel the query: tokio-postgres keeps paging the rest of the
    /// response before the connection is closed.
    ///
    /// Rows are pulled only as the returned stream is polled, so a slow
    /// consumer backpressures QuestDB instead of buffering here. Connection
    /// failures surface from this call; query failures come through the
    /// stream.
    pub async fn stream_data(
        &self,
        table: DataTable,
        key: String,
        start_us: i64,
        end_us: i64,
    ) -> Result<DataStream, tokio_postgres::Error> {
        let client = self.pg().await?;
        Ok(Box::pin(async_stream::try_stream! {
            let cols = table.columns();
            let mut cursor = start_us;
            let mut size = WINDOW_START;
            while let Some((lo, hi)) = next_window(cursor, end_us, size) {
                let sql = window_sql(table, &key, lo, hi);
                // Bound first: `try_stream!` cannot rewrite a `?` hidden inside `pin!`.
                let rows = client.simple_query_raw(&sql).await?;
                let mut rows = std::pin::pin!(rows);
                let mut n = 0usize;
                while let Some(msg) = rows.try_next().await? {
                    if let SimpleQueryMessage::Row(r) = msg {
                        n += 1;
                        let ts = r.get(cols.len() - 1).unwrap_or_default().to_string();
                        yield DataRow { ts, json: row_json(cols, |i| r.get(i)) };
                    }
                }
                cursor = hi;
                size = next_size(size, n);
            }
        }))
    }

    /// Counts and time spans for `keys`, one summary each, in order.
    ///
    /// Uses the simple query protocol with the key inlined: QuestDB's PGWire
    /// has known issues binding parameters, and every key is schema-validated
    /// (`index_id` to `[A-Z0-9_]+`, `series_ticker` to `[A-Z0-9]+`, `product`
    /// to `[A-Z0-9]+-[A-Z0-9]+`) so inlining is safe (quotes are escaped
    /// regardless).
    ///
    /// The queries run sequentially on one connection: each is a filtered
    /// scan of a table the ILP writer is also busy on, so issuing them
    /// concurrently only lengthens them all. Fails on the first error so
    /// callers keep their previous snapshot.
    pub async fn summaries(&self, keys: &[SummaryKey<'_>]) -> Result<Vec<TableSummary>, tokio_postgres::Error> {
        let client = self.pg().await?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let row = client.simple_query(&summary_sql(*key)).await?.into_iter().find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            });
            let col = |i: usize| -> Option<String> { row.as_ref().and_then(|r| r.get(i)).map(str::to_string) };
            out.push(TableSummary {
                table: key.table,
                rows: col(0).and_then(|s| s.parse().ok()).unwrap_or(0),
                first_ts: col(1),
                last_ts: col(2),
            });
        }
        Ok(out)
    }

    /// Rows `index_values_hist` already holds for one index within
    /// `[start_ms, end_ms)`, counted per calendar-aligned `sample_by` bucket
    /// (a QuestDB interval such as `1h`) and keyed by bucket start in ms.
    /// See `summaries` for why the id is inlined.
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
}

/// One exported row: its `ts` as QuestDB prints it (RFC3339, microseconds)
/// and every column as a JSON object.
#[derive(Debug, Clone)]
pub struct DataRow {
    pub ts: String,
    pub json: Value,
}

pub type DataStream = Pin<Box<dyn Stream<Item = Result<DataRow, tokio_postgres::Error>> + Send>>;

/// First window of a data stream, in µs.
const WINDOW_START: i64 = 60 * 1_000_000;
const WINDOW_MIN: i64 = 1_000_000;
const WINDOW_MAX: i64 = 24 * 3_600 * 1_000_000;
/// Rows a window aims for: about 2.5 minutes of `contract_book_live` at
/// today's rate, or a day of candles.
const WINDOW_TARGET_ROWS: usize = 50_000;

/// The half-open window starting at `cursor`, clipped to `end_us`; `None`
/// once the range is exhausted.
fn next_window(cursor: i64, end_us: i64, size_us: i64) -> Option<(i64, i64)> {
    (cursor < end_us).then(|| (cursor, cursor.saturating_add(size_us).min(end_us)))
}

/// The next window size after one of `size_us` returned `rows`: doubled
/// while sparse, halved while dense, clamped to `[WINDOW_MIN, WINDOW_MAX]`.
fn next_size(size_us: i64, rows: usize) -> i64 {
    let next = if rows > WINDOW_TARGET_ROWS {
        size_us / 2
    } else if rows < WINDOW_TARGET_ROWS / 4 {
        size_us.saturating_mul(2)
    } else {
        size_us
    };
    next.clamp(WINDOW_MIN, WINDOW_MAX)
}

/// One window's query: every column by name (timestamps pre-formatted by
/// QuestDB so they arrive as RFC3339 text), ordered by the designated
/// timestamp, which is free. `key` comes from a schema-validated market
/// document, never from the client; see `summaries` for the inlining.
fn window_sql(table: DataTable, key: &str, start_us: i64, end_us: i64) -> String {
    let cols = table
        .columns()
        .iter()
        .map(|c| match c.kind {
            ColKind::Timestamp => format!("cast({0} AS string) AS {0}", c.name),
            _ => c.name.to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let key = key.replace('\'', "''");
    format!(
        "SELECT {cols} FROM {} WHERE {} = '{key}' AND ts >= '{}' AND ts < '{}' ORDER BY ts",
        table.name(),
        table.key_column(),
        ts_literal_us(start_us),
        ts_literal_us(end_us),
    )
}

/// A simple-query row as a JSON object: `get(i)` is column `i`'s text, or
/// `None` for SQL NULL. Numbers become JSON numbers (a non-finite double
/// becomes null), everything else a string.
fn row_json<'a>(cols: &[Column], get: impl Fn(usize) -> Option<&'a str>) -> Value {
    let fields = cols.iter().enumerate().map(|(i, c)| {
        let value = match (get(i), c.kind) {
            (None, _) => Value::Null,
            (Some(s), ColKind::Double) => {
                s.parse::<f64>().ok().and_then(Number::from_f64).map_or(Value::Null, Value::Number)
            }
            (Some(s), ColKind::Long) => s.parse::<i64>().ok().map_or(Value::Null, Value::from),
            (Some(s), ColKind::Text | ColKind::Timestamp) => Value::String(s.to_string()),
        };
        (c.name.to_string(), value)
    });
    Value::Object(fields.collect())
}

/// `ms` as a timestamp literal QuestDB compares against `ts`.
fn ts_literal(ms: i64) -> String {
    ts_literal_us(ms * 1_000)
}

/// `us` (microseconds, `ts`'s own precision) as a timestamp literal.
fn ts_literal_us(us: i64) -> String {
    DateTime::<Utc>::from_timestamp_micros(us).unwrap_or_default().to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn push(buf: &mut Buffer, row: &Row) -> questdb::Result<()> {
    match row {
        Row::Index(row) => push_index(buf, row),
        Row::Candle(row) => push_candle(buf, row),
        Row::ContractTicker(row) => push_contract_ticker(buf, row),
        Row::ContractBook(row) => push_contract_book(buf, row),
        Row::CoinbaseTicker(row) => push_coinbase_ticker(buf, row),
        Row::CoinbaseBook(row) => push_coinbase_book(buf, row),
    }
}

fn micros(ms: i64) -> TimestampMicros {
    TimestampMicros::new(ms * 1_000)
}

fn push_contract_ticker(buf: &mut Buffer, row: &ContractTickerRow) -> questdb::Result<()> {
    buf.table(CONTRACT_TICKER_TABLE)?
        .symbol("ticker", row.ticker.as_str())?
        .symbol("series_ticker", row.series_ticker.as_str())?
        .symbol("source", row.source)?
        .column_f64_opt("floor_strike", row.floor_strike)?
        .column_f64_opt("price", row.price)?
        .column_f64_opt("yes_bid", row.yes_bid)?
        .column_f64_opt("yes_ask", row.yes_ask)?
        .column_f64_opt("yes_bid_size", row.yes_bid_size)?
        .column_f64_opt("yes_ask_size", row.yes_ask_size)?
        .column_f64_opt("last_trade_size", row.last_trade_size)?
        .column_f64_opt("volume", row.volume)?
        .column_f64_opt("open_interest", row.open_interest)?
        .column_i64_opt("dollar_volume", row.dollar_volume)?
        .column_i64_opt("dollar_open_interest", row.dollar_open_interest)?
        .column_ts("received_at", micros(row.received_at_ms))?;
    buf.at(TimestampNanos::new(row.ts_ms * 1_000_000))
}

fn push_contract_book(buf: &mut Buffer, row: &ContractBookRow) -> questdb::Result<()> {
    buf.table(CONTRACT_BOOK_TABLE)?
        .symbol("ticker", row.ticker.as_str())?
        .symbol("series_ticker", row.series_ticker.as_str())?
        .symbol("side", row.side)?
        .symbol("kind", row.kind.as_str())?
        .symbol("source", row.source)?
        .column_f64("price", row.price)?
        .column_f64("size", row.size)?
        .column_i64("seq", row.seq as i64)?
        .column_ts("received_at", micros(row.received_at_ms))?;
    buf.at(TimestampNanos::new(row.ts_ms * 1_000_000))
}

fn push_coinbase_ticker(buf: &mut Buffer, row: &CoinbaseTickerRow) -> questdb::Result<()> {
    buf.table(COINBASE_TICKER_TABLE)?
        .symbol("product", row.product.as_str())?
        .symbol("source", row.source)?
        .column_f64_opt("price", row.price)?
        .column_f64_opt("best_bid", row.best_bid)?
        .column_f64_opt("best_ask", row.best_ask)?
        .column_f64_opt("best_bid_qty", row.best_bid_qty)?
        .column_f64_opt("best_ask_qty", row.best_ask_qty)?
        .column_f64_opt("volume_24h", row.volume_24h)?
        .column_f64_opt("low_24h", row.low_24h)?
        .column_f64_opt("high_24h", row.high_24h)?
        .column_f64_opt("low_52w", row.low_52w)?
        .column_f64_opt("high_52w", row.high_52w)?
        .column_f64_opt("pct_chg_24h", row.pct_chg_24h)?
        .column_i64("sequence_num", row.sequence_num as i64)?
        .column_ts("received_at", micros(row.received_at_ms))?;
    buf.at(TimestampNanos::new(row.ts_ns))
}

fn push_coinbase_book(buf: &mut Buffer, row: &CoinbaseBookRow) -> questdb::Result<()> {
    buf.table(COINBASE_BOOK_TABLE)?
        .symbol("product", row.product.as_str())?
        .symbol("side", row.side)?
        .symbol("kind", row.kind.as_str())?
        .symbol("source", row.source)?
        .column_f64("price", row.price)?
        .column_f64("size", row.size)?
        .column_i64("sequence_num", row.sequence_num as i64)?
        .column_ts("received_at", TimestampMicros::new(row.received_at_ns / 1_000))?;
    buf.at(TimestampNanos::new(row.ts_ns))
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
        b.column_ts("received_at", micros(received))?;
    }
    buf.at(TimestampNanos::new(row.ts_ms * 1_000_000))
}

/// `first` plus whatever else arrives on `rx` before `deadline`, capped at
/// `ILP_BATCH`. Returns early when the channel closes so shutdown does not
/// wait out the window. Polls rather than blocking with a timeout: the
/// writer lives on a plain OS thread (`Sender::flush` is blocking), and a
/// few-millisecond wobble is nothing against a half-second window.
fn collect_batch(rx: &mut mpsc::Receiver<Row>, first: Row, deadline: Instant) -> Vec<Row> {
    let mut batch = vec![first];
    while batch.len() < ILP_BATCH {
        match rx.try_recv() {
            Ok(row) => batch.push(row),
            Err(TryRecvError::Empty) => {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                std::thread::sleep(DRAIN_POLL.min(deadline - now));
            }
            Err(TryRecvError::Disconnected) => break,
        }
    }
    batch
}

/// Owns the sync `questdb-rs` sender on its own OS thread. Blocks on the
/// first row, then keeps collecting for `FLUSH_INTERVAL` (or until
/// `ILP_BATCH` rows are queued) and writes the lot in one flush, so live
/// traffic lands within half a second and QuestDB gets a couple of commits
/// per second rather than one per row.
fn start_ilp_writer(conf: String, mut rx: mpsc::Receiver<Row>) -> Result<()> {
    let mut sender = Sender::from_conf(&conf).context("QUESTDB_ILP_CONF")?;
    let mut buf = sender.new_buffer();
    std::thread::Builder::new()
        .name("questdb-ilp".into())
        .spawn(move || {
            while let Some(first) = rx.blocking_recv() {
                let started = Instant::now();
                let batch = collect_batch(&mut rx, first, started + FLUSH_INTERVAL);
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
                    Ok(()) => {
                        tracing::debug!(rows = batch.len(), ms = started.elapsed().as_millis(), "ilp flush")
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> Row {
        Row::Index(IndexRow {
            table: Table::Live,
            index_id: "BRTI".into(),
            value: 1.0,
            ts_ms: 0,
            received_at_ms: None,
            source: "t",
        })
    }

    #[test]
    fn summary_sql_escapes_and_targets_the_key() {
        let sql = summary_sql(SummaryKey { table: CONTRACT_BOOK_TABLE, column: "series_ticker", key: "KXBTC15M" });
        assert!(sql.ends_with("FROM contract_book_live WHERE series_ticker = 'KXBTC15M'"), "{sql}");
        for needed in ["count()", "min(ts)", "max(ts)"] {
            assert!(sql.contains(needed), "{sql}");
        }
        for gone in ["dateadd", "now()", "value"] {
            assert!(!sql.contains(gone), "{sql}");
        }
        let sql = summary_sql(SummaryKey { table: "t", column: "c", key: "O'Brien" });
        assert!(sql.ends_with("WHERE c = 'O''Brien'"), "{sql}");
    }

    #[test]
    fn batch_caps_at_ilp_batch() {
        let (tx, mut rx) = mpsc::channel(ILP_BATCH + 100);
        for _ in 0..ILP_BATCH + 10 {
            tx.try_send(row()).unwrap();
        }
        let started = Instant::now();
        let batch = collect_batch(&mut rx, row(), started + Duration::from_secs(10));
        assert_eq!(batch.len(), ILP_BATCH);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn batch_waits_out_the_window_for_late_rows() {
        let (tx, mut rx) = mpsc::channel(8);
        tx.try_send(row()).unwrap();
        let late = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            tx.try_send(row()).unwrap();
        });
        let batch = collect_batch(&mut rx, row(), Instant::now() + Duration::from_millis(200));
        late.join().unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn batch_stops_when_the_channel_closes() {
        let (tx, mut rx) = mpsc::channel(8);
        tx.try_send(row()).unwrap();
        tx.try_send(row()).unwrap();
        drop(tx);
        let started = Instant::now();
        let batch = collect_batch(&mut rx, row(), started + Duration::from_secs(10));
        assert_eq!(batch.len(), 3);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn batch_returns_at_the_deadline_when_idle() {
        let (_tx, mut rx) = mpsc::channel::<Row>(8);
        let started = Instant::now();
        let batch = collect_batch(&mut rx, row(), started + Duration::from_millis(30));
        assert_eq!(batch.len(), 1);
        assert!(started.elapsed() >= Duration::from_millis(30));
    }

    /// End-to-end against a real QuestDB (`docker compose up -d questdb`):
    /// ILP rows land, `summaries` counts them, and `stream_data` walks them
    /// back in order across several windows. Idempotent thanks to DEDUP.
    /// Run with `cargo test -- --ignored live_questdb`.
    #[tokio::test]
    #[ignore]
    async fn live_questdb_roundtrip() {
        let conninfo = "host=localhost port=8812 user=admin password=quest dbname=qdb".to_string();
        let db = Questdb::connect("http::addr=localhost:9000;", conninfo).await.unwrap();
        const N: i64 = 3_000;
        let base_ms: i64 = 1_700_000_000_000; // 2023-11-14T22:13:20Z
        for i in 0..N {
            let row = IndexRow {
                table: Table::Live,
                index_id: "ZZTEST".into(),
                value: i as f64,
                ts_ms: base_ms + i * 1_000,
                received_at_ms: (i % 2 == 0).then_some(base_ms + i * 1_000 + 5),
                source: "test",
            };
            db.ilp.send(row.into()).await.unwrap();
        }

        let key = SummaryKey { table: Table::Live.name(), column: "index_id", key: "ZZTEST" };
        let started = Instant::now();
        let summary = loop {
            let s = db.summaries(&[key]).await.unwrap().remove(0);
            if s.rows >= N {
                break s;
            }
            assert!(started.elapsed() < Duration::from_secs(30), "only {} rows after 30 s", s.rows);
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        assert_eq!(summary.rows, N);
        assert_eq!(summary.first_ts.as_deref(), Some("2023-11-14T22:13:20.000000Z"));
        assert_eq!(summary.last_ts.as_deref(), Some("2023-11-14T23:03:19.000000Z"));

        let (min_us, max_us) = db.data_bounds(DataTable::IndexValuesLive).await.unwrap().unwrap();
        let (first_us, last_us) = (base_ms * 1_000, (base_ms + (N - 1) * 1_000) * 1_000);
        assert!(min_us <= first_us && max_us >= last_us, "{min_us} {max_us}");

        // 50 minutes of one row per second from a 60 s first window: several
        // windows, growing as they come back sparse.
        let stream = db.stream_data(DataTable::IndexValuesLive, "ZZTEST".into(), first_us, last_us + 1).await.unwrap();
        let rows: Vec<DataRow> = stream.try_collect().await.unwrap();
        assert_eq!(rows.len(), N as usize);
        assert!(rows.windows(2).all(|w| w[0].ts < w[1].ts), "rows out of order");
        assert_eq!(
            rows[0].json,
            serde_json::json!({
                "index_id": "ZZTEST", "source": "test", "value": 0.0,
                "received_at": "2023-11-14T22:13:20.005000Z", "ts": "2023-11-14T22:13:20.000000Z"
            })
        );
        assert_eq!(rows[1].json["received_at"], Value::Null);
        assert_eq!(rows[1].json["value"], serde_json::json!(1.0));

        // A sub-range is half-open on both ends.
        let stream = db
            .stream_data(
                DataTable::IndexValuesLive,
                "ZZTEST".into(),
                first_us + 1_000_000_000,
                first_us + 1_010_000_000,
            )
            .await
            .unwrap();
        let rows: Vec<DataRow> = stream.try_collect().await.unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0].ts, "2023-11-14T22:30:00.000000Z");

        // Nothing for an unknown key.
        let stream = db.stream_data(DataTable::IndexValuesLive, "NOPE".into(), first_us, last_us + 1).await.unwrap();
        assert!(stream.try_collect::<Vec<_>>().await.unwrap().is_empty());
    }

    #[test]
    fn data_table_parses_names_and_aliases() {
        for t in DataTable::ALL {
            assert_eq!(DataTable::parse(t.name()), Some(t));
            assert_eq!(DataTable::parse(t.alias()), Some(t));
            assert_eq!(DataTable::parse(&t.name().replace('_', "-").to_uppercase()), Some(t));
        }
        assert_eq!(DataTable::parse("book"), Some(DataTable::ContractBookLive));
        assert_eq!(DataTable::parse("coinbase_book"), Some(DataTable::CoinbaseBookLive));
        assert_eq!(DataTable::parse("index_values_live"), Some(DataTable::IndexValuesLive));
        assert_eq!(DataTable::parse("orders"), None);
        assert_eq!(DataTable::parse(""), None);
        assert_eq!(DataTable::IndexValuesHist.key_column(), "index_id");
        assert_eq!(DataTable::ContractCandlesHist.key_column(), "series_ticker");
        assert_eq!(DataTable::CoinbaseTickerLive.key_column(), "product");
    }

    /// The exported column list must be exactly what `ensure_tables` creates.
    #[test]
    fn columns_match_the_ddl() {
        for t in DataTable::ALL {
            let ddl = t.ddl();
            let open = ddl.find('(').unwrap();
            let close = ddl.find(") TIMESTAMP(ts)").unwrap();
            let in_ddl: Vec<&str> =
                ddl[open + 1..close].split(',').map(|c| c.trim().split(' ').next().unwrap()).collect();
            let listed: Vec<&str> = t.columns().iter().map(|c| c.name).collect();
            assert_eq!(listed, in_ddl, "{}", t.name());
            assert_eq!(listed.last(), Some(&"ts"));
            assert!(ddl.starts_with(&format!("CREATE TABLE IF NOT EXISTS {} (", t.name())));
        }
    }

    #[test]
    fn windows_tile_the_range_exactly() {
        let (start, end) = (1_000, 3_500);
        let mut cursor = start;
        let mut seen = Vec::new();
        while let Some((lo, hi)) = next_window(cursor, end, 1_000) {
            assert!(lo < hi && hi <= end);
            seen.push((lo, hi));
            cursor = hi;
        }
        assert_eq!(seen, vec![(1_000, 2_000), (2_000, 3_000), (3_000, 3_500)]);
        assert_eq!(next_window(end, end, 1_000), None);
        assert_eq!(next_window(i64::MAX - 5, i64::MAX, 1_000), Some((i64::MAX - 5, i64::MAX)));
    }

    #[test]
    fn window_size_adapts_and_clamps() {
        let s = WINDOW_START;
        assert_eq!(next_size(s, 0), 2 * s);
        assert_eq!(next_size(s, WINDOW_TARGET_ROWS / 4 - 1), 2 * s);
        assert_eq!(next_size(s, WINDOW_TARGET_ROWS / 4), s);
        assert_eq!(next_size(s, WINDOW_TARGET_ROWS), s);
        assert_eq!(next_size(s, WINDOW_TARGET_ROWS + 1), s / 2);
        assert_eq!(next_size(WINDOW_MIN, usize::MAX), WINDOW_MIN);
        assert_eq!(next_size(WINDOW_MAX, 0), WINDOW_MAX);
        assert_eq!(next_size(i64::MAX, 0), WINDOW_MAX);
    }

    #[test]
    fn window_sql_lists_columns_and_bounds() {
        let sql = window_sql(DataTable::CoinbaseBookLive, "BTC-USD", 0, 1_500_000);
        assert_eq!(
            sql,
            "SELECT product, side, kind, source, price, size, sequence_num, \
             cast(received_at AS string) AS received_at, cast(ts AS string) AS ts \
             FROM coinbase_book_live WHERE product = 'BTC-USD' \
             AND ts >= '1970-01-01T00:00:00.000000Z' AND ts < '1970-01-01T00:00:01.500000Z' ORDER BY ts"
        );
        assert!(window_sql(DataTable::IndexValuesLive, "O'B", 0, 1).contains("index_id = 'O''B'"));
    }

    #[test]
    fn row_json_types_values() {
        let cols = [
            col("s", ColKind::Text),
            col("d", ColKind::Double),
            col("l", ColKind::Long),
            col("t", ColKind::Timestamp),
            col("n", ColKind::Double),
            col("nan", ColKind::Double),
            col("bad", ColKind::Long),
        ];
        let values = [
            Some("BTC-USD"),
            Some("0.42"),
            Some("-7"),
            Some("2026-09-24T00:00:00.000001Z"),
            None,
            Some("NaN"),
            Some("x"),
        ];
        let json = row_json(&cols, |i| values[i]);
        assert_eq!(
            json,
            serde_json::json!({
                "s": "BTC-USD", "d": 0.42, "l": -7, "t": "2026-09-24T00:00:00.000001Z",
                "n": null, "nan": null, "bad": null
            })
        );
    }
}
