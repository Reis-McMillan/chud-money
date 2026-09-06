//! QuestDB: ILP/HTTP ingestion on a dedicated blocking thread, PGWire queries.
//!
//! Two tables with an identical column set hold index values from different
//! sources: `index_values_live` (5Hz websocket) and `index_values_hist`
//! (REST pass-through backfill). Both are DEDUP'd on `(ts, index_id)` so
//! re-ingesting a window or overlapping after a reconnect is idempotent.

use anyhow::{Context, Result};
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
    pub ilp: mpsc::Sender<IndexRow>,
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
}

fn push(buf: &mut Buffer, row: &IndexRow) -> questdb::Result<()> {
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
fn start_ilp_writer(conf: String, mut rx: mpsc::Receiver<IndexRow>) -> Result<()> {
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
