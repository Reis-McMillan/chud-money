//! Offload of old QuestDB partitions to Google Cloud Storage as Parquet.
//!
//! Open-source QuestDB has no tiered storage (that is Enterprise), but it can
//! stream any query result as Parquet from `GET /exp?fmt=parquet`, with the
//! codec set server-side (`cairo.parquet.export.compression.codec`). So the
//! app only moves bytes: every `RUN_INTERVAL`, for each table and each UTC
//! day at least `ARCHIVE_AFTER_DAYS` old that still has a partition, it
//!
//! 1. exports the day one hour at a time and streams each hour into
//!    `gs://<bucket>/<prefix>/<table>/date=<YYYY-MM-DD>/<HH>.parquet` (an
//!    hour bounds both the export's run time against QuestDB's query timeout
//!    and the size of any one upload);
//! 2. reads each object's Parquet footer back and checks its row count
//!    against QuestDB's count for that hour;
//! 3. drops the partition, only once every hour matched and a recount shows
//!    nothing arrived in the meantime.
//!
//! GCS is the record of progress: an hour whose object already holds the
//! expected number of rows is not uploaded again, so a run cut short (a
//! restart, an error) resumes where it stopped. A day that keeps failing is
//! logged and retried on the next run; it is never dropped unverified.
//!
//! Archived data is not readable through this API afterwards. Read it from
//! the bucket directly (DuckDB, BigQuery external tables, pandas).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use chrono::{NaiveDate, Utc};
use futures_util::TryStreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{FooterTail, ParquetMetaDataReader};
use tokio::time::{Instant, interval_at};

use crate::config::ArchiveConfig;
use crate::db::questdb::{DataTable, Questdb, day_bounds_us, ts_literal_us};

const RUN_INTERVAL: Duration = Duration::from_secs(6 * 3_600);
/// Let the feeds connect and settle before the first export competes with
/// them for QuestDB's CPU.
const STARTUP_DELAY: Duration = Duration::from_secs(5 * 60);
/// One hour of the busiest book table is a few hundred MB of Parquet at
/// most; this only catches a wedged connection.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Multipart part size, which is also what an upload holds in memory per
/// in-flight part.
const PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_INFLIGHT_PARTS: usize = 2;

/// How long to wait for QuestDB to apply already-committed WAL transactions
/// before counting a day's rows.
const WAL_WAIT: Duration = Duration::from_secs(60);
const WAL_POLL: Duration = Duration::from_secs(1);

const HOUR_US: i64 = 3_600 * 1_000_000;

/// Builds the GCS client and starts the archive loop. Credentials come from
/// `GOOGLE_APPLICATION_CREDENTIALS` (a service-account key file) or the other
/// `GOOGLE_*` variables `object_store` understands.
pub fn spawn(questdb: Questdb, config: ArchiveConfig) -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(
        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&config.bucket)
            .build()
            .with_context(|| format!("configuring GCS bucket {}", config.bucket))?,
    );
    let http = reqwest::Client::builder().timeout(EXPORT_TIMEOUT).build().context("building QuestDB HTTP client")?;
    let archiver = Archiver { questdb, store, http, config };
    tracing::info!(
        bucket = %archiver.config.bucket,
        prefix = %archiver.config.prefix,
        after_days = archiver.config.after_days,
        "questdb archive enabled"
    );
    tokio::spawn(archiver.run());
    Ok(())
}

struct Archiver {
    questdb: Questdb,
    store: Arc<dyn ObjectStore>,
    http: reqwest::Client,
    config: ArchiveConfig,
}

impl Archiver {
    async fn run(self) {
        let mut tick = interval_at(Instant::now() + STARTUP_DELAY, RUN_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.run_once().await;
        }
    }

    /// One pass over every table. Failures are per day: one bad day does not
    /// hold up the rest.
    async fn run_once(&self) {
        let cutoff = cutoff(Utc::now().date_naive(), self.config.after_days);
        for table in DataTable::ALL {
            let days = match self.questdb.partition_days(table).await {
                Ok(days) => days,
                Err(e) => {
                    tracing::warn!(table = table.name(), error = %e, "archive: listing partitions failed");
                    continue;
                }
            };
            for day in days.into_iter().filter(|d| *d < cutoff) {
                let started = std::time::Instant::now();
                match self.archive_day(table, day).await {
                    Ok(rows) => tracing::info!(
                        table = table.name(),
                        %day,
                        rows,
                        elapsed_s = started.elapsed().as_secs(),
                        "archive: day offloaded and partition dropped"
                    ),
                    Err(e) => {
                        tracing::warn!(table = table.name(), %day, error = format!("{e:#}"), "archive: day skipped")
                    }
                }
            }
        }
    }

    /// Uploads and verifies every hour of `day`, then drops its partition.
    /// Returns the number of rows archived.
    async fn archive_day(&self, table: DataTable, day: NaiveDate) -> Result<u64> {
        // A count taken while WAL transactions are still pending would miss
        // their rows, and they would be dropped without being archived.
        self.wait_for_wal(table).await?;
        let hours = self.questdb.hour_counts(table, day).await?;
        for &(hour, rows) in &hours {
            let path = object_path(&self.config.prefix, table, day, hour);
            if parquet_rows(self.store.as_ref(), &path).await? == Some(rows) {
                continue;
            }
            self.export_hour(table, day, hour, &path).await.with_context(|| format!("exporting {path}"))?;
            match parquet_rows(self.store.as_ref(), &path).await? {
                Some(n) if n == rows => {}
                Some(n) => bail!("{path} holds {n} rows, QuestDB has {rows}"),
                None => bail!("{path} is missing or not a readable Parquet file after upload"),
            }
        }
        self.wait_for_wal(table).await?;
        let recount = self.questdb.hour_counts(table, day).await?;
        ensure!(recount == hours, "rows changed while archiving; retrying next run");
        self.questdb.drop_day(table, day).await.context("dropping partition")?;
        Ok(hours.iter().map(|(_, n)| n).sum())
    }

    /// Waits until every WAL transaction committed to `table` before this
    /// call has been applied. Under live ingestion the table is rarely fully
    /// caught up at any instant, so the target is the sequencer position now,
    /// not "no pending transactions".
    async fn wait_for_wal(&self, table: DataTable) -> Result<()> {
        let status = self.questdb.wal_status(table).await?.context("table not listed in wal_tables()")?;
        let target = status.sequencer_txn;
        let deadline = Instant::now() + WAL_WAIT;
        let mut status = status;
        loop {
            ensure!(!status.suspended, "table is suspended");
            if status.writer_txn >= target {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "WAL apply is lagging (writer at {}, waiting for {target})",
                status.writer_txn
            );
            tokio::time::sleep(WAL_POLL).await;
            status = self.questdb.wal_status(table).await?.context("table not listed in wal_tables()")?;
        }
    }

    /// Streams one hour of `table` from QuestDB's `/exp` straight into a
    /// multipart upload at `path`; nothing larger than a few parts is held in
    /// memory.
    async fn export_hour(&self, table: DataTable, day: NaiveDate, hour: u32, path: &Path) -> Result<()> {
        let sql = hour_sql(table, day, hour);
        let response = self
            .http
            .get(format!("{}/exp", self.config.questdb_http_url))
            .query(&[("query", sql.as_str()), ("fmt", "parquet")])
            .basic_auth(&self.config.questdb_user, Some(&self.config.questdb_password))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("questdb /exp returned {status}: {body}");
        }

        let upload = self.store.put_multipart(path).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, PART_SIZE);
        let mut body = response.bytes_stream();
        let copied: Result<()> = async {
            while let Some(chunk) = body.try_next().await? {
                writer.wait_for_capacity(MAX_INFLIGHT_PARTS).await?;
                writer.put(chunk);
            }
            Ok(())
        }
        .await;
        match copied {
            Ok(()) => {
                writer.finish().await?;
                Ok(())
            }
            Err(e) => {
                // Best effort: an abandoned multipart upload otherwise lingers
                // (and is billed) until a bucket lifecycle rule removes it.
                let _ = writer.abort().await;
                Err(e)
            }
        }
    }
}

/// Days strictly before this are archived: today and the `after_days - 1`
/// days before it stay in QuestDB.
fn cutoff(today: NaiveDate, after_days: u32) -> NaiveDate {
    today - chrono::Days::new(u64::from(after_days.saturating_sub(1)))
}

/// `<prefix>/<table>/date=<YYYY-MM-DD>/<HH>.parquet`: Hive-style on the
/// day, so DuckDB, Spark and BigQuery read `date` as a column.
fn object_path(prefix: &str, table: DataTable, day: NaiveDate, hour: u32) -> Path {
    let key = format!("{}/date={}/{hour:02}.parquet", table.name(), day.format("%Y-%m-%d"));
    if prefix.is_empty() { Path::from(key) } else { Path::from(format!("{prefix}/{key}")) }
}

fn hour_sql(table: DataTable, day: NaiveDate, hour: u32) -> String {
    let start = day_bounds_us(day).0 + i64::from(hour) * HOUR_US;
    format!(
        "SELECT * FROM {} WHERE ts >= '{}' AND ts < '{}'",
        table.name(),
        ts_literal_us(start),
        ts_literal_us(start + HOUR_US)
    )
}

/// The row count recorded in the Parquet footer of the object at `path`;
/// `None` if there is no object, or it is not a complete Parquet file (a
/// truncated export ends without a footer).
async fn parquet_rows(store: &dyn ObjectStore, path: &Path) -> Result<Option<u64>> {
    let size = match store.head(path).await {
        Ok(meta) => meta.size,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    // Leading magic, then at least the footer.
    let tail_len = FOOTER_SIZE as u64;
    if size < tail_len + 4 {
        return Ok(None);
    }
    let tail = store.get_range(path, size - tail_len..size).await?;
    let Ok(tail) = FooterTail::try_from(&tail[..]) else { return Ok(None) };
    let meta_len = tail.metadata_length() as u64;
    if tail.is_encrypted_footer() || meta_len + tail_len + 4 > size {
        return Ok(None);
    }
    let meta = store.get_range(path, size - tail_len - meta_len..size - tail_len).await?;
    let Ok(meta) = ParquetMetaDataReader::decode_metadata(&meta) else { return Ok(None) };
    Ok(u64::try_from(meta.file_metadata().num_rows()).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use parquet::data_type::Int64Type;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    fn day(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    fn parquet_file(values: &[i64]) -> Bytes {
        let schema = Arc::new(parse_message_type("message m { required int64 x; }").unwrap());
        let mut out = Vec::new();
        let mut writer = SerializedFileWriter::new(&mut out, schema, Arc::new(WriterProperties::default())).unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column.typed::<Int64Type>().write_batch(values, None, None).unwrap();
        column.close().unwrap();
        group.close().unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    #[test]
    fn cutoff_keeps_today_and_the_days_before_it() {
        // after_days = 7 on Nov 8 keeps Nov 2..=8 and archives Nov 1 and earlier.
        let c = cutoff(day("2026-11-08"), 7);
        assert_eq!(c, day("2026-11-02"));
        assert!(day("2026-11-01") < c);
        // after_days = 1 keeps only today.
        assert_eq!(cutoff(day("2026-11-08"), 1), day("2026-11-08"));
    }

    #[test]
    fn object_path_layout() {
        let p = object_path("", DataTable::CoinbaseBookLive, day("2026-10-03"), 7);
        assert_eq!(p.as_ref(), "coinbase_book_live/date=2026-10-03/07.parquet");
        let p = object_path("questdb/archive", DataTable::IndexValuesLive, day("2026-10-03"), 23);
        assert_eq!(p.as_ref(), "questdb/archive/index_values_live/date=2026-10-03/23.parquet");
    }

    #[test]
    fn hour_sql_selects_one_hour() {
        assert_eq!(
            hour_sql(DataTable::ContractBookLive, day("2026-10-03"), 23),
            "SELECT * FROM contract_book_live WHERE ts >= '2026-10-03T23:00:00.000000Z' \
             AND ts < '2026-10-04T00:00:00.000000Z'"
        );
    }

    #[tokio::test]
    async fn parquet_rows_reads_the_footer_and_rejects_partial_files() {
        let store = InMemory::new();
        let path = Path::from("t/date=2026-10-03/00.parquet");
        assert_eq!(parquet_rows(&store, &path).await.unwrap(), None);

        let file = parquet_file(&[1, 2, 3]);
        store.put(&path, file.clone().into()).await.unwrap();
        assert_eq!(parquet_rows(&store, &path).await.unwrap(), Some(3));

        store.put(&path, file.slice(..file.len() - 1).into()).await.unwrap();
        assert_eq!(parquet_rows(&store, &path).await.unwrap(), None);

        store.put(&path, Bytes::from_static(b"PAR1").into()).await.unwrap();
        assert_eq!(parquet_rows(&store, &path).await.unwrap(), None);
    }
}
