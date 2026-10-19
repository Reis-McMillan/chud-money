//! Per-market QuestDB summary cache.
//!
//! `GET /{tag}` used to run seven aggregates inline; against tens of millions
//! of book rows that took tens of seconds and sometimes tripped QuestDB's
//! query timeout. Instead one task per market maintains the summaries in
//! `FeedShared::summary`, refreshed every `REFRESH_INTERVAL`, and the handler
//! serves the last snapshot without touching QuestDB.
//!
//! Counting is incremental: each table's rows are counted once, in windows
//! of at most a day, and a refresh counts only what landed after the previous
//! one (see `Questdb::advance_summaries`). A full recount of the book table
//! had outgrown QuestDB's 60 s query timeout. The first pass after a start
//! walks the whole history a day at a time and may span several refreshes;
//! the snapshot stays empty until it completes. Rows written behind the
//! cursor afterwards (out-of-order feeds, historical backfills) are missed
//! until the next restart.
//!
//! Refreshes are serialized process-wide (each competes with the ILP writer
//! for the same CPU), so with N markets the effective period is
//! `max(REFRESH_INTERVAL, N × refresh duration)`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::time::{MissedTickBehavior, interval, timeout};

use super::FeedShared;
use crate::db::questdb::{DataTable, SummaryKey, SummaryProgress, TableSummary};
use crate::model::market::Market;
use crate::state::AppState;

const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Guard against a wedged connection; QuestDB's own limit is 60 s per query.
/// Progress made before the deadline is kept, so a long first pass resumes
/// on the next tick rather than starting over.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(120);
/// One market's refresh at a time, whatever the number of markets.
static REFRESH_SLOT: Semaphore = Semaphore::const_new(1);

/// What QuestDB holds for one market. `None` fields: the market names no
/// `coinbase_product`.
#[derive(Debug, Clone, Serialize)]
pub struct QuestdbSummary {
    pub live: TableSummary,
    pub hist: TableSummary,
    pub contracts: TableSummary,
    pub contract_ticker: TableSummary,
    pub contract_book: TableSummary,
    pub coinbase_ticker: Option<TableSummary>,
    pub coinbase_book: Option<TableSummary>,
}

/// The cached summaries plus how fresh they are.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SummarySnapshot {
    /// `None` until the first refresh of this feed completes.
    pub tables: Option<QuestdbSummary>,
    /// When `tables` was computed, not when the last attempt ran.
    pub refreshed_at: Option<DateTime<Utc>>,
    /// Why the last attempt failed; `tables` is then stale but still served.
    pub error: Option<String>,
}

/// The tables and keys that make up a market's summary, in the order
/// [`QuestdbSummary`] lists them; the coinbase pair only with a product.
fn summary_keys(market: &Market) -> Vec<SummaryKey<'_>> {
    let index = market.index_id.as_str();
    let series = market.series_ticker.as_str();
    let mut keys = vec![
        SummaryKey { table: DataTable::IndexValuesLive, key: index },
        SummaryKey { table: DataTable::IndexValuesHist, key: index },
        SummaryKey { table: DataTable::ContractCandlesHist, key: series },
        SummaryKey { table: DataTable::ContractTickerLive, key: series },
        SummaryKey { table: DataTable::ContractBookLive, key: series },
    ];
    if let Some(product) = market.coinbase_product.as_deref() {
        keys.push(SummaryKey { table: DataTable::CoinbaseTickerLive, key: product });
        keys.push(SummaryKey { table: DataTable::CoinbaseBookLive, key: product });
    }
    keys
}

/// Brings `progress` (one entry per key of `summary_keys`, kept across
/// refreshes) up to date and assembles the summary.
async fn collect(
    state: &AppState,
    market: &Market,
    progress: &mut [SummaryProgress],
) -> Result<QuestdbSummary, tokio_postgres::Error> {
    let keys = summary_keys(market);
    let mut it = state.questdb.advance_summaries(&keys, progress).await?.into_iter();
    // `summaries` returns exactly one entry per key, in order.
    let mut next = || it.next().unwrap_or_default();
    let has_product = market.coinbase_product.is_some();
    Ok(QuestdbSummary {
        live: next(),
        hist: next(),
        contracts: next(),
        contract_ticker: next(),
        contract_book: next(),
        coinbase_ticker: has_product.then(&mut next),
        coinbase_book: has_product.then(&mut next),
    })
}

/// Refreshes `shared.summary` every `REFRESH_INTERVAL`, starting right away.
/// A failed attempt keeps the previous tables and records the error.
pub async fn run_summary_refresh(state: AppState, shared: Arc<FeedShared>) {
    let tag = shared.market.tag.as_str();
    let mut progress = vec![SummaryProgress::default(); summary_keys(&shared.market).len()];
    let mut tick = interval(REFRESH_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Ok(_slot) = REFRESH_SLOT.acquire().await else { return };
        let started = Instant::now();
        let result = timeout(REFRESH_TIMEOUT, collect(&state, &shared.market, &mut progress)).await;
        let elapsed_ms = started.elapsed().as_millis();
        let mut snap = shared.summary.write().await;
        match result {
            Ok(Ok(tables)) => {
                snap.tables = Some(tables);
                snap.refreshed_at = Some(Utc::now());
                snap.error = None;
                tracing::info!(%tag, elapsed_ms, "summary refreshed");
            }
            Ok(Err(e)) => {
                let error = describe(&e);
                tracing::warn!(%tag, elapsed_ms, %error, "summary refresh failed");
                snap.error = Some(error);
            }
            Err(_) => {
                tracing::warn!(%tag, elapsed_ms, "summary refresh timed out; progress kept");
                snap.error = Some(format!("timed out after {REFRESH_TIMEOUT:?}"));
            }
        }
    }
}

/// `tokio_postgres::Error` displays a server-side failure as just "db error";
/// the message QuestDB sent (a query timeout, say) is in the source.
fn describe(e: &tokio_postgres::Error) -> String {
    match std::error::Error::source(e) {
        Some(source) => format!("{e}: {source}"),
        None => e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KalshiEnv;
    use crate::db::questdb::{CANDLES_TABLE, CONTRACT_BOOK_TABLE, CONTRACT_TICKER_TABLE, Table};
    use crate::model::market::{KalshiInfo, ProxyInfo};
    use serde_json::json;

    fn market(product: Option<&str>) -> Market {
        Market {
            tag: "btc-15m".into(),
            series_ticker: "KXBTC15M".into(),
            index_id: "BRTI".into(),
            coinbase_product: product.map(str::to_string),
            title: "BTC 15m".into(),
            kalshi: KalshiInfo {
                env: KalshiEnv::Prod,
                rest_base: String::new(),
                ws_url: String::new(),
                channels: vec![],
            },
            proxy: ProxyInfo { ticker_ws: String::new(), orderbook_ws: String::new() },
            created_at: String::new(),
        }
    }

    /// Literally the body of `GET /{tag}` before the first refresh lands.
    #[test]
    fn empty_snapshot_serializes_with_null_fields() {
        let v = serde_json::to_value(SummarySnapshot::default()).unwrap();
        assert_eq!(v, json!({ "tables": null, "refreshed_at": null, "error": null }));
    }

    /// Regression guard for the fields dropped from the summary.
    #[test]
    fn summary_json_shape() {
        let summary = |table| TableSummary { table, rows: 3, first_ts: Some("a".into()), last_ts: Some("b".into()) };
        let tables = QuestdbSummary {
            live: summary(Table::Live.name()),
            hist: summary(Table::Hist.name()),
            contracts: summary(CANDLES_TABLE),
            contract_ticker: summary(CONTRACT_TICKER_TABLE),
            contract_book: summary(CONTRACT_BOOK_TABLE),
            coinbase_ticker: None,
            coinbase_book: None,
        };
        let v = serde_json::to_value(SummarySnapshot { tables: Some(tables), ..Default::default() }).unwrap();
        let book = &v["tables"]["contract_book"];
        assert_eq!(book, &json!({ "table": "contract_book_live", "rows": 3, "first_ts": "a", "last_ts": "b" }));
        assert!(v["tables"]["coinbase_ticker"].is_null());
        for gone in ["rows_last_hour", "last_value", "min_value", "max_value", "markets"] {
            assert!(book.get(gone).is_none(), "{gone} should be gone");
        }
    }

    #[test]
    fn coinbase_keys_only_when_the_market_names_a_product() {
        let without = market(None);
        let keys = summary_keys(&without);
        assert_eq!(keys.len(), 5);
        assert!(keys.iter().all(|k| k.key == "BRTI" || k.key == "KXBTC15M"));
        assert_eq!((keys[0].table.name(), keys[0].table.key_column()), ("index_values_live", "index_id"));
        assert_eq!((keys[4].table.name(), keys[4].table.key_column()), ("contract_book_live", "series_ticker"));

        let with = market(Some("BTC-USD"));
        let keys = summary_keys(&with);
        assert_eq!(keys.len(), 7);
        assert_eq!(
            (keys[5].table, keys[5].table.key_column(), keys[5].key),
            (DataTable::CoinbaseTickerLive, "product", "BTC-USD")
        );
        assert_eq!(
            (keys[6].table, keys[6].table.key_column(), keys[6].key),
            (DataTable::CoinbaseBookLive, "product", "BTC-USD")
        );
    }
}
