//! Registry of background feed tasks: one Kalshi session per market document,
//! a Coinbase session each for the spot product and the perpetual future a
//! market names, and a task that keeps the market's QuestDB summary cache
//! fresh.

pub mod coinbase;
pub mod summary;
pub mod task;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;

use crate::db::questdb::CoinbaseMarket;
use crate::kalshi::book::OrderBook;
use crate::kalshi::client::OpenMarket;
use crate::model::market::{Market, valid_coinbase_perp_product, valid_coinbase_product};
use crate::state::AppState;
use summary::SummarySnapshot;

const TICKER_CAPACITY: usize = 64;
const ORDERBOOK_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Default, Serialize)]
pub struct FeedStatus {
    pub connected: bool,
    pub reconnects: u32,
    pub open_tickers: Vec<String>,
    /// Full Kalshi market records for `open_tickers`, refreshed every poll.
    pub open_markets: Vec<OpenMarket>,
    pub last_value: Option<f64>,
    pub last_msg_at: Option<DateTime<Utc>>,
    /// CF Benchmarks 5Hz frames.
    pub ticker_msgs: u64,
    /// Kalshi per-market `ticker` frames.
    pub contract_ticker_msgs: u64,
    pub orderbook_msgs: u64,
    /// Rows not written because the QuestDB queue was full.
    pub dropped_rows: u64,
    pub last_error: Option<String>,
    /// `None` for a market without a `coinbase_product`.
    pub coinbase: Option<CoinbaseFeedStatus>,
    /// `None` for a market without a `coinbase_perp_product`.
    pub coinbase_perp: Option<CoinbaseFeedStatus>,
}

impl FeedStatus {
    /// The status of `market`'s Coinbase session, if the market has one.
    pub fn coinbase_mut(&mut self, market: CoinbaseMarket) -> Option<&mut CoinbaseFeedStatus> {
        match market {
            CoinbaseMarket::Spot => self.coinbase.as_mut(),
            CoinbaseMarket::Perp => self.coinbase_perp.as_mut(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CoinbaseFeedStatus {
    pub product: String,
    pub connected: bool,
    pub reconnects: u32,
    pub ticker_msgs: u64,
    pub book_msgs: u64,
    pub dropped_rows: u64,
    pub last_price: Option<f64>,
    pub last_msg_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

/// Channels, status and the QuestDB summary cache shared between a market's
/// tasks and its readers.
pub struct FeedShared {
    pub market: Market,
    /// Raw `cfbenchmarks_value_5hz` envelopes as received from Kalshi.
    pub ticker_tx: broadcast::Sender<Arc<str>>,
    /// Raw `orderbook_snapshot` / `orderbook_delta` envelopes.
    pub orderbook_tx: broadcast::Sender<Arc<str>>,
    /// Current book per market ticker, rebuilt from the frames above so a
    /// proxy client that connects mid-session can be handed a snapshot.
    pub books: RwLock<HashMap<String, OrderBook>>,
    pub status: RwLock<FeedStatus>,
    /// Written by `summary::run_summary_refresh`, read by `GET /{tag}`.
    pub summary: RwLock<SummarySnapshot>,
}

pub struct FeedHandle {
    pub shared: Arc<FeedShared>,
    task: JoinHandle<()>,
    coinbase_task: Option<JoinHandle<()>>,
    coinbase_perp_task: Option<JoinHandle<()>>,
    summary_task: JoinHandle<()>,
}

impl Drop for FeedHandle {
    fn drop(&mut self) {
        self.task.abort();
        for task in [&self.coinbase_task, &self.coinbase_perp_task].into_iter().flatten() {
            task.abort();
        }
        self.summary_task.abort();
    }
}

#[derive(Clone, Default)]
pub struct FeedRegistry {
    inner: Arc<RwLock<HashMap<String, FeedHandle>>>,
}

impl FeedRegistry {
    pub async fn get(&self, tag: &str) -> Option<Arc<FeedShared>> {
        self.inner.read().await.get(tag).map(|h| h.shared.clone())
    }

    pub async fn status(&self, tag: &str) -> Option<FeedStatus> {
        match self.get(tag).await {
            Some(s) => Some(s.status.read().await.clone()),
            None => None,
        }
    }

    /// The market's cached QuestDB summaries, or `None` if no feed is running.
    pub async fn summary(&self, tag: &str) -> Option<SummarySnapshot> {
        match self.get(tag).await {
            Some(s) => Some(s.summary.read().await.clone()),
            None => None,
        }
    }

    /// Start the feed(s) for `market`. Returns `false` (and does nothing) if
    /// one is already running for that tag.
    pub async fn spawn(&self, state: &AppState, market: Market) -> bool {
        let mut map = self.inner.write().await;
        if map.contains_key(&market.tag) {
            return false;
        }
        let tag = market.tag.clone();
        let checked = |field: &str, product: &Option<String>, valid: fn(&str) -> bool| match product {
            Some(p) if valid(p) => Some(p.clone()),
            Some(p) => {
                tracing::warn!(%tag, product = %p, "{field} is not a valid product id; no coinbase feed for it");
                None
            }
            None => None,
        };
        let product = checked("coinbase_product", &market.coinbase_product, valid_coinbase_product);
        let perp = checked("coinbase_perp_product", &market.coinbase_perp_product, valid_coinbase_perp_product);
        let cb_status = |p: &String| CoinbaseFeedStatus { product: p.clone(), ..Default::default() };
        let (ticker_tx, _) = broadcast::channel(TICKER_CAPACITY);
        let (orderbook_tx, _) = broadcast::channel(ORDERBOOK_CAPACITY);
        let status = FeedStatus {
            coinbase: product.as_ref().map(cb_status),
            coinbase_perp: perp.as_ref().map(cb_status),
            ..Default::default()
        };
        let shared = Arc::new(FeedShared {
            market,
            ticker_tx,
            orderbook_tx,
            books: RwLock::new(HashMap::new()),
            status: RwLock::new(status),
            summary: RwLock::new(SummarySnapshot::default()),
        });
        let task = tokio::spawn(task::run_feed(state.clone(), shared.clone()));
        let coinbase = |p, m| tokio::spawn(coinbase::run_coinbase_feed(state.clone(), shared.clone(), p, m));
        let coinbase_task = product.map(|p| coinbase(p, CoinbaseMarket::Spot));
        let coinbase_perp_task = perp.map(|p| coinbase(p, CoinbaseMarket::Perp));
        let summary_task = tokio::spawn(summary::run_summary_refresh(state.clone(), shared.clone()));
        map.insert(tag, FeedHandle { shared, task, coinbase_task, coinbase_perp_task, summary_task });
        true
    }

    /// Abort and forget the feed for `tag`. Returns whether one existed.
    pub async fn stop(&self, tag: &str) -> bool {
        self.inner.write().await.remove(tag).is_some()
    }
}
