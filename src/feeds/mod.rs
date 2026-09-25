//! Registry of background feed tasks: one Kalshi session per market document,
//! a Coinbase session for markets that name a spot product, and a task that
//! keeps the market's QuestDB summary cache fresh.

pub mod coinbase;
pub mod summary;
pub mod task;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;

use crate::kalshi::book::OrderBook;
use crate::kalshi::client::OpenMarket;
use crate::model::market::{Market, valid_coinbase_product};
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
    summary_task: JoinHandle<()>,
}

impl Drop for FeedHandle {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(task) = &self.coinbase_task {
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
        let product = match market.coinbase_product.clone() {
            Some(p) if valid_coinbase_product(&p) => Some(p),
            Some(p) => {
                tracing::warn!(%tag, product = %p, "coinbase_product is not a product id; no coinbase feed");
                None
            }
            None => None,
        };
        let (ticker_tx, _) = broadcast::channel(TICKER_CAPACITY);
        let (orderbook_tx, _) = broadcast::channel(ORDERBOOK_CAPACITY);
        let status = FeedStatus {
            coinbase: product.as_ref().map(|p| CoinbaseFeedStatus { product: p.clone(), ..Default::default() }),
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
        let coinbase_task =
            product.map(|p| tokio::spawn(coinbase::run_coinbase_feed(state.clone(), shared.clone(), p)));
        let summary_task = tokio::spawn(summary::run_summary_refresh(state.clone(), shared.clone()));
        map.insert(tag, FeedHandle { shared, task, coinbase_task, summary_task });
        true
    }

    /// Abort and forget the feed for `tag`. Returns whether one existed.
    pub async fn stop(&self, tag: &str) -> bool {
        self.inner.write().await.remove(tag).is_some()
    }
}
