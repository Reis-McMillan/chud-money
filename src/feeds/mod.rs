//! Registry of background feed tasks, one per market document.

pub mod task;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;

use crate::kalshi::book::OrderBook;
use crate::kalshi::client::OpenMarket;
use crate::model::market::Market;
use crate::state::AppState;

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
    pub ticker_msgs: u64,
    pub orderbook_msgs: u64,
    pub last_error: Option<String>,
}

/// Channels and status shared between a feed task and its subscribers.
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
}

pub struct FeedHandle {
    pub shared: Arc<FeedShared>,
    task: JoinHandle<()>,
}

impl Drop for FeedHandle {
    fn drop(&mut self) {
        self.task.abort();
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

    /// Start the feed for `market`. Returns `false` (and does nothing) if one
    /// is already running for that tag.
    pub async fn spawn(&self, state: &AppState, market: Market) -> bool {
        let mut map = self.inner.write().await;
        if map.contains_key(&market.tag) {
            return false;
        }
        let tag = market.tag.clone();
        let (ticker_tx, _) = broadcast::channel(TICKER_CAPACITY);
        let (orderbook_tx, _) = broadcast::channel(ORDERBOOK_CAPACITY);
        let shared = Arc::new(FeedShared {
            market,
            ticker_tx,
            orderbook_tx,
            books: RwLock::new(HashMap::new()),
            status: RwLock::new(FeedStatus::default()),
        });
        let task = tokio::spawn(task::run_feed(state.clone(), shared.clone()));
        map.insert(tag, FeedHandle { shared, task });
        true
    }

    /// Abort and forget the feed for `tag`. Returns whether one existed.
    pub async fn stop(&self, tag: &str) -> bool {
        self.inner.write().await.remove(tag).is_some()
    }
}
