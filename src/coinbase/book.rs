//! In-memory Coinbase level2 book, rebuilt from the `l2_data` channel so the
//! proxy can hand clients the top of the book without touching QuestDB.
//!
//! Coinbase sends a full snapshot on subscribe, then absolute per-level
//! updates (`new_quantity` is the level's new size; `0` removes it). Prices
//! are keyed as fixed-point integers so they order exactly; the frame carries
//! them back as the `f64`s Coinbase sent.

use std::collections::BTreeMap;

use serde_json::json;

use crate::db::questdb::{BookKind, CoinbaseMarket};

/// Price keys are `price * PRICE_SCALE`, rounded: eight decimals covers every
/// Coinbase quote increment.
const PRICE_SCALE: f64 = 1e8;

/// Levels per side in a [`CoinbaseBook::top_frame`].
pub const TOP_DEPTH: usize = 25;

/// One side of the book as `(price, size)`, best first.
pub type Levels = Vec<(f64, f64)>;

#[derive(Debug, Default)]
pub struct CoinbaseBook {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    /// Whether the first snapshot has arrived; before it the book is partial.
    ready: bool,
}

fn key(price: f64) -> i64 {
    (price * PRICE_SCALE).round() as i64
}

fn price(key: i64) -> f64 {
    key as f64 / PRICE_SCALE
}

impl CoinbaseBook {
    /// Called once per snapshot event, before its levels are applied.
    pub fn begin_snapshot(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.ready = true;
    }

    /// Sets one level. `side` is Coinbase's `bid` or `offer`; a size of zero
    /// (or less) removes the level. Updates before the first snapshot are
    /// ignored.
    pub fn apply(&mut self, kind: BookKind, side: &str, price: f64, size: f64) {
        if kind != BookKind::Snapshot && !self.ready {
            return;
        }
        let levels = match side {
            "bid" => &mut self.bids,
            "offer" => &mut self.asks,
            _ => return,
        };
        if size > 0.0 && size.is_finite() {
            levels.insert(key(price), size);
        } else {
            levels.remove(&key(price));
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// The best `depth` levels of each side, best first: bids descending,
    /// asks ascending.
    pub fn top(&self, depth: usize) -> (Levels, Levels) {
        let bids = self.bids.iter().rev().take(depth).map(|(k, s)| (price(*k), *s)).collect();
        let asks = self.asks.iter().take(depth).map(|(k, s)| (price(*k), *s)).collect();
        (bids, asks)
    }

    /// `{"type":"coinbase_book","market":..,"product":..,"bids":[[p,q],..],"asks":[[p,q],..],"ts_ms":..}`
    pub fn top_frame(&self, market: CoinbaseMarket, product: &str, depth: usize, ts_ms: i64) -> String {
        let (bids, asks) = self.top(depth);
        let pairs = |v: Levels| v.into_iter().map(|(p, q)| [p, q]).collect::<Vec<_>>();
        json!({
            "type": "coinbase_book",
            "market": market.label(),
            "product": product,
            "bids": pairs(bids),
            "asks": pairs(asks),
            "ts_ms": ts_ms,
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn snapshot(book: &mut CoinbaseBook, levels: &[(&str, f64, f64)]) {
        book.begin_snapshot();
        for (side, p, q) in levels {
            book.apply(BookKind::Snapshot, side, *p, *q);
        }
    }

    #[test]
    fn updates_before_a_snapshot_are_ignored() {
        let mut book = CoinbaseBook::default();
        book.apply(BookKind::Update, "bid", 100.0, 1.0);
        assert!(!book.is_ready());
        assert_eq!(book.top(5), (vec![], vec![]));
    }

    #[test]
    fn snapshot_replaces_and_updates_set_or_remove() {
        let mut book = CoinbaseBook::default();
        snapshot(&mut book, &[("bid", 100.0, 1.0), ("bid", 99.5, 2.0), ("offer", 100.5, 3.0)]);
        book.apply(BookKind::Update, "bid", 100.25, 0.5);
        book.apply(BookKind::Update, "bid", 99.5, 0.0);
        book.apply(BookKind::Update, "offer", 100.5, 4.0);
        book.apply(BookKind::Update, "sideways", 1.0, 1.0);
        assert_eq!(book.top(5), (vec![(100.25, 0.5), (100.0, 1.0)], vec![(100.5, 4.0)]));

        // A second snapshot (after a reconnect) starts from scratch.
        snapshot(&mut book, &[("offer", 101.0, 1.0)]);
        assert_eq!(book.top(5), (vec![], vec![(101.0, 1.0)]));
    }

    #[test]
    fn top_orders_best_first_and_truncates() {
        let mut book = CoinbaseBook::default();
        let levels: Vec<_> = (0..10)
            .flat_map(|i| [("bid", 100.0 - i as f64 * 0.01, 1.0), ("offer", 100.01 + i as f64 * 0.01, 1.0)])
            .collect();
        snapshot(&mut book, &levels);
        let (bids, asks) = book.top(3);
        assert_eq!(bids.iter().map(|l| l.0).collect::<Vec<_>>(), vec![100.0, 99.99, 99.98]);
        assert_eq!(asks.iter().map(|l| l.0).collect::<Vec<_>>(), vec![100.01, 100.02, 100.03]);
    }

    #[test]
    fn frame_encoding() {
        let mut book = CoinbaseBook::default();
        snapshot(&mut book, &[("bid", 75850.0, 243.0), ("offer", 75865.0, 59.0)]);
        let v: Value = serde_json::from_str(&book.top_frame(CoinbaseMarket::Perp, "BIP-20DEC30-CDE", 25, 7)).unwrap();
        assert_eq!(
            v,
            json!({
                "type": "coinbase_book", "market": "perp", "product": "BIP-20DEC30-CDE",
                "bids": [[75850.0, 243.0]], "asks": [[75865.0, 59.0]], "ts_ms": 7,
            })
        );
    }
}
