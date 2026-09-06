//! Order book state rebuilt from `orderbook_snapshot` + `orderbook_delta`.
//!
//! Kalshi quotes both sides as *bids* in cents: a resting yes bid at 47 and a
//! resting no bid at 51 mean "buy yes at 47" and "sell yes at 100 - 51 = 49".

use std::collections::BTreeMap;

#[derive(Default)]
pub struct OrderBook {
    /// price (cents) -> resting contracts
    pub yes: BTreeMap<i64, i64>,
    pub no: BTreeMap<i64, i64>,
    pub seq: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

impl Side {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "yes" => Some(Side::Yes),
            "no" => Some(Side::No),
            _ => None,
        }
    }
}

impl OrderBook {
    pub fn snapshot(&mut self, yes: &[[i64; 2]], no: &[[i64; 2]], seq: u64) {
        self.yes = yes.iter().map(|l| (l[0], l[1])).filter(|(_, q)| *q > 0).collect();
        self.no = no.iter().map(|l| (l[0], l[1])).filter(|(_, q)| *q > 0).collect();
        self.seq = seq;
    }

    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64, seq: u64) {
        let levels = match side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };
        let qty = levels.entry(price).or_insert(0);
        *qty += delta;
        if *qty <= 0 {
            levels.remove(&price);
        }
        self.seq = seq;
    }

    /// Best price someone will pay for a yes contract, and the size there.
    pub fn best_yes_bid(&self) -> Option<(i64, i64)> {
        self.yes.iter().next_back().map(|(p, q)| (*p, *q))
    }

    /// Cheapest yes offer, derived from the best resting no bid.
    pub fn best_yes_ask(&self) -> Option<(i64, i64)> {
        self.no.iter().next_back().map(|(p, q)| (100 - *p, *q))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_removes_emptied_level() {
        let mut b = OrderBook::default();
        b.snapshot(&[[47, 120], [46, 50]], &[[51, 80]], 1);
        assert_eq!(b.best_yes_bid(), Some((47, 120)));
        assert_eq!(b.best_yes_ask(), Some((49, 80)));

        b.apply_delta(Side::Yes, 47, -120, 2);
        assert_eq!(b.best_yes_bid(), Some((46, 50)));

        b.apply_delta(Side::No, 55, 10, 3);
        assert_eq!(b.best_yes_ask(), Some((45, 10)));
    }
}
