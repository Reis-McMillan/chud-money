//! Order book state rebuilt from `orderbook_snapshot` + `orderbook_delta`.
//!
//! Kalshi quotes both sides as *bids*: a resting yes bid at $0.47 and a
//! resting no bid at $0.51 mean "buy yes at 0.47" and "sell yes at 1 - 0.51 =
//! 0.49". Current frames carry sub-cent prices as decimal strings
//! (`price_dollars: "0.0330"`) and fractional sizes (`delta_fp: "-232.21"`,
//! snapshot levels under `yes_dollars_fp` / `no_dollars_fp` or the older
//! `yes_dollars` / `no_dollars`); older frames carried integer cents
//! (`price: 47`) and whole contracts (`delta: -120`). All are accepted and
//! normalised to fixed-point integers: prices in units of $0.0001, sizes in
//! hundredths of a contract.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::json;

/// $1.00 in price units.
pub const PRICE_ONE: i64 = 10_000;
/// One side of a book as `(price, size)` pairs in the fixed-point units above.
pub type Levels = Vec<(i64, i64)>;
const PRICE_SCALE: u32 = 4;
const QTY_SCALE: u32 = 2;

/// `msg` of an `orderbook_snapshot` or `orderbook_delta` frame.
#[derive(Debug, Deserialize)]
pub struct OrderbookMsg {
    pub market_ticker: String,
    // snapshot, legacy integer-cent form
    #[serde(default)]
    pub yes: Option<Vec<[i64; 2]>>,
    #[serde(default)]
    pub no: Option<Vec<[i64; 2]>>,
    // snapshot, decimal-string form
    #[serde(default, alias = "yes_dollars_fp")]
    pub yes_dollars: Option<Vec<[String; 2]>>,
    #[serde(default, alias = "no_dollars_fp")]
    pub no_dollars: Option<Vec<[String; 2]>>,
    // delta, legacy form
    #[serde(default)]
    pub price: Option<i64>,
    #[serde(default)]
    pub delta: Option<i64>,
    // delta, decimal-string form
    #[serde(default)]
    pub price_dollars: Option<String>,
    #[serde(default)]
    pub delta_fp: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    /// When Kalshi recorded the change (deltas only).
    #[serde(default)]
    pub ts_ms: Option<i64>,
}

impl OrderbookMsg {
    pub fn is_snapshot(&self) -> bool {
        self.yes.is_some() || self.no.is_some() || self.yes_dollars.is_some() || self.no_dollars.is_some()
    }

    fn levels(int: Option<&Vec<[i64; 2]>>, dec: Option<&Vec<[String; 2]>>) -> Levels {
        if let Some(rows) = dec {
            rows.iter()
                .filter_map(|[p, q]| Some((parse_scaled(p, PRICE_SCALE)?, parse_scaled(q, QTY_SCALE)?)))
                .collect()
        } else if let Some(rows) = int {
            rows.iter().map(|[p, q]| (cents_to_price(*p), whole_to_qty(*q))).collect()
        } else {
            Vec::new()
        }
    }

    /// Both sides of a snapshot as `(price, size)` levels, yes then no.
    pub fn snapshot_levels(&self) -> (Levels, Levels) {
        (
            OrderbookMsg::levels(self.yes.as_ref(), self.yes_dollars.as_ref()),
            OrderbookMsg::levels(self.no.as_ref(), self.no_dollars.as_ref()),
        )
    }

    /// A delta's `(side, price, signed size change)`, if the frame is one.
    pub fn delta_parts(&self) -> Option<(Side, i64, i64)> {
        let side = Side::parse(self.side.as_deref()?)?;
        let price = match (&self.price_dollars, self.price) {
            (Some(s), _) => parse_scaled(s, PRICE_SCALE)?,
            (None, Some(c)) => cents_to_price(c),
            (None, None) => return None,
        };
        let delta = match (&self.delta_fp, self.delta) {
            (Some(s), _) => parse_scaled(s, QTY_SCALE)?,
            (None, Some(d)) => whole_to_qty(d),
            (None, None) => return None,
        };
        Some((side, price, delta))
    }
}

fn cents_to_price(cents: i64) -> i64 {
    cents * (PRICE_ONE / 100)
}

fn whole_to_qty(n: i64) -> i64 {
    n * 10_i64.pow(QTY_SCALE)
}

/// A fixed-point price back to dollars.
pub fn price_to_dollars(price: i64) -> f64 {
    price as f64 / PRICE_ONE as f64
}

/// A fixed-point size back to contracts.
pub fn qty_to_contracts(qty: i64) -> f64 {
    qty as f64 / 10_i64.pow(QTY_SCALE) as f64
}

/// Parse a decimal string like `"-232.21"` into an integer scaled by
/// `10^scale`, truncating extra fractional digits. Avoids float drift.
pub fn parse_scaled(s: &str, scale: u32) -> Option<i64> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = s.split_once('.').unwrap_or((s, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.chars().all(|c| c.is_ascii_digit()) || !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let scale = scale as usize;
    let mut frac: String = frac_part.chars().take(scale).collect();
    while frac.len() < scale {
        frac.push('0');
    }
    let int_v: i64 = if int_part.is_empty() { 0 } else { int_part.parse().ok()? };
    let frac_v: i64 = if frac.is_empty() { 0 } else { frac.parse().ok()? };
    let v = int_v.checked_mul(10_i64.pow(scale as u32))?.checked_add(frac_v)?;
    Some(if neg { -v } else { v })
}

/// Format a scaled integer back to a decimal string with exactly `scale` digits.
pub fn format_scaled(v: i64, scale: u32) -> String {
    let div = 10_i64.pow(scale);
    let sign = if v < 0 { "-" } else { "" };
    let v = v.abs();
    format!("{sign}{}.{:0width$}", v / div, v % div, width = scale as usize)
}

#[derive(Default)]
pub struct OrderBook {
    /// price ($0.0001 units) -> resting size (hundredths of a contract)
    pub yes: BTreeMap<i64, i64>,
    pub no: BTreeMap<i64, i64>,
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    pub fn as_str(self) -> &'static str {
        match self {
            Side::Yes => "yes",
            Side::No => "no",
        }
    }
}

impl OrderBook {
    pub fn snapshot(&mut self, yes: &[(i64, i64)], no: &[(i64, i64)], seq: u64) {
        self.yes = yes.iter().copied().filter(|(_, q)| *q > 0).collect();
        self.no = no.iter().copied().filter(|(_, q)| *q > 0).collect();
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

    /// Apply a parsed `orderbook_snapshot` or `orderbook_delta` `msg`.
    /// Returns `false` if the message had neither shape.
    pub fn apply_msg(&mut self, msg: &OrderbookMsg, seq: u64) -> bool {
        if msg.is_snapshot() {
            let (yes, no) = msg.snapshot_levels();
            self.snapshot(&yes, &no, seq);
            return true;
        }
        if let Some((side, price, delta)) = msg.delta_parts() {
            self.apply_delta(side, price, delta, seq);
            return true;
        }
        false
    }

    /// Serialize as an `orderbook_snapshot` envelope in Kalshi's current wire
    /// shape (`yes_dollars` / `no_dollars` string pairs), so proxy clients can
    /// treat replayed and live snapshots identically.
    pub fn snapshot_frame(&self, market_ticker: &str) -> String {
        let levels = |m: &BTreeMap<i64, i64>| {
            m.iter().map(|(p, q)| [format_scaled(*p, PRICE_SCALE), format_scaled(*q, QTY_SCALE)]).collect::<Vec<_>>()
        };
        json!({
            "type": "orderbook_snapshot",
            "sid": 0,
            "seq": self.seq,
            "msg": {
                "market_ticker": market_ticker,
                "yes_dollars": levels(&self.yes),
                "no_dollars": levels(&self.no),
            },
        })
        .to_string()
    }

    /// Best price someone will pay for a yes contract, and the size there.
    #[allow(dead_code)]
    pub fn best_yes_bid(&self) -> Option<(i64, i64)> {
        self.yes.iter().next_back().map(|(p, q)| (*p, *q))
    }

    /// Cheapest yes offer, derived from the best resting no bid.
    #[allow(dead_code)]
    pub fn best_yes_ask(&self) -> Option<(i64, i64)> {
        self.no.iter().next_back().map(|(p, q)| (PRICE_ONE - *p, *q))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(v: serde_json::Value) -> OrderbookMsg {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn parses_scaled_decimals() {
        assert_eq!(parse_scaled("0.0330", 4), Some(330));
        assert_eq!(parse_scaled("1", 4), Some(10_000));
        assert_eq!(parse_scaled("-232.21", 2), Some(-23221));
        assert_eq!(parse_scaled("232.2", 2), Some(23220));
        assert_eq!(parse_scaled("232.219", 2), Some(23221));
        assert_eq!(parse_scaled(".5", 2), Some(50));
        assert_eq!(parse_scaled("abc", 2), None);
        assert_eq!(parse_scaled("", 2), None);
        assert_eq!(format_scaled(330, 4), "0.0330");
        assert_eq!(format_scaled(-23221, 2), "-232.21");
        assert_eq!(format_scaled(10_000, 4), "1.0000");
        assert_eq!(price_to_dollars(330), 0.033);
        assert_eq!(qty_to_contracts(-23221), -232.21);
        assert_eq!(Side::parse("no").map(Side::as_str), Some("no"));
    }

    #[test]
    fn fp_snapshot_keys_and_delta_timestamp() {
        let snap = msg(json!({
            "market_ticker": "T",
            "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
            "no_dollars_fp": [["0.5400", "20.00"]]
        }));
        assert!(snap.is_snapshot());
        let (yes, no) = snap.snapshot_levels();
        assert_eq!(yes, vec![(800, 30000), (2200, 33300)]);
        assert_eq!(no, vec![(5400, 2000)]);

        let delta = msg(json!({
            "market_ticker": "T", "price_dollars": "0.9600", "delta_fp": "-54.00", "side": "yes", "ts_ms": 1669149841000i64
        }));
        assert!(!delta.is_snapshot());
        assert_eq!(delta.delta_parts(), Some((Side::Yes, 9600, -5400)));
        assert_eq!(delta.ts_ms, Some(1_669_149_841_000));
    }

    #[test]
    fn delta_removes_emptied_level() {
        let mut b = OrderBook::default();
        b.snapshot(&[(4700, 12000), (4600, 5000)], &[(5100, 8000)], 1);
        assert_eq!(b.best_yes_bid(), Some((4700, 12000)));
        assert_eq!(b.best_yes_ask(), Some((4900, 8000)));

        b.apply_delta(Side::Yes, 4700, -12000, 2);
        assert_eq!(b.best_yes_bid(), Some((4600, 5000)));

        b.apply_delta(Side::No, 5500, 1000, 3);
        assert_eq!(b.best_yes_ask(), Some((4500, 1000)));
    }

    #[test]
    fn legacy_integer_frames() {
        let mut b = OrderBook::default();
        assert!(b.apply_msg(&msg(json!({ "market_ticker": "T", "yes": [[47, 120]], "no": [[51, 80]] })), 1));
        assert_eq!(b.best_yes_bid(), Some((4700, 12000)));
        assert!(b.apply_msg(&msg(json!({ "market_ticker": "T", "price": 46, "delta": 30, "side": "yes" })), 2));
        assert_eq!(b.yes.get(&4600), Some(&3000));
    }

    #[test]
    fn dollar_frames_and_replay() {
        let mut b = OrderBook::default();
        let snap = msg(json!({
            "market_ticker": "T",
            "yes_dollars": [["0.0100", "1.00"], ["0.4700", "120.00"]],
            "no_dollars": [["0.0330", "232.21"]]
        }));
        assert!(b.apply_msg(&snap, 7));
        let delta = msg(json!({
            "market_ticker": "T", "price_dollars": "0.0330", "delta_fp": "-232.21", "side": "no"
        }));
        assert!(b.apply_msg(&delta, 8));
        assert!(b.no.is_empty(), "level emptied by fractional delta is removed");
        assert_eq!(b.seq, 8);

        let delta2 = msg(json!({
            "market_ticker": "T", "price_dollars": "0.0310", "delta_fp": "232.21", "side": "no"
        }));
        assert!(b.apply_msg(&delta2, 9));
        assert_eq!(b.best_yes_ask(), Some((9690, 23221)));

        let frame: serde_json::Value = serde_json::from_str(&b.snapshot_frame("T")).unwrap();
        assert_eq!(frame["type"], "orderbook_snapshot");
        assert_eq!(frame["seq"], 9);
        assert_eq!(frame["msg"]["yes_dollars"], json!([["0.0100", "1.00"], ["0.4700", "120.00"]]));
        assert_eq!(frame["msg"]["no_dollars"], json!([["0.0310", "232.21"]]));

        // A snapshot with one side omitted clears the other side.
        assert!(b.apply_msg(&msg(json!({ "market_ticker": "T", "yes_dollars": [["0.4000", "1.00"]] })), 10));
        assert!(b.no.is_empty());
        assert_eq!(b.best_yes_bid(), Some((4000, 100)));
    }

    #[test]
    fn unknown_shape_is_rejected() {
        let mut b = OrderBook::default();
        assert!(!b.apply_msg(&msg(json!({ "market_ticker": "T" })), 1));
        assert!(!b.apply_msg(&msg(json!({ "market_ticker": "T", "side": "yes" })), 1));
    }
}
