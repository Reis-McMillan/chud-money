//! Wire types for the Kalshi v2 websocket.

use serde::Deserialize;
use serde_json::{Value, json};

pub const CHANNEL_CF_5HZ: &str = "cfbenchmarks_value_5hz";
pub const CHANNEL_ORDERBOOK: &str = "orderbook_delta";
/// Per-market last price, best yes bid/ask, volume and open interest, sent
/// whenever any of them changes.
pub const CHANNEL_TICKER: &str = "ticker";

/// Envelope shared by every server message. Command replies (`subscribed`,
/// `ok`, `error`) echo the client's `id`; data frames carry `sid` and `seq`.
#[derive(Debug, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub sid: Option<u64>,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub msg: Value,
}

/// `msg` of a `cfbenchmarks_value_5hz` frame.
#[derive(Debug, Clone, Deserialize)]
pub struct CfValue5Hz {
    pub index_id: String,
    pub value_usd: String,
    #[serde(default)]
    pub source_ts_ms: Option<i64>,
    #[serde(default)]
    pub received_at: Option<i64>,
}

impl CfValue5Hz {
    pub fn value(&self) -> Option<f64> {
        self.value_usd.parse().ok()
    }
}

/// `msg` of a `ticker` frame. Every numeric field is a decimal string
/// (`_dollars` for prices, `_fp` for fixed-point contract counts) except the
/// notional totals; all are optional so a frame missing one still parses.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TickerMsg {
    pub market_ticker: String,
    #[serde(default)]
    pub price_dollars: Option<String>,
    #[serde(default)]
    pub yes_bid_dollars: Option<String>,
    #[serde(default)]
    pub yes_ask_dollars: Option<String>,
    #[serde(default)]
    pub yes_bid_size_fp: Option<String>,
    #[serde(default)]
    pub yes_ask_size_fp: Option<String>,
    #[serde(default)]
    pub last_trade_size_fp: Option<String>,
    #[serde(default)]
    pub volume_fp: Option<String>,
    #[serde(default)]
    pub open_interest_fp: Option<String>,
    #[serde(default)]
    pub dollar_volume: Option<i64>,
    #[serde(default)]
    pub dollar_open_interest: Option<i64>,
    #[serde(default)]
    pub ts_ms: Option<i64>,
}

/// A decimal-string field as a number; `None` if absent or malformed.
pub fn decimal(field: &Option<String>) -> Option<f64> {
    field.as_deref().and_then(|s| s.trim().parse().ok())
}

/// `msg` of a `subscribed` reply.
#[derive(Debug, Deserialize)]
pub struct Subscribed {
    pub channel: String,
    pub sid: u64,
}

pub fn subscribe_cmd(id: u64, params: Value) -> String {
    json!({ "id": id, "cmd": "subscribe", "params": params }).to_string()
}

pub fn subscribe_index(id: u64, index_id: &str) -> String {
    subscribe_cmd(id, json!({ "channels": [CHANNEL_CF_5HZ], "index_ids": [index_id] }))
}

fn subscribe_markets<'a, I>(id: u64, channel: &str, tickers: I) -> String
where
    I: IntoIterator<Item = &'a String>,
{
    let tickers: Vec<&str> = tickers.into_iter().map(String::as_str).collect();
    subscribe_cmd(id, json!({ "channels": [channel], "market_tickers": tickers }))
}

pub fn subscribe_orderbook<'a, I>(id: u64, tickers: I) -> String
where
    I: IntoIterator<Item = &'a String>,
{
    subscribe_markets(id, CHANNEL_ORDERBOOK, tickers)
}

pub fn subscribe_ticker<'a, I>(id: u64, tickers: I) -> String
where
    I: IntoIterator<Item = &'a String>,
{
    subscribe_markets(id, CHANNEL_TICKER, tickers)
}

/// Add or remove markets on existing per-market subscriptions (`sids`).
pub fn update_markets_cmd(id: u64, sids: &[u64], action: &str, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "update_subscription",
        "params": { "sids": sids, "market_tickers": tickers, "action": action },
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ticker_frame() {
        let text = r#"{"type":"ticker","sid":11,"seq":4,"msg":{
            "market_id":"9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1","market_ticker":"FED-23DEC-T3.00",
            "price_dollars":"0.4800","yes_bid_dollars":"0.4500","yes_ask_dollars":"0.5300",
            "volume_fp":"33896.00","open_interest_fp":"20422.00","dollar_volume":16948,
            "dollar_open_interest":10211,"yes_bid_size_fp":"300.00","yes_ask_size_fp":"150.00",
            "last_trade_size_fp":"25.00","ts":1669149841,"ts_ms":1669149841000,"time":"2022-11-22T20:44:01Z"}}"#;
        let env: Envelope = serde_json::from_str(text).unwrap();
        assert_eq!((env.typ.as_str(), env.sid, env.seq), ("ticker", Some(11), 4));
        let msg: TickerMsg = serde_json::from_value(env.msg).unwrap();
        assert_eq!(msg.market_ticker, "FED-23DEC-T3.00");
        assert_eq!(decimal(&msg.price_dollars), Some(0.48));
        assert_eq!(decimal(&msg.yes_bid_size_fp), Some(300.0));
        assert_eq!(msg.dollar_volume, Some(16948));
        assert_eq!(msg.ts_ms, Some(1_669_149_841_000));

        // A sparse frame (no trade yet) still parses.
        let msg: TickerMsg =
            serde_json::from_value(json!({ "market_ticker": "T", "yes_bid_dollars": "0.10" })).unwrap();
        assert_eq!(decimal(&msg.price_dollars), None);
        assert_eq!(decimal(&msg.yes_bid_dollars), Some(0.1));
        assert_eq!(decimal(&Some("abc".into())), None);
    }

    #[test]
    fn subscribe_and_update_commands() {
        let tickers = vec!["A".to_string(), "B".to_string()];
        let cmd: Value = serde_json::from_str(&subscribe_ticker(3, &tickers)).unwrap();
        assert_eq!(
            cmd,
            json!({ "id": 3, "cmd": "subscribe", "params": { "channels": ["ticker"], "market_tickers": ["A", "B"] } })
        );
        let cmd: Value = serde_json::from_str(&subscribe_orderbook(4, &tickers)).unwrap();
        assert_eq!(cmd["params"]["channels"], json!(["orderbook_delta"]));

        let cmd: Value = serde_json::from_str(&update_markets_cmd(5, &[7, 9], "add_markets", &tickers[1..])).unwrap();
        assert_eq!(cmd["cmd"], "update_subscription");
        assert_eq!(cmd["params"], json!({ "sids": [7, 9], "market_tickers": ["B"], "action": "add_markets" }));
    }
}
