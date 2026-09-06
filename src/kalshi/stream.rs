//! Wire types for the Kalshi v2 websocket.

use serde::Deserialize;
use serde_json::{Value, json};

pub const CHANNEL_CF_5HZ: &str = "cfbenchmarks_value_5hz";
pub const CHANNEL_ORDERBOOK: &str = "orderbook_delta";

/// Envelope shared by every server message. Command replies (`subscribed`,
/// `ok`, `error`) echo the client's `id`; data frames carry `sid` and `seq`.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // sid/seq are decoded for completeness; frames are forwarded raw
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

pub fn subscribe_orderbook<'a, I>(id: u64, tickers: I) -> String
where
    I: IntoIterator<Item = &'a String>,
{
    let tickers: Vec<&str> = tickers.into_iter().map(String::as_str).collect();
    subscribe_cmd(id, json!({ "channels": [CHANNEL_ORDERBOOK], "market_tickers": tickers }))
}

/// Add or remove markets on an existing orderbook subscription.
pub fn update_markets_cmd(id: u64, sid: u64, action: &str, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "update_subscription",
        "params": { "sids": [sid], "market_tickers": tickers, "action": action },
    })
    .to_string()
}
