//! Wire types for the Coinbase Advanced Trade market-data websocket.
//!
//! One `subscribe` message per channel, sent within five seconds of
//! connecting or the server drops the connection. `ticker` and `level2` are
//! public; a JWT (see `auth::Auth::ws_bearer`) is optional but recommended.
//! Every server frame is an envelope with a per-connection `sequence_num`.

use chrono::DateTime;
use serde::Deserialize;
use serde_json::{Value, json};

pub const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

/// One server ping per second; subscribed so an idle product still shows life.
pub const CHANNEL_HEARTBEATS: &str = "heartbeats";
/// Last price, 24h stats and best bid/ask, on every match.
pub const CHANNEL_TICKER: &str = "ticker";
/// Full order book: a snapshot, then absolute per-level updates.
pub const CHANNEL_LEVEL2: &str = "level2";
/// `level2` frames arrive under this channel name.
pub const CHANNEL_L2_DATA: &str = "l2_data";
/// Replies to subscribe / unsubscribe.
pub const CHANNEL_SUBSCRIPTIONS: &str = "subscriptions";

pub fn subscribe_cmd(channel: &str, product: &str, jwt: Option<&str>) -> String {
    let mut cmd = json!({ "type": "subscribe", "product_ids": [product], "channel": channel });
    if let Some(jwt) = jwt {
        cmd["jwt"] = Value::String(jwt.to_string());
    }
    cmd.to_string()
}

/// Every server frame. Error replies (`{"type":"error","message":...}`)
/// have no `channel`, so it defaults to empty.
#[derive(Debug, Deserialize)]
pub struct Envelope {
    #[serde(default)]
    pub channel: String,
    /// Coinbase's send time, RFC 3339 with nanoseconds.
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub sequence_num: u64,
    #[serde(default)]
    pub events: Vec<Value>,
}

/// One `events[]` entry of a `ticker` frame. Its `type` (`snapshot` on
/// subscribe, `update` after) carries no extra information, so it is not kept.
#[derive(Debug, Deserialize)]
pub struct TickerEvent {
    #[serde(default)]
    pub tickers: Vec<Ticker>,
}

/// All decimal strings; optional so a field Coinbase omits still parses.
#[derive(Debug, Default, Deserialize)]
pub struct Ticker {
    pub product_id: String,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub best_bid: Option<String>,
    #[serde(default)]
    pub best_ask: Option<String>,
    #[serde(default)]
    pub best_bid_quantity: Option<String>,
    #[serde(default)]
    pub best_ask_quantity: Option<String>,
    #[serde(default)]
    pub volume_24_h: Option<String>,
    #[serde(default)]
    pub low_24_h: Option<String>,
    #[serde(default)]
    pub high_24_h: Option<String>,
    #[serde(default)]
    pub low_52_w: Option<String>,
    #[serde(default)]
    pub high_52_w: Option<String>,
    #[serde(default)]
    pub price_percent_chg_24_h: Option<String>,
}

/// One `events[]` entry of an `l2_data` frame.
#[derive(Debug, Deserialize)]
pub struct L2Event {
    #[serde(rename = "type", default)]
    pub typ: String,
    pub product_id: String,
    #[serde(default)]
    pub updates: Vec<L2Update>,
}

#[derive(Debug, Deserialize)]
pub struct L2Update {
    /// `bid` or `offer`.
    pub side: String,
    /// When the matching engine applied it, RFC 3339.
    #[serde(default)]
    pub event_time: String,
    pub price_level: String,
    /// The new absolute size at the level; `"0"` removes it.
    pub new_quantity: String,
}

/// A decimal-string field as a number; `None` if absent or malformed.
pub fn decimal(field: &Option<String>) -> Option<f64> {
    field.as_deref().and_then(|s| s.trim().parse().ok())
}

/// An RFC 3339 instant as nanoseconds since the epoch.
pub fn rfc3339_nanos(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s).ok()?.timestamp_nanos_opt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_with_and_without_jwt() {
        let cmd: Value = serde_json::from_str(&subscribe_cmd(CHANNEL_LEVEL2, "BTC-USD", None)).unwrap();
        assert_eq!(cmd, json!({ "type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2" }));
        let cmd: Value = serde_json::from_str(&subscribe_cmd(CHANNEL_TICKER, "BTC-USD", Some("tok"))).unwrap();
        assert_eq!(cmd["jwt"], "tok");
        assert_eq!(cmd["channel"], "ticker");
    }

    #[test]
    fn parses_ticker_frame() {
        let text = r#"{"channel":"ticker","client_id":"","timestamp":"2024-01-15T10:30:45.123456789Z","sequence_num":7,
            "events":[{"type":"snapshot","tickers":[{"type":"ticker","product_id":"BTC-USD","price":"42500.00",
            "volume_24_h":"1000000","low_24_h":"41000.00","high_24_h":"43000.00","low_52_w":"35000.00",
            "high_52_w":"48000.00","price_percent_chg_24_h":"2.5","best_bid":"42499.00","best_ask":"42501.00",
            "best_bid_quantity":"5.0","best_ask_quantity":"5.5"}]}]}"#;
        let env: Envelope = serde_json::from_str(text).unwrap();
        assert_eq!((env.channel.as_str(), env.sequence_num), ("ticker", 7));
        assert_eq!(rfc3339_nanos(&env.timestamp), Some(1_705_314_645_123_456_789));
        let event: TickerEvent = serde_json::from_value(env.events[0].clone()).unwrap();
        let t = &event.tickers[0];
        assert_eq!(t.product_id, "BTC-USD");
        assert_eq!(decimal(&t.price), Some(42_500.0));
        assert_eq!(decimal(&t.best_ask_quantity), Some(5.5));
        assert_eq!(decimal(&t.price_percent_chg_24_h), Some(2.5));
    }

    #[test]
    fn parses_level2_frame() {
        let text = r#"{"channel":"l2_data","client_id":"","timestamp":"2024-01-15T10:30:45Z","sequence_num":2,
            "events":[{"type":"update","product_id":"BTC-USD","updates":[
              {"side":"bid","event_time":"2024-01-15T10:30:44.999Z","price_level":"42499.00","new_quantity":"0"},
              {"side":"offer","event_time":"2024-01-15T10:30:44.999Z","price_level":"42501.00","new_quantity":"1.25"}]}]}"#;
        let env: Envelope = serde_json::from_str(text).unwrap();
        assert_eq!(env.channel, CHANNEL_L2_DATA);
        let event: L2Event = serde_json::from_value(env.events[0].clone()).unwrap();
        assert_eq!((event.typ.as_str(), event.product_id.as_str()), ("update", "BTC-USD"));
        assert_eq!(event.updates.len(), 2);
        assert_eq!(event.updates[0].side, "bid");
        assert_eq!(event.updates[0].new_quantity.parse::<f64>().unwrap(), 0.0);
        assert_eq!(rfc3339_nanos(&event.updates[1].event_time), Some(1_705_314_644_999_000_000));
        assert_eq!(rfc3339_nanos("nope"), None);

        // Heartbeats and subscription acks are plain envelopes too.
        let env: Envelope =
            serde_json::from_str(r#"{"channel":"heartbeats","timestamp":"2024-01-15T10:30:45Z","sequence_num":3,"events":[{"current_time":"x","heartbeat_counter":1}]}"#)
                .unwrap();
        assert_eq!(env.channel, CHANNEL_HEARTBEATS);
    }
}
