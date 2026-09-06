//! Signed REST access to Kalshi, including the CF Benchmarks pass-through.

use anyhow::Context;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request};

use crate::config::KalshiEndpoints;
use crate::kalshi::auth::Auth;

pub const WS_PATH: &str = "/trade-api/ws/v2";
pub const MARKETS_PATH: &str = "/trade-api/v2/markets";
/// Forwarded by Kalshi to `https://www.cfbenchmarks.com/api/v1/history/values`.
pub const CF_HISTORY_PATH: &str = "/trade-api/v2/cfbenchmarks/history/values";

#[derive(thiserror::Error, Debug)]
pub enum KalshiError {
    #[error("rate limited by kalshi")]
    RateLimited,
    #[error("kalshi returned {status}: {body}")]
    Status { status: u16, body: String },
    #[error("kalshi request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("unexpected kalshi response: {0}")]
    Parse(String),
}

pub struct KalshiClient {
    http: reqwest::Client,
    pub endpoints: KalshiEndpoints,
    auth: Auth,
}

/// One open market inside a series, as returned by `GET /trade-api/v2/markets`.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // title/close_time are parsed for logging and future use
pub struct OpenMarket {
    pub ticker: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub close_time: String,
}

#[derive(Deserialize)]
struct MarketsResponse {
    #[serde(default)]
    markets: Vec<OpenMarket>,
}

/// One historical index observation from the CF Benchmarks history endpoint.
#[derive(Debug, Clone)]
pub struct HistoryValue {
    pub time_ms: i64,
    pub value: f64,
}

impl HistoryValue {
    /// Lenient parse: `time` may be an integer or numeric string (ms), `value`
    /// may be a number or a decimal string. Returns `None` if either is missing.
    fn from_value(v: &Value) -> Option<Self> {
        let time_ms = match v.get("time")? {
            Value::Number(n) => n.as_i64()?,
            Value::String(s) => s.parse().ok()?,
            _ => return None,
        };
        let value = match v.get("value")? {
            Value::Number(n) => n.as_f64()?,
            Value::String(s) => s.parse().ok()?,
            _ => return None,
        };
        Some(Self { time_ms, value })
    }
}

impl KalshiClient {
    pub fn new(endpoints: KalshiEndpoints, auth: Auth) -> Self {
        Self { http: reqwest::Client::new(), endpoints, auth }
    }

    pub fn key_id(&self) -> &str {
        &self.auth.key_id
    }

    /// Signed GET. `path` must exclude the query string (the signature covers
    /// only the path); `query` is appended by reqwest.
    pub async fn signed_get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, KalshiError> {
        let (ts, sig) = self.auth.sign("GET", path);
        let resp = self
            .http
            .get(format!("{}{path}", self.endpoints.rest))
            .query(query)
            .header("KALSHI-ACCESS-KEY", &self.auth.key_id)
            .header("KALSHI-ACCESS-SIGNATURE", sig)
            .header("KALSHI-ACCESS-TIMESTAMP", ts)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.as_u16() == 429 {
            return Err(KalshiError::RateLimited);
        }
        if !status.is_success() {
            return Err(KalshiError::Status { status: status.as_u16(), body });
        }
        serde_json::from_str(&body).map_err(|e| KalshiError::Parse(format!("{e}: {body}")))
    }

    /// Open markets in a series, sorted by ticker.
    pub async fn open_markets(&self, series: &str) -> Result<Vec<OpenMarket>, KalshiError> {
        let mut parsed: MarketsResponse = self
            .signed_get(
                MARKETS_PATH,
                &[
                    ("series_ticker", series.to_string()),
                    ("status", "open".to_string()),
                    ("limit", "200".to_string()),
                ],
            )
            .await?;
        parsed.markets.sort_by(|a, b| a.ticker.cmp(&b.ticker));
        Ok(parsed.markets)
    }

    /// Historical index values via the CF Benchmarks pass-through.
    ///
    /// `timestamp_ms` must be truncated to the `timespan` granularity (the
    /// upstream API rejects unaligned starts). Returns the parsed values plus
    /// the raw upstream payload so callers can log its shape.
    pub async fn cf_history_values(
        &self,
        index_id: &str,
        timestamp_ms: i64,
        timespan: &str,
    ) -> Result<(Vec<HistoryValue>, Value), KalshiError> {
        let resp: Value = self
            .signed_get(
                CF_HISTORY_PATH,
                &[
                    ("id", index_id.to_string()),
                    ("timestamp", timestamp_ms.to_string()),
                    ("timespan", timespan.to_string()),
                ],
            )
            .await?;

        let payload = resp
            .pointer("/data/payload")
            .cloned()
            .ok_or_else(|| KalshiError::Parse(format!("missing data.payload in {resp}")))?;

        let items: Vec<Value> = match &payload {
            Value::Array(a) => a.clone(),
            Value::Object(o) => o
                .get("values")
                .or_else(|| o.get("data"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_else(|| vec![payload.clone()]),
            _ => Vec::new(),
        };
        let mut values: Vec<HistoryValue> = items.iter().filter_map(HistoryValue::from_value).collect();
        values.sort_by_key(|v| v.time_ms);
        Ok((values, payload))
    }

    /// Signed handshake request for the market-data websocket. Kalshi
    /// authenticates the connection itself, even for public channels.
    pub fn ws_request(&self) -> anyhow::Result<Request<()>> {
        let (ts, sig) = self.auth.sign("GET", WS_PATH);
        let mut request = self.endpoints.ws.into_client_request().context("building ws request")?;
        let headers = request.headers_mut();
        headers.insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&self.auth.key_id)?);
        headers.insert("KALSHI-ACCESS-SIGNATURE", HeaderValue::from_str(&sig)?);
        headers.insert("KALSHI-ACCESS-TIMESTAMP", HeaderValue::from_str(&ts)?);
        Ok(request)
    }
}
