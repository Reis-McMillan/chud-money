//! Signed REST access to Coinbase's Advanced Trade API.

use std::time::Duration;

use serde::Deserialize;

use crate::coinbase::auth::{API_HOST, Auth};

/// Same budget as the Kalshi client: abandon a slow request and let the
/// ingest job retry it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

/// A request must ask for fewer candles than this.
pub const MAX_CANDLES_PER_REQUEST: i64 = 350;
const ONE_MINUTE: &str = "ONE_MINUTE";

#[derive(thiserror::Error, Debug)]
pub enum CoinbaseError {
    #[error("rate limited by coinbase")]
    RateLimited,
    #[error("coinbase returned {status}: {body}")]
    Status { status: u16, body: String },
    #[error("coinbase request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("unexpected coinbase response: {0}")]
    Parse(String),
    #[error("cannot sign coinbase request: {0}")]
    Sign(String),
}

impl CoinbaseError {
    /// Worth retrying: throttling, network failures and upstream 5xx.
    pub fn is_transient(&self) -> bool {
        match self {
            CoinbaseError::RateLimited | CoinbaseError::Transport(_) => true,
            CoinbaseError::Status { status, .. } => *status >= 500,
            CoinbaseError::Parse(_) | CoinbaseError::Sign(_) => false,
        }
    }
}

/// One spot candle, in quote currency (volume in base currency).
#[derive(Debug, Clone, PartialEq)]
pub struct CoinbaseCandle {
    /// Start of the period, in ms.
    pub start_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Wire form: every field is a decimal string.
#[derive(Deserialize)]
struct RawCandle {
    start: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

#[derive(Deserialize)]
struct CandlesResponse {
    #[serde(default)]
    candles: Vec<RawCandle>,
}

impl RawCandle {
    fn parse(&self) -> Option<CoinbaseCandle> {
        Some(CoinbaseCandle {
            start_ms: self.start.parse::<i64>().ok()? * 1_000,
            open: self.open.parse().ok()?,
            high: self.high.parse().ok()?,
            low: self.low.parse().ok()?,
            close: self.close.parse().ok()?,
            volume: self.volume.parse().ok()?,
        })
    }
}

fn parse_candles(body: &str) -> Result<Vec<CoinbaseCandle>, CoinbaseError> {
    let resp: CandlesResponse =
        serde_json::from_str(body).map_err(|e| CoinbaseError::Parse(format!("{e}: {body}")))?;
    let mut candles: Vec<CoinbaseCandle> = resp.candles.iter().filter_map(RawCandle::parse).collect();
    candles.sort_by_key(|c| c.start_ms);
    Ok(candles)
}

pub struct CoinbaseClient {
    http: reqwest::Client,
    auth: Auth,
}

impl CoinbaseClient {
    pub fn new(auth: Auth) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build()
            .expect("static reqwest client configuration");
        Self { http, auth }
    }

    pub fn key_id(&self) -> &str {
        &self.auth.key_id
    }

    /// One-minute candles of `product` (e.g. `BTC-USD`) starting within
    /// `[start_ms, end_ms]`, oldest first. The range must hold fewer than
    /// `MAX_CANDLES_PER_REQUEST` minutes. Minutes without a trade have no
    /// candle.
    pub async fn candles(
        &self,
        product: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<CoinbaseCandle>, CoinbaseError> {
        let path = format!("/api/v3/brokerage/products/{product}/candles");
        let token = self.auth.bearer("GET", &path).map_err(|e| CoinbaseError::Sign(format!("{e:#}")))?;
        let resp = self
            .http
            .get(format!("https://{API_HOST}{path}"))
            .query(&[
                ("start", start_ms.div_euclid(1_000).to_string()),
                ("end", end_ms.div_euclid(1_000).to_string()),
                ("granularity", ONE_MINUTE.to_string()),
            ])
            .bearer_auth(token)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.as_u16() == 429 {
            return Err(CoinbaseError::RateLimited);
        }
        if !status.is_success() {
            return Err(CoinbaseError::Status { status: status.as_u16(), body });
        }
        parse_candles(&body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_candles_oldest_first() {
        let body = r#"{"candles":[
            {"start":"1768604400","low":"95441.28","high":"95482.72","open":"95473.97","close":"95459.41","volume":"2.34292548"},
            {"start":"1768604340","low":"95441.28","high":"95475.53","open":"95441.29","close":"95473.99","volume":"1.27015616"}
        ]}"#;
        let candles = parse_candles(body).unwrap();
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].start_ms, 1_768_604_340_000);
        assert_eq!(candles[1].close, 95459.41);
        assert_eq!(candles[1].volume, 2.34292548);
        assert!(parse_candles(r#"{"candles":[]}"#).unwrap().is_empty());
        assert!(parse_candles("<html>").is_err());
    }
}
