//! Signed REST access to Kalshi.

use std::time::Duration;

use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request};

use crate::config::KalshiEndpoints;
use crate::kalshi::auth::Auth;

/// A request that takes longer than this is abandoned rather than left to
/// hang its caller.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Kalshi drops idle keep-alive connections without notice; reusing one fails
/// with "connection closed before message completed", so let them go first.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

pub const WS_PATH: &str = "/trade-api/ws/v2";
pub const MARKETS_PATH: &str = "/trade-api/v2/markets";

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
/// Exposed verbatim in `FeedStatus::open_markets` so clients can show the
/// settlement target (`floor_strike`, e.g. the 60s BRTI average before
/// `open_time` for KXBTC15M) alongside the live index.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenMarket {
    pub ticker: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub open_time: String,
    #[serde(default)]
    pub close_time: String,
    #[serde(default)]
    pub strike_type: Option<String>,
    #[serde(default)]
    pub floor_strike: Option<f64>,
    #[serde(default)]
    pub cap_strike: Option<f64>,
    #[serde(default)]
    pub yes_sub_title: Option<String>,
}

#[derive(Deserialize)]
struct MarketsResponse {
    #[serde(default)]
    markets: Vec<OpenMarket>,
}

impl KalshiClient {
    pub fn new(endpoints: KalshiEndpoints, auth: Auth) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build()
            .expect("static reqwest client configuration");
        Self { http, endpoints, auth }
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
