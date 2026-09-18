//! Signed REST access to Kalshi, including the CF Benchmarks pass-through.

use anyhow::Context;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request};

use crate::config::KalshiEndpoints;
use crate::db::questdb::Ohlc;
use crate::kalshi::auth::Auth;

pub const WS_PATH: &str = "/trade-api/ws/v2";
pub const MARKETS_PATH: &str = "/trade-api/v2/markets";
/// Forwarded by Kalshi to `https://www.cfbenchmarks.com/api/v1/history/values`.
pub const CF_HISTORY_PATH: &str = "/trade-api/v2/cfbenchmarks/history/values";
/// Kalshi serves recent data from its live endpoints and moves everything
/// older than the cutoffs reported here to the `/historical` ones.
pub const HISTORICAL_CUTOFF_PATH: &str = "/trade-api/v2/historical/cutoff";
pub const HISTORICAL_MARKETS_PATH: &str = "/trade-api/v2/historical/markets";
/// Largest page either market listing allows.
const MARKETS_PAGE_LIMIT: &str = "1000";

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

/// Which set of endpoints holds a market; see `HISTORICAL_CUTOFF_PATH`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Live,
    Historical,
}

impl Tier {
    pub fn other(self) -> Self {
        match self {
            Tier::Live => Tier::Historical,
            Tier::Historical => Tier::Live,
        }
    }
}

/// A market of a series, reduced to what a candlestick backfill needs.
#[derive(Debug, Clone)]
pub struct SeriesMarket {
    pub ticker: String,
    pub open_ms: i64,
    pub close_ms: i64,
    pub floor_strike: Option<f64>,
    pub tier: Tier,
}

#[derive(Deserialize)]
struct MarketsPage {
    #[serde(default)]
    cursor: String,
    #[serde(default)]
    markets: Vec<OpenMarket>,
}

#[derive(Deserialize)]
struct CutoffResponse {
    market_settled_ts: DateTime<Utc>,
}

/// One candlestick of a market's contract prices, in dollars.
#[derive(Debug, Clone, PartialEq)]
pub struct Candlestick {
    /// End of the period (Kalshi's `end_period_ts`), in ms.
    pub end_ms: i64,
    pub yes_bid: Ohlc,
    pub yes_ask: Ohlc,
    /// Trade prices; all `None` for a period without trades.
    pub price: Ohlc,
    pub price_mean: Option<f64>,
    pub volume: Option<f64>,
    pub open_interest: Option<f64>,
}

/// Reads `key` from `v` as a decimal string or number. The live endpoints
/// suffix their fixed-point fields (`close_dollars`, `volume_fp`) while the
/// historical ones use the bare name (`close`, `volume`), so both are tried.
fn decimal(v: &Value, key: &str, suffix: &str) -> Option<f64> {
    match v.get(format!("{key}{suffix}")).or_else(|| v.get(key))? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn ohlc(v: Option<&Value>) -> Ohlc {
    let Some(v) = v else { return Ohlc::default() };
    Ohlc {
        open: decimal(v, "open", "_dollars"),
        high: decimal(v, "high", "_dollars"),
        low: decimal(v, "low", "_dollars"),
        close: decimal(v, "close", "_dollars"),
    }
}

impl Candlestick {
    fn from_value(v: &Value) -> Option<Self> {
        Some(Self {
            end_ms: v.get("end_period_ts")?.as_i64()? * 1_000,
            yes_bid: ohlc(v.get("yes_bid")),
            yes_ask: ohlc(v.get("yes_ask")),
            price: ohlc(v.get("price")),
            price_mean: v.get("price").and_then(|p| decimal(p, "mean", "_dollars")),
            volume: decimal(v, "volume", "_fp"),
            open_interest: decimal(v, "open_interest", "_fp"),
        })
    }
}

/// One historical index observation from the CF Benchmarks history endpoint.
#[derive(Debug, Clone)]
pub struct HistoryValue {
    pub time_ms: i64,
    pub value: f64,
}

impl HistoryValue {
    /// Lenient parse: `time` may be an integer or numeric string (ms) or an
    /// ISO instant; `value` may be a number or a decimal string. Returns
    /// `None` if either is missing.
    fn from_value(v: &Value) -> Option<Self> {
        let time_ms = match v.get("time")? {
            Value::Number(n) => n.as_i64()?,
            Value::String(s) => match s.parse::<i64>() {
                Ok(ms) => ms,
                Err(_) => DateTime::parse_from_rfc3339(s).ok()?.timestamp_millis(),
            },
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

    /// Settlement time before which a market and its candlesticks are only
    /// available from the historical endpoints.
    pub async fn historical_cutoff(&self) -> Result<DateTime<Utc>, KalshiError> {
        let resp: CutoffResponse = self.signed_get(HISTORICAL_CUTOFF_PATH, &[]).await?;
        Ok(resp.market_settled_ts)
    }

    /// One page of a series' markets from `tier`, newest close first, plus
    /// the cursor of the next page (`None` on the last one). Only the live
    /// listing can filter by time: `min_close_ms` is ignored for
    /// `Tier::Historical`, whose callers stop paging once they are past the
    /// range they want. Markets with unparseable times are dropped.
    pub async fn series_markets_page(
        &self,
        series: &str,
        tier: Tier,
        min_close_ms: i64,
        cursor: Option<&str>,
    ) -> Result<(Vec<SeriesMarket>, Option<String>), KalshiError> {
        let mut query = vec![("series_ticker", series.to_string()), ("limit", MARKETS_PAGE_LIMIT.to_string())];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor.to_string()));
        }
        let path = match tier {
            Tier::Live => {
                query.push(("min_close_ts", (min_close_ms.div_euclid(1_000)).to_string()));
                MARKETS_PATH
            }
            Tier::Historical => HISTORICAL_MARKETS_PATH,
        };
        let page: MarketsPage = self.signed_get(path, &query).await?;
        let ms = |s: &str| DateTime::parse_from_rfc3339(s).ok().map(|t| t.timestamp_millis());
        let markets = page
            .markets
            .into_iter()
            .filter_map(|m| {
                Some(SeriesMarket {
                    open_ms: ms(&m.open_time)?,
                    close_ms: ms(&m.close_time)?,
                    ticker: m.ticker,
                    floor_strike: m.floor_strike,
                    tier,
                })
            })
            .collect();
        Ok((markets, Some(page.cursor).filter(|c| !c.is_empty())))
    }

    /// Candlesticks of one market whose period ends within
    /// `[start_ms, end_ms]`, oldest first. `period_minutes` must be 1, 60 or
    /// 1440. A market lives in exactly one tier; asking the wrong one is a 404.
    pub async fn market_candlesticks(
        &self,
        series: &str,
        ticker: &str,
        tier: Tier,
        start_ms: i64,
        end_ms: i64,
        period_minutes: u32,
    ) -> Result<Vec<Candlestick>, KalshiError> {
        let path = match tier {
            Tier::Live => format!("/trade-api/v2/series/{series}/markets/{ticker}/candlesticks"),
            Tier::Historical => format!("{HISTORICAL_MARKETS_PATH}/{ticker}/candlesticks"),
        };
        let resp: Value = self
            .signed_get(
                &path,
                &[
                    ("start_ts", start_ms.div_euclid(1_000).to_string()),
                    ("end_ts", end_ms.div_euclid(1_000).to_string()),
                    ("period_interval", period_minutes.to_string()),
                ],
            )
            .await?;
        let items = resp
            .get("candlesticks")
            .and_then(Value::as_array)
            .ok_or_else(|| KalshiError::Parse(format!("missing candlesticks in {resp}")))?;
        let mut candles: Vec<Candlestick> = items.iter().filter_map(Candlestick::from_value).collect();
        candles.sort_by_key(|c| c.end_ms);
        Ok(candles)
    }

    /// Historical index values via the CF Benchmarks pass-through.
    ///
    /// `timestamp_ms` must be the start of a `timespan` period (the upstream
    /// API rejects unaligned starts) and is sent as an ISO instant, the only
    /// format it accepts. Returns the parsed values plus the raw upstream
    /// payload so callers can log its shape.
    pub async fn cf_history_values(
        &self,
        index_id: &str,
        timestamp_ms: i64,
        timespan: &str,
    ) -> Result<(Vec<HistoryValue>, Value), KalshiError> {
        let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
            .ok_or_else(|| KalshiError::Parse(format!("timestamp {timestamp_ms} out of range")))?
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        let resp: Value = self
            .signed_get(
                CF_HISTORY_PATH,
                &[
                    ("id", index_id.to_string()),
                    ("timestamp", timestamp),
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_live_and_historical_candlesticks() {
        let live = json!({
            "end_period_ts": 1789750020,
            "open_interest_fp": "270345.85",
            "price": { "close_dollars": "0.2400", "high_dollars": "0.3700", "low_dollars": "0.2400",
                       "mean_dollars": "0.3068", "open_dollars": "0.3500", "previous_dollars": "0.3500" },
            "volume_fp": "205910.16",
            "yes_ask": { "close_dollars": "0.2500", "high_dollars": "0.3700", "low_dollars": "0.2500", "open_dollars": "0.3600" },
            "yes_bid": { "close_dollars": "0.2400", "high_dollars": "0.3600", "low_dollars": "0.2400", "open_dollars": "0.3500" }
        });
        let historical = json!({
            "end_period_ts": 1789750020,
            "open_interest": "270345.85",
            "price": { "close": "0.2400", "high": "0.3700", "low": "0.2400", "mean": "0.3068",
                       "open": "0.3500", "previous": "0.3500" },
            "volume": "205910.16",
            "yes_ask": { "close": "0.2500", "high": "0.3700", "low": "0.2500", "open": "0.3600" },
            "yes_bid": { "close": "0.2400", "high": "0.3600", "low": "0.2400", "open": "0.3500" }
        });
        let c = Candlestick::from_value(&live).unwrap();
        assert_eq!(c, Candlestick::from_value(&historical).unwrap());
        assert_eq!(c.end_ms, 1_789_750_020_000);
        assert_eq!(c.yes_bid.close, Some(0.24));
        assert_eq!(c.yes_ask.open, Some(0.36));
        assert_eq!(c.price.high, Some(0.37));
        assert_eq!(c.price_mean, Some(0.3068));
        assert_eq!(c.volume, Some(205910.16));
        assert_eq!(c.open_interest, Some(270345.85));
    }

    #[test]
    fn candlestick_without_trades_has_no_price() {
        let v = json!({
            "end_period_ts": 1,
            "price": { "close": null, "open": null },
            "volume": "0.00",
            "yes_ask": { "close": "0.5000" },
            "yes_bid": {}
        });
        let c = Candlestick::from_value(&v).unwrap();
        assert_eq!(c.price, Ohlc::default());
        assert_eq!(c.price_mean, None);
        assert_eq!(c.yes_ask.close, Some(0.5));
        assert_eq!(c.volume, Some(0.0));
    }
}
