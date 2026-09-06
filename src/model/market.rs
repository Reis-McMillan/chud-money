//! The `markets` model: one document per tracked Kalshi series.

use std::sync::LazyLock;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{Model, ModelSpec};
use crate::config::{Config, KalshiEnv};
use crate::kalshi::stream::{CHANNEL_CF_5HZ, CHANNEL_ORDERBOOK};

/// Path segments that would collide with fixed routes if used as a tag.
pub const RESERVED_TAGS: &[&str] = &["add", "ingest", "ws"];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    /// URL-safe identity, e.g. `btc-15m`.
    pub tag: String,
    /// Kalshi series ticker, e.g. `KXBTC15M`. Individual 15-minute markets in
    /// the series are discovered live and never stored.
    pub series_ticker: String,
    /// CF Benchmarks index the series settles on, e.g. `BRTI`.
    pub index_id: String,
    pub title: String,
    pub kalshi: KalshiInfo,
    pub proxy: ProxyInfo,
    /// RFC 3339. Kept as a string so BSON and JSON validation see one shape.
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiInfo {
    pub env: KalshiEnv,
    pub rest_base: String,
    pub ws_url: String,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyInfo {
    pub ticker_ws: String,
    pub orderbook_ws: String,
}

/// Body of `POST /add`.
#[derive(Debug, Deserialize)]
pub struct AddMarket {
    pub tag: String,
    pub series_ticker: String,
    pub index_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub kalshi_env: Option<KalshiEnv>,
}

impl Market {
    pub fn from_add(input: AddMarket, config: &Config) -> Self {
        let env = input.kalshi_env.unwrap_or(config.kalshi_env);
        let endpoints = env.endpoints();
        let base = &config.public_ws_base;
        Self {
            title: input.title.unwrap_or_else(|| format!("{} ({})", input.series_ticker, input.index_id)),
            proxy: ProxyInfo {
                ticker_ws: format!("{base}/ws/{}/ticker", input.tag),
                orderbook_ws: format!("{base}/ws/{}/orderbook", input.tag),
            },
            kalshi: KalshiInfo {
                env,
                rest_base: endpoints.rest.to_string(),
                ws_url: endpoints.ws.to_string(),
                channels: vec![CHANNEL_CF_5HZ.to_string(), CHANNEL_ORDERBOOK.to_string()],
            },
            tag: input.tag,
            series_ticker: input.series_ticker,
            index_id: input.index_id,
            created_at: Utc::now().to_rfc3339(),
        }
    }
}

pub static MARKET_SPEC: LazyLock<ModelSpec> = LazyLock::new(|| {
    ModelSpec::new(
        "markets",
        &["tag"],
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "market",
            "type": "object",
            "additionalProperties": false,
            "required": ["tag", "series_ticker", "index_id", "title", "kalshi", "proxy", "created_at"],
            "properties": {
                "tag": {
                    "type": "string",
                    "pattern": "^[a-z0-9][a-z0-9-]{1,63}$",
                    "not": { "enum": RESERVED_TAGS }
                },
                "series_ticker": { "type": "string", "pattern": "^[A-Z0-9]+$" },
                "index_id": { "type": "string", "pattern": "^[A-Z0-9_]+$" },
                "title": { "type": "string", "minLength": 1, "maxLength": 200 },
                "kalshi": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["env", "rest_base", "ws_url", "channels"],
                    "properties": {
                        "env": { "enum": ["prod", "demo"] },
                        "rest_base": { "type": "string", "pattern": "^https?://" },
                        "ws_url": { "type": "string", "pattern": "^wss?://" },
                        "channels": { "type": "array", "minItems": 1, "items": { "type": "string" } }
                    }
                },
                "proxy": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["ticker_ws", "orderbook_ws"],
                    "properties": {
                        "ticker_ws": { "type": "string", "pattern": "^wss?://" },
                        "orderbook_ws": { "type": "string", "pattern": "^wss?://" }
                    }
                },
                "created_at": { "type": "string", "minLength": 20 }
            }
        }),
    )
});

impl Model for Market {
    fn spec() -> &'static ModelSpec {
        &MARKET_SPEC
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            bind_addr: "0.0.0.0:3000".into(),
            public_ws_base: "ws://localhost:3000".into(),
            mongo_uri: String::new(),
            mongo_db: String::new(),
            questdb_ilp_conf: String::new(),
            questdb_pg_conninfo: String::new(),
            kalshi_env: KalshiEnv::Prod,
            kalshi_key_id: String::new(),
            kalshi_key_path: String::new(),
        }
    }

    fn add(tag: &str) -> AddMarket {
        AddMarket {
            tag: tag.into(),
            series_ticker: "KXBTC15M".into(),
            index_id: "BRTI".into(),
            title: None,
            kalshi_env: None,
        }
    }

    #[test]
    fn valid_market_passes() {
        let m = Market::from_add(add("btc-15m"), &test_config());
        let v = serde_json::to_value(&m).unwrap();
        MARKET_SPEC.validate(&v).unwrap();
        assert_eq!(m.proxy.ticker_ws, "ws://localhost:3000/ws/btc-15m/ticker");
        assert_eq!(MARKET_SPEC.identity_filter(&v).unwrap().get_str("tag").unwrap(), "btc-15m");
    }

    #[test]
    fn bad_tag_rejected() {
        let m = Market::from_add(add("BAD TAG"), &test_config());
        let err = MARKET_SPEC.validate(&serde_json::to_value(&m).unwrap()).unwrap_err();
        assert!(matches!(err, super::super::ModelError::Validation(_)));
    }

    #[test]
    fn reserved_tag_rejected() {
        let m = Market::from_add(add("ingest"), &test_config());
        assert!(MARKET_SPEC.validate(&serde_json::to_value(&m).unwrap()).is_err());
    }

    #[test]
    fn extra_field_rejected() {
        let m = Market::from_add(add("btc-15m"), &test_config());
        let mut v = serde_json::to_value(&m).unwrap();
        v["live_price"] = json!(0.5);
        assert!(MARKET_SPEC.validate(&v).is_err());
    }
}
