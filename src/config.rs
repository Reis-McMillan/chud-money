//! Process configuration, loaded from the environment (and `.env` via dotenvy).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Kalshi REST + websocket base URLs for one environment.
#[derive(Clone, Copy, Debug)]
pub struct KalshiEndpoints {
    pub rest: &'static str,
    pub ws: &'static str,
}

pub const PROD: KalshiEndpoints = KalshiEndpoints {
    rest: "https://external-api.kalshi.com",
    ws: "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
};

pub const DEMO: KalshiEndpoints = KalshiEndpoints {
    rest: "https://demo-api.kalshi.co",
    ws: "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KalshiEnv {
    Prod,
    Demo,
}

impl KalshiEnv {
    pub fn endpoints(self) -> KalshiEndpoints {
        match self {
            KalshiEnv::Prod => PROD,
            KalshiEnv::Demo => DEMO,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            KalshiEnv::Prod => "prod",
            KalshiEnv::Demo => "demo",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Base URL of the Verys auth service, e.g. `https://api.verys.mcmlln.dev`.
    /// The signing key is fetched from `{auth_url}/.well-known/jwks.json`.
    pub auth_url: String,
    /// Expected `iss` of Verys access tokens. Defaults to `auth_url`.
    pub verys_issuer: String,
    pub bind_addr: String,
    /// This API's Verys client id: the `audience` the SPA exchanges its
    /// session for, and therefore the expected `aud` of every access token.
    pub client_id: String,
    /// Base used to compose the proxy websocket URLs stored in market documents,
    /// e.g. `ws://localhost:3000`.
    pub public_ws_base: String,
    pub mongo_uri: String,
    pub mongo_db: String,
    /// Passed verbatim to `questdb::ingress::Sender::from_conf`.
    pub questdb_ilp_conf: String,
    /// libpq-style connection string for QuestDB's PGWire port.
    pub questdb_pg_conninfo: String,
    pub kalshi_env: KalshiEnv,
    pub kalshi_key_id: String,
    pub kalshi_key_path: String,
    /// Coinbase (CDP) API key: id and base64 Ed25519 private key. Coinbase
    /// ingests are refused unless both are set.
    pub cdp_key: Option<(String, String)>,
}

fn var_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let kalshi_env = match var_or("KALSHI_ENV", "demo").as_str() {
            "prod" => KalshiEnv::Prod,
            "demo" => KalshiEnv::Demo,
            other => anyhow::bail!("KALSHI_ENV must be 'prod' or 'demo', got {other:?}"),
        };
        let pg_host = var_or("QUESTDB_PG_HOST", "localhost");
        let pg_port = var_or("QUESTDB_PG_PORT", "8812");
        let pg_user = var_or("QUESTDB_PG_USER", "admin");
        let pg_password = var_or("QUESTDB_PG_PASSWORD", "quest");
        let pg_db = var_or("QUESTDB_PG_DB", "qdb");

        let auth_url = var_or("AUTH_URL", "http://localhost:8081").trim_end_matches('/').to_string();
        let verys_issuer = var_or("VERYS_ISSUER", &auth_url).trim_end_matches('/').to_string();

        let non_empty = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let cdp_key = match (non_empty("CDP_API_KEY_ID"), non_empty("CDP_API_KEY_SECRET")) {
            (Some(id), Some(secret)) => Some((id, secret)),
            (None, None) => None,
            // Not fatal: everything but the coinbase ingest works without it.
            (id, _) => {
                let missing = if id.is_some() { "CDP_API_KEY_SECRET" } else { "CDP_API_KEY_ID" };
                tracing::warn!("{missing} is not set; coinbase ingest is disabled");
                None
            }
        };

        Ok(Self {
            auth_url,
            verys_issuer,
            bind_addr: var_or("BIND_ADDR", "0.0.0.0:3000"),
            client_id: std::env::var("CHUD_MONEY_API_CLIENT_ID")
                .context("set CHUD_MONEY_API_CLIENT_ID to this API's Verys client id")?,
            public_ws_base: var_or("PUBLIC_WS_BASE", "ws://localhost:3000")
                .trim_end_matches('/')
                .to_string(),
            mongo_uri: var_or("MONGO_URI", "mongodb://localhost:27017"),
            mongo_db: var_or("MONGO_DB", "chud"),
            questdb_ilp_conf: var_or("QUESTDB_ILP_CONF", "http::addr=localhost:9000;"),
            questdb_pg_conninfo: format!(
                "host={pg_host} port={pg_port} user={pg_user} password={pg_password} dbname={pg_db}"
            ),
            kalshi_env,
            kalshi_key_id: std::env::var("KALSHI_API_KEY_ID")
                .context("set KALSHI_API_KEY_ID to your Kalshi API key id")?,
            kalshi_key_path: var_or("KALSHI_PRIVATE_KEY_PATH", "kalshi_key.pem"),
            cdp_key,
        })
    }
}
