//! Shared application state handed to every axum handler.

use std::sync::Arc;

use verys_rs_client::VerysClient;

use crate::coinbase::auth::Auth as CoinbaseAuth;
use crate::config::Config;
use crate::controllers::ingest::IngestJobs;
use crate::db::mongo::Mongo;
use crate::db::questdb::Questdb;
use crate::feeds::FeedRegistry;
use crate::kalshi::client::KalshiClient;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub mongo: Mongo,
    pub questdb: Questdb,
    pub kalshi: Arc<KalshiClient>,
    /// Signs Coinbase websocket subscriptions; `None` when no CDP API key is
    /// configured, in which case the public channels are used unauthenticated.
    pub coinbase_auth: Option<Arc<CoinbaseAuth>>,
    pub feeds: FeedRegistry,
    pub ingest_jobs: IngestJobs,
    /// Used only to fetch and cache the Verys signing key (JWKS).
    pub verys_client: Arc<VerysClient>,
}
