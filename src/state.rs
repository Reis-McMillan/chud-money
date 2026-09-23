//! Shared application state handed to every axum handler.

use std::sync::Arc;

use verys_rs_client::VerysClient;

use crate::coinbase::client::CoinbaseClient;
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
    /// `None` when no CDP API key is configured.
    pub coinbase: Option<Arc<CoinbaseClient>>,
    pub feeds: FeedRegistry,
    pub ingest_jobs: IngestJobs,
    /// Used only to fetch and cache the Verys signing key (JWKS).
    pub verys_client: Arc<VerysClient>,
}
