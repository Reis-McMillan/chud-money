//! Shared application state handed to every axum handler.

use std::sync::Arc;

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
    pub feeds: FeedRegistry,
    pub ingest_jobs: IngestJobs,
}
