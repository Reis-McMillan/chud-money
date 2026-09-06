//! MongoDB connection and index management for model specs.

use anyhow::{Context, Result};
use mongodb::bson::{Document, doc};
use mongodb::options::IndexOptions;
use mongodb::{Client, Database, IndexModel};

use crate::model::{ModelError, ModelSpec};

#[derive(Clone)]
pub struct Mongo {
    pub db: Database,
}

impl Mongo {
    pub async fn connect(uri: &str, db_name: &str) -> Result<Self> {
        let client = Client::with_uri_str(uri).await.context("parsing MONGO_URI")?;
        let db = client.database(db_name);
        db.run_command(doc! { "ping": 1 }).await.context("pinging mongodb")?;
        Ok(Self { db })
    }

    /// Create a unique compound index over each spec's identity fields.
    /// Idempotent: MongoDB ignores an identical existing index.
    pub async fn ensure_indexes(&self, specs: &[&ModelSpec]) -> Result<(), ModelError> {
        for spec in specs {
            let mut keys = Document::new();
            for field in spec.identity {
                keys.insert(*field, 1);
            }
            let index = IndexModel::builder()
                .keys(keys)
                .options(IndexOptions::builder().unique(true).name(spec.index_name()).build())
                .build();
            self.db.collection::<Document>(spec.collection).create_index(index).await?;
            tracing::info!(collection = spec.collection, identity = ?spec.identity, "ensured unique index");
        }
        Ok(())
    }
}
