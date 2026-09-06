//! Model layer: a `Model` trait over MongoDB collections described by a
//! `ModelSpec` (collection name, JSON schema, identity fields).

pub mod market;
pub mod spec;

use futures_util::TryStreamExt;
use mongodb::Collection;
use mongodb::bson::{Document, doc, from_document, to_document};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::db::mongo::Mongo;
pub use spec::ModelSpec;

#[derive(thiserror::Error, Debug)]
pub enum ModelError {
    #[error("validation failed: {0:?}")]
    Validation(Vec<String>),
    #[error("missing identity field {0}")]
    MissingIdentity(&'static str),
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error("bson conversion failed: {0}")]
    Bson(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertResult {
    Inserted,
    Replaced,
}

pub trait Model: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {
    fn spec() -> &'static ModelSpec;

    fn collection(db: &Mongo) -> Collection<Document> {
        db.db.collection::<Document>(Self::spec().collection)
    }

    /// Validate against the spec's JSON schema, then `replace_one` filtered on
    /// the identity fields with `upsert: true`.
    async fn upsert(&self, db: &Mongo) -> Result<UpsertResult, ModelError> {
        let spec = Self::spec();
        let json = serde_json::to_value(self)?;
        spec.validate(&json)?;
        let filter = spec.identity_filter(&json)?;
        let doc = to_document(self).map_err(|e| ModelError::Bson(e.to_string()))?;
        let result = Self::collection(db).replace_one(filter, doc).upsert(true).await?;
        Ok(if result.upserted_id.is_some() { UpsertResult::Inserted } else { UpsertResult::Replaced })
    }

    /// Delete one document matching `filter`. Returns whether one was removed.
    async fn delete(db: &Mongo, filter: Document) -> Result<bool, ModelError> {
        Ok(Self::collection(db).delete_one(filter).await?.deleted_count == 1)
    }

    async fn all(db: &Mongo) -> Result<Vec<Self>, ModelError> {
        let docs: Vec<Document> = Self::collection(db).find(doc! {}).await?.try_collect().await?;
        docs.into_iter()
            .map(|d| from_document(d).map_err(|e| ModelError::Bson(e.to_string())))
            .collect()
    }

    async fn find(db: &Mongo, filter: Document) -> Result<Option<Self>, ModelError> {
        match Self::collection(db).find_one(filter).await? {
            Some(d) => Ok(Some(from_document(d).map_err(|e| ModelError::Bson(e.to_string()))?)),
            None => Ok(None),
        }
    }
}
