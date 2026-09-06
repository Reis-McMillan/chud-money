//! `ModelSpec`: the MongoDB collection, JSON schema, and identity fields that
//! describe one model. The schema is compiled once and reused.

use jsonschema::Validator;
use mongodb::bson::{Document, to_bson};
use serde_json::Value;

use super::ModelError;

pub struct ModelSpec {
    pub collection: &'static str,
    /// Kept for introspection (e.g. exposing schemas to the SPA later).
    #[allow(dead_code)]
    pub schema: Value,
    /// Fields that uniquely identify a document; the collection gets a unique
    /// compound index over them and `upsert` filters on them.
    pub identity: &'static [&'static str],
    validator: Validator,
}

impl ModelSpec {
    /// Panics if the schema does not compile: specs are static and a broken
    /// schema is a programming error.
    pub fn new(collection: &'static str, identity: &'static [&'static str], schema: Value) -> Self {
        assert!(!identity.is_empty(), "model {collection} needs at least one identity field");
        let validator = jsonschema::validator_for(&schema)
            .unwrap_or_else(|e| panic!("schema for {collection} does not compile: {e}"));
        Self { collection, schema, identity, validator }
    }

    pub fn validate(&self, instance: &Value) -> Result<(), ModelError> {
        let errors: Vec<String> = self
            .validator
            .iter_errors(instance)
            .map(|e| format!("{}: {e}", e.instance_path()))
            .collect();
        if errors.is_empty() { Ok(()) } else { Err(ModelError::Validation(errors)) }
    }

    /// `{identity_field: value, ...}` extracted from a serialized document.
    pub fn identity_filter(&self, instance: &Value) -> Result<Document, ModelError> {
        let mut filter = Document::new();
        for key in self.identity {
            let value = instance.get(*key).ok_or(ModelError::MissingIdentity(key))?;
            filter.insert(*key, to_bson(value).map_err(|e| ModelError::Bson(e.to_string()))?);
        }
        Ok(filter)
    }

    pub fn index_name(&self) -> String {
        format!("{}_identity", self.collection)
    }
}
