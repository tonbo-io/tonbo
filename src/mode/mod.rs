//! Dynamic mode configuration and helpers.
//!
//! Tonbo previously abstracted storage layouts behind a `Mode` trait so that
//! multiple implementations could plug into the same `DB` surface. We are
//! committing to the dynamic, Arrow `RecordBatch` layout as the sole runtime
//! representation, so this module now only contains the configuration and
//! helper utilities needed to build that layout.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    extractor::{KeyExtractError, KeyProjection, projection_for_columns},
    inmem::{immutable::memtable::MVCC_COMMIT_COL, mutable::DynMem},
    manifest::TableDefinition,
    transaction::CommitAckMode,
};

mod dyn_config;

/// Configuration bundle for constructing a `DynMode`.
pub struct DynModeConfig {
    /// Arrow schema describing the dynamic table.
    pub(crate) schema: SchemaRef,
    /// Extractor used to derive logical keys from dynamic batches.
    pub(crate) extractor: Arc<dyn KeyProjection>,
    /// WAL acknowledgement mode for transactional commits.
    pub(crate) commit_ack_mode: CommitAckMode,
}

impl DynModeConfig {
    /// Validate the extractor against `schema` and construct the config bundle.
    pub(crate) fn new(
        schema: SchemaRef,
        extractor: Box<dyn KeyProjection>,
    ) -> Result<Self, KeyExtractError> {
        extractor.validate_schema(&schema)?;
        let extractor: Arc<dyn KeyProjection> = extractor.into();
        Ok(Self {
            schema,
            extractor,
            commit_ack_mode: CommitAckMode::default(),
        })
    }

    /// Override the commit acknowledgement mode for transactional writes.
    pub fn with_commit_ack_mode(mut self, mode: CommitAckMode) -> Self {
        self.commit_ack_mode = mode;
        self
    }

    /// Clone the schema associated with this configuration.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Build the dynamic storage parameters and mutable memtable backing the DB.
    ///
    /// Returns `(schema, delete_schema, commit_ack_mode, mutable)`.
    pub(crate) fn build(
        self,
    ) -> Result<(SchemaRef, SchemaRef, CommitAckMode, DynMem), KeyExtractError> {
        let DynModeConfig {
            schema,
            extractor,
            commit_ack_mode,
        } = self;
        extractor.validate_schema(&schema)?;
        let key_schema = extractor.key_schema();
        let delete_schema = build_delete_schema(&key_schema);
        let key_columns = key_schema.fields().len();
        let delete_projection =
            projection_for_columns(delete_schema.clone(), (0..key_columns).collect())?;
        let delete_projection: Arc<dyn KeyProjection> = delete_projection.into();

        let mutable = DynMem::new(schema.clone(), extractor, delete_projection);
        Ok((schema, delete_schema, commit_ack_mode, mutable))
    }
}

fn build_delete_schema(key_schema: &SchemaRef) -> SchemaRef {
    let mut fields = key_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<Field>>();
    fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false));
    std::sync::Arc::new(Schema::new(fields))
}

/// Derive the table definition used when registering a table in the manifest.
pub(crate) fn table_definition(config: &DynModeConfig, table_name: &str) -> TableDefinition {
    let key_columns = config
        .extractor
        .key_indices()
        .iter()
        .map(|idx| config.schema.field(*idx).name().clone())
        .collect();
    TableDefinition {
        name: table_name.to_string(),
        schema_fingerprint: fingerprint_schema(&config.schema),
        primary_key_columns: key_columns,
        retention: None,
        schema_version: 0,
    }
}

fn fingerprint_schema(schema: &SchemaRef) -> String {
    let mut hasher = Sha256::new();
    let value =
        serde_json::to_value(schema.as_ref()).expect("arrow schema serialization should not fail");
    let canonical = canonicalize_json(value);
    let bytes =
        serde_json::to_vec(&canonical).expect("canonical schema serialization should not fail");
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let sorted = entries
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json(value)))
                .collect();
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}
