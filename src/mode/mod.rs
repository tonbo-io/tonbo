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

/// Flattened dynamic storage parameters used by the database.
#[derive(Clone)]
pub struct DynModeState {
    pub(crate) schema: SchemaRef,
    pub(crate) delete_schema: SchemaRef,
    pub(crate) extractor: Arc<dyn KeyProjection>,
    pub(crate) delete_projection: Arc<dyn KeyProjection>,
    pub(crate) commit_ack_mode: CommitAckMode,
}

/// Backwards-compatible alias used by examples/tests that referenced the old dynamic mode type.
pub type DynMode = DynModeState;

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

    /// Build the dynamic storage state and mutable memtable backing the DB.
    pub(crate) fn build(self) -> Result<(DynModeState, DynMem), KeyExtractError> {
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

        let mutable = DynMem::new(schema.clone());
        Ok((
            DynModeState {
                schema,
                delete_schema,
                extractor,
                delete_projection,
                commit_ack_mode,
            },
            mutable,
        ))
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

impl DynModeState {
    /// Convenience wrapper mirroring the previous dynamic mode API.
    pub fn table_definition(config: &DynModeConfig, table_name: &str) -> TableDefinition {
        table_definition(config, table_name)
    }

    /// Build the dynamic mode state and mutable memtable.
    pub fn build(config: DynModeConfig) -> Result<(Self, DynMem), KeyExtractError> {
        config.build()
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
