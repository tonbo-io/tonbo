//! Common test utilities for integration tests.

use std::sync::Arc;

use arrow_schema::{Field, Schema};
use tonbo::{db::DynModeConfig, schema::SchemaBuilder};

/// Convenience helper that builds a DynMode configuration with embedded PK metadata.
pub fn config_with_pk(fields: Vec<Field>, primary_key: &[&str]) -> DynModeConfig {
    assert!(
        !primary_key.is_empty(),
        "schema builder requires at least one primary-key column"
    );

    let schema = Arc::new(Schema::new(fields));
    let builder = SchemaBuilder::from_schema(schema);
    let builder = if primary_key.len() == 1 {
        builder.primary_key(primary_key[0].to_string())
    } else {
        builder.composite_key(primary_key.iter().copied().collect::<Vec<_>>())
    }
    .with_metadata();

    builder
        .build()
        .expect("schema builder configuration should succeed")
}
