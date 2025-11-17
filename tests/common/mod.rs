use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use tonbo::{mode::DynModeConfig, schema::SchemaBuilder};

/// Build a schema + dyn-mode config with the provided primary-key columns.
pub fn schema_and_config(fields: Vec<Field>, primary_key: &[&str]) -> (SchemaRef, DynModeConfig) {
    assert!(
        !primary_key.is_empty(),
        "schema builder requires at least one primary-key column"
    );

    let schema = Arc::new(Schema::new(fields));
    let builder = SchemaBuilder::from_schema(Arc::clone(&schema));
    let builder = if primary_key.len() == 1 {
        builder.primary_key(primary_key[0].to_string())
    } else {
        builder.composite_key(primary_key.iter().copied().collect::<Vec<_>>())
    }
    .with_metadata();

    let config = builder
        .build()
        .expect("schema builder configuration should succeed");
    (Arc::clone(&config.schema), config)
}

/// Convenience helper that only returns the schema with embedded metadata.
#[allow(dead_code)]
pub fn schema_with_pk(fields: Vec<Field>, primary_key: &[&str]) -> SchemaRef {
    schema_and_config(fields, primary_key).0
}

/// Convenience helper that only returns the DynMode configuration.
#[allow(dead_code)]
pub fn config_with_pk(fields: Vec<Field>, primary_key: &[&str]) -> DynModeConfig {
    schema_and_config(fields, primary_key).1
}
