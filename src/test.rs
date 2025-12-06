//! Test utilities and helpers for tonbo.
//!
//! This module contains internal test utilities available under `#[cfg(test)]`.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
#[cfg(feature = "tokio")]
use arrow_schema::{Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

#[cfg(feature = "tokio")]
use crate::{mode::DynModeConfig, schema::SchemaBuilder};

/// Trait for types that can be converted into a `DynRow`.
pub(crate) trait IntoDynRow {
    /// Convert into a `DynRow`.
    fn into_dyn_row(self) -> DynRow;
}

impl IntoDynRow for DynRow {
    fn into_dyn_row(self) -> DynRow {
        self
    }
}

impl IntoDynRow for Vec<Option<DynCell>> {
    fn into_dyn_row(self) -> DynRow {
        DynRow(self)
    }
}

/// Build a `RecordBatch` from dynamic rows, validating nullability.
///
/// Accepts either `Vec<DynRow>` or `Vec<Vec<Option<DynCell>>>` for convenience.
///
/// # Errors
/// Returns [`DynError`] if any row violates the schema or array construction fails.
pub(crate) fn build_batch<R: IntoDynRow>(
    schema: SchemaRef,
    rows: Vec<R>,
) -> Result<RecordBatch, DynError> {
    let mut builders = DynBuilders::new(schema.clone(), rows.len());
    for row in rows {
        builders.append_option_row(Some(row.into_dyn_row()))?;
    }
    builders.try_finish_into_batch()
}

/// Convenience helper that builds a DynMode configuration with embedded PK metadata.
#[cfg(feature = "tokio")]
pub(crate) fn config_with_pk(fields: Vec<Field>, primary_key: &[&str]) -> DynModeConfig {
    assert!(
        !primary_key.is_empty(),
        "schema builder requires at least one primary-key column"
    );

    let schema = SchemaRef::new(Schema::new(fields));
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
