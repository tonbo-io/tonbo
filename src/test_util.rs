//! Test-only helpers for building dynamic Arrow batches.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use typed_arrow_dyn::{DynBuilders, DynError, DynRow};

/// Build a `RecordBatch` from dynamic rows, validating nullability.
///
/// # Errors
/// Returns [`DynError`] if any row violates the schema or array construction fails.
pub(crate) fn build_batch(schema: SchemaRef, rows: Vec<DynRow>) -> Result<RecordBatch, DynError> {
    let mut builders = DynBuilders::new(schema.clone(), rows.len());
    for row in rows {
        builders.append_option_row(Some(row))?;
    }
    builders.try_finish_into_batch().map_err(Into::into)
}
