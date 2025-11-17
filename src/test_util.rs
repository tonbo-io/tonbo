//! Test-only helpers for building dynamic Arrow batches.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

pub trait IntoDynRow {
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
/// # Errors
/// Returns [`DynError`] if any row violates the schema or array construction fails.
pub(crate) fn build_batch<R>(schema: SchemaRef, rows: Vec<R>) -> Result<RecordBatch, DynError>
where
    R: IntoDynRow,
{
    let dyn_rows: Vec<DynRow> = rows.into_iter().map(|row| row.into_dyn_row()).collect();
    let mut builders = DynBuilders::new(schema.clone(), dyn_rows.len());
    for row in dyn_rows {
        builders.append_option_row(Some(row))?;
    }
    builders.try_finish_into_batch().map_err(Into::into)
}
