//! Test-only helpers for building dynamic Arrow batches.
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use typed_arrow_dyn::{
    DynCell, DynColumnBuilder, DynError, new_dyn_builder, validate_nullability,
};

/// Build a `RecordBatch` from dynamic rows, validating nullability.
///
/// # Errors
/// Returns [`DynError`] if any row violates the schema or array construction fails.
pub(crate) fn build_batch(
    schema: SchemaRef,
    rows: Vec<Vec<Option<DynCell>>>,
) -> Result<RecordBatch, DynError> {
    let num_cols = schema.fields().len();
    let mut builders: Vec<Box<dyn DynColumnBuilder>> = schema
        .fields()
        .iter()
        .map(|f| new_dyn_builder(f.data_type()))
        .collect();
    for row in rows {
        if row.len() != num_cols {
            return Err(DynError::ArityMismatch {
                expected: num_cols,
                got: row.len(),
            });
        }
        for (idx, (cell, builder)) in row.into_iter().zip(builders.iter_mut()).enumerate() {
            match cell {
                None => builder.append_null(),
                Some(cell) => builder.append_dyn(cell).map_err(|e| e.at_col(idx))?,
            }
        }
    }

    finish_builders(schema, builders)
}

fn finish_builders(
    schema: SchemaRef,
    mut builders: Vec<Box<dyn DynColumnBuilder>>,
) -> Result<RecordBatch, DynError> {
    let mut arrays = Vec::with_capacity(builders.len());
    for builder in builders.iter_mut() {
        arrays.push(builder.try_finish()?);
    }

    validate_nullability(&schema, &arrays)?;
    RecordBatch::try_new(schema, arrays).map_err(|e| DynError::Builder {
        message: e.to_string(),
    })
}
