use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::errors::KeyExtractError;
use crate::key::{KeyOwned, KeyViewRaw};

/// Schema-validated projection that can materialise logical keys from record batches.
pub trait KeyProjection {
    /// Ensure the projection is compatible with `schema`.
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError>;

    /// Populate `out` with a borrowed key view for `row` in `batch`.
    ///
    /// Implementations must fully overwrite `out`; callers may reuse the buffer across rows.
    fn project_view(
        &self,
        batch: &RecordBatch,
        row: usize,
        out: &mut KeyViewRaw,
    ) -> Result<(), KeyExtractError>;

    /// Materialise an owned key for `row` using the borrowed projection.
    fn project_owned(&self, batch: &RecordBatch, row: usize) -> Result<KeyOwned, KeyExtractError> {
        let mut view = KeyViewRaw::new();
        self.project_view(batch, row, &mut view)?;
        Ok(view.to_owned())
    }
}
