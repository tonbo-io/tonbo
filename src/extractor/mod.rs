//! Arrow `RecordBatch` key extraction helpers.
//!
//! These APIs are Tonbo-specific shims that turn Arrow batches into the
//! zero-copy key views defined under [`crate::key`]. The compile-time typed
//! record support that previously lived here has been removed; keeping the module
//! focused on extraction keeps the ingest path clear.

mod errors;
mod extractors;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
pub(crate) use errors::KeyExtractError;
pub(crate) use extractors::{projection_for_columns, projection_for_field, row_from_batch};

use crate::key::KeyRow;

/// Schema-validated projection that can materialise logical keys from record batches.
pub trait KeyProjection {
    /// Ensure the projection is compatible with `schema`.
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError>;

    /// Project borrowed key views for the requested `rows` (in order) from `batch`.
    fn project_view(
        &self,
        batch: &RecordBatch,
        rows: &[usize],
    ) -> Result<Vec<KeyRow>, KeyExtractError>;

    /// Return the zero-based column indices participating in the projection.
    fn key_indices(&self) -> Vec<usize>;
}
