use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::errors::KeyExtractError;
use crate::key::{KeyOwned, KeyViewRaw};

/// Batch-level key extractor: implementors produce a key of type `K` from a `RecordBatch` row.
pub trait BatchKeyExtractor<K> {
    /// Validate this extractor against a schema.
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError>;
    /// Extract an owned key for the row.
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<K, KeyExtractError>;
}

/// Trait-object friendly dynamic extractor that yields owned keys.
pub trait DynKeyExtractor {
    /// Validate this extractor against a schema.
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError>;
    /// Extract an owned key from the specified row.
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<KeyOwned, KeyExtractError>;
    /// Populate a `KeyViewRaw` with borrowed components for the specified row.
    ///
    /// Callers should provide an empty `out`; implementations may reuse the buffer.
    fn key_view_at(
        &self,
        batch: &RecordBatch,
        row: usize,
        out: &mut KeyViewRaw,
    ) -> Result<(), KeyExtractError>;
}
