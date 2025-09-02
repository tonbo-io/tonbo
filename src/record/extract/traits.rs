use typed_arrow::{arrow_array::RecordBatch, arrow_schema::SchemaRef};

use super::{errors::KeyExtractError, key_dyn::KeyDyn};

/// Batch-level key extractor: implementors produce a key of type `K` from a `RecordBatch` row.
pub trait BatchKeyExtractor<K> {
    /// Validate this extractor against a schema.
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError>;
    /// Extract an owned key for the row.
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<K, KeyExtractError>;
}

/// Trait-object friendly dynamic extractor that yields `KeyDyn`.
pub trait DynKeyExtractor {
    /// Validate this extractor against a schema.
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError>;
    /// Extract a `KeyDyn` from the specified row.
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<KeyDyn, KeyExtractError>;
}

/// Generic adapter turning any `BatchKeyExtractor<K>` into a `DynKeyExtractor` using
/// `Into<KeyDyn>`.
pub struct DynFromBatch<E, K> {
    pub(crate) inner: E,
    pub(crate) _phantom: std::marker::PhantomData<K>,
}

impl<E, K> DynFromBatch<E, K> {
    /// Wrap a batch extractor for dynamic use.
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<E, K> DynKeyExtractor for DynFromBatch<E, K>
where
    E: BatchKeyExtractor<K>,
    K: Into<KeyDyn>,
{
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        self.inner.validate_schema(schema)
    }
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<KeyDyn, KeyExtractError> {
        Ok(self.inner.key_at(batch, row)?.into())
    }
}
