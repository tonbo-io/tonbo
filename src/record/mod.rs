pub mod dynamic;
pub mod error;
pub mod key;
pub mod option;
#[cfg(test)]
pub(crate) mod test;

use std::{fmt::Debug, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema as ArrowSchema};
pub use dynamic::*;
use fusio_log::{Decode, Encode};
pub use key::*;
use option::OptionRecordRef;
use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};

use crate::version::timestamp::Ts;

pub trait ArrowArrays: Sized {
    type Record: Record;

    type Builder: ArrowArraysBuilder<Self>;

    fn builder(schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder;

    fn get(
        &self,
        offset: u32,
        projection_mask: &ProjectionMask,
    ) -> Option<Option<<Self::Record as Record>::Ref<'_>>>;

    fn as_record_batch(&self) -> &RecordBatch;
}

pub trait ArrowArraysBuilder<S>: Send
where
    S: ArrowArrays,
{
    fn push(
        &mut self,
        key: Ts<<<<S::Record as Record>::Schema as Schema>::Key as Key>::Ref<'_>>,
        row: Option<<S::Record as Record>::Ref<'_>>,
    );

    fn written_size(&self) -> usize;

    fn finish(&mut self, indices: Option<&[usize]>) -> S;
}

pub trait Schema: Debug + Send + Sync {
    type Record: Record<Schema = Self>;

    type Columns: ArrowArrays<Record = Self::Record>;

    type Key: Key;

    /// Returns the [`arrow::datatypes::Schema`] of the record.
    ///
    /// **Note**: The first column should be `_null`, and the second column should be `_ts`.
    fn arrow_schema(&self) -> &Arc<ArrowSchema>;

    /// Returns all primary key column paths and the row-group sorting configuration.
    ///
    /// The returned slices should reference stable storage: either static data or internal fields
    /// owned by the schema. The `ColumnPath` slice should list Parquet paths for each PK column.
    /// The `SortingColumn` slice defines the row-group sort order, typically `_ts` followed by PKs.
    fn primary_key_paths_and_sorting(&self) -> (&[ColumnPath], &[SortingColumn]);

    /// Returns all primary key column indices.
    ///
    /// For single-column primary keys this should return a one-element slice containing the same
    /// index that would have been returned by the legacy single-index API.
    fn primary_key_indices(&self) -> &[usize];

    // Note: Implementations should ensure parity with `primary_key_indices()` for included
    // columns, and include `_ts` in sorting columns.
}

pub trait Record: 'static + Sized + Decode + Debug + Send + Sync {
    type Schema: Schema<Record = Self>;

    type Ref<'r>: RecordRef<'r, Record = Self>
    where
        Self: 'r;

    /// Returns the primary key of the record. This should be the type defined in the
    /// [`Schema`].
    fn key(&self) -> <<<Self as Record>::Schema as Schema>::Key as Key>::Ref<'_> {
        self.as_record_ref().key()
    }

    /// Returns owned value of record (self)
    fn as_owned_value(&self) -> Self;

    /// Returns a reference to the record.
    fn as_record_ref(&self) -> Self::Ref<'_>;

    /// Applies projection mask
    fn projection(&mut self, projection_mask: &ProjectionMask);

    /// Returns the size of the record in bytes.
    fn size(&self) -> usize;
}

pub trait RecordRef<'r>: Clone + Sized + Encode + Send + Sync {
    type Record: Record;

    /// Returns the primary key of the record. This should be the type that defined in the
    /// [`Schema`].
    fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r>;

    /// Do projection on the record. Only keep the columns specified in the projection mask.
    ///
    /// Note: Primary key column(s) are always kept.
    fn projection(&mut self, projection_mask: &ProjectionMask);

    /// Get the [`RecordRef`] from the [`RecordBatch`] at the given offset.
    ///
    /// `full_schema` is the combination of `_null`, `_ts` and all fields defined in the [`Schema`].
    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        projection_mask: &'r ProjectionMask,
        full_schema: &'r Arc<ArrowSchema>,
    ) -> OptionRecordRef<'r, Self>;
}
