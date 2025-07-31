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

    /// Returns the index of the primary key column.
    fn primary_key_index(&self) -> usize;

    /// Returns the ([`ColumnPath`], [`Vec<SortingColumn>`]) of the primary key column, representing
    /// the location of the primary key column in the parquet schema and the sort order within a
    /// RowGroup of a leaf column
    fn primary_key_path(&self) -> (ColumnPath, Vec<SortingColumn>);
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

    /// Returns a reference to the record.
    fn as_record_ref(&self) -> Self::Ref<'_>;

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
    /// **Note**: Primary key column are always kept.
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
