mod key;
pub mod option;
pub mod runtime;
#[cfg(test)]
pub(crate) mod test;

use std::{error::Error, fmt::Debug, io, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema as ArrowSchema};
use fusio_log::{Decode, Encode};
pub use key::{Key, KeyRef};
use option::OptionRecordRef;
use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};
pub use runtime::*;
use thiserror::Error;

use crate::inmem::immutable::ArrowArrays;

pub trait Schema: Debug + Send + Sync {
    type Record: Record<Schema = Self>;

    type Columns: ArrowArrays<Record = Self::Record>;

    type Key: Key;

    fn arrow_schema(&self) -> &Arc<ArrowSchema>;

    fn primary_key_index(&self) -> usize;

    fn primary_key_path(&self) -> (ColumnPath, Vec<SortingColumn>);
}

pub trait Record: 'static + Sized + Decode + Debug + Send + Sync {
    type Schema: Schema<Record = Self>;

    type Ref<'r>: RecordRef<'r, Record = Self>
    where
        Self: 'r;

    fn key(&self) -> <<<Self as Record>::Schema as Schema>::Key as Key>::Ref<'_> {
        self.as_record_ref().key()
    }

    fn as_record_ref(&self) -> Self::Ref<'_>;

    fn size(&self) -> usize;
}

pub trait RecordRef<'r>: Clone + Sized + Encode + Send + Sync {
    type Record: Record;

    fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r>;

    fn projection(&mut self, projection_mask: &ProjectionMask);

    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        projection_mask: &'r ProjectionMask,
        full_schema: &'r Arc<ArrowSchema>,
    ) -> OptionRecordRef<'r, Self>;
}

#[derive(Debug, Error)]
pub enum RecordEncodeError {
    #[error("record's field: {field_name} encode error: {error}")]
    Encode {
        field_name: String,
        error: Box<dyn Error + Send + Sync + 'static>,
    },
    #[error("record io error: {0}")]
    Io(#[from] io::Error),
    #[error("record fusio error: {0}")]
    Fusio(#[from] fusio::Error),
}

#[derive(Debug, Error)]
pub enum RecordDecodeError {
    #[error("record's field: {field_name} decode error: {error}")]
    Decode {
        field_name: String,
        error: Box<dyn Error + Send + Sync + 'static>,
    },
    #[error("record io error: {0}")]
    Io(#[from] io::Error),
    #[error("record fusio error: {0}")]
    Fusio(#[from] fusio::Error),
}
