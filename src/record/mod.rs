pub mod internal;
mod key;
pub mod runtime;
#[cfg(test)]
mod str;

use std::{error::Error, fmt::Debug, io, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema};
use internal::InternalRecordRef;
pub use key::{Key, KeyRef};
use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};
pub use runtime::*;
use thiserror::Error;

use crate::{
    inmem::immutable::ArrowArrays,
    serdes::{Decode, Encode},
};

#[allow(unused)]
pub(crate) enum RecordInstance {
    Normal,
    Runtime(DynRecord),
}

#[allow(unused)]
impl RecordInstance {
    pub(crate) fn primary_key_index<R>(&self) -> usize
    where
        R: Record,
    {
        match self {
            RecordInstance::Normal => R::primary_key_index(),
            RecordInstance::Runtime(record) => record.primary_key_index(),
        }
    }

    pub(crate) fn arrow_schema<R>(&self) -> Arc<Schema>
    where
        R: Record,
    {
        match self {
            RecordInstance::Normal => R::arrow_schema().clone(),
            RecordInstance::Runtime(record) => record.arrow_schema(),
        }
    }
}

pub trait Record: 'static + Sized + Decode + Debug + Send + Sync {
    type Columns: ArrowArrays<Record = Self>;

    type Key: Key;

    type Ref<'r>: RecordRef<'r, Record = Self>
    where
        Self: 'r;

    fn key(&self) -> <<Self as Record>::Key as Key>::Ref<'_> {
        self.as_record_ref().key()
    }

    fn primary_key_index() -> usize;

    fn primary_key_path() -> (ColumnPath, Vec<SortingColumn>);

    fn as_record_ref(&self) -> Self::Ref<'_>;

    fn arrow_schema() -> &'static Arc<Schema>;

    fn size(&self) -> usize;
}

pub trait RecordRef<'r>: Clone + Sized + Encode + Send + Sync {
    type Record: Record;

    fn key(self) -> <<Self::Record as Record>::Key as Key>::Ref<'r>;

    fn projection(&mut self, projection_mask: &ProjectionMask);

    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        projection_mask: &'r ProjectionMask,
        full_schema: &'r Arc<Schema>,
    ) -> InternalRecordRef<'r, Self>;
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
