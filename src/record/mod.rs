pub mod internal;
mod str;

use std::{error::Error, fmt::Debug, hash::Hash, io, sync::Arc};

use arrow::{
    array::{Datum, RecordBatch},
    datatypes::Schema,
};
use internal::InternalRecordRef;
use parquet::arrow::ProjectionMask;
use thiserror::Error;

use crate::{
    inmem::immutable::ArrowArrays,
    serdes::{Decode, Encode},
};

pub trait Key:
    'static + Encode + Decode + Ord + Clone + Send + Sync + Hash + std::fmt::Debug
{
    type Ref<'r>: KeyRef<'r, Key = Self> + Copy + Debug
    where
        Self: 'r;

    fn as_key_ref(&self) -> Self::Ref<'_>;

    fn to_arrow_datum(&self) -> impl Datum;
}

pub trait KeyRef<'r>: Clone + Encode + PartialEq<Self::Key> + Ord {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(&self) -> Self::Key;
}

pub trait Record: 'static + Sized + Decode {
    type Columns: ArrowArrays<Record = Self>;

    type Key: Key;

    type Ref<'r>: RecordRef<'r, Record = Self> + Copy
    where
        Self: 'r;

    fn key(&self) -> <<Self as Record>::Key as Key>::Ref<'_> {
        self.as_record_ref().key()
    }

    fn primary_key_index() -> usize;

    fn as_record_ref(&self) -> Self::Ref<'_>;

    fn arrow_schema() -> &'static Arc<Schema>;
}

pub trait RecordRef<'r>: Clone + Sized + Copy + Encode {
    type Record: Record;

    fn key(self) -> <<Self::Record as Record>::Key as Key>::Ref<'r>;

    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        projection_mask: &'r ProjectionMask,
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
}
