pub(crate) mod internal;
mod str;

use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{Datum, RecordBatch},
    datatypes::Schema,
};
use internal::InternalRecordRef;

use crate::{
    inmem::immutable::ArrowArrays,
    serdes::{Decode, Encode},
};

pub trait Key: 'static + Debug + Encode + Decode + Ord + Clone + Send {
    type Ref<'r>: KeyRef<'r, Key = Self> + Copy
    where
        Self: 'r;

    fn as_key_ref(&self) -> Self::Ref<'_>;

    fn to_arrow_datum(&self) -> impl Datum;
}

pub trait KeyRef<'r>: Clone + Encode + PartialEq<Self::Key> + Ord {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(&self) -> Self::Key;
}

pub trait Record: 'static + Sized {
    type Columns: ArrowArrays<Record = Self>;

    type Key: Key;

    type Ref<'r>: RecordRef<'r, Record = Self> + Copy
    where
        Self: 'r;

    fn key(&self) -> <<Self as Record>::Key as Key>::Ref<'_> {
        self.as_record_ref().key()
    }

    fn as_record_ref(&self) -> Self::Ref<'_>;

    fn arrow_schema() -> &'static Arc<Schema>;
}

pub trait RecordRef<'r>: Clone + Sized + Copy {
    type Record: Record;

    fn key(self) -> <<Self::Record as Record>::Key as Key>::Ref<'r>;

    fn from_record_batch(record_batch: &'r RecordBatch, offset: usize) -> InternalRecordRef<Self>;
}
