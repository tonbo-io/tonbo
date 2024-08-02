mod num;
mod str;

use std::hash::Hash;

use arrow::array::Datum;

use crate::serdes::{Decode, Encode};

pub trait Key:
    'static + Encode + Decode + Ord + Clone + Send + Sync + Hash + std::fmt::Debug
{
    type Ref<'r>: KeyRef<'r, Key = Self> + Copy
    where
        Self: 'r;

    fn as_key_ref(&self) -> Self::Ref<'_>;

    fn to_arrow_datum(&self) -> impl Datum;
}

pub trait KeyRef<'r>: Clone + Encode + Ord + std::fmt::Debug {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(&self) -> Self::Key;
}
