mod num;
mod str;

use std::{hash::Hash, sync::Arc};

use arrow::array::Datum;
use fusio_log::{Decode, Encode};
pub use num::*;

pub trait Key:
    'static + Encode + Decode + Ord + Clone + Send + Sync + Hash + std::fmt::Debug
{
    type Ref<'r>: KeyRef<'r, Key = Self>
    where
        Self: 'r;

    fn as_key_ref(&self) -> Self::Ref<'_>;

    fn to_arrow_datum(&self) -> Arc<dyn Datum>;

    fn as_i32(&self) -> i32;

    fn as_i64(&self) -> i64;

    fn to_bytes(&self) -> &[u8];
}

pub trait KeyRef<'r>: Clone + Encode + Send + Sync + Ord + std::fmt::Debug {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(self) -> Self::Key;
}
