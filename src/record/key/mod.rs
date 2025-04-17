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
}

pub trait KeyRef<'r>: Clone + Encode + Send + Sync + Ord + std::fmt::Debug {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(self) -> Self::Key;
}
