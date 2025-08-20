mod composite;
mod datetime;
mod list;
mod num;
mod str;
mod timestamp;

use std::{hash::Hash, sync::Arc};

use arrow::array::Datum;
pub use datetime::*;
use fusio_log::{Decode, Encode};
pub use list::*;
pub use num::*;
pub use str::*;
pub use timestamp::*;

pub trait Key:
    'static + Encode + Decode + Ord + Clone + Send + Sync + Hash + std::fmt::Debug
{
    type Ref<'r>: KeyRef<'r, Key = Self>;

    fn as_key_ref(&self) -> Self::Ref<'_>;

    /// Returns Arrow scalar Datums corresponding to the primary key columns.
    ///
    /// For single-column primary keys, this will return a single-element vector. For composite
    /// keys, it returns a vector with one Datum per component in lexicographic order.
    fn to_arrow_datums(&self) -> Vec<Arc<dyn Datum>>;
}

pub trait KeyRef<'r>: Clone + Encode + Send + Sync + Ord + std::fmt::Debug {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(self) -> Self::Key;
}
