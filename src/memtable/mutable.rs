use crossbeam_skiplist::SkipMap;

use crate::{
    schema::{Row, Schema},
    version::{Timestamp, Ts},
};

#[allow(dead_code)]
pub(crate) struct MutableMemTable<R: Row> {
    data: SkipMap<Ts<<R::Schema as Schema>::Key>, Option<R>>,
}

impl<R> MutableMemTable<R>
where
    R: Row + Send,
    <R::Schema as Schema>::Key: Send,
{
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            data: SkipMap::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn insert(&self, key: <R::Schema as Schema>::Key, ts: Timestamp, row: R) {
        self.data.insert(Ts::new(key, ts), Some(row));
    }

    #[allow(dead_code)]
    pub(crate) fn remove(&self, key: <R::Schema as Schema>::Key, ts: Timestamp) {
        self.data.insert(Ts::new(key, ts), None);
    }
}

impl<R> IntoIterator for MutableMemTable<R>
where
    R: Row,
{
    type Item = (Ts<<R::Schema as Schema>::Key>, Option<R>);
    type IntoIter = crossbeam_skiplist::map::IntoIter<Ts<<R::Schema as Schema>::Key>, Option<R>>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
