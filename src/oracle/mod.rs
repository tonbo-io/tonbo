pub(crate) mod timestamp;

use std::{
    collections::{btree_map::Entry, BTreeMap, HashSet},
    hash::Hash,
    ops::Bound,
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

use arrow::{
    array::{PrimitiveArray, Scalar},
    datatypes::UInt32Type,
};
use thiserror::Error;

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub(crate) struct Timestamp(u32);

pub(crate) const EPOCH: Timestamp = Timestamp(0);

impl From<u32> for Timestamp {
    fn from(ts: u32) -> Self {
        Self(ts)
    }
}

impl From<Timestamp> for u32 {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl Timestamp {
    pub(crate) fn to_arrow_scalar(self) -> Scalar<PrimitiveArray<UInt32Type>> {
        PrimitiveArray::<UInt32Type>::new_scalar(self.0)
    }
}

#[derive(Debug)]
pub(crate) struct Oracle<K> {
    now: AtomicU32,
    in_read: Mutex<BTreeMap<Timestamp, usize>>,
    committed_txns: Mutex<BTreeMap<Timestamp, HashSet<K>>>,
}

impl<K> Default for Oracle<K> {
    fn default() -> Self {
        Self {
            now: Default::default(),
            in_read: Default::default(),
            committed_txns: Default::default(),
        }
    }
}

impl<K> Oracle<K>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn start_read(&self) -> Timestamp {
        let mut in_read = self.in_read.lock().unwrap();
        let now = self.now.load(Ordering::Relaxed).into();
        match in_read.entry(now) {
            Entry::Vacant(v) => {
                v.insert(1);
            }
            Entry::Occupied(mut o) => {
                *o.get_mut() += 1;
            }
        }
        now
    }

    pub(crate) fn read_commit(&self, ts: Timestamp) {
        match self.in_read.lock().unwrap().entry(ts) {
            Entry::Vacant(_) => panic!("commit non-existing read"),
            Entry::Occupied(mut o) => match o.get_mut() {
                1 => {
                    o.remove();
                }
                n => {
                    *n -= 1;
                }
            },
        }
    }

    pub(crate) fn start_write(&self) -> Timestamp {
        (self.now.fetch_add(1, Ordering::Relaxed) + 1).into()
    }

    pub(crate) fn write_commit(
        &self,
        read_at: Timestamp,
        write_at: Timestamp,
        in_write: HashSet<K>,
    ) -> Result<(), WriteConflict<K>> {
        let mut committed_txns = self.committed_txns.lock().unwrap();
        let conflicts = committed_txns
            .range((Bound::Excluded(read_at), Bound::Excluded(write_at)))
            .flat_map(|(_, txn)| txn.intersection(&in_write))
            .cloned()
            .collect::<Vec<_>>();

        if !conflicts.is_empty() {
            return Err(WriteConflict { keys: conflicts });
        }

        // TODO: clean committed transactions
        committed_txns.insert(write_at, in_write);
        Ok(())
    }
}

#[derive(Debug, Error)]
#[error("transaction write conflict: {keys:?}")]
pub struct WriteConflict<K> {
    keys: Vec<K>,
}
