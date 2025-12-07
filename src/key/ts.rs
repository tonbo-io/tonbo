//! Composite `(key, timestamp)` helpers used by MVCC-aware structures.

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use super::{KeyOwned, KeyRow};
use crate::mvcc::Timestamp;

/// Borrowed composite key pairing a raw key view with its commit timestamp.
#[derive(Clone, Debug)]
pub struct KeyTsViewRaw {
    key: KeyRow,
    ts: Timestamp,
}

impl KeyTsViewRaw {
    /// Build a new `(key, timestamp)` view from a key row.
    pub(crate) fn new(key: KeyRow, ts: Timestamp) -> Self {
        Self { key, ts }
    }

    /// Build a raw view derived from an owned key. Safe because the owned key
    /// retains the backing buffers.
    pub(crate) fn from_owned(key: &KeyOwned, ts: Timestamp) -> Self {
        let key_row =
            KeyRow::from_owned(key).expect("KeyOwned should only contain supported key components");
        Self::new(key_row, ts)
    }

    /// Borrow the key component.
    pub(crate) fn key(&self) -> &KeyRow {
        &self.key
    }

    /// Commit timestamp carried by the entry.
    pub(crate) fn timestamp(&self) -> Timestamp {
        self.ts
    }

    /// Decompose the view.
    pub(crate) fn into_parts(self) -> (KeyRow, Timestamp) {
        (self.key, self.ts)
    }
}

impl PartialEq for KeyTsViewRaw {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts && self.key == other.key
    }
}

impl Eq for KeyTsViewRaw {}

impl PartialOrd for KeyTsViewRaw {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyTsViewRaw {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => other.ts.cmp(&self.ts),
            ordering => ordering,
        }
    }
}

impl Hash for KeyTsViewRaw {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.ts.hash(state);
    }
}

/// Owned `(key, timestamp)` pair.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyTsOwned {
    key: KeyOwned,
    ts: Timestamp,
}

impl KeyTsOwned {
    /// Construct an owned composite key.
    pub(crate) fn new(key: KeyOwned, ts: Timestamp) -> Self {
        Self { key, ts }
    }

    /// Borrow the owned key.
    pub(crate) fn key(&self) -> &KeyOwned {
        &self.key
    }

    /// Commit timestamp carried by the entry.
    pub(crate) fn timestamp(&self) -> Timestamp {
        self.ts
    }

    /// Visit as a borrowed raw view.
    pub(crate) fn as_raw_view(&self) -> KeyTsViewRaw {
        KeyTsViewRaw::from_owned(&self.key, self.ts)
    }
}

impl PartialOrd for KeyTsOwned {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyTsOwned {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => other.ts.cmp(&self.ts),
            ordering => ordering,
        }
    }
}

impl From<(KeyOwned, Timestamp)> for KeyTsOwned {
    fn from((key, ts): (KeyOwned, Timestamp)) -> Self {
        KeyTsOwned::new(key, ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_orders_descending_timestamps_per_key() {
        let key = KeyOwned::from("a");
        let v1 = KeyTsViewRaw::from_owned(&key, Timestamp::new(1));
        let v0 = KeyTsViewRaw::from_owned(&key, Timestamp::new(0));

        assert!(v1 < v0);
    }

    #[test]
    fn owned_orders_key_then_timestamp() {
        let a1 = KeyTsOwned::new(KeyOwned::from("a"), Timestamp::new(1));
        let a0 = KeyTsOwned::new(KeyOwned::from("a"), Timestamp::new(0));
        let b1 = KeyTsOwned::new(KeyOwned::from("b"), Timestamp::new(1));

        assert!(a1 < a0);
        assert!(a0 < b1);
    }
}
