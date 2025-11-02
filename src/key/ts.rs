//! Composite `(key, timestamp)` helpers used by MVCC-aware structures.

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use super::{KeyOwned, KeyViewRaw};
use crate::mvcc::Timestamp;

/// Borrowed composite key pairing a raw key view with its commit timestamp.
#[derive(Clone, Debug)]
pub struct KeyTsViewRaw {
    key: KeyViewRaw,
    ts: Timestamp,
}

impl KeyTsViewRaw {
    /// Build a new `(key, timestamp)` view from a raw key.
    ///
    /// # Safety
    ///
    /// Callers must ensure the `KeyViewRaw`'s borrowed slices remain valid for
    /// the lifetime of the returned view.
    pub unsafe fn new_unchecked(key: KeyViewRaw, ts: Timestamp) -> Self {
        Self { key, ts }
    }

    /// Build a raw view derived from an owned key. Safe because the owned key
    /// retains the backing buffers.
    pub fn from_owned(key: &KeyOwned, ts: Timestamp) -> Self {
        // Safe because `KeyViewRaw::from_owned` borrows from an owned `Arc`.
        unsafe { Self::new_unchecked(KeyViewRaw::from_owned(key), ts) }
    }

    /// Borrow the key component.
    pub fn key(&self) -> &KeyViewRaw {
        &self.key
    }

    /// Commit timestamp carried by the entry.
    pub fn timestamp(&self) -> Timestamp {
        self.ts
    }

    /// Decompose the view.
    pub fn into_parts(self) -> (KeyViewRaw, Timestamp) {
        (self.key, self.ts)
    }

    /// Return an owned counterpart.
    pub fn to_owned(&self) -> KeyTsOwned {
        KeyTsOwned::new(self.key.to_owned(), self.ts)
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
    pub fn new(key: KeyOwned, ts: Timestamp) -> Self {
        Self { key, ts }
    }

    /// Borrow the owned key.
    pub fn key(&self) -> &KeyOwned {
        &self.key
    }

    /// Commit timestamp carried by the entry.
    pub fn timestamp(&self) -> Timestamp {
        self.ts
    }

    /// Visit as a borrowed raw view.
    pub fn as_raw_view(&self) -> KeyTsViewRaw {
        KeyTsViewRaw::from_owned(&self.key, self.ts)
    }

    /// Decompose the pair.
    pub fn into_parts(self) -> (KeyOwned, Timestamp) {
        (self.key, self.ts)
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
