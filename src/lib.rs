use std::{cmp::Ordering, collections::BTreeSet};

use typed_arrow::{
    arrow_array::RecordBatch,
    schema::{BuildRows, SchemaMeta},
};

// Key: blanket-implemented for all Ord types
// Record comparable by a logical key (single or composite)
pub trait Record: SchemaMeta + BuildRows {
    fn cmp_key(&self, other: &Self) -> Ordering;
}

// Per-field macro: implement crate::Record when a field is marked as key
// Requires typed-arrow's `ext-hooks` feature in this crate's dependency.
macro_rules! key_field {
    // Valid key: non-nullable, non-nested
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = false, is_nested = false, ext = (key $($rest:tt)*)) => {
        impl crate::Record for $owner {
            type Key = $ty;
            fn key(&self) -> &Self::Key {
                &self.$fname
            }
        }
    };
    // Reject nullable keys
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = true, is_nested = $nested:expr, ext = (key $($rest:tt)*)) => {
        compile_error!("#[record(ext(key))] field cannot be nullable");
    };
    // Reject nested struct keys
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = $nul:expr, is_nested = true, ext = (key $($rest:tt)*)) => {
        compile_error!("#[record(ext(key))] field cannot be #[record(nested)]");
    };
    // Default: no-op for other fields
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = $nul:expr, is_nested = $nested:expr, ext = ($($rest:tt)*)) => {};
}

#[derive(Debug)]
struct Ordered<R> {
    inner: R,
}

impl<R: Record> PartialEq for Ordered<R> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.cmp_key(&other.inner) == Ordering::Equal
    }
}

impl<R: Record> Eq for Ordered<R> {}

impl<R: Record> PartialOrd for Ordered<R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<R: Record> Ord for Ordered<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner.cmp_key(&other.inner)
    }
}

#[derive(Debug)]
struct MutableMemTable<R> {
    inner: BTreeSet<Ordered<R>>,
}

impl<R: Record> MutableMemTable<R> {
    fn new() -> Self {
        MutableMemTable {
            inner: BTreeSet::new(),
        }
    }

    fn insert(&mut self, value: R) {
        self.inner.insert(Ordered { inner: value });
    }
}

struct ImmutableMemTable {
    inner: RecordBatch,
}

// Record macro: composite cascading key
// Usage: #[record(record_macro = crate::composite_key, ext(id, ts, seq))]
#[macro_export]
macro_rules! composite_key {
    (owner = $owner:ty, len = $len:expr, ext = ($first:ident $(, $rest:ident)* $(,)?)) => {
        impl $crate::Record for $owner {
            fn cmp_key(&self, other: &Self) -> ::core::cmp::Ordering {
                let ord = self.$first.cmp(&other.$first);
                if ord != ::core::cmp::Ordering::Equal { return ord; }
                $(
                    let ord = self.$rest.cmp(&other.$rest);
                    if ord != ::core::cmp::Ordering::Equal { return ord; }
                )*
                ::core::cmp::Ordering::Equal
            }
        }
    };
    (owner = $owner:ty, len = $len:expr, ext = ()) => { compile_error!("composite_key requires at least one field in ext(...)"); };
}

#[cfg(test)]
mod tests {
    use crate::MutableMemTable;

    #[derive(typed_arrow::Record)]
    #[record(record_macro = crate::composite_key, ext(id))]
    struct OneKey {
        id: u32,
    }

    #[derive(typed_arrow::Record, Debug)]
    #[record(record_macro = crate::composite_key, ext(user_id, ts, deleted))]
    struct TwoKeys {
        user_id: u64,
        ts: i64,
        deleted: bool,
        text: String,
    }

    #[test]
    fn test_insert_one_key() {
        let mut table = MutableMemTable::<OneKey>::new();
        table.insert(OneKey { id: 2 });
        table.insert(OneKey { id: 1 });
        table.insert(OneKey { id: 3 });
        assert_eq!(table.inner.len(), 3);
    }

    #[test]
    fn test_cascade_two_keys() {
        let mut table = MutableMemTable::<TwoKeys>::new();
        table.insert(TwoKeys {
            user_id: 1,
            ts: 20,
            deleted: false,
            text: "Hello".to_string(),
        });
        table.insert(TwoKeys {
            user_id: 1,
            ts: 10,
            deleted: false,
            text: "World".to_string(),
        });
        table.insert(TwoKeys {
            user_id: 2,
            ts: 5,
            deleted: false,
            text: "Foo".to_string(),
        });
        assert_eq!(table.inner.len(), 3);

        dbg!(table);
    }
}
