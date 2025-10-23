//! Read-only immutable memtables.
//!
//! Currently only the dynamic runtime-schema layout is wired through. The
//! `Mode` trait keeps room to add typed immutable storage again later.

pub(crate) mod keys;
pub(crate) mod memtable;

/// Convenience alias for the immutable segment associated with a `Mode`.
pub(crate) type Immutable<M> =
    memtable::ImmutableMemTable<<M as crate::db::Mode>::Key, <M as crate::db::Mode>::ImmLayout>;
