//! Read-only immutable memtables.
//!
//! Currently only the dynamic runtime-schema layout is wired through. The
//! `Mode` trait keeps room to add typed immutable storage again later.

pub(crate) mod keys;
pub(crate) mod memtable;
