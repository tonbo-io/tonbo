//! In-memory structures (mutable and immutable memtables).
//!
//! - `mutable/` contains the columnar mutable memtable with a last-writer key index.
//! - `immutable/` contains typed arrays and the zero-copy immutable memtable.

pub(crate) mod immutable;
pub(crate) mod mutable;
pub(crate) mod policy;
