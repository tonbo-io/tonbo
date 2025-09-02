//! Read-only immutable memtables.
//!
//! - `typed`: typed, ArrowArrays-like storage and key index
//! - `dynamic`: runtime schema snapshot indexed by `KeyDyn`
//! - `arrays`: typed array wrappers and builders
//! - `keys`: zero-copy owning key types for string/binary

pub(crate) mod arrays;
pub(crate) mod keys;
pub(crate) mod memtable;
