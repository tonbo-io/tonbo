//! Mutable memtable(s).
//!
//! This module implements a columnar-style mutable memtable with a
//! last-writer-wins key index. Today only the dynamic runtime-schema layout
//! (`DynLayout`) is active; new typed layouts can slot in beside it later.

pub(crate) mod dyn_mem;
mod key_size;
pub(crate) mod memtable;
mod metrics;

pub(crate) use dyn_mem::DynMem;
pub(crate) use key_size::KeyHeapSize;
pub(crate) use metrics::MutableMemTableMetrics;

/// Shared metrics interface implemented by mutable memtables.
///
/// This abstraction keeps the `Mode` trait flexible if additional layouts are
/// added in the future; for now it is implemented by the dynamic layout only.
pub trait MutableLayout<K: Ord> {
    /// Approximate memory used by keys plus per-entry overhead.
    fn approx_bytes(&self) -> usize;
}
