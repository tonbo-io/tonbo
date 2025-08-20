//! Mutable memtable(s).
//!
//! This module implements a columnar-style mutable memtable with a
//! last-writer-wins key index. For typed mode it buffers typed rows and
//! maintains an index; for dynamic mode it attaches incoming `RecordBatch`
//! chunks and indexes them by key. Scans over the mutable state return
//! full row values.

mod key_size;
pub(crate) mod memtable;
mod metrics;

pub(crate) use key_size::KeyHeapSize;
pub(crate) use metrics::MutableMemTableMetrics;

/// Shared metrics interface implemented by mutable memtables.
///
/// This abstracts over typed and dynamic mutable implementations so callers can
/// obtain approximate memory usage without knowing the concrete storage type.
pub trait MutableLayout<K: Ord> {
    /// Approximate memory used by keys plus per-entry overhead.
    fn approx_bytes(&self) -> usize;
}
