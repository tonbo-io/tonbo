//! Shared observability helpers.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
#[allow(unused)]
struct TombstoneMetricsInner {
    emitted_tombstones: AtomicUsize,
    suppressed_versions: AtomicUsize,
}

/// Tracks how many tombstones were observed and how many older versions they suppressed.
#[derive(Clone, Default)]
#[allow(unused)]
pub(crate) struct TombstoneMetrics {
    inner: Arc<TombstoneMetricsInner>,
}

impl TombstoneMetrics {
    /// Record a tombstone entry encountered while scanning.
    pub(crate) fn _record_tombstone(&self) {
        self.inner
            .emitted_tombstones
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a tombstone suppressed an older version of the same key.
    pub(crate) fn _record_suppressed(&self) {
        self.inner
            .suppressed_versions
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Tombstones encountered so far.
    pub(crate) fn _emitted_tombstones(&self) -> usize {
        self.inner.emitted_tombstones.load(Ordering::Relaxed)
    }

    /// Older versions skipped because a tombstone was visible for the same key.
    pub(crate) fn _suppressed_versions(&self) -> usize {
        self.inner.suppressed_versions.load(Ordering::Relaxed)
    }
}
