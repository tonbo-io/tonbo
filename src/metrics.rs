//! Public metrics surface for Tonbo.
//!
//! These metrics are designed to be stable and usable by both production
//! monitoring and benchmark harnesses.

use std::time::Duration;

use parking_lot::Mutex;

use crate::inmem::mutable::MutableMemTableMetricsSnapshot;

/// Snapshot of database-level metrics.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct DbMetricsSnapshot {
    /// WAL metrics, when durability is enabled.
    pub wal: Option<WalMetricsSnapshot>,
    /// Flush/compaction metrics derived from immutable flushes.
    pub flush: Option<FlushMetricsSnapshot>,
    /// Mutable memtable metrics.
    pub memtable: Option<MemtableMetricsSnapshot>,
    /// Compaction metrics (placeholder until implemented).
    pub compaction: Option<CompactionMetricsSnapshot>,
}

/// WAL metrics exposed for observability.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct WalMetricsSnapshot {
    /// Current depth of the writer queue.
    pub queue_depth: usize,
    /// Highest observed queue depth, if tracked.
    pub max_queue_depth: Option<usize>,
    /// Total bytes written by the WAL writer.
    pub bytes_written: u64,
    /// Number of sync calls performed by the WAL writer.
    pub sync_operations: u64,
    /// Number of times the manifest advanced the WAL floor.
    pub wal_floor_advancements: u64,
    /// Total WAL segments physically pruned.
    pub wal_segments_pruned: u64,
    /// Total WAL segments flagged for deletion during dry-runs.
    pub wal_prune_dry_runs: u64,
    /// Number of failed prune attempts.
    pub wal_prune_failures: u64,
    /// Latency summary for WAL append -> durable acknowledgements.
    pub append_latency: Option<LatencySnapshot>,
}

/// Aggregated flush metrics captured from immutable->SST flushes.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct FlushMetricsSnapshot {
    /// Number of flush events recorded.
    pub flush_count: u64,
    /// Total bytes persisted across all flushes.
    pub total_bytes: u64,
    /// Total elapsed time (microseconds) spent flushing.
    pub total_us: u128,
    /// Longest single flush duration (microseconds).
    pub max_us: u64,
    /// Shortest single flush duration (microseconds).
    pub min_us: u64,
}

/// Simple latency summary.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct LatencySnapshot {
    /// Number of events recorded.
    pub count: u64,
    /// Total latency across all events (microseconds).
    pub total_us: u128,
    /// Longest observed latency (microseconds).
    pub max_us: u64,
    /// Shortest observed latency (microseconds).
    pub min_us: u64,
}

impl LatencySnapshot {
    /// Mean latency in microseconds (best-effort).
    pub fn mean_us(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.total_us as f64 / self.count as f64)
    }
}

/// Mutable memtable metrics.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct MemtableMetricsSnapshot {
    /// Total entries currently held in the memtable.
    pub entries: usize,
    /// Number of insert operations observed.
    pub inserts: u64,
    /// Number of replace operations observed.
    pub replaces: u64,
    /// Approximate key bytes tracked in the memtable index.
    pub approx_key_bytes: usize,
    /// Estimated entry overhead per key (bytes).
    pub entry_overhead: usize,
}

impl From<MutableMemTableMetricsSnapshot> for MemtableMetricsSnapshot {
    fn from(snapshot: MutableMemTableMetricsSnapshot) -> Self {
        Self {
            entries: snapshot.entries,
            inserts: snapshot.inserts,
            replaces: snapshot.replaces,
            approx_key_bytes: snapshot.approx_key_bytes,
            entry_overhead: snapshot.entry_overhead,
        }
    }
}

/// Placeholder compaction metrics (expand once compaction telemetry stabilizes).
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct CompactionMetricsSnapshot {
    /// Whether compaction metrics are supported by the current build.
    pub supported: bool,
}

#[derive(Debug, Default)]
pub(crate) struct FlushMetrics {
    inner: Mutex<FlushMetricsState>,
}

#[derive(Debug, Default, Clone, Copy)]
struct FlushMetricsState {
    flush_count: u64,
    total_bytes: u64,
    total_us: u128,
    max_us: u64,
    min_us: u64,
}

impl FlushMetrics {
    pub(crate) fn record(&self, bytes: u64, duration: Duration) {
        let mut guard = self.inner.lock();
        guard.flush_count = guard.flush_count.saturating_add(1);
        guard.total_bytes = guard.total_bytes.saturating_add(bytes);
        let micros = duration.as_micros();
        guard.total_us = guard.total_us.saturating_add(micros);
        let us = u64::try_from(micros).unwrap_or(u64::MAX);
        if guard.min_us == 0 || us < guard.min_us {
            guard.min_us = us;
        }
        if us > guard.max_us {
            guard.max_us = us;
        }
    }

    pub(crate) fn snapshot(&self) -> FlushMetricsSnapshot {
        let guard = self.inner.lock();
        if guard.flush_count == 0 {
            return FlushMetricsSnapshot::default();
        }
        FlushMetricsSnapshot {
            flush_count: guard.flush_count,
            total_bytes: guard.total_bytes,
            total_us: guard.total_us,
            max_us: guard.max_us,
            min_us: guard.min_us,
        }
    }
}
