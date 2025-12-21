//! Public metrics surface for Tonbo.
//!
//! These metrics are designed to be stable and usable by both production
//! monitoring and benchmark harnesses.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

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
    /// Compaction metrics when major compaction is enabled.
    pub compaction: Option<CompactionMetricsSnapshot>,
    /// Read-path metrics aggregated across scans.
    pub read_path: Option<ReadPathMetricsSnapshot>,
    /// Cache metrics (if a cache is enabled).
    pub cache: Option<CacheMetricsSnapshot>,
    /// Object-store metrics for Tonbo-managed reads/writes.
    pub object_store: Option<ObjectStoreMetricsSnapshot>,
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

/// Aggregated compaction metrics.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct CompactionMetricsSnapshot {
    /// Whether compaction metrics are supported by the current build.
    pub supported: bool,
    /// Number of completed compaction runs.
    pub runs: u64,
    /// Number of failed compaction runs.
    pub failures: u64,
    /// CAS conflicts encountered while publishing compaction edits.
    pub cas_conflicts: u64,
    /// Total number of input SSTs compacted.
    pub input_ssts: u64,
    /// Total number of output SSTs produced.
    pub output_ssts: u64,
    /// Total input bytes across compaction runs.
    pub input_bytes: u64,
    /// Total output bytes across compaction runs.
    pub output_bytes: u64,
    /// Total input rows across compaction runs.
    pub input_rows: u64,
    /// Total output rows across compaction runs.
    pub output_rows: u64,
    /// Total compaction duration (microseconds).
    pub total_us: u128,
    /// Longest single compaction duration (microseconds).
    pub max_us: u64,
    /// Shortest single compaction duration (microseconds).
    pub min_us: u64,
}

/// Aggregated read-path metrics (scan planning/execution).
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ReadPathMetricsSnapshot {
    /// Number of scan plans built.
    pub plan_count: u64,
    /// Total time spent planning scans (microseconds).
    pub plan_total_us: u64,
    /// Longest single plan duration (microseconds).
    pub plan_max_us: u64,
    /// Number of scan streams executed.
    pub scan_count: u64,
    /// Total time spent executing scans (microseconds).
    pub scan_total_us: u64,
    /// Longest single scan duration (microseconds).
    pub scan_max_us: u64,
    /// Number of merged entries observed (pre-residual predicate).
    pub entries_seen: u64,
    /// Number of rows examined after MVCC/tombstone filtering.
    pub rows_examined: u64,
    /// Number of rows filtered by residual predicates.
    pub rows_filtered: u64,
    /// Number of rows emitted to callers.
    pub rows_emitted: u64,
    /// Number of record batches emitted.
    pub batches_emitted: u64,
}

/// Cache metrics snapshot (optional).
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct CacheMetricsSnapshot {
    /// Whether cache metrics are supported by the current build.
    pub supported: bool,
    /// Cache entries currently held.
    pub entries: u64,
    /// Bytes currently held in cache.
    pub bytes: u64,
    /// Cache hits observed.
    pub hits: u64,
    /// Cache misses observed.
    pub misses: u64,
    /// Cache evictions observed.
    pub evictions: u64,
}

/// Object-store metrics for Tonbo-managed I/O.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ObjectStoreMetricsSnapshot {
    /// Number of read operations issued.
    pub read_ops: u64,
    /// Total bytes read (best-effort, logical).
    pub read_bytes: u64,
    /// Number of write operations issued.
    pub write_ops: u64,
    /// Total bytes written (best-effort, logical).
    pub write_bytes: u64,
    /// Number of list operations issued.
    pub list_ops: u64,
    /// Number of head/stat operations issued.
    pub head_ops: u64,
    /// Number of delete operations issued.
    pub delete_ops: u64,
    /// Number of object-store errors observed.
    pub errors: u64,
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

#[derive(Debug, Default)]
pub(crate) struct CompactionMetrics {
    inner: Mutex<CompactionMetricsState>,
}

#[derive(Debug, Default, Clone, Copy)]
struct CompactionMetricsState {
    runs: u64,
    failures: u64,
    cas_conflicts: u64,
    input_ssts: u64,
    output_ssts: u64,
    input_bytes: u64,
    output_bytes: u64,
    input_rows: u64,
    output_rows: u64,
    total_us: u128,
    max_us: u64,
    min_us: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CompactionIoStats {
    pub(crate) input_ssts: u64,
    pub(crate) output_ssts: u64,
    pub(crate) input_bytes: u64,
    pub(crate) output_bytes: u64,
    pub(crate) input_rows: u64,
    pub(crate) output_rows: u64,
}

impl CompactionMetrics {
    pub(crate) fn record_success(&self, stats: CompactionIoStats, duration: Duration) {
        let mut guard = self.inner.lock();
        guard.runs = guard.runs.saturating_add(1);
        guard.input_ssts = guard.input_ssts.saturating_add(stats.input_ssts);
        guard.output_ssts = guard.output_ssts.saturating_add(stats.output_ssts);
        guard.input_bytes = guard.input_bytes.saturating_add(stats.input_bytes);
        guard.output_bytes = guard.output_bytes.saturating_add(stats.output_bytes);
        guard.input_rows = guard.input_rows.saturating_add(stats.input_rows);
        guard.output_rows = guard.output_rows.saturating_add(stats.output_rows);
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

    pub(crate) fn record_failure(&self) {
        let mut guard = self.inner.lock();
        guard.failures = guard.failures.saturating_add(1);
    }

    pub(crate) fn record_cas_conflict(&self) {
        let mut guard = self.inner.lock();
        guard.cas_conflicts = guard.cas_conflicts.saturating_add(1);
    }

    pub(crate) fn snapshot(&self) -> CompactionMetricsSnapshot {
        let guard = self.inner.lock();
        CompactionMetricsSnapshot {
            supported: true,
            runs: guard.runs,
            failures: guard.failures,
            cas_conflicts: guard.cas_conflicts,
            input_ssts: guard.input_ssts,
            output_ssts: guard.output_ssts,
            input_bytes: guard.input_bytes,
            output_bytes: guard.output_bytes,
            input_rows: guard.input_rows,
            output_rows: guard.output_rows,
            total_us: guard.total_us,
            max_us: guard.max_us,
            min_us: guard.min_us,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReadPathMetrics {
    plan: Mutex<ReadPathPlanMetrics>,
    scan: Mutex<ReadPathScanMetrics>,
    entries_seen: AtomicU64,
    rows_examined: AtomicU64,
    rows_filtered: AtomicU64,
    rows_emitted: AtomicU64,
    batches_emitted: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy)]
struct ReadPathPlanMetrics {
    count: u64,
    total_us: u64,
    max_us: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct ReadPathScanMetrics {
    count: u64,
    total_us: u64,
    max_us: u64,
}

impl ReadPathMetrics {
    pub(crate) fn record_plan(&self, duration: Duration) {
        let micros = duration.as_micros();
        let us = u64::try_from(micros).unwrap_or(u64::MAX);
        let mut guard = self.plan.lock();
        guard.count = guard.count.saturating_add(1);
        guard.total_us = guard.total_us.saturating_add(us);
        if us > guard.max_us {
            guard.max_us = us;
        }
    }

    pub(crate) fn record_scan_start(&self) {
        let mut guard = self.scan.lock();
        guard.count = guard.count.saturating_add(1);
    }

    pub(crate) fn record_scan_duration(&self, duration: Duration) {
        let micros = duration.as_micros();
        let us = u64::try_from(micros).unwrap_or(u64::MAX);
        let mut guard = self.scan.lock();
        guard.total_us = guard.total_us.saturating_add(us);
        if us > guard.max_us {
            guard.max_us = us;
        }
    }

    pub(crate) fn record_entry_seen(&self) {
        self.entries_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_row_examined(&self) {
        self.rows_examined.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_row_filtered(&self) {
        self.rows_filtered.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_row_emitted(&self) {
        self.rows_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_batch_emitted(&self) {
        self.batches_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ReadPathMetricsSnapshot {
        let plan = self.plan.lock();
        let scan = self.scan.lock();
        ReadPathMetricsSnapshot {
            plan_count: plan.count,
            plan_total_us: plan.total_us,
            plan_max_us: plan.max_us,
            scan_count: scan.count,
            scan_total_us: scan.total_us,
            scan_max_us: scan.max_us,
            entries_seen: self.entries_seen.load(Ordering::Relaxed),
            rows_examined: self.rows_examined.load(Ordering::Relaxed),
            rows_filtered: self.rows_filtered.load(Ordering::Relaxed),
            rows_emitted: self.rows_emitted.load(Ordering::Relaxed),
            batches_emitted: self.batches_emitted.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ObjectStoreMetrics {
    read_ops: AtomicU64,
    read_bytes: AtomicU64,
    write_ops: AtomicU64,
    write_bytes: AtomicU64,
    list_ops: AtomicU64,
    head_ops: AtomicU64,
    delete_ops: AtomicU64,
    errors: AtomicU64,
}

impl ObjectStoreMetrics {
    pub(crate) fn record_read(&self, bytes: u64) {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_write(&self, bytes: u64) {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_list(&self) {
        self.list_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_head(&self) {
        self.head_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_delete(&self) {
        self.delete_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ObjectStoreMetricsSnapshot {
        ObjectStoreMetricsSnapshot {
            read_ops: self.read_ops.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_ops: self.write_ops.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            list_ops: self.list_ops.load(Ordering::Relaxed),
            head_ops: self.head_ops.load(Ordering::Relaxed),
            delete_ops: self.delete_ops.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}
