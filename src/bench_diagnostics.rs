use std::{sync::Arc, time::Duration};

use parking_lot::Mutex;

/// Optional callbacks used to capture bench-only diagnostics without modifying the
/// core database paths.
pub trait BenchObserver: Send + Sync {
    /// Record a flush event exposed by the engine.
    fn record_flush(&self, bytes: u64, duration: Duration);

    /// Return a flush snapshot for reporting.
    fn flush_snapshot(&self) -> FlushDiagnosticsSnapshot;

    /// Update the stored WAL diagnostics snapshot if available.
    fn update_wal(&self, snapshot: WalDiagnosticsSnapshot);

    /// Produce a combined snapshot (WAL + flush) for reporting.
    fn snapshot(&self) -> BenchDiagnosticsSnapshot;
}

/// Snapshot of engine diagnostics captured for benchmark reporting.
#[derive(Debug, Default, Clone)]
pub struct BenchDiagnosticsSnapshot {
    /// WAL-level diagnostics, if durability is enabled.
    pub wal: Option<WalDiagnosticsSnapshot>,
    /// Flush/compaction diagnostics captured during the run.
    pub flush: Option<FlushDiagnosticsSnapshot>,
}

/// Aggregated WAL diagnostics derived from internal metrics.
#[derive(Debug, Default, Clone)]
pub struct WalDiagnosticsSnapshot {
    /// Total bytes written by the WAL writer.
    pub bytes_written: u64,
    /// Number of sync calls performed by the WAL writer.
    pub sync_operations: u64,
    /// Last observed queue depth from the WAL writer.
    pub queue_depth: usize,
    /// Highest observed queue depth.
    pub max_queue_depth: usize,
    /// Latency summary for WAL append -> durable acknowledgements.
    pub append_latency: Option<LatencySnapshot>,
}

/// Aggregated flush diagnostics captured from immutable->SST flushes.
#[derive(Debug, Default, Clone)]
pub struct FlushDiagnosticsSnapshot {
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

/// Bench-only recorder that accumulates flush/compaction timings inside the engine.
#[derive(Default, Clone)]
pub struct BenchDiagnosticsRecorder {
    inner: Arc<Mutex<BenchDiagnosticsState>>,
}

#[derive(Default)]
struct BenchDiagnosticsState {
    flush_events: Vec<FlushEvent>,
    wal: Option<WalDiagnosticsSnapshot>,
}

#[derive(Clone, Copy)]
struct FlushEvent {
    bytes: u64,
    duration: Duration,
}

impl BenchObserver for BenchDiagnosticsRecorder {
    /// Record a flush event into the in-memory accumulator.
    fn record_flush(&self, bytes: u64, duration: Duration) {
        let mut guard = self.inner.lock();
        guard.flush_events.push(FlushEvent { bytes, duration });
    }

    fn flush_snapshot(&self) -> FlushDiagnosticsSnapshot {
        let guard = self.inner.lock();
        if guard.flush_events.is_empty() {
            return FlushDiagnosticsSnapshot::default();
        }

        let mut snapshot = FlushDiagnosticsSnapshot::default();
        for event in &guard.flush_events {
            snapshot.flush_count = snapshot.flush_count.saturating_add(1);
            snapshot.total_bytes = snapshot.total_bytes.saturating_add(event.bytes);
            let micros = event.duration.as_micros();
            snapshot.total_us = snapshot.total_us.saturating_add(micros);
            let us = event.duration.as_micros() as u64;
            if snapshot.min_us == 0 || us < snapshot.min_us {
                snapshot.min_us = us;
            }
            if us > snapshot.max_us {
                snapshot.max_us = us;
            }
        }
        snapshot
    }

    fn update_wal(&self, snapshot: WalDiagnosticsSnapshot) {
        let mut guard = self.inner.lock();
        guard.wal = Some(snapshot);
    }

    fn snapshot(&self) -> BenchDiagnosticsSnapshot {
        BenchDiagnosticsSnapshot {
            wal: self.inner.lock().wal.clone(),
            flush: Some(self.flush_snapshot()),
        }
    }
}
