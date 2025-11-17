//! Metrics and observability glue for the WAL.

/// Collection of WAL metrics exposed to monitoring systems.
#[derive(Default, Debug)]
pub struct WalMetrics {
    /// Current depth of the writer queue.
    pub queue_depth: usize,
    /// Bytes written since process start.
    pub bytes_written: u64,
    /// Number of durability operations performed.
    pub sync_operations: u64,
    /// Number of times the manifest advanced the WAL floor.
    pub wal_floor_advancements: u64,
    /// Total WAL segments physically pruned.
    pub wal_segments_pruned: u64,
    /// Total WAL segments flagged for deletion during dry-runs.
    pub wal_prune_dry_runs: u64,
    /// Number of failed prune attempts.
    pub wal_prune_failures: u64,
}

impl WalMetrics {
    /// Record a queue depth update.
    pub fn record_queue_depth(&mut self, depth: usize) {
        self.queue_depth = depth;
    }

    /// Record additional written bytes.
    pub fn record_bytes_written(&mut self, bytes: u64) {
        self.bytes_written = self.bytes_written.saturating_add(bytes);
    }

    /// Record a durability operation.
    pub fn record_sync(&mut self) {
        self.sync_operations = self.sync_operations.saturating_add(1);
    }

    /// Record an advancement of the WAL retention floor.
    pub fn record_wal_floor_advance(&mut self) {
        self.wal_floor_advancements = self.wal_floor_advancements.saturating_add(1);
    }

    /// Record physical WAL segment deletions.
    pub fn record_wal_pruned(&mut self, segments: u64) {
        self.wal_segments_pruned = self.wal_segments_pruned.saturating_add(segments);
    }

    /// Record the number of segments that would be deleted in dry-run mode.
    pub fn record_wal_prune_dry_run(&mut self, segments: u64) {
        self.wal_prune_dry_runs = self.wal_prune_dry_runs.saturating_add(segments);
    }

    /// Record a prune failure.
    pub fn record_wal_prune_failure(&mut self) {
        self.wal_prune_failures = self.wal_prune_failures.saturating_add(1);
    }
}
