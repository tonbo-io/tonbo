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
}
