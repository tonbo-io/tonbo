use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct MutableMemTableMetrics {
    entries: AtomicUsize,
    inserts: AtomicU64,
    replaces: AtomicU64,
    approx_key_bytes: AtomicUsize,
    entry_overhead: AtomicUsize,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct MutableMemTableMetricsSnapshot {
    pub entries: usize,
    pub inserts: u64,
    pub replaces: u64,
    pub approx_key_bytes: usize,
    pub entry_overhead: usize,
}

impl MutableMemTableMetrics {
    pub(crate) fn new(entry_overhead: usize) -> Self {
        Self {
            entries: AtomicUsize::new(0),
            inserts: AtomicU64::new(0),
            replaces: AtomicU64::new(0),
            approx_key_bytes: AtomicUsize::new(0),
            entry_overhead: AtomicUsize::new(entry_overhead),
        }
    }

    pub(crate) fn record_write(&self, has_existing: bool, key_bytes: usize) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
        if has_existing {
            self.replaces.fetch_add(1, Ordering::Relaxed);
        } else {
            self.entries.fetch_add(1, Ordering::Relaxed);
            self.approx_key_bytes
                .fetch_add(key_bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> MutableMemTableMetricsSnapshot {
        MutableMemTableMetricsSnapshot {
            entries: self.entries.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            replaces: self.replaces.load(Ordering::Relaxed),
            approx_key_bytes: self.approx_key_bytes.load(Ordering::Relaxed),
            entry_overhead: self.entry_overhead.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn reset_counters(&self) {
        self.entries.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.replaces.store(0, Ordering::Relaxed);
        self.approx_key_bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for MutableMemTableMetrics {
    fn default() -> Self {
        Self::new(0)
    }
}
