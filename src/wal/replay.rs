//! Recovery helpers for scanning WAL segments.

use crate::wal::{WalConfig, WalResult, frame::WalEvent};

/// Scans WAL segments on disk and yields decoded events.
pub struct Replayer {
    /// Configuration snapshot guiding where segments reside.
    cfg: WalConfig,
}

impl Replayer {
    /// Create a new replayer using the provided configuration.
    pub fn new(cfg: WalConfig) -> Self {
        Self { cfg }
    }

    /// Iterate through WAL segments and produce events.
    pub fn scan(&self) -> WalResult<Vec<WalEvent>> {
        Err(crate::wal::WalError::Unimplemented("Replayer::scan"))
    }

    /// Access the configuration.
    pub fn config(&self) -> &WalConfig {
        &self.cfg
    }
}
