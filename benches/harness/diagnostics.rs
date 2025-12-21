//! Diagnostics collection for benchmarks.
//!
//! Uses `DbMetricsSnapshot` from the public metrics API directly,
//! adding only write amplification calculation on top.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tonbo::metrics::DbMetricsSnapshot;

use crate::harness::BackendRun;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiagnosticsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub skip_physical_bytes: bool,
}

/// Output from diagnostics collection - uses public metrics API types directly.
#[derive(Debug, Default, Serialize, Clone)]
pub struct DiagnosticsOutput {
    /// Engine metrics snapshot from `db.metrics_snapshot()`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<DbMetricsSnapshot>,
    /// Write amplification (physical / logical bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_amplification: Option<WriteAmplification>,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct WriteAmplification {
    pub logical_bytes_written: u64,
    pub physical_bytes_written: u64,
    pub amplification_ratio: f64,
}

/// Collects diagnostics during benchmark runs.
#[derive(Clone)]
pub struct DiagnosticsCollector {
    inner: Option<Arc<Mutex<DiagnosticsState>>>,
}

#[derive(Default)]
struct DiagnosticsState {
    config: DiagnosticsConfig,
    logical_bytes: u64,
    engine_snapshot: Option<DbMetricsSnapshot>,
}

impl DiagnosticsCollector {
    pub fn new(enabled: bool) -> Self {
        if !enabled {
            return Self { inner: None };
        }
        Self {
            inner: Some(Arc::new(Mutex::new(DiagnosticsState::default()))),
        }
    }

    pub fn from_config(config: Option<DiagnosticsConfig>) -> Self {
        let enabled = config.as_ref().is_some_and(|c| c.enabled);
        let collector = Self::new(enabled);
        if let (Some(inner), Some(cfg)) = (&collector.inner, config) {
            inner.lock().expect("mutex poisoned").config = cfg;
        }
        collector
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn record_logical_bytes(&self, bytes: u64) {
        if let Some(inner) = &self.inner {
            let mut guard = inner.lock().expect("mutex poisoned");
            guard.logical_bytes = guard.logical_bytes.saturating_add(bytes);
        }
    }

    pub fn record_engine_snapshot(&self, snapshot: DbMetricsSnapshot) {
        if let Some(inner) = &self.inner {
            let mut guard = inner.lock().expect("mutex poisoned");
            guard.engine_snapshot = Some(snapshot);
        }
    }

    pub async fn finalize(
        &self,
        backend: &BackendRun,
    ) -> anyhow::Result<Option<DiagnosticsOutput>> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };

        let guard = inner.lock().expect("mutex poisoned");
        let logical_bytes = guard.logical_bytes;
        let engine_snapshot = guard.engine_snapshot.clone();
        let skip_physical = guard.config.skip_physical_bytes;
        drop(guard);

        let write_amplification = if skip_physical {
            None
        } else {
            let physical_bytes = backend.physical_bytes().await?;
            let ratio = if logical_bytes == 0 {
                0.0
            } else {
                physical_bytes as f64 / logical_bytes as f64
            };
            Some(WriteAmplification {
                logical_bytes_written: logical_bytes,
                physical_bytes_written: physical_bytes,
                amplification_ratio: ratio,
            })
        };

        Ok(Some(DiagnosticsOutput {
            engine: engine_snapshot,
            write_amplification,
        }))
    }
}
