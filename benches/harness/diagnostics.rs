use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
#[cfg(any(test, tonbo_bench))]
use tonbo::bench_diagnostics::{BenchDiagnosticsSnapshot, LatencySnapshot};

use crate::harness::{BackendRun, BenchConfig};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticsLevel {
    Basic,
    Full,
}

impl Default for DiagnosticsLevel {
    fn default() -> Self {
        DiagnosticsLevel::Basic
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiagnosticsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub level: DiagnosticsLevel,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            level: DiagnosticsLevel::Basic,
        }
    }
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct DiagnosticsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal: Option<WalDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush: Option<FlushDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sst: Option<FlushDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction: Option<Unsupported>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_path: Option<Unsupported>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<Unsupported>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_amplification: Option<WriteAmplification>,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct Unsupported {
    pub supported: bool,
}

impl Unsupported {
    pub fn unsupported() -> Self {
        Self { supported: false }
    }
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct WalDiagnostics {
    pub bytes_written: u64,
    pub sync_operations: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<QueueDepth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append_latency: Option<LatencySummary>,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct QueueDepth {
    pub last: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<usize>,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct FlushDiagnostics {
    pub count: u64,
    pub total_bytes: u64,
    pub total_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_per_sec: Option<f64>,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct LatencySummary {
    pub count: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: f64,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct WriteAmplification {
    pub logical_bytes_written: u64,
    pub physical_bytes_written: u64,
    pub amplification_ratio: f64,
}

#[derive(Clone)]
pub struct DiagnosticsCollector {
    inner: Option<Arc<Mutex<DiagnosticsState>>>,
}

#[derive(Default)]
struct DiagnosticsState {
    config: DiagnosticsConfig,
    logical_bytes: u64,
    engine: EngineDiagnostics,
}

#[derive(Default, Clone)]
struct EngineDiagnostics {
    wal: Option<WalDiagnostics>,
    flush: Option<FlushDiagnostics>,
}

impl DiagnosticsCollector {
    pub fn from_config(config: Option<DiagnosticsConfig>) -> Self {
        let enabled = config.as_ref().map_or(false, |cfg| cfg.enabled);
        if !enabled {
            return Self { inner: None };
        }
        Self {
            inner: Some(Arc::new(Mutex::new(DiagnosticsState {
                config: config.unwrap_or_default(),
                logical_bytes: 0,
                engine: EngineDiagnostics::default(),
            }))),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn record_logical_bytes(&self, bytes: u64) {
        if let Some(inner) = &self.inner {
            let mut guard = inner.lock().expect("diagnostics mutex poisoned");
            guard.logical_bytes = guard.logical_bytes.saturating_add(bytes);
        }
    }

    #[cfg(any(test, tonbo_bench))]
    pub fn record_engine_snapshot(&self, snapshot: BenchDiagnosticsSnapshot) {
        if let Some(inner) = &self.inner {
            // Engine snapshot comes from bench-only diagnostics compiled for bench/test builds.
            let mut guard = inner.lock().expect("diagnostics mutex poisoned");
            let flush = snapshot.flush.as_ref().and_then(|f| {
                if f.flush_count == 0 {
                    None
                } else {
                    Some(convert_flush(f))
                }
            });
            guard.engine = EngineDiagnostics {
                wal: snapshot.wal.as_ref().map(convert_wal),
                flush,
            };
        }
    }

    #[cfg(not(any(test, tonbo_bench)))]
    pub fn record_engine_snapshot(&self, _snapshot: ()) {}

    pub async fn finalize(
        &self,
        backend: &BackendRun,
    ) -> anyhow::Result<Option<DiagnosticsOutput>> {
        let inner = match &self.inner {
            Some(inner) => inner.clone(),
            None => return Ok(None),
        };
        let guard = inner.lock().expect("diagnostics mutex poisoned");
        let logical_bytes = guard.logical_bytes;
        let engine = guard.engine.clone();
        drop(guard);

        let physical_bytes = backend.physical_bytes().await?;
        let amplification_ratio = if logical_bytes == 0 {
            0.0
        } else {
            physical_bytes as f64 / logical_bytes as f64
        };

        let mut output = DiagnosticsOutput::default();
        output.write_amplification = Some(WriteAmplification {
            logical_bytes_written: logical_bytes,
            physical_bytes_written: physical_bytes,
            amplification_ratio,
        });
        if let Some(wal) = engine.wal.clone() {
            output.wal = Some(wal);
        }
        if let Some(flush) = engine.flush.clone() {
            output.flush = Some(flush.clone());
            output.sst = Some(flush);
        }

        // We don't have compaction/read_path/cache metrics yet; surface unsupported placeholders.
        output.compaction = Some(Unsupported::unsupported());
        output.read_path = Some(Unsupported::unsupported());
        output.cache = Some(Unsupported::unsupported());

        Ok(Some(output))
    }
}

#[cfg(any(test, tonbo_bench))]
fn convert_wal(snapshot: &tonbo::bench_diagnostics::WalDiagnosticsSnapshot) -> WalDiagnostics {
    let append_latency = snapshot.append_latency.as_ref().map(|lat| to_latency(lat));
    WalDiagnostics {
        bytes_written: snapshot.bytes_written,
        sync_operations: snapshot.sync_operations,
        queue_depth: Some(QueueDepth {
            last: snapshot.queue_depth,
            max: Some(snapshot.max_queue_depth),
        }),
        append_latency,
    }
}

#[cfg(any(test, tonbo_bench))]
fn to_latency(lat: &LatencySnapshot) -> LatencySummary {
    let mean = lat.mean_us().unwrap_or(0.0);
    LatencySummary {
        count: lat.count,
        min_us: lat.min_us,
        max_us: lat.max_us,
        mean_us: mean,
    }
}

#[cfg(any(test, tonbo_bench))]
fn convert_flush(
    snapshot: &tonbo::bench_diagnostics::FlushDiagnosticsSnapshot,
) -> FlushDiagnostics {
    if snapshot.flush_count == 0 {
        return FlushDiagnostics::default();
    }
    let total_ms = (snapshot.total_us / 1_000) as u64;
    let bytes_per_sec = if snapshot.total_us == 0 {
        None
    } else {
        let secs = snapshot.total_us as f64 / 1_000_000.0;
        Some(snapshot.total_bytes as f64 / secs)
    };
    FlushDiagnostics {
        count: snapshot.flush_count,
        total_bytes: snapshot.total_bytes,
        total_ms,
        max_ms: Some(snapshot.max_us / 1_000),
        min_ms: Some(snapshot.min_us / 1_000),
        bytes_per_sec,
    }
}

pub fn diagnostics_config(config: &BenchConfig) -> Option<DiagnosticsConfig> {
    config.diagnostics.clone().filter(|cfg| cfg.enabled)
}
