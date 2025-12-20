use std::{
    env,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use tonbo::metrics::{
    DbMetricsSnapshot, FlushMetricsSnapshot, LatencySnapshot, WalMetricsSnapshot,
};

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
    /// Skip expensive filesystem walks for write amplification accounting.
    #[serde(default)]
    pub skip_physical_bytes: bool,
    /// Optional sampling interval (milliseconds) for time-series snapshots.
    #[serde(default)]
    pub sample_interval_ms: Option<u64>,
    /// Maximum number of samples to retain in a run.
    #[serde(default)]
    pub max_samples: Option<usize>,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            level: DiagnosticsLevel::Basic,
            skip_physical_bytes: false,
            sample_interval_ms: None,
            max_samples: None,
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
    pub memtable: Option<MemtableDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction: Option<Unsupported>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_path: Option<Unsupported>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<Unsupported>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_amplification: Option<WriteAmplification>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<DiagnosticsSample>>,
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
pub struct MemtableDiagnostics {
    pub entries: usize,
    pub inserts: u64,
    pub replaces: u64,
    pub approx_key_bytes: usize,
    pub entry_overhead: usize,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct WriteAmplification {
    pub logical_bytes_written: u64,
    pub physical_bytes_written: u64,
    pub amplification_ratio: f64,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct DiagnosticsSample {
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal: Option<WalDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush: Option<FlushDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memtable: Option<MemtableDiagnostics>,
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
    samples: Vec<DiagnosticsSample>,
    #[allow(dead_code)]
    last_sample_elapsed_ms: Option<u64>,
}

#[derive(Default, Clone)]
struct EngineDiagnostics {
    wal: Option<WalDiagnostics>,
    flush: Option<FlushDiagnostics>,
    memtable: Option<MemtableDiagnostics>,
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
                samples: Vec::new(),
                last_sample_elapsed_ms: None,
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

    pub fn record_engine_snapshot(&self, snapshot: DbMetricsSnapshot) {
        if let Some(inner) = &self.inner {
            // Engine snapshot comes from the public metrics API.
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
                memtable: snapshot.memtable.as_ref().map(convert_memtable),
            };
        }
    }

    #[allow(dead_code)]
    pub fn should_sample(&self, elapsed: std::time::Duration) -> bool {
        let Some(inner) = &self.inner else {
            return false;
        };
        let guard = inner.lock().expect("diagnostics mutex poisoned");
        let Some(interval_ms) = guard.config.sample_interval_ms else {
            return false;
        };
        if interval_ms == 0 {
            return false;
        }
        if let Some(max_samples) = guard.config.max_samples
            && guard.samples.len() >= max_samples
        {
            return false;
        }
        let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        match guard.last_sample_elapsed_ms {
            Some(last_ms) => elapsed_ms.saturating_sub(last_ms) >= interval_ms,
            None => true,
        }
    }

    #[allow(dead_code)]
    pub fn record_engine_sample(&self, elapsed: std::time::Duration, snapshot: DbMetricsSnapshot) {
        if let Some(inner) = &self.inner {
            let mut guard = inner.lock().expect("diagnostics mutex poisoned");
            let Some(interval_ms) = guard.config.sample_interval_ms else {
                return;
            };
            if interval_ms == 0 {
                return;
            }
            if let Some(max_samples) = guard.config.max_samples
                && guard.samples.len() >= max_samples
            {
                return;
            }
            let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
            guard.last_sample_elapsed_ms = Some(elapsed_ms);
            guard.samples.push(DiagnosticsSample {
                elapsed_ms,
                wal: snapshot.wal.as_ref().map(convert_wal),
                flush: snapshot.flush.as_ref().and_then(|f| {
                    if f.flush_count == 0 {
                        None
                    } else {
                        Some(convert_flush(f))
                    }
                }),
                memtable: snapshot.memtable.as_ref().map(convert_memtable),
            });
        }
    }

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
        let samples = guard.samples.clone();
        let cfg = guard.config.clone();
        drop(guard);

        let physical_bytes = if cfg.skip_physical_bytes {
            None
        } else {
            Some(backend.physical_bytes().await?)
        };

        let mut output = DiagnosticsOutput::default();
        if let Some(physical) = physical_bytes {
            let amplification_ratio = if logical_bytes == 0 {
                0.0
            } else {
                physical as f64 / logical_bytes as f64
            };
            output.write_amplification = Some(WriteAmplification {
                logical_bytes_written: logical_bytes,
                physical_bytes_written: physical,
                amplification_ratio,
            });
        }
        if let Some(wal) = engine.wal.clone() {
            output.wal = Some(wal);
        }
        if let Some(flush) = engine.flush.clone() {
            output.flush = Some(flush.clone());
            output.sst = Some(flush);
        }
        if let Some(memtable) = engine.memtable.clone() {
            output.memtable = Some(memtable);
        }

        // We don't have compaction/read_path/cache metrics yet; surface unsupported placeholders.
        output.compaction = Some(Unsupported::unsupported());
        output.read_path = Some(Unsupported::unsupported());
        output.cache = Some(Unsupported::unsupported());
        if !samples.is_empty() {
            output.samples = Some(samples);
        }

        Ok(Some(output))
    }
}

fn convert_wal(snapshot: &WalMetricsSnapshot) -> WalDiagnostics {
    let append_latency = snapshot.append_latency.as_ref().map(to_latency);
    WalDiagnostics {
        bytes_written: snapshot.bytes_written,
        sync_operations: snapshot.sync_operations,
        queue_depth: Some(QueueDepth {
            last: snapshot.queue_depth,
            max: snapshot.max_queue_depth,
        }),
        append_latency,
    }
}

fn to_latency(lat: &LatencySnapshot) -> LatencySummary {
    let mean = lat.mean_us().unwrap_or(0.0);
    LatencySummary {
        count: lat.count,
        min_us: lat.min_us,
        max_us: lat.max_us,
        mean_us: mean,
    }
}

fn convert_flush(snapshot: &FlushMetricsSnapshot) -> FlushDiagnostics {
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

fn convert_memtable(snapshot: &tonbo::metrics::MemtableMetricsSnapshot) -> MemtableDiagnostics {
    MemtableDiagnostics {
        entries: snapshot.entries,
        inserts: snapshot.inserts,
        replaces: snapshot.replaces,
        approx_key_bytes: snapshot.approx_key_bytes,
        entry_overhead: snapshot.entry_overhead,
    }
}

pub fn diagnostics_config(config: &BenchConfig) -> Option<DiagnosticsConfig> {
    let mut cfg = config.diagnostics.clone().unwrap_or_default();
    let mut enabled = cfg.enabled;

    if let Ok(value) = env::var("TONBO_BENCH_DIAG_SAMPLE_MS") {
        if let Ok(ms) = value.parse::<u64>() {
            cfg.sample_interval_ms = Some(ms);
            enabled = true;
        }
    }

    if let Ok(value) = env::var("TONBO_BENCH_DIAG_MAX_SAMPLES") {
        if let Ok(max) = value.parse::<usize>() {
            cfg.max_samples = Some(max);
            enabled = true;
        }
    }

    cfg.enabled = enabled;
    if cfg.enabled { Some(cfg) } else { None }
}
