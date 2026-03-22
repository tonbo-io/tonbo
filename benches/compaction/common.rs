use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    path::{Path, PathBuf},
    pin::Pin,
    process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput, black_box};
use fusio::{
    IoBuf, IoBufMut, Read, Write,
    disk::LocalFs,
    durability::FileCommit,
    dynamic::MaybeSendFuture,
    error::Error as FusioError,
    executor::tokio::TokioExecutor,
    fs::{CasCondition, FileMeta, FileSystemTag, Fs, FsCas, OpenOptions},
    impls::remotes::aws::fs::AmazonS3,
    path::{Path as FusioPath, PathPart},
};
use fusio_manifest::ObjectHead;
use futures::StreamExt;
use serde::Serialize;
use thiserror::Error;
use tokio::{runtime::Runtime, time::sleep};
use tonbo::db::{
    AwsCreds, CompactionMetrics, CompactionMetricsSnapshot, CompactionOptions, CompactionStrategy,
    DB, DBError, DbBuildError, DbBuilder, LeveledPlannerConfig, ObjectSpec, S3Spec,
    ScanSetupProfile, SstSweepSummary, WalSyncPolicy,
};

pub(crate) const BENCH_ID: &str = "compaction_local";
pub(crate) const BENCH_SCHEMA_VERSION: u32 = 13;

const DEFAULT_INGEST_BATCHES: usize = 640;
const DEFAULT_DATASET_SCALE: usize = 1;
const DEFAULT_ROWS_PER_BATCH: usize = 64;
const DEFAULT_KEY_SPACE: usize = 2_048;
const DEFAULT_ARTIFACT_ITERATIONS: usize = 48;
const DEFAULT_CRITERION_SAMPLE_SIZE: usize = 20;
const DEFAULT_COMPACTION_WAIT_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_COMPACTION_POLL_INTERVAL_MS: u64 = 50;
const DEFAULT_COMPACTION_PERIODIC_TICK_MS: u64 = 200;
const DEFAULT_SEED: u64 = 584;
const DEFAULT_SWEEP_L0_TRIGGERS: &[usize] = &[8];
const DEFAULT_SWEEP_MAX_INPUTS: &[usize] = &[6];
const DEFAULT_SWEEP_MAX_TASK_BYTES: &[Option<usize>] = &[None];
const DEFAULT_WRITE_FREQUENCY_PERIODIC_TICKS_MS: &[u64] = &[50, 200];
const DEFAULT_ENABLE_READ_WHILE_COMPACTION: bool = true;
const DEFAULT_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY: bool = true;
const DEFAULT_BACKEND: BenchBackend = BenchBackend::Local;
const INGEST_RETRY_MAX_ATTEMPTS: usize = 8;
const INGEST_RETRY_BACKOFF_BASE_MS: u64 = 10;
const COMPACTION_QUIESCED_STABLE_POLLS: usize = 3;

#[derive(Clone)]
pub(crate) enum BenchmarkDb {
    Local(DB<ProbedFs<LocalFs>, TokioExecutor>),
    ObjectStore(DB<ProbedFs<AmazonS3>, TokioExecutor>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BenchBackend {
    Local,
    ObjectStore,
}

impl BenchBackend {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::ObjectStore => "object_store",
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct IoProbe {
    inner: Arc<IoProbeState>,
}

#[derive(Default)]
struct IoProbeState {
    read_ops: AtomicU64,
    write_ops: AtomicU64,
    list_ops: AtomicU64,
    remove_ops: AtomicU64,
    copy_ops: AtomicU64,
    link_ops: AtomicU64,
    cas_load_ops: AtomicU64,
    cas_put_ops: AtomicU64,
    head_ops: AtomicU64,
    sst_request_ops: AtomicU64,
    wal_request_ops: AtomicU64,
    manifest_request_ops: AtomicU64,
    other_request_ops: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    sst_paths: Mutex<HashSet<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IoCountersArtifact {
    read_ops: u64,
    write_ops: u64,
    list_ops: u64,
    remove_ops: u64,
    copy_ops: u64,
    link_ops: u64,
    cas_load_ops: u64,
    cas_put_ops: u64,
    head_ops: u64,
    sst_request_ops: u64,
    wal_request_ops: u64,
    manifest_request_ops: u64,
    other_request_ops: u64,
    bytes_read: u64,
    bytes_written: u64,
    ssts_touched: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct StorageVolumeArtifact {
    object_count: u64,
    total_bytes: u64,
    sst_object_count: u64,
    sst_bytes: u64,
    wal_object_count: u64,
    wal_bytes: u64,
    manifest_object_count: u64,
    manifest_bytes: u64,
    other_object_count: u64,
    other_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LogicalVolumeArtifact {
    sst_count: usize,
    sst_bytes: u64,
    wal_bytes: Option<u64>,
    manifest_bytes: Option<u64>,
    total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PhysicalStaleEstimateArtifact {
    candidate_sst_objects: u64,
    candidate_sst_bytes: u64,
    live_sst_objects: u64,
    live_sst_bytes: u64,
    physical_sst_objects: u64,
    physical_sst_bytes: u64,
    stale_sst_byte_amplification_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GcSweepArtifact {
    deleted_objects: u64,
    deleted_bytes: u64,
    delete_failures: u64,
    duration_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GcCountersArtifact {
    sweep_runs: u64,
    deleted_objects: u64,
    deleted_bytes: u64,
    delete_failures: u64,
    sweep_duration_ms_total: u64,
    gc_plan_write_runs: u64,
    gc_plan_overwrite_non_empty: u64,
    gc_plan_previous_sst_candidates: u64,
    gc_plan_written_sst_candidates: u64,
    gc_plan_take_runs: u64,
    gc_plan_taken_sst_candidates: u64,
    gc_plan_authorized_sst_candidates: u64,
    gc_plan_blocked_sst_candidates: u64,
    gc_plan_requeued_sst_candidates: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GcObservationArtifact {
    volume_before_gc: StorageVolumeArtifact,
    volume_after_gc: StorageVolumeArtifact,
    physical_stale_estimate_before_explicit_sweep: PhysicalStaleEstimateArtifact,
    physical_stale_estimate_after_explicit_sweep: PhysicalStaleEstimateArtifact,
    reclaimed_sst_objects: u64,
    reclaimed_sst_bytes: u64,
    persisted_plan_before_explicit_sweep: Option<GcPlanInspectionArtifact>,
    explicit_sweep_result: GcSweepArtifact,
    cumulative_before_explicit_sweep: GcCountersArtifact,
    cumulative_after_explicit_sweep: GcCountersArtifact,
    persisted_plan_after_explicit_sweep: Option<GcPlanInspectionArtifact>,
}

#[derive(Debug, Clone, Serialize)]
struct GcDerivedArtifact {
    sst_sweep_runs: u64,
    sst_sweep_duration_ms_total: u64,
    sst_deleted_objects_total: u64,
    sst_deleted_bytes_total: u64,
    sst_sweep_runs_before_explicit_sweep: u64,
    sst_sweep_duration_ms_before_explicit_sweep: u64,
    sst_deleted_objects_before_explicit_sweep: u64,
    sst_deleted_bytes_before_explicit_sweep: u64,
    explicit_sweep_runs: u64,
    explicit_sweep_duration_ms: u64,
    explicit_sweep_deleted_objects: u64,
    explicit_sweep_deleted_bytes: u64,
    sst_deleted_bytes_per_ms: Option<f64>,
    sst_deleted_objects_per_ms: Option<f64>,
    gc_time_share_pct_of_setup: Option<f64>,
    gc_time_share_pct_of_total_elapsed: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GcPlanCandidateArtifact {
    sst_id: u64,
    level: u32,
    data_path: String,
    delete_path: Option<String>,
    authorized: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GcPlanInspectionArtifact {
    staged_sst_candidates: u64,
    authorized_sst_candidates: u64,
    blocked_sst_candidates: u64,
    obsolete_wal_segments: u64,
    protected_versions: u64,
    active_snapshot_versions: u64,
    protected_sst_objects: u64,
    blocker: String,
    candidates: Vec<GcPlanCandidateArtifact>,
}

impl LogicalVolumeArtifact {
    pub(crate) fn from_version(version: VersionSummary) -> Self {
        let wal_bytes = None;
        let manifest_bytes = None;
        let mut total_bytes = version.sst_bytes;
        if let Some(bytes) = wal_bytes {
            total_bytes = total_bytes.saturating_add(bytes);
        }
        if let Some(bytes) = manifest_bytes {
            total_bytes = total_bytes.saturating_add(bytes);
        }

        Self {
            sst_count: version.sst_count,
            sst_bytes: version.sst_bytes,
            wal_bytes,
            manifest_bytes,
            total_bytes,
        }
    }
}

impl PhysicalStaleEstimateArtifact {
    pub(crate) fn from_volume(
        physical: &StorageVolumeArtifact,
        logical: &LogicalVolumeArtifact,
    ) -> Self {
        let live_sst_objects = u64::try_from(logical.sst_count).unwrap_or(u64::MAX);
        let candidate_sst_objects = physical.sst_object_count.saturating_sub(live_sst_objects);
        let candidate_sst_bytes = physical.sst_bytes.saturating_sub(logical.sst_bytes);
        let stale_sst_byte_amplification_pct = if logical.sst_bytes == 0 {
            0.0
        } else {
            (candidate_sst_bytes as f64 / logical.sst_bytes as f64) * 100.0
        };

        Self {
            candidate_sst_objects,
            candidate_sst_bytes,
            live_sst_objects,
            live_sst_bytes: logical.sst_bytes,
            physical_sst_objects: physical.sst_object_count,
            physical_sst_bytes: physical.sst_bytes,
            stale_sst_byte_amplification_pct,
        }
    }
}

impl GcSweepArtifact {
    fn from_summary(summary: SstSweepSummary) -> Self {
        Self {
            deleted_objects: summary.deleted_objects,
            deleted_bytes: summary.deleted_bytes,
            delete_failures: summary.delete_failures,
            duration_ms: summary.duration_ms,
        }
    }
}

impl GcCountersArtifact {
    fn from_snapshot(snapshot: CompactionMetricsSnapshot) -> Self {
        Self {
            sweep_runs: snapshot.sst_sweep_runs,
            deleted_objects: snapshot.sst_deleted_objects,
            deleted_bytes: snapshot.sst_deleted_bytes,
            delete_failures: snapshot.sst_delete_failures,
            sweep_duration_ms_total: snapshot.sst_sweep_duration_ms_total,
            gc_plan_write_runs: snapshot.gc_plan_write_runs,
            gc_plan_overwrite_non_empty: snapshot.gc_plan_overwrite_non_empty,
            gc_plan_previous_sst_candidates: snapshot.gc_plan_previous_sst_candidates,
            gc_plan_written_sst_candidates: snapshot.gc_plan_written_sst_candidates,
            gc_plan_take_runs: snapshot.gc_plan_take_runs,
            gc_plan_taken_sst_candidates: snapshot.gc_plan_taken_sst_candidates,
            gc_plan_authorized_sst_candidates: snapshot.gc_plan_authorized_sst_candidates,
            gc_plan_blocked_sst_candidates: snapshot.gc_plan_blocked_sst_candidates,
            gc_plan_requeued_sst_candidates: snapshot.gc_plan_requeued_sst_candidates,
        }
    }
}

impl GcObservationArtifact {
    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        volume_before_gc: StorageVolumeArtifact,
        volume_after_gc: StorageVolumeArtifact,
        physical_stale_estimate_before_explicit_sweep: PhysicalStaleEstimateArtifact,
        physical_stale_estimate_after_explicit_sweep: PhysicalStaleEstimateArtifact,
        persisted_plan_before_explicit_sweep: Option<GcPlanInspectionArtifact>,
        explicit_sweep: SstSweepSummary,
        cumulative_before_explicit_sweep: CompactionMetricsSnapshot,
        cumulative_after_explicit_sweep: CompactionMetricsSnapshot,
        persisted_plan_after_explicit_sweep: Option<GcPlanInspectionArtifact>,
    ) -> Self {
        Self {
            reclaimed_sst_objects: volume_before_gc
                .sst_object_count
                .saturating_sub(volume_after_gc.sst_object_count),
            reclaimed_sst_bytes: volume_before_gc
                .sst_bytes
                .saturating_sub(volume_after_gc.sst_bytes),
            volume_before_gc,
            volume_after_gc,
            physical_stale_estimate_before_explicit_sweep,
            physical_stale_estimate_after_explicit_sweep,
            persisted_plan_before_explicit_sweep,
            explicit_sweep_result: GcSweepArtifact::from_summary(explicit_sweep),
            cumulative_before_explicit_sweep: GcCountersArtifact::from_snapshot(
                cumulative_before_explicit_sweep,
            ),
            cumulative_after_explicit_sweep: GcCountersArtifact::from_snapshot(
                cumulative_after_explicit_sweep,
            ),
            persisted_plan_after_explicit_sweep,
        }
    }
}

impl GcDerivedArtifact {
    fn from_gc_observation(
        gc: &GcObservationArtifact,
        setup_elapsed_ns: u64,
        total_elapsed_ns: u64,
    ) -> Self {
        let total_duration_ms = gc.cumulative_after_explicit_sweep.sweep_duration_ms_total;
        let total_runs = gc.cumulative_after_explicit_sweep.sweep_runs;
        let total_deleted_objects = gc.cumulative_after_explicit_sweep.deleted_objects;
        let total_deleted_bytes = gc.cumulative_after_explicit_sweep.deleted_bytes;
        let before_duration_ms = gc.cumulative_before_explicit_sweep.sweep_duration_ms_total;
        let before_runs = gc.cumulative_before_explicit_sweep.sweep_runs;
        let before_deleted_objects = gc.cumulative_before_explicit_sweep.deleted_objects;
        let before_deleted_bytes = gc.cumulative_before_explicit_sweep.deleted_bytes;
        let explicit_sweep_runs = total_runs.saturating_sub(before_runs);
        let explicit_sweep_duration_ms = total_duration_ms.saturating_sub(before_duration_ms);
        let explicit_sweep_deleted_objects =
            total_deleted_objects.saturating_sub(before_deleted_objects);
        let explicit_sweep_deleted_bytes = total_deleted_bytes.saturating_sub(before_deleted_bytes);

        Self {
            sst_sweep_runs: total_runs,
            sst_sweep_duration_ms_total: total_duration_ms,
            sst_deleted_objects_total: total_deleted_objects,
            sst_deleted_bytes_total: total_deleted_bytes,
            sst_sweep_runs_before_explicit_sweep: before_runs,
            sst_sweep_duration_ms_before_explicit_sweep: before_duration_ms,
            sst_deleted_objects_before_explicit_sweep: before_deleted_objects,
            sst_deleted_bytes_before_explicit_sweep: before_deleted_bytes,
            explicit_sweep_runs,
            explicit_sweep_duration_ms,
            explicit_sweep_deleted_objects,
            explicit_sweep_deleted_bytes,
            sst_deleted_bytes_per_ms: ratio_per_ms(total_deleted_bytes, total_duration_ms),
            sst_deleted_objects_per_ms: ratio_per_ms(total_deleted_objects, total_duration_ms),
            gc_time_share_pct_of_setup: pct_of_elapsed_ms(total_duration_ms, setup_elapsed_ns),
            gc_time_share_pct_of_total_elapsed: pct_of_elapsed_ms(
                total_duration_ms,
                total_elapsed_ns,
            ),
        }
    }
}

impl GcPlanInspectionArtifact {
    fn from_inspection(inspection: tonbo::db::SstGcInspection) -> Self {
        let blocker = if inspection.staged_sst_candidates == 0 {
            "no staged SST GC candidates remain in the manifest plan".to_string()
        } else if inspection.authorized_sst_candidates > 0 {
            "some staged SST candidates are authorized for deletion".to_string()
        } else if inspection.active_snapshot_versions > 0 {
            format!(
                "all staged SST candidates are still protected by {} live in-process snapshot \
                 pin(s); they will only become reclaimable after those snapshots drop",
                inspection.active_snapshot_versions
            )
        } else {
            "all staged SST candidates are still protected by the current manifest root set"
                .to_string()
        };
        Self {
            staged_sst_candidates: inspection.staged_sst_candidates,
            authorized_sst_candidates: inspection.authorized_sst_candidates,
            blocked_sst_candidates: inspection.blocked_sst_candidates,
            obsolete_wal_segments: inspection.obsolete_wal_segments,
            protected_versions: inspection.protected_versions,
            active_snapshot_versions: inspection.active_snapshot_versions,
            protected_sst_objects: inspection.protected_sst_objects,
            blocker,
            candidates: inspection
                .candidates
                .into_iter()
                .map(|candidate| GcPlanCandidateArtifact {
                    sst_id: candidate.sst_id,
                    level: candidate.level,
                    data_path: candidate.data_path,
                    delete_path: candidate.delete_path,
                    authorized: candidate.authorized,
                })
                .collect(),
        }
    }
}

fn empty_compaction_metrics_snapshot() -> CompactionMetricsSnapshot {
    CompactionMetricsSnapshot {
        job_count: 0,
        job_failures: 0,
        cas_retries: 0,
        cas_aborts: 0,
        queue_drops_planner_full: 0,
        queue_drops_planner_closed: 0,
        queue_drops_cascade_full: 0,
        queue_drops_cascade_closed: 0,
        cascades_scheduled: 0,
        cascades_blocked_cooldown: 0,
        cascades_blocked_budget: 0,
        backpressure_slowdown: 0,
        backpressure_stall: 0,
        trigger_kick: 0,
        trigger_periodic: 0,
        bytes_in: 0,
        bytes_out: 0,
        rows_in: 0,
        rows_out: 0,
        tombstones_in: 0,
        tombstones_out: 0,
        duration_ms_total: 0,
        sst_sweep_runs: 0,
        sst_deleted_objects: 0,
        sst_deleted_bytes: 0,
        sst_sweep_duration_ms_total: 0,
        sst_delete_failures: 0,
        gc_plan_write_runs: 0,
        gc_plan_overwrite_non_empty: 0,
        gc_plan_previous_sst_candidates: 0,
        gc_plan_previous_wal_candidates: 0,
        gc_plan_written_sst_candidates: 0,
        gc_plan_written_wal_candidates: 0,
        gc_plan_take_runs: 0,
        gc_plan_taken_sst_candidates: 0,
        gc_plan_authorized_sst_candidates: 0,
        gc_plan_blocked_sst_candidates: 0,
        gc_plan_requeued_sst_candidates: 0,
        last_job: None,
    }
}

impl IoProbe {
    pub(crate) fn snapshot(&self) -> IoCountersArtifact {
        let ssts_touched = match self.inner.sst_paths.lock() {
            Ok(paths) => paths.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };
        IoCountersArtifact {
            read_ops: self.inner.read_ops.load(Ordering::Relaxed),
            write_ops: self.inner.write_ops.load(Ordering::Relaxed),
            list_ops: self.inner.list_ops.load(Ordering::Relaxed),
            remove_ops: self.inner.remove_ops.load(Ordering::Relaxed),
            copy_ops: self.inner.copy_ops.load(Ordering::Relaxed),
            link_ops: self.inner.link_ops.load(Ordering::Relaxed),
            cas_load_ops: self.inner.cas_load_ops.load(Ordering::Relaxed),
            cas_put_ops: self.inner.cas_put_ops.load(Ordering::Relaxed),
            head_ops: self.inner.head_ops.load(Ordering::Relaxed),
            sst_request_ops: self.inner.sst_request_ops.load(Ordering::Relaxed),
            wal_request_ops: self.inner.wal_request_ops.load(Ordering::Relaxed),
            manifest_request_ops: self.inner.manifest_request_ops.load(Ordering::Relaxed),
            other_request_ops: self.inner.other_request_ops.load(Ordering::Relaxed),
            bytes_read: self.inner.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.inner.bytes_written.load(Ordering::Relaxed),
            ssts_touched,
        }
    }

    pub(crate) fn reset(&self) {
        self.inner.read_ops.store(0, Ordering::Relaxed);
        self.inner.write_ops.store(0, Ordering::Relaxed);
        self.inner.list_ops.store(0, Ordering::Relaxed);
        self.inner.remove_ops.store(0, Ordering::Relaxed);
        self.inner.copy_ops.store(0, Ordering::Relaxed);
        self.inner.link_ops.store(0, Ordering::Relaxed);
        self.inner.cas_load_ops.store(0, Ordering::Relaxed);
        self.inner.cas_put_ops.store(0, Ordering::Relaxed);
        self.inner.head_ops.store(0, Ordering::Relaxed);
        self.inner.sst_request_ops.store(0, Ordering::Relaxed);
        self.inner.wal_request_ops.store(0, Ordering::Relaxed);
        self.inner.manifest_request_ops.store(0, Ordering::Relaxed);
        self.inner.other_request_ops.store(0, Ordering::Relaxed);
        self.inner.bytes_read.store(0, Ordering::Relaxed);
        self.inner.bytes_written.store(0, Ordering::Relaxed);
        match self.inner.sst_paths.lock() {
            Ok(mut paths) => paths.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }

    fn record_read(&self, path: &FusioPath, bytes: u64) {
        saturating_add(&self.inner.read_ops, 1);
        saturating_add(&self.inner.bytes_read, bytes);
        self.record_request_path(path);
    }

    fn record_write(&self, path: &FusioPath, bytes: u64) {
        saturating_add(&self.inner.write_ops, 1);
        saturating_add(&self.inner.bytes_written, bytes);
        self.record_request_path(path);
    }

    fn record_list(&self, path: &FusioPath) {
        saturating_add(&self.inner.list_ops, 1);
        self.record_request_path(path);
    }

    fn record_remove(&self, path: &FusioPath) {
        saturating_add(&self.inner.remove_ops, 1);
        self.record_request_path(path);
    }

    fn record_copy(&self, path: &FusioPath) {
        saturating_add(&self.inner.copy_ops, 1);
        self.record_request_path(path);
    }

    fn record_link(&self, path: &FusioPath) {
        saturating_add(&self.inner.link_ops, 1);
        self.record_request_path(path);
    }

    fn record_cas_load(&self, path: &FusioPath) {
        saturating_add(&self.inner.cas_load_ops, 1);
        self.record_request_path(path);
    }

    fn record_cas_put(&self, path: &FusioPath) {
        saturating_add(&self.inner.cas_put_ops, 1);
        self.record_request_path(path);
    }

    fn record_head(&self, path: &FusioPath) {
        saturating_add(&self.inner.head_ops, 1);
        self.record_request_path(path);
    }

    fn record_request_path(&self, path: &FusioPath) {
        if is_sst_path(path) {
            saturating_add(&self.inner.sst_request_ops, 1);
            self.record_sst(path);
        } else if is_wal_path(path.as_ref()) {
            saturating_add(&self.inner.wal_request_ops, 1);
        } else if is_manifest_path(path.as_ref()) {
            saturating_add(&self.inner.manifest_request_ops, 1);
        } else {
            saturating_add(&self.inner.other_request_ops, 1);
        }
    }

    fn record_sst(&self, path: &FusioPath) {
        if !is_sst_path(path) {
            return;
        }
        match self.inner.sst_paths.lock() {
            Ok(mut paths) => {
                let _ = paths.insert(path.to_string());
            }
            Err(poisoned) => {
                let mut paths = poisoned.into_inner();
                let _ = paths.insert(path.to_string());
            }
        }
    }
}

fn saturating_add(counter: &AtomicU64, delta: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(delta))
    });
}

fn is_sst_path(path: &FusioPath) -> bool {
    let raw = path.as_ref();
    (raw.contains("/sst/") || raw.starts_with("sst/")) && raw.ends_with(".parquet")
}

#[derive(Clone)]
pub(crate) struct ProbedFs<FS> {
    inner: FS,
    probe: IoProbe,
}

impl<FS> ProbedFs<FS> {
    pub(crate) fn new(inner: FS, probe: IoProbe) -> Self {
        Self { inner, probe }
    }
}

pub(crate) struct ProbedFile<F> {
    inner: F,
    path: FusioPath,
    probe: IoProbe,
}

impl<F> Read for ProbedFile<F>
where
    F: Read,
{
    async fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> (Result<(), FusioError>, B) {
        let (result, buf) = self.inner.read_exact_at(buf, pos).await;
        if result.is_ok() {
            let bytes = u64::try_from(buf.bytes_init()).unwrap_or(u64::MAX);
            self.probe.record_read(&self.path, bytes);
        }
        (result, buf)
    }

    async fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> (Result<(), FusioError>, Vec<u8>) {
        let initial_len = buf.len();
        let (result, buf) = self.inner.read_to_end_at(buf, pos).await;
        if result.is_ok() {
            let read_bytes = buf.len().saturating_sub(initial_len);
            let bytes = u64::try_from(read_bytes).unwrap_or(u64::MAX);
            self.probe.record_read(&self.path, bytes);
        }
        (result, buf)
    }

    async fn size(&self) -> Result<u64, FusioError> {
        self.inner.size().await
    }
}

impl<F> Write for ProbedFile<F>
where
    F: Write,
{
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), FusioError>, B) {
        let bytes = u64::try_from(buf.bytes_init()).unwrap_or(u64::MAX);
        let (result, buf) = self.inner.write_all(buf).await;
        if result.is_ok() {
            self.probe.record_write(&self.path, bytes);
        }
        (result, buf)
    }

    async fn flush(&mut self) -> Result<(), FusioError> {
        self.inner.flush().await
    }

    async fn close(&mut self) -> Result<(), FusioError> {
        self.inner.close().await
    }
}

impl<F> FileCommit for ProbedFile<F>
where
    F: FileCommit,
{
    async fn commit(&mut self) -> Result<(), FusioError> {
        self.inner.commit().await
    }
}

impl<FS> Fs for ProbedFs<FS>
where
    FS: Fs,
{
    type File = ProbedFile<<FS as Fs>::File>;

    fn file_system(&self) -> FileSystemTag {
        self.inner.file_system()
    }

    async fn open_options(
        &self,
        path: &FusioPath,
        options: OpenOptions,
    ) -> Result<Self::File, FusioError> {
        let file = self.inner.open_options(path, options).await?;
        Ok(ProbedFile {
            inner: file,
            path: path.clone(),
            probe: self.probe.clone(),
        })
    }

    async fn create_dir_all(path: &FusioPath) -> Result<(), FusioError> {
        FS::create_dir_all(path).await
    }

    async fn list(
        &self,
        path: &FusioPath,
    ) -> Result<
        impl futures::Stream<Item = Result<FileMeta, FusioError>> + fusio::dynamic::MaybeSend,
        FusioError,
    > {
        self.probe.record_list(path);
        self.inner.list(path).await
    }

    async fn remove(&self, path: &FusioPath) -> Result<(), FusioError> {
        self.probe.record_remove(path);
        self.inner.remove(path).await
    }

    async fn copy(&self, from: &FusioPath, to: &FusioPath) -> Result<(), FusioError> {
        let _ = from;
        self.probe.record_copy(to);
        self.inner.copy(from, to).await
    }

    async fn link(&self, from: &FusioPath, to: &FusioPath) -> Result<(), FusioError> {
        let _ = from;
        self.probe.record_link(to);
        self.inner.link(from, to).await
    }
}

impl<FS> FsCas for ProbedFs<FS>
where
    FS: FsCas,
{
    fn load_with_tag(
        &self,
        path: &FusioPath,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, FusioError>> + '_>>
    {
        self.probe.record_cas_load(path);
        self.inner.load_with_tag(path)
    }

    fn put_conditional(
        &self,
        path: &FusioPath,
        payload: &[u8],
        content_type: Option<&str>,
        metadata: Option<Vec<(String, String)>>,
        condition: CasCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<String, FusioError>> + '_>> {
        self.probe.record_cas_put(path);
        self.inner
            .put_conditional(path, payload, content_type, metadata, condition)
    }
}

impl<FS> ObjectHead for ProbedFs<FS>
where
    FS: ObjectHead,
{
    fn head_metadata<'a>(
        &'a self,
        path: &'a FusioPath,
    ) -> Pin<
        Box<dyn MaybeSendFuture<Output = Result<Option<HashMap<String, String>>, FusioError>> + 'a>,
    > {
        self.probe.record_head(path);
        self.inner.head_metadata(path)
    }
}

#[derive(Debug, Error)]
pub(crate) enum BenchError {
    #[error("invalid environment variable `{name}`: {value}")]
    InvalidEnv { name: &'static str, value: String },
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("database build error: {0}")]
    DbBuild(#[from] DbBuildError),
    #[error("database operation error: {0}")]
    Db(#[from] DBError),
    #[error("{0}")]
    Message(String),
    #[error("scenario `{scenario_id}` skipped: {reason}")]
    ScenarioSkipped { scenario_id: String, reason: String },
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompactionSweepPoint {
    pub(crate) l0_trigger: usize,
    pub(crate) max_inputs: usize,
    pub(crate) max_task_bytes: Option<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectStoreBenchConfig {
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) session_token: Option<String>,
    pub(crate) prefix_base: String,
}

impl ObjectStoreBenchConfig {
    pub(crate) fn from_env() -> Result<Self, String> {
        let required = [
            ("TONBO_S3_BUCKET", "bucket name"),
            ("TONBO_S3_REGION", "region"),
            ("TONBO_S3_ACCESS_KEY", "access key"),
            ("TONBO_S3_SECRET_KEY", "secret key"),
        ];
        let mut missing = Vec::new();
        for &(var, _) in &required {
            let value = env::var(var).ok();
            let absent = match value.as_deref() {
                Some(v) => v.is_empty(),
                None => true,
            };
            if absent {
                missing.push(var);
            }
        }
        if !missing.is_empty() {
            return Err(format!(
                "object_store backend requires {}",
                missing.join(", ")
            ));
        }

        let bucket = env::var("TONBO_S3_BUCKET")
            .map_err(|err| format!("failed reading TONBO_S3_BUCKET: {err}"))?;
        let region = env::var("TONBO_S3_REGION")
            .map_err(|err| format!("failed reading TONBO_S3_REGION: {err}"))?;
        let access_key = env::var("TONBO_S3_ACCESS_KEY")
            .map_err(|err| format!("failed reading TONBO_S3_ACCESS_KEY: {err}"))?;
        let secret_key = env::var("TONBO_S3_SECRET_KEY")
            .map_err(|err| format!("failed reading TONBO_S3_SECRET_KEY: {err}"))?;
        let endpoint = env::var("TONBO_S3_ENDPOINT").ok();
        let session_token = env::var("TONBO_S3_SESSION_TOKEN").ok();
        let prefix_base =
            env::var("TONBO_BENCH_OBJECT_PREFIX").unwrap_or_else(|_| "tonbo-bench".to_string());

        Ok(Self {
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token,
            prefix_base,
        })
    }

    pub(crate) fn object_spec(&self, run_id: &str, scenario_id: &str) -> ObjectSpec {
        let credentials = match &self.session_token {
            Some(token) => AwsCreds::with_session_token(
                self.access_key.clone(),
                self.secret_key.clone(),
                token.clone(),
            ),
            None => AwsCreds::new(self.access_key.clone(), self.secret_key.clone()),
        };
        let mut s3 = S3Spec::new(
            self.bucket.clone(),
            format!("{}/{run_id}/{scenario_id}", self.prefix_base),
            credentials,
        );
        s3.region = Some(self.region.clone());
        s3.endpoint = self.endpoint.clone();
        s3.sign_payload = Some(true);
        ObjectSpec::s3(s3)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedConfig {
    pub(crate) ingest_batches: usize,
    pub(crate) ingest_batches_base: usize,
    pub(crate) dataset_scale: usize,
    pub(crate) backend: BenchBackend,
    pub(crate) rows_per_batch: usize,
    pub(crate) key_space: usize,
    pub(crate) artifact_iterations: usize,
    pub(crate) criterion_sample_size: usize,
    pub(crate) compaction_wait_timeout_ms: u64,
    pub(crate) compaction_poll_interval_ms: u64,
    pub(crate) compaction_periodic_tick_ms: u64,
    pub(crate) seed: u64,
    pub(crate) wal_sync_policy: WalSyncPolicy,
    pub(crate) wal_sync_policy_name: String,
    pub(crate) sweep_l0_triggers: Vec<usize>,
    pub(crate) sweep_max_inputs: Vec<usize>,
    pub(crate) sweep_max_task_bytes: Vec<Option<usize>>,
    pub(crate) write_frequency_periodic_ticks_ms: Vec<u64>,
    pub(crate) enable_read_while_compaction: bool,
    pub(crate) enable_write_throughput_vs_compaction_frequency: bool,
}

impl ResolvedConfig {
    pub(crate) fn from_env() -> Result<Self, BenchError> {
        let ingest_batches_base = env_usize(
            "TONBO_COMPACTION_BENCH_INGEST_BATCHES",
            DEFAULT_INGEST_BATCHES,
        )?;
        let dataset_scale = env_usize("TONBO_BENCH_DATASET_SCALE", DEFAULT_DATASET_SCALE)?;
        let backend = env_backend("TONBO_BENCH_BACKEND", DEFAULT_BACKEND)?;
        let rows_per_batch = env_usize(
            "TONBO_COMPACTION_BENCH_ROWS_PER_BATCH",
            DEFAULT_ROWS_PER_BATCH,
        )?;
        let key_space = env_usize("TONBO_COMPACTION_BENCH_KEY_SPACE", DEFAULT_KEY_SPACE)?;
        let artifact_iterations = env_usize(
            "TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS",
            DEFAULT_ARTIFACT_ITERATIONS,
        )?;
        let criterion_sample_size = env_usize(
            "TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE",
            DEFAULT_CRITERION_SAMPLE_SIZE,
        )?;
        let compaction_wait_timeout_ms = env_u64(
            "TONBO_COMPACTION_BENCH_COMPACTION_WAIT_TIMEOUT_MS",
            DEFAULT_COMPACTION_WAIT_TIMEOUT_MS,
        )?;
        let compaction_poll_interval_ms = env_u64(
            "TONBO_COMPACTION_BENCH_COMPACTION_POLL_INTERVAL_MS",
            DEFAULT_COMPACTION_POLL_INTERVAL_MS,
        )?;
        let compaction_periodic_tick_ms = env_u64(
            "TONBO_COMPACTION_BENCH_COMPACTION_PERIODIC_TICK_MS",
            DEFAULT_COMPACTION_PERIODIC_TICK_MS,
        )?;
        let seed = env_u64("TONBO_COMPACTION_BENCH_SEED", DEFAULT_SEED)?;
        let (wal_sync_policy, wal_sync_policy_name) = env_wal_sync_policy()?;
        let sweep_l0_triggers = env_usize_list(
            "TONBO_COMPACTION_BENCH_SWEEP_L0_TRIGGERS",
            DEFAULT_SWEEP_L0_TRIGGERS,
        )?;
        let sweep_max_inputs = env_usize_list(
            "TONBO_COMPACTION_BENCH_SWEEP_MAX_INPUTS",
            DEFAULT_SWEEP_MAX_INPUTS,
        )?;
        let sweep_max_task_bytes = env_optional_usize_list(
            "TONBO_COMPACTION_BENCH_SWEEP_MAX_TASK_BYTES",
            DEFAULT_SWEEP_MAX_TASK_BYTES,
        )?;
        let write_frequency_periodic_ticks_ms = env_u64_list(
            "TONBO_COMPACTION_BENCH_WRITE_FREQUENCY_PERIODIC_TICKS_MS",
            DEFAULT_WRITE_FREQUENCY_PERIODIC_TICKS_MS,
        )?;
        let enable_read_while_compaction = env_bool(
            "TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION",
            DEFAULT_ENABLE_READ_WHILE_COMPACTION,
        )?;
        let enable_write_throughput_vs_compaction_frequency = env_bool(
            "TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY",
            DEFAULT_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY,
        )?;

        if rows_per_batch == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_ROWS_PER_BATCH",
                value: "must be > 0".to_string(),
            });
        }
        if ingest_batches_base == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_INGEST_BATCHES",
                value: "must be > 0".to_string(),
            });
        }
        if dataset_scale == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_BENCH_DATASET_SCALE",
                value: "must be > 0".to_string(),
            });
        }
        if key_space == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_KEY_SPACE",
                value: "must be > 0".to_string(),
            });
        }
        if artifact_iterations == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS",
                value: "must be > 0".to_string(),
            });
        }
        if criterion_sample_size < 10 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE",
                value: "criterion requires sample_size >= 10".to_string(),
            });
        }
        if compaction_poll_interval_ms == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_COMPACTION_POLL_INTERVAL_MS",
                value: "must be > 0".to_string(),
            });
        }
        if compaction_periodic_tick_ms == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_COMPACTION_PERIODIC_TICK_MS",
                value: "must be > 0".to_string(),
            });
        }
        if compaction_wait_timeout_ms < compaction_poll_interval_ms {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_COMPACTION_WAIT_TIMEOUT_MS",
                value: "must be >= TONBO_COMPACTION_BENCH_COMPACTION_POLL_INTERVAL_MS".to_string(),
            });
        }
        let ingest_batches =
            ingest_batches_base
                .checked_mul(dataset_scale)
                .ok_or(BenchError::InvalidEnv {
                    name: "TONBO_BENCH_DATASET_SCALE",
                    value: format!(
                        "scaled ingest batches overflowed: {ingest_batches_base} * {dataset_scale}"
                    ),
                })?;

        Ok(Self {
            ingest_batches,
            ingest_batches_base,
            dataset_scale,
            backend,
            rows_per_batch,
            key_space,
            artifact_iterations,
            criterion_sample_size,
            compaction_wait_timeout_ms,
            compaction_poll_interval_ms,
            compaction_periodic_tick_ms,
            seed,
            wal_sync_policy,
            wal_sync_policy_name,
            sweep_l0_triggers,
            sweep_max_inputs,
            sweep_max_task_bytes,
            write_frequency_periodic_ticks_ms,
            enable_read_while_compaction,
            enable_write_throughput_vs_compaction_frequency,
        })
    }

    pub(crate) fn compaction_sweep_points(&self) -> Vec<CompactionSweepPoint> {
        let mut points = Vec::new();
        for &l0_trigger in &self.sweep_l0_triggers {
            for &max_inputs in &self.sweep_max_inputs {
                for &max_task_bytes in &self.sweep_max_task_bytes {
                    points.push(CompactionSweepPoint {
                        l0_trigger,
                        max_inputs,
                        max_task_bytes,
                    });
                }
            }
        }
        points
    }

    pub(crate) fn artifact(&self) -> ResolvedConfigArtifact {
        ResolvedConfigArtifact {
            ingest_batches: self.ingest_batches,
            ingest_batches_base: self.ingest_batches_base,
            dataset_scale: self.dataset_scale,
            backend: self.backend.as_str().to_string(),
            rows_per_batch: self.rows_per_batch,
            key_space: self.key_space,
            artifact_iterations: self.artifact_iterations,
            criterion_sample_size: self.criterion_sample_size,
            compaction_wait_timeout_ms: self.compaction_wait_timeout_ms,
            compaction_poll_interval_ms: self.compaction_poll_interval_ms,
            compaction_periodic_tick_ms: self.compaction_periodic_tick_ms,
            seed: self.seed,
            wal_sync_policy: self.wal_sync_policy_name.clone(),
            sweep_l0_triggers: self.sweep_l0_triggers.clone(),
            sweep_max_inputs: self.sweep_max_inputs.clone(),
            sweep_max_task_bytes: self.sweep_max_task_bytes.clone(),
            write_frequency_periodic_ticks_ms: self.write_frequency_periodic_ticks_ms.clone(),
            enable_read_while_compaction: self.enable_read_while_compaction,
            enable_write_throughput_vs_compaction_frequency: self
                .enable_write_throughput_vs_compaction_frequency,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VersionSummary {
    pub(crate) sst_count: usize,
    pub(crate) sst_bytes: u64,
    pub(crate) level_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) enum CompactionProfile {
    Disabled,
    Default { periodic_tick_ms: u64 },
    Swept(CompactionTuning),
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompactionTuning {
    pub(crate) l0_trigger: usize,
    pub(crate) max_inputs: usize,
    pub(crate) max_task_bytes: Option<usize>,
    pub(crate) periodic_tick_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScenarioWorkload {
    ReadOnly,
    ReadWhileCompaction,
    WriteThroughput,
    SweepEstimate,
}

#[derive(Clone, Debug)]
pub(crate) enum ScenarioStorageTarget {
    Local { root: PathBuf },
    ObjectStore { spec: ObjectSpec },
}

#[derive(Clone)]
pub(crate) struct WriteWorkloadState {
    schema: SchemaRef,
    rows_per_batch: usize,
    key_space: usize,
    seed: u64,
    next_batch: Arc<AtomicU64>,
}

impl WriteWorkloadState {
    pub(crate) fn new(
        schema: SchemaRef,
        rows_per_batch: usize,
        key_space: usize,
        seed: u64,
        start_batch: u64,
    ) -> Self {
        Self {
            schema,
            rows_per_batch,
            key_space,
            seed,
            next_batch: Arc::new(AtomicU64::new(start_batch)),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ScenarioState {
    pub(crate) scenario_id: &'static str,
    pub(crate) scenario_name: String,
    pub(crate) scenario_variant_id: String,
    pub(crate) benchmark_id: String,
    pub(crate) workload: ScenarioWorkload,
    pub(crate) storage_target: ScenarioStorageTarget,
    pub(crate) dimensions: ScenarioDimensionsArtifact,
    pub(crate) db: BenchmarkDb,
    pub(crate) io_probe: IoProbe,
    pub(crate) setup_elapsed_ns: u64,
    pub(crate) setup_io: IoCountersArtifact,
    pub(crate) rows_per_op_hint: usize,
    pub(crate) version_before_compaction: VersionSummary,
    pub(crate) version_ready: VersionSummary,
    pub(crate) volume_before_compaction: StorageVolumeArtifact,
    pub(crate) volume_ready: StorageVolumeArtifact,
    pub(crate) gc_observation: Option<GcObservationArtifact>,
    pub(crate) write_state: Option<WriteWorkloadState>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkArtifact {
    pub(crate) schema_version: u32,
    pub(crate) benchmark_id: &'static str,
    pub(crate) run_id: String,
    pub(crate) generated_at_unix_ms: u64,
    pub(crate) config: ResolvedConfigArtifact,
    pub(crate) scenarios: Vec<ScenarioArtifact>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ResolvedConfigArtifact {
    ingest_batches: usize,
    ingest_batches_base: usize,
    dataset_scale: usize,
    backend: String,
    rows_per_batch: usize,
    key_space: usize,
    artifact_iterations: usize,
    criterion_sample_size: usize,
    compaction_wait_timeout_ms: u64,
    compaction_poll_interval_ms: u64,
    compaction_periodic_tick_ms: u64,
    seed: u64,
    wal_sync_policy: String,
    sweep_l0_triggers: Vec<usize>,
    sweep_max_inputs: Vec<usize>,
    sweep_max_task_bytes: Vec<Option<usize>>,
    write_frequency_periodic_ticks_ms: Vec<u64>,
    enable_read_while_compaction: bool,
    enable_write_throughput_vs_compaction_frequency: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ScenarioArtifact {
    scenario_id: String,
    scenario_name: String,
    scenario_variant_id: String,
    dimensions: ScenarioDimensionsArtifact,
    setup: ScenarioSetupArtifact,
    summary: ScenarioSummaryArtifact,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ScenarioDimensionsArtifact {
    scenario_variant_id: String,
    workload: ScenarioWorkload,
    sweep: CompactionSweepArtifact,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompactionSweepArtifact {
    l0_trigger: Option<usize>,
    max_inputs: Option<usize>,
    max_task_bytes: Option<usize>,
    periodic_tick_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ScenarioSetupArtifact {
    total_elapsed_ns: u64,
    total_elapsed_ms: f64,
    rows_per_scan: usize,
    sst_count_before_compaction: usize,
    level_count_before_compaction: usize,
    sst_count_ready: usize,
    level_count_ready: usize,
    logical_before_compaction: LogicalVolumeArtifact,
    logical_ready: LogicalVolumeArtifact,
    volume_before_compaction: StorageVolumeArtifact,
    volume_ready: StorageVolumeArtifact,
    physical_stale_estimate_before_compaction: PhysicalStaleEstimateArtifact,
    physical_stale_estimate_ready: PhysicalStaleEstimateArtifact,
    gc_observation: Option<GcObservationArtifact>,
    io: IoCountersArtifact,
}

#[derive(Debug, Serialize)]
struct ScenarioSummaryArtifact {
    iterations: usize,
    total_elapsed_ns: u64,
    total_elapsed_ms: f64,
    rows_processed: u64,
    throughput: ThroughputSummary,
    latency_ns: LatencySummary,
    read_path_latency_ns: Option<ReadPathLatencySummary>,
    read_path_internal_ns: Option<ReadPathInternalSummary>,
    physical_stale_estimate: Option<PhysicalStaleEstimateArtifact>,
    gc: Option<GcDerivedArtifact>,
    io: IoCountersArtifact,
}

#[derive(Debug, Serialize)]
struct ThroughputSummary {
    ops_per_sec: f64,
    rows_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct LatencySummary {
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    mean: f64,
}

struct ScenarioMeasurement {
    iterations: usize,
    total_elapsed_ns: u64,
    latencies_ns: Vec<u64>,
    rows_processed: u64,
    read_path: Option<ReadPathAggregate>,
    physical_stale_estimate: Option<PhysicalStaleEstimateArtifact>,
    io: IoCountersArtifact,
}

#[derive(Debug, Serialize)]
struct ReadPathLatencySummary {
    mean_prepare_ns: f64,
    mean_consume_ns: f64,
    mean_total_ns: f64,
    prepare_share_pct: f64,
    consume_share_pct: f64,
    mean_batches_per_scan: f64,
}

#[derive(Debug, Serialize)]
struct ReadPathInternalSummary {
    mean_snapshot_ns: f64,
    mean_plan_scan_ns: f64,
    mean_build_scan_streams_ns: f64,
    mean_merge_init_ns: f64,
    mean_package_init_ns: f64,
}

#[derive(Clone, Copy, Default)]
struct ReadPathInternalBreakdown {
    snapshot_ns: u64,
    plan_scan_ns: u64,
    build_scan_streams_ns: u64,
    merge_init_ns: u64,
    package_init_ns: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct ReadPathBreakdown {
    prepare_ns: u64,
    consume_ns: u64,
    batch_count: usize,
    internal: ReadPathInternalBreakdown,
}

#[derive(Default)]
struct ReadPathAggregate {
    samples: usize,
    prepare_ns_total: u128,
    consume_ns_total: u128,
    batch_count_total: u128,
    snapshot_ns_total: u128,
    plan_scan_ns_total: u128,
    build_scan_streams_ns_total: u128,
    merge_init_ns_total: u128,
    package_init_ns_total: u128,
}

impl ReadPathAggregate {
    fn record(&mut self, breakdown: ReadPathBreakdown) {
        self.samples = self.samples.saturating_add(1);
        self.prepare_ns_total = self
            .prepare_ns_total
            .saturating_add(u128::from(breakdown.prepare_ns));
        self.consume_ns_total = self
            .consume_ns_total
            .saturating_add(u128::from(breakdown.consume_ns));
        self.batch_count_total = self
            .batch_count_total
            .saturating_add(breakdown.batch_count as u128);
        self.snapshot_ns_total = self
            .snapshot_ns_total
            .saturating_add(u128::from(breakdown.internal.snapshot_ns));
        self.plan_scan_ns_total = self
            .plan_scan_ns_total
            .saturating_add(u128::from(breakdown.internal.plan_scan_ns));
        self.build_scan_streams_ns_total = self
            .build_scan_streams_ns_total
            .saturating_add(u128::from(breakdown.internal.build_scan_streams_ns));
        self.merge_init_ns_total = self
            .merge_init_ns_total
            .saturating_add(u128::from(breakdown.internal.merge_init_ns));
        self.package_init_ns_total = self
            .package_init_ns_total
            .saturating_add(u128::from(breakdown.internal.package_init_ns));
    }

    fn to_summary(&self) -> Option<ReadPathLatencySummary> {
        if self.samples == 0 {
            return None;
        }
        let samples = self.samples as f64;
        let mean_prepare_ns = self.prepare_ns_total as f64 / samples;
        let mean_consume_ns = self.consume_ns_total as f64 / samples;
        let mean_total_ns = mean_prepare_ns + mean_consume_ns;
        let (prepare_share_pct, consume_share_pct) = if mean_total_ns > 0.0 {
            (
                (mean_prepare_ns / mean_total_ns) * 100.0,
                (mean_consume_ns / mean_total_ns) * 100.0,
            )
        } else {
            (0.0, 0.0)
        };
        Some(ReadPathLatencySummary {
            mean_prepare_ns,
            mean_consume_ns,
            mean_total_ns,
            prepare_share_pct,
            consume_share_pct,
            mean_batches_per_scan: self.batch_count_total as f64 / samples,
        })
    }

    fn to_internal_summary(&self) -> Option<ReadPathInternalSummary> {
        if self.samples == 0 {
            return None;
        }
        let samples = self.samples as f64;
        Some(ReadPathInternalSummary {
            mean_snapshot_ns: self.snapshot_ns_total as f64 / samples,
            mean_plan_scan_ns: self.plan_scan_ns_total as f64 / samples,
            mean_build_scan_streams_ns: self.build_scan_streams_ns_total as f64 / samples,
            mean_merge_init_ns: self.merge_init_ns_total as f64 / samples,
            mean_package_init_ns: self.package_init_ns_total as f64 / samples,
        })
    }
}

struct OperationResult {
    rows: usize,
    read_path: Option<ReadPathBreakdown>,
    physical_stale_estimate: Option<PhysicalStaleEstimateArtifact>,
}

impl ScenarioDimensionsArtifact {
    pub(crate) fn baseline(variant_id: impl Into<String>, workload: ScenarioWorkload) -> Self {
        Self {
            scenario_variant_id: variant_id.into(),
            workload,
            sweep: CompactionSweepArtifact {
                l0_trigger: None,
                max_inputs: None,
                max_task_bytes: None,
                periodic_tick_ms: None,
            },
        }
    }

    pub(crate) fn swept(
        variant_id: impl Into<String>,
        workload: ScenarioWorkload,
        tuning: &CompactionTuning,
    ) -> Self {
        Self {
            scenario_variant_id: variant_id.into(),
            workload,
            sweep: CompactionSweepArtifact {
                l0_trigger: Some(tuning.l0_trigger),
                max_inputs: Some(tuning.max_inputs),
                max_task_bytes: tuning.max_task_bytes,
                periodic_tick_ms: Some(tuning.periodic_tick_ms),
            },
        }
    }
}

async fn run_scenario_operation(scenario: &ScenarioState) -> Result<OperationResult, BenchError> {
    match scenario.workload {
        ScenarioWorkload::ReadOnly => {
            let (rows, read_path) = read_all_rows(&scenario.db).await?;
            Ok(OperationResult {
                rows,
                read_path: Some(read_path),
                physical_stale_estimate: None,
            })
        }
        ScenarioWorkload::ReadWhileCompaction => {
            let write_state = scenario.write_state.as_ref().ok_or_else(|| {
                BenchError::Message(format!(
                    "scenario `{}` missing write workload state",
                    scenario.benchmark_id
                ))
            })?;
            let write_future = ingest_next_write_batch(&scenario.db, write_state);
            let read_future = read_all_rows(&scenario.db);
            let (write_result, read_result) = tokio::join!(write_future, read_future);
            let _ = write_result?;
            let (rows, read_path) = read_result?;
            Ok(OperationResult {
                rows,
                read_path: Some(read_path),
                physical_stale_estimate: None,
            })
        }
        ScenarioWorkload::WriteThroughput => {
            let write_state = scenario.write_state.as_ref().ok_or_else(|| {
                BenchError::Message(format!(
                    "scenario `{}` missing write workload state",
                    scenario.benchmark_id
                ))
            })?;
            let rows = ingest_next_write_batch(&scenario.db, write_state).await?;
            Ok(OperationResult {
                rows,
                read_path: None,
                physical_stale_estimate: None,
            })
        }
        ScenarioWorkload::SweepEstimate => {
            let version = latest_version_summary(&scenario.db).await?;
            let physical = snapshot_storage_target_volume(&scenario.storage_target).await?;
            let logical = LogicalVolumeArtifact::from_version(version);
            let estimate = PhysicalStaleEstimateArtifact::from_volume(&physical, &logical);
            Ok(OperationResult {
                rows: 1,
                read_path: None,
                physical_stale_estimate: Some(estimate),
            })
        }
    }
}

async fn ingest_next_write_batch(
    db: &BenchmarkDb,
    write_state: &WriteWorkloadState,
) -> Result<usize, BenchError> {
    let batch_idx_u64 = write_state.next_batch.fetch_add(1, Ordering::Relaxed);
    let batch_idx = usize::try_from(batch_idx_u64).unwrap_or(usize::MAX);
    for attempt in 1..=INGEST_RETRY_MAX_ATTEMPTS {
        let batch = build_batch(
            &write_state.schema,
            write_state.rows_per_batch,
            write_state.key_space,
            write_state.seed,
            batch_idx,
        )?;
        let ingest_result = match db {
            BenchmarkDb::Local(inner) => inner.ingest(batch).await,
            BenchmarkDb::ObjectStore(inner) => inner.ingest(batch).await,
        };
        match ingest_result {
            Ok(()) => {
                return Ok(write_state.rows_per_batch);
            }
            Err(err) => {
                if !is_retryable_ingest_error(&err) || attempt == INGEST_RETRY_MAX_ATTEMPTS {
                    return Err(BenchError::Db(err));
                }
                let delay_ms = (attempt as u64).saturating_mul(INGEST_RETRY_BACKOFF_BASE_MS);
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
    Err(BenchError::Message(
        "ingest retry loop exited unexpectedly".to_string(),
    ))
}

fn is_retryable_ingest_error(err: &DBError) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    text.contains("precondition failed")
        || text.contains("cas conflict")
        || text.contains("compare-and-swap")
}

pub(crate) fn build_runtime() -> Result<Runtime, BenchError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| BenchError::Message(format!("tokio runtime build failed: {err}")))
}

pub(crate) fn build_run_id() -> String {
    format!("{}-{}", unix_epoch_ms(), process::id())
}

pub(crate) fn artifact_path(run_id: &str) -> PathBuf {
    PathBuf::from("target")
        .join("tonbo-bench")
        .join(format!("{BENCH_ID}-{run_id}.json"))
}

pub(crate) fn run_criterion(
    c: &mut Criterion,
    runtime: &Arc<Runtime>,
    scenarios: &[ScenarioState],
    sample_size: usize,
) {
    let mut group = c.benchmark_group(BENCH_ID);
    group.sample_size(sample_size);

    for scenario in scenarios {
        group.throughput(Throughput::Elements(usize_to_u64(
            scenario.rows_per_op_hint,
        )));
        let scenario = scenario.clone();
        let runtime = Arc::clone(runtime);
        let benchmark_id = scenario.benchmark_id.clone();
        group.bench_function(&benchmark_id, move |b| {
            let scenario = scenario.clone();
            let runtime = Arc::clone(&runtime);
            b.iter_custom(move |iters| {
                let scenario = scenario.clone();
                let started = Instant::now();
                runtime.block_on(async {
                    for _ in 0..iters {
                        match run_scenario_operation(&scenario).await {
                            Ok(result) => {
                                black_box(result.rows);
                            }
                            Err(err) => {
                                panic!(
                                    "scenario `{}` operation failed: {err}",
                                    scenario.benchmark_id
                                );
                            }
                        }
                    }
                });
                started.elapsed()
            });
        });
    }

    group.finish();
}

pub(crate) fn build_artifact(
    config: &ResolvedConfig,
    run_id: &str,
    runtime: &Runtime,
    scenarios: &[ScenarioState],
) -> Result<BenchmarkArtifact, BenchError> {
    let scenario_artifacts = measure_for_artifact(runtime, scenarios, config.artifact_iterations)?;

    Ok(BenchmarkArtifact {
        schema_version: BENCH_SCHEMA_VERSION,
        benchmark_id: BENCH_ID,
        run_id: run_id.to_string(),
        generated_at_unix_ms: unix_epoch_ms(),
        config: config.artifact(),
        scenarios: scenario_artifacts,
    })
}

pub(crate) fn print_directional_report(artifact: &BenchmarkArtifact, config: &ResolvedConfig) {
    let baseline = artifact
        .scenarios
        .iter()
        .find(|scenario| scenario.scenario_id == "read_baseline");
    let quiesced = artifact
        .scenarios
        .iter()
        .find(|scenario| scenario.scenario_id == "read_compaction_quiesced");
    let (Some(baseline), Some(quiesced)) = (baseline, quiesced) else {
        return;
    };

    let read_ops_drop = pct_drop(baseline.summary.io.read_ops, quiesced.summary.io.read_ops);
    let bytes_drop = pct_drop(
        baseline.summary.io.bytes_read,
        quiesced.summary.io.bytes_read,
    );
    let mean_improve = pct_drop_float(
        baseline.summary.latency_ns.mean,
        quiesced.summary.latency_ns.mean,
    );
    let p99_improve = pct_drop(
        baseline.summary.latency_ns.p99,
        quiesced.summary.latency_ns.p99,
    );
    let physical_stale_sst_bytes = quiesced
        .setup
        .physical_stale_estimate_ready
        .candidate_sst_bytes;
    let physical_stale_sst_objects = quiesced
        .setup
        .physical_stale_estimate_ready
        .candidate_sst_objects;

    let observed = format!(
        "read_ops_delta={}, bytes_delta={}, mean_delta={}, p99_delta={}, \
         physical_stale_estimate_sst_objects={}, physical_stale_estimate_sst_bytes={}",
        fmt_pct_opt(read_ops_drop),
        fmt_pct_opt(bytes_drop),
        fmt_pct_opt(mean_improve),
        fmt_pct_opt(p99_improve),
        physical_stale_sst_objects,
        physical_stale_sst_bytes
    );
    let baseline_latency = &baseline.summary.latency_ns;
    let quiesced_latency = &quiesced.summary.latency_ns;
    let interpretation = build_interpretation(
        config.backend,
        read_ops_drop,
        bytes_drop,
        mean_improve,
        p99_improve,
    );

    eprintln!("tonbo benchmark directional report");
    eprintln!("  Directional Question: CPU vs I/O bound?");
    eprintln!("  Scenario Set: compaction.read_baseline vs compaction.read_compaction_quiesced");
    eprintln!("  Dataset Scale: {}", config.dataset_scale);
    eprintln!("  Backend: {}", config.backend.as_str());
    eprintln!(
        "  Latency (ns): baseline(mean={:.2}, p50={}, p95={}, p99={}) quiesced(mean={:.2}, \
         p50={}, p95={}, p99={})",
        baseline_latency.mean,
        baseline_latency.p50,
        baseline_latency.p95,
        baseline_latency.p99,
        quiesced_latency.mean,
        quiesced_latency.p50,
        quiesced_latency.p95,
        quiesced_latency.p99,
    );
    eprintln!("  Observed: {observed}");
    eprintln!("  Interpretation: {interpretation}");
    eprintln!(
        "  Implication: rerun with larger TONBO_BENCH_DATASET_SCALE, compare local vs \
         object_store, and track p99 alongside mean."
    );

    if let (Some(gc_off), Some(gc_on)) = (
        artifact
            .scenarios
            .iter()
            .find(|scenario| scenario.scenario_id == "read_compaction_quiesced"),
        artifact
            .scenarios
            .iter()
            .find(|scenario| scenario.scenario_id == "read_compaction_quiesced_after_gc"),
    ) {
        print_gc_latency_report("read", gc_off, gc_on);
    }
    if let (Some(gc_off), Some(gc_on)) = (
        artifact
            .scenarios
            .iter()
            .find(|scenario| scenario.scenario_id == "write_heavy_no_sst_sweep"),
        artifact
            .scenarios
            .iter()
            .find(|scenario| scenario.scenario_id == "write_heavy_with_sst_sweep"),
    ) {
        print_gc_latency_report("write", gc_off, gc_on);
    }
}

fn print_gc_latency_report(label: &str, gc_off: &ScenarioArtifact, gc_on: &ScenarioArtifact) {
    let mean_delta = pct_drop_float(
        gc_off.summary.latency_ns.mean,
        gc_on.summary.latency_ns.mean,
    );
    let p99_delta = pct_drop(gc_off.summary.latency_ns.p99, gc_on.summary.latency_ns.p99);
    let throughput_delta = pct_drop_float(
        gc_off.summary.throughput.ops_per_sec,
        gc_on.summary.throughput.ops_per_sec,
    )
    .map(|pct| -pct);

    eprintln!("tonbo benchmark gc comparison");
    eprintln!(
        "  Workload: {label} ({}) vs {}",
        gc_off.scenario_id, gc_on.scenario_id
    );
    eprintln!(
        "  Latency delta: mean={}, p99={}, throughput_delta={}",
        fmt_pct_opt(mean_delta),
        fmt_pct_opt(p99_delta),
        fmt_pct_opt(throughput_delta),
    );
    if let Some(gc) = gc_on.setup.gc_observation.as_ref() {
        if let Some(derived) = gc_on.summary.gc.as_ref() {
            eprintln!(
                "  GC timing: setup_elapsed_ms={:.3}, steady_state_elapsed_ms={:.3}, \
                 total_sweep_runs={}, total_sweep_duration_ms={}, total_deleted_objects={}, \
                 total_deleted_bytes={}, total_deleted_bytes_per_ms={}, \
                 total_deleted_objects_per_ms={}, before_explicit_sweep_runs={}, \
                 before_explicit_sweep_duration_ms={}, before_explicit_sweep_deleted_objects={}, \
                 before_explicit_sweep_deleted_bytes={}, explicit_sweep_runs={}, \
                 explicit_sweep_duration_ms={}, explicit_sweep_deleted_objects={}, \
                 explicit_sweep_deleted_bytes={}, gc_time_share_pct_of_setup={}, \
                 gc_time_share_pct_of_total_elapsed={}",
                gc_on.setup.total_elapsed_ms,
                gc_on.summary.total_elapsed_ms,
                derived.sst_sweep_runs,
                derived.sst_sweep_duration_ms_total,
                derived.sst_deleted_objects_total,
                derived.sst_deleted_bytes_total,
                fmt_ratio_opt(derived.sst_deleted_bytes_per_ms),
                fmt_ratio_opt(derived.sst_deleted_objects_per_ms),
                derived.sst_sweep_runs_before_explicit_sweep,
                derived.sst_sweep_duration_ms_before_explicit_sweep,
                derived.sst_deleted_objects_before_explicit_sweep,
                derived.sst_deleted_bytes_before_explicit_sweep,
                derived.explicit_sweep_runs,
                derived.explicit_sweep_duration_ms,
                derived.explicit_sweep_deleted_objects,
                derived.explicit_sweep_deleted_bytes,
                fmt_pct_opt(derived.gc_time_share_pct_of_setup),
                fmt_pct_opt(derived.gc_time_share_pct_of_total_elapsed),
            );
        }
        eprintln!(
            "  Physical SST: before(objects={}, bytes={}) after(objects={}, bytes={}) \
             reclaimed(objects={}, bytes={})",
            gc.volume_before_gc.sst_object_count,
            gc.volume_before_gc.sst_bytes,
            gc.volume_after_gc.sst_object_count,
            gc.volume_after_gc.sst_bytes,
            gc.reclaimed_sst_objects,
            gc.reclaimed_sst_bytes,
        );
        eprintln!(
            "  Physical estimate before explicit sweep: stale_sst_objects={}, stale_sst_bytes={}, \
             live_sst_objects={}, live_sst_bytes={}, physical_sst_objects={}, \
             physical_sst_bytes={}",
            gc.physical_stale_estimate_before_explicit_sweep
                .candidate_sst_objects,
            gc.physical_stale_estimate_before_explicit_sweep
                .candidate_sst_bytes,
            gc.physical_stale_estimate_before_explicit_sweep
                .live_sst_objects,
            gc.physical_stale_estimate_before_explicit_sweep
                .live_sst_bytes,
            gc.physical_stale_estimate_before_explicit_sweep
                .physical_sst_objects,
            gc.physical_stale_estimate_before_explicit_sweep
                .physical_sst_bytes,
        );
        eprintln!(
            "  Cumulative sweep counters: before(runs={}, plan_writes={}, \
             plan_overwrite_non_empty={}, plan_takes={}) after(runs={}, plan_writes={}, \
             plan_overwrite_non_empty={}, plan_takes={})",
            gc.cumulative_before_explicit_sweep.sweep_runs,
            gc.cumulative_before_explicit_sweep.gc_plan_write_runs,
            gc.cumulative_before_explicit_sweep
                .gc_plan_overwrite_non_empty,
            gc.cumulative_before_explicit_sweep.gc_plan_take_runs,
            gc.cumulative_after_explicit_sweep.sweep_runs,
            gc.cumulative_after_explicit_sweep.gc_plan_write_runs,
            gc.cumulative_after_explicit_sweep
                .gc_plan_overwrite_non_empty,
            gc.cumulative_after_explicit_sweep.gc_plan_take_runs,
        );
        if let Some(plan) = gc.persisted_plan_before_explicit_sweep.as_ref() {
            eprintln!(
                "  Persisted plan before explicit sweep: staged_ssts={}, authorized_ssts={}, \
                 blocked_ssts={}, protected_versions={}, active_snapshot_versions={}, \
                 protected_sst_objects={}, obsolete_wal_segments={}",
                plan.staged_sst_candidates,
                plan.authorized_sst_candidates,
                plan.blocked_sst_candidates,
                plan.protected_versions,
                plan.active_snapshot_versions,
                plan.protected_sst_objects,
                plan.obsolete_wal_segments,
            );
            eprintln!(
                "  Persisted plan blocker before explicit sweep: {}",
                plan.blocker
            );
        } else {
            eprintln!("  Persisted plan before explicit sweep: none");
        }
        eprintln!(
            "  Explicit sweep result: deleted_objects={}, deleted_bytes={}, delete_failures={}, \
             duration_ms={}",
            gc.explicit_sweep_result.deleted_objects,
            gc.explicit_sweep_result.deleted_bytes,
            gc.explicit_sweep_result.delete_failures,
            gc.explicit_sweep_result.duration_ms,
        );
        eprintln!(
            "  Physical estimate after explicit sweep: stale_sst_objects={}, stale_sst_bytes={}, \
             live_sst_objects={}, live_sst_bytes={}, physical_sst_objects={}, \
             physical_sst_bytes={}",
            gc.physical_stale_estimate_after_explicit_sweep
                .candidate_sst_objects,
            gc.physical_stale_estimate_after_explicit_sweep
                .candidate_sst_bytes,
            gc.physical_stale_estimate_after_explicit_sweep
                .live_sst_objects,
            gc.physical_stale_estimate_after_explicit_sweep
                .live_sst_bytes,
            gc.physical_stale_estimate_after_explicit_sweep
                .physical_sst_objects,
            gc.physical_stale_estimate_after_explicit_sweep
                .physical_sst_bytes,
        );
        if let Some(plan) = gc.persisted_plan_after_explicit_sweep.as_ref() {
            eprintln!(
                "  Persisted plan after explicit sweep: staged_ssts={}, authorized_ssts={}, \
                 blocked_ssts={}, protected_versions={}, active_snapshot_versions={}, \
                 protected_sst_objects={}, obsolete_wal_segments={}",
                plan.staged_sst_candidates,
                plan.authorized_sst_candidates,
                plan.blocked_sst_candidates,
                plan.protected_versions,
                plan.active_snapshot_versions,
                plan.protected_sst_objects,
                plan.obsolete_wal_segments,
            );
            eprintln!(
                "  Persisted plan blocker after explicit sweep: {}",
                plan.blocker
            );
        } else {
            eprintln!("  Persisted plan after explicit sweep: none");
        }
        if let Some(interpretation) = explain_gc_observation(gc) {
            eprintln!("  Interpretation: {interpretation}");
        }
    }
}

fn explain_gc_observation(gc: &GcObservationArtifact) -> Option<String> {
    let physical = &gc.physical_stale_estimate_before_explicit_sweep;
    let has_physical_stale_estimate =
        physical.candidate_sst_objects > 0 || physical.candidate_sst_bytes > 0;
    let persisted_plan_empty = gc.persisted_plan_before_explicit_sweep.is_none();
    let prior_staged_or_authorized_candidates = gc
        .cumulative_before_explicit_sweep
        .gc_plan_written_sst_candidates
        > 0
        || gc
            .cumulative_before_explicit_sweep
            .gc_plan_authorized_sst_candidates
            > 0;

    if has_physical_stale_estimate && persisted_plan_empty {
        let message = if prior_staged_or_authorized_candidates {
            "physical estimate is non-zero while the persisted plan is empty: the storage delta is \
             broader than the current staged plan, and earlier compaction-triggered sweeps already \
             wrote/took authorized candidates before this explicit sweep point"
        } else {
            "physical estimate is non-zero while the persisted plan is empty: this means \
             stale-looking SST objects still exist physically, but no persisted GC plan remained \
             staged for the explicit sweep to consume"
        };
        return Some(message.to_string());
    }

    None
}

pub(crate) fn benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

pub(crate) fn scenario_root(run_id: &str, scenario_id: &str) -> PathBuf {
    PathBuf::from("target")
        .join("tonbo-bench")
        .join("workspaces")
        .join(run_id)
        .join(scenario_id)
}

pub(crate) async fn open_benchmark_db(
    schema: &SchemaRef,
    root: &Path,
    config: &ResolvedConfig,
    compaction_profile: &CompactionProfile,
    io_probe: &IoProbe,
) -> Result<BenchmarkDb, BenchError> {
    let root_string = absolutize(root)?.to_string_lossy().into_owned();
    let fs = Arc::new(ProbedFs::new(LocalFs {}, io_probe.clone()));
    let compaction_metrics = Arc::new(CompactionMetrics::new());
    let mut builder = DbBuilder::from_schema_key_name(Arc::clone(schema), "id")?
        .on_durable_fs(fs, root_string)?
        .wal_sync_policy(config.wal_sync_policy.clone())
        .with_minor_compaction(1, 0);

    match compaction_profile {
        CompactionProfile::Disabled => {}
        CompactionProfile::Default { periodic_tick_ms } => {
            let options = CompactionOptions::new()
                .periodic_tick(Duration::from_millis(*periodic_tick_ms))
                .compaction_metrics(Arc::clone(&compaction_metrics));
            builder = builder.with_compaction_options(options);
        }
        CompactionProfile::Swept(tuning) => {
            let options =
                build_swept_compaction_options(tuning).compaction_metrics(compaction_metrics);
            builder = builder.with_compaction_options(options);
        }
    }

    let db = builder.open().await.map_err(BenchError::from)?;
    Ok(BenchmarkDb::Local(db))
}

pub(crate) async fn open_object_store_benchmark_db(
    schema: &SchemaRef,
    object_spec: &ObjectSpec,
    config: &ResolvedConfig,
    compaction_profile: &CompactionProfile,
    io_probe: &IoProbe,
) -> Result<BenchmarkDb, BenchError> {
    let (fs, root) = build_probed_object_store_fs(object_spec, io_probe)?;
    let compaction_metrics = Arc::new(CompactionMetrics::new());
    let mut builder = DbBuilder::from_schema_key_name(Arc::clone(schema), "id")?
        .object_store_with_fs(fs, root)?
        .wal_sync_policy(config.wal_sync_policy.clone())
        .with_minor_compaction(1, 0);

    match compaction_profile {
        CompactionProfile::Disabled => {}
        CompactionProfile::Default { periodic_tick_ms } => {
            let options = CompactionOptions::new()
                .periodic_tick(Duration::from_millis(*periodic_tick_ms))
                .compaction_metrics(Arc::clone(&compaction_metrics));
            builder = builder.with_compaction_options(options);
        }
        CompactionProfile::Swept(tuning) => {
            let options =
                build_swept_compaction_options(tuning).compaction_metrics(compaction_metrics);
            builder = builder.with_compaction_options(options);
        }
    }

    let db = builder.open().await.map_err(BenchError::from)?;
    Ok(BenchmarkDb::ObjectStore(db))
}

pub(crate) async fn snapshot_local_storage_volume(
    root: &Path,
) -> Result<StorageVolumeArtifact, BenchError> {
    let fs = LocalFs {};
    let absolute = absolutize(root)?;
    let root_path = FusioPath::from_filesystem_path(&absolute).map_err(|err| {
        BenchError::Message(format!(
            "failed to convert benchmark root `{}` to fusio path: {err}",
            absolute.display()
        ))
    })?;
    snapshot_storage_volume_with_fs(&fs, &root_path).await
}

pub(crate) async fn snapshot_object_store_storage_volume(
    object_spec: &ObjectSpec,
) -> Result<StorageVolumeArtifact, BenchError> {
    let probe = IoProbe::default();
    let (fs, root) = build_probed_object_store_fs(object_spec, &probe)?;
    snapshot_storage_volume_with_fs(fs.as_ref(), &root).await
}

pub(crate) async fn snapshot_storage_target_volume(
    target: &ScenarioStorageTarget,
) -> Result<StorageVolumeArtifact, BenchError> {
    match target {
        ScenarioStorageTarget::Local { root } => snapshot_local_storage_volume(root).await,
        ScenarioStorageTarget::ObjectStore { spec } => {
            snapshot_object_store_storage_volume(spec).await
        }
    }
}

pub(crate) async fn run_gc_observation(
    db: &BenchmarkDb,
    target: &ScenarioStorageTarget,
) -> Result<GcObservationArtifact, BenchError> {
    let volume_before_gc = snapshot_storage_target_volume(target).await?;
    let logical_before_sweep =
        LogicalVolumeArtifact::from_version(latest_version_summary(db).await?);
    let physical_stale_estimate_before_explicit_sweep =
        PhysicalStaleEstimateArtifact::from_volume(&volume_before_gc, &logical_before_sweep);
    let cumulative_before_explicit_sweep = match db {
        BenchmarkDb::Local(inner) => inner.compaction_metrics_snapshot(),
        BenchmarkDb::ObjectStore(inner) => inner.compaction_metrics_snapshot(),
    }
    .unwrap_or_else(empty_compaction_metrics_snapshot);
    let persisted_plan_before_explicit_sweep = match db {
        BenchmarkDb::Local(inner) => inner.inspect_sst_gc_plan().await,
        BenchmarkDb::ObjectStore(inner) => inner.inspect_sst_gc_plan().await,
    }
    .map_err(BenchError::from)?
    .map(GcPlanInspectionArtifact::from_inspection);
    let explicit_sweep_result = match db {
        BenchmarkDb::Local(inner) => inner.sweep_sst_objects().await,
        BenchmarkDb::ObjectStore(inner) => inner.sweep_sst_objects().await,
    }
    .map_err(BenchError::from)?;
    let volume_after_gc = snapshot_storage_target_volume(target).await?;
    let logical_after_sweep =
        LogicalVolumeArtifact::from_version(latest_version_summary(db).await?);
    let physical_stale_estimate_after_explicit_sweep =
        PhysicalStaleEstimateArtifact::from_volume(&volume_after_gc, &logical_after_sweep);
    let cumulative_after_explicit_sweep = match db {
        BenchmarkDb::Local(inner) => inner.compaction_metrics_snapshot(),
        BenchmarkDb::ObjectStore(inner) => inner.compaction_metrics_snapshot(),
    }
    .unwrap_or_else(empty_compaction_metrics_snapshot);
    let persisted_plan_after_explicit_sweep = match db {
        BenchmarkDb::Local(inner) => inner.inspect_sst_gc_plan().await,
        BenchmarkDb::ObjectStore(inner) => inner.inspect_sst_gc_plan().await,
    }
    .map_err(BenchError::from)?
    .map(GcPlanInspectionArtifact::from_inspection);

    Ok(GcObservationArtifact::from_parts(
        volume_before_gc,
        volume_after_gc,
        physical_stale_estimate_before_explicit_sweep,
        physical_stale_estimate_after_explicit_sweep,
        persisted_plan_before_explicit_sweep,
        explicit_sweep_result,
        cumulative_before_explicit_sweep,
        cumulative_after_explicit_sweep,
        persisted_plan_after_explicit_sweep,
    ))
}

fn build_probed_object_store_fs(
    object_spec: &ObjectSpec,
    io_probe: &IoProbe,
) -> Result<(Arc<ProbedFs<AmazonS3>>, FusioPath), BenchError> {
    let ObjectSpec::S3(spec) = object_spec;
    use fusio::impls::remotes::aws::{credential::AwsCredential, fs::AmazonS3Builder};

    let region = spec
        .region
        .clone()
        .unwrap_or_else(|| "us-east-1".to_string());
    let mut builder = AmazonS3Builder::new(spec.bucket.clone()).region(region);
    if let Some(endpoint) = &spec.endpoint {
        builder = builder.endpoint(endpoint.clone());
    }
    let credential = AwsCredential {
        key_id: spec.credentials.access_key.clone(),
        secret_key: spec.credentials.secret_key.clone(),
        token: spec.credentials.session_token.clone(),
    };
    builder = builder.credential(credential);
    if let Some(sign) = spec.sign_payload {
        builder = builder.sign_payload(sign);
    }
    if let Some(checksum) = spec.checksum {
        builder = builder.checksum(checksum);
    }
    let fs = Arc::new(ProbedFs::new(builder.build(), io_probe.clone()));
    let root = if spec.prefix.is_empty() {
        FusioPath::default()
    } else {
        FusioPath::parse(&spec.prefix).map_err(|err| {
            BenchError::Message(format!(
                "invalid object-store benchmark prefix `{}`: {err}",
                spec.prefix
            ))
        })?
    };
    Ok((fs, root))
}

fn build_swept_compaction_options(tuning: &CompactionTuning) -> CompactionOptions {
    let planner = LeveledPlannerConfig {
        l0_trigger: tuning.l0_trigger.max(1),
        l0_max_inputs: tuning.max_inputs.max(1),
        max_inputs_per_task: tuning.max_inputs.max(1),
        max_task_bytes: tuning.max_task_bytes,
        ..LeveledPlannerConfig::default()
    };
    CompactionOptions::new()
        .strategy(CompactionStrategy::Leveled(planner))
        .periodic_tick(Duration::from_millis(tuning.periodic_tick_ms.max(1)))
}

async fn snapshot_storage_volume_with_fs<FS: Fs>(
    fs: &FS,
    root: &FusioPath,
) -> Result<StorageVolumeArtifact, BenchError> {
    let mut pending = VecDeque::new();
    let mut visited = HashSet::new();
    pending.push_back(root.clone());
    let mut volume = StorageVolumeArtifact::default();
    while let Some(dir) = pending.pop_front() {
        if !visited.insert(dir.to_string()) {
            continue;
        }
        let stream = fs.list(&dir).await.map_err(|err| {
            BenchError::Message(format!(
                "failed to list storage under `{}`: {err}",
                dir.as_ref()
            ))
        })?;
        futures::pin_mut!(stream);
        while let Some(meta_result) = stream.next().await {
            let meta = meta_result.map_err(|err| {
                BenchError::Message(format!(
                    "failed to read storage metadata under `{}`: {err}",
                    dir.as_ref()
                ))
            })?;
            let path = meta.path.as_ref();
            if !is_probably_file_path(path) {
                pending.push_back(meta.path);
                continue;
            }
            let size = meta.size;
            volume.object_count = volume.object_count.saturating_add(1);
            volume.total_bytes = volume.total_bytes.saturating_add(size);
            if is_sst_path(&meta.path) {
                volume.sst_object_count = volume.sst_object_count.saturating_add(1);
                volume.sst_bytes = volume.sst_bytes.saturating_add(size);
            } else if is_wal_path(path) {
                volume.wal_object_count = volume.wal_object_count.saturating_add(1);
                volume.wal_bytes = volume.wal_bytes.saturating_add(size);
            } else if is_manifest_path(path) {
                volume.manifest_object_count = volume.manifest_object_count.saturating_add(1);
                volume.manifest_bytes = volume.manifest_bytes.saturating_add(size);
            } else {
                volume.other_object_count = volume.other_object_count.saturating_add(1);
                volume.other_bytes = volume.other_bytes.saturating_add(size);
            }
        }
    }
    Ok(volume)
}

fn is_probably_file_path(path: &str) -> bool {
    let Some(name) = path.rsplit('/').next() else {
        return false;
    };
    name.contains('.')
}

fn is_wal_path(path: &str) -> bool {
    path.contains("/wal/") || path.starts_with("wal/")
}

fn is_manifest_path(path: &str) -> bool {
    path.contains("/manifest/") || path.starts_with("manifest/")
}

pub(crate) async fn ingest_workload(
    db: &BenchmarkDb,
    schema: &SchemaRef,
    config: &ResolvedConfig,
) -> Result<(), BenchError> {
    for batch_idx in 0..config.ingest_batches {
        let batch = build_batch(
            schema,
            config.rows_per_batch,
            config.key_space,
            config.seed,
            batch_idx,
        )?;
        match db {
            BenchmarkDb::Local(inner) => inner.ingest(batch).await?,
            BenchmarkDb::ObjectStore(inner) => inner.ingest(batch).await?,
        }
    }
    Ok(())
}

fn build_batch(
    schema: &SchemaRef,
    rows_per_batch: usize,
    key_space: usize,
    seed: u64,
    batch_idx: usize,
) -> Result<RecordBatch, BenchError> {
    let mut ids = Vec::with_capacity(rows_per_batch);
    let mut values = Vec::with_capacity(rows_per_batch);

    for row_idx in 0..rows_per_batch {
        let global_idx = batch_idx
            .saturating_mul(rows_per_batch)
            .saturating_add(row_idx);
        let key_idx = deterministic_key_slot(global_idx, key_space, seed);
        ids.push(format!("k{key_idx:08}"));
        values.push(deterministic_value(global_idx, seed));
    }

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .map_err(BenchError::from)
}

fn deterministic_key_slot(global_idx: usize, key_space: usize, seed: u64) -> usize {
    if key_space <= 1 {
        return 0;
    }
    let mixed = (global_idx as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(seed);
    (mixed % key_space as u64) as usize
}

fn deterministic_value(global_idx: usize, seed: u64) -> i64 {
    let mixed = (global_idx as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(seed ^ 0x9E37_79B9_7F4A_7C15);
    (mixed >> 1) as i64
}

pub(crate) async fn latest_version_summary_if_any(
    db: &BenchmarkDb,
) -> Result<Option<VersionSummary>, BenchError> {
    let versions = match db {
        BenchmarkDb::Local(inner) => inner.list_versions(1).await,
        BenchmarkDb::ObjectStore(inner) => inner.list_versions(1).await,
    }
    .map_err(|err| BenchError::Message(format!("list_versions failed: {err}")))?;
    Ok(versions.into_iter().next().map(|version| VersionSummary {
        sst_count: version.sst_count,
        sst_bytes: version.sst_bytes,
        level_count: version.level_count,
    }))
}

pub(crate) async fn latest_version_summary(db: &BenchmarkDb) -> Result<VersionSummary, BenchError> {
    let Some(version) = latest_version_summary_if_any(db).await? else {
        return Err(BenchError::Message(
            "list_versions returned no versions".to_string(),
        ));
    };
    Ok(version)
}

pub(crate) async fn wait_for_first_compaction_observed(
    db: &BenchmarkDb,
    before: VersionSummary,
    config: &ResolvedConfig,
) -> Result<(), BenchError> {
    let timeout = Duration::from_millis(config.compaction_wait_timeout_ms);
    let poll = Duration::from_millis(config.compaction_poll_interval_ms);
    let started = Instant::now();

    loop {
        let current = latest_version_summary(db).await?;
        if current.sst_count < before.sst_count || current.level_count > before.level_count {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(BenchError::Message(format!(
                "timed out waiting for compaction: before(ssts={}, levels={}) current(ssts={}, \
                 levels={}) timeout_ms={}",
                before.sst_count,
                before.level_count,
                current.sst_count,
                current.level_count,
                config.compaction_wait_timeout_ms
            )));
        }
        sleep(poll).await;
    }
}

pub(crate) async fn wait_for_compaction_quiesced(
    db: &BenchmarkDb,
    before: VersionSummary,
    config: &ResolvedConfig,
) -> Result<(), BenchError> {
    let timeout = Duration::from_millis(config.compaction_wait_timeout_ms);
    let poll = Duration::from_millis(config.compaction_poll_interval_ms);
    let started = Instant::now();
    let mut observed_compaction = false;
    let mut last_after_observed = before;
    let mut stable_polls = 0usize;

    loop {
        let current = latest_version_summary(db).await?;
        let changed =
            current.sst_count < before.sst_count || current.level_count > before.level_count;

        if !observed_compaction {
            if changed {
                observed_compaction = true;
                last_after_observed = current;
                stable_polls = 0;
            }
        } else if current.sst_count == last_after_observed.sst_count
            && current.level_count == last_after_observed.level_count
        {
            stable_polls = stable_polls.saturating_add(1);
            if stable_polls >= COMPACTION_QUIESCED_STABLE_POLLS {
                return Ok(());
            }
        } else {
            stable_polls = 0;
            last_after_observed = current;
        }

        if started.elapsed() >= timeout {
            return Err(BenchError::Message(format!(
                "timed out waiting for compaction quiescence: before(ssts={}, levels={}) \
                 current(ssts={}, levels={}) observed={} stable_polls={} timeout_ms={}",
                before.sst_count,
                before.level_count,
                current.sst_count,
                current.level_count,
                observed_compaction,
                stable_polls,
                config.compaction_wait_timeout_ms
            )));
        }
        sleep(poll).await;
    }
}

pub(crate) async fn read_all_rows(
    db: &BenchmarkDb,
) -> Result<(usize, ReadPathBreakdown), BenchError> {
    match db {
        BenchmarkDb::Local(inner) => read_all_rows_local(inner).await,
        BenchmarkDb::ObjectStore(inner) => read_all_rows_object_store(inner).await,
    }
}

async fn read_all_rows_local(
    db: &DB<ProbedFs<LocalFs>, TokioExecutor>,
) -> Result<(usize, ReadPathBreakdown), BenchError> {
    let plan_started = Instant::now();
    let (mut stream, profile) = db
        .scan()
        .stream_with_profile()
        .await
        .map_err(|err| BenchError::Message(format!("scan stream failed: {err}")))?;
    let prepare_ns = duration_ns_u64(plan_started.elapsed());

    let consume_started = Instant::now();
    let mut rows = 0usize;
    let mut batch_count = 0usize;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|err| BenchError::Message(format!("scan stream failed: {err}")))?;
        rows = rows.saturating_add(batch.num_rows());
        batch_count = batch_count.saturating_add(1);
    }
    let consume_ns = duration_ns_u64(consume_started.elapsed());
    Ok((
        rows,
        ReadPathBreakdown {
            prepare_ns,
            consume_ns,
            batch_count,
            internal: internal_breakdown(profile),
        },
    ))
}

async fn read_all_rows_object_store(
    db: &DB<ProbedFs<AmazonS3>, TokioExecutor>,
) -> Result<(usize, ReadPathBreakdown), BenchError> {
    let plan_started = Instant::now();
    let (mut stream, profile) = db
        .scan()
        .stream_with_profile()
        .await
        .map_err(|err| BenchError::Message(format!("scan stream failed: {err}")))?;
    let prepare_ns = duration_ns_u64(plan_started.elapsed());

    let consume_started = Instant::now();
    let mut rows = 0usize;
    let mut batch_count = 0usize;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|err| BenchError::Message(format!("scan stream failed: {err}")))?;
        rows = rows.saturating_add(batch.num_rows());
        batch_count = batch_count.saturating_add(1);
    }
    let consume_ns = duration_ns_u64(consume_started.elapsed());
    Ok((
        rows,
        ReadPathBreakdown {
            prepare_ns,
            consume_ns,
            batch_count,
            internal: internal_breakdown(profile),
        },
    ))
}

fn internal_breakdown(profile: ScanSetupProfile) -> ReadPathInternalBreakdown {
    ReadPathInternalBreakdown {
        snapshot_ns: profile.snapshot_ns(),
        plan_scan_ns: profile.plan_scan_ns(),
        build_scan_streams_ns: profile.build_scan_streams_ns(),
        merge_init_ns: profile.merge_init_ns(),
        package_init_ns: profile.package_init_ns(),
    }
}

fn measure_for_artifact(
    runtime: &Runtime,
    scenarios: &[ScenarioState],
    iterations: usize,
) -> Result<Vec<ScenarioArtifact>, BenchError> {
    let mut artifacts = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let measurement = measure_scenario(runtime, scenario, iterations)?;
        artifacts.push(to_scenario_artifact(scenario, measurement));
    }
    Ok(artifacts)
}

fn measure_scenario(
    runtime: &Runtime,
    scenario: &ScenarioState,
    iterations: usize,
) -> Result<ScenarioMeasurement, BenchError> {
    scenario.io_probe.reset();
    runtime.block_on(async {
        let mut latencies_ns = Vec::with_capacity(iterations);
        let mut rows_processed = 0u64;
        let mut read_path = ReadPathAggregate::default();
        let mut physical_stale_estimate = None;
        let started = Instant::now();
        for _ in 0..iterations {
            let op_started = Instant::now();
            let result = run_scenario_operation(scenario).await?;
            rows_processed = rows_processed.saturating_add(usize_to_u64(result.rows));
            if let Some(breakdown) = result.read_path {
                read_path.record(breakdown);
            }
            if let Some(estimate) = result.physical_stale_estimate {
                physical_stale_estimate = Some(estimate);
            }
            black_box(result.rows);
            latencies_ns.push(duration_ns_u64(op_started.elapsed()));
        }
        Ok(ScenarioMeasurement {
            iterations,
            total_elapsed_ns: duration_ns_u64(started.elapsed()),
            latencies_ns,
            rows_processed,
            read_path: if read_path.samples > 0 {
                Some(read_path)
            } else {
                None
            },
            physical_stale_estimate,
            io: scenario.io_probe.snapshot(),
        })
    })
}

fn to_scenario_artifact(
    scenario: &ScenarioState,
    measurement: ScenarioMeasurement,
) -> ScenarioArtifact {
    let read_path_latency = measurement
        .read_path
        .as_ref()
        .and_then(ReadPathAggregate::to_summary);
    let read_path_internal = measurement
        .read_path
        .as_ref()
        .and_then(ReadPathAggregate::to_internal_summary);
    let latency = latency_summary(&measurement.latencies_ns);
    let total_rows = measurement.rows_processed as f64;
    let total_elapsed_secs = if measurement.total_elapsed_ns == 0 {
        0.0
    } else {
        measurement.total_elapsed_ns as f64 / 1_000_000_000.0
    };
    let throughput = if total_elapsed_secs > 0.0 {
        ThroughputSummary {
            ops_per_sec: measurement.iterations as f64 / total_elapsed_secs,
            rows_per_sec: total_rows / total_elapsed_secs,
        }
    } else {
        ThroughputSummary {
            ops_per_sec: 0.0,
            rows_per_sec: 0.0,
        }
    };
    let total_elapsed_ms = measurement.total_elapsed_ns as f64 / 1_000_000.0;
    let gc = scenario.gc_observation.as_ref().map(|gc| {
        GcDerivedArtifact::from_gc_observation(
            gc,
            scenario.setup_elapsed_ns,
            measurement.total_elapsed_ns,
        )
    });

    let logical_before_compaction =
        LogicalVolumeArtifact::from_version(scenario.version_before_compaction);
    let logical_ready = LogicalVolumeArtifact::from_version(scenario.version_ready);

    ScenarioArtifact {
        scenario_id: scenario.scenario_id.to_string(),
        scenario_name: scenario.scenario_name.clone(),
        scenario_variant_id: scenario.scenario_variant_id.clone(),
        dimensions: scenario.dimensions.clone(),
        setup: ScenarioSetupArtifact {
            total_elapsed_ns: scenario.setup_elapsed_ns,
            total_elapsed_ms: scenario.setup_elapsed_ns as f64 / 1_000_000.0,
            rows_per_scan: scenario.rows_per_op_hint,
            sst_count_before_compaction: scenario.version_before_compaction.sst_count,
            level_count_before_compaction: scenario.version_before_compaction.level_count,
            sst_count_ready: scenario.version_ready.sst_count,
            level_count_ready: scenario.version_ready.level_count,
            logical_before_compaction: logical_before_compaction.clone(),
            logical_ready: logical_ready.clone(),
            volume_before_compaction: scenario.volume_before_compaction.clone(),
            volume_ready: scenario.volume_ready.clone(),
            physical_stale_estimate_before_compaction: PhysicalStaleEstimateArtifact::from_volume(
                &scenario.volume_before_compaction,
                &logical_before_compaction,
            ),
            physical_stale_estimate_ready: PhysicalStaleEstimateArtifact::from_volume(
                &scenario.volume_ready,
                &logical_ready,
            ),
            gc_observation: scenario.gc_observation.clone(),
            io: scenario.setup_io.clone(),
        },
        summary: ScenarioSummaryArtifact {
            iterations: measurement.iterations,
            total_elapsed_ns: measurement.total_elapsed_ns,
            total_elapsed_ms,
            rows_processed: measurement.rows_processed,
            throughput,
            latency_ns: latency,
            read_path_latency_ns: read_path_latency,
            read_path_internal_ns: read_path_internal,
            physical_stale_estimate: measurement.physical_stale_estimate,
            gc,
            io: measurement.io,
        },
    }
}

fn latency_summary(samples: &[u64]) -> LatencySummary {
    if samples.is_empty() {
        return LatencySummary {
            min: 0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
            mean: 0.0,
        };
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum = sorted
        .iter()
        .fold(0u128, |acc, value| acc.saturating_add(u128::from(*value)));
    let mean = sum as f64 / sorted.len() as f64;

    LatencySummary {
        min: sorted.first().copied().unwrap_or(0),
        p50: percentile(&sorted, 50),
        p95: percentile(&sorted, 95),
        p99: percentile(&sorted, 99),
        max: sorted.last().copied().unwrap_or(0),
        mean,
    }
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let capped_pct = pct.min(100);
    let idx = (sorted.len() - 1).saturating_mul(capped_pct) / 100;
    sorted.get(idx).copied().unwrap_or(0)
}

pub(crate) async fn write_artifact_json(
    path: &Path,
    artifact: &BenchmarkArtifact,
) -> Result<(), BenchError> {
    let bytes = serde_json::to_vec_pretty(artifact)
        .map_err(|err| BenchError::Message(format!("failed to encode artifact json: {err}")))?;
    let absolute = absolutize(path)?;
    let Some(parent) = absolute.parent() else {
        return Err(BenchError::Message(format!(
            "artifact path has no parent directory: {}",
            absolute.display()
        )));
    };
    let file_name = absolute
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            BenchError::Message(format!(
                "artifact path has invalid filename: {}",
                absolute.display()
            ))
        })?;

    let fs = LocalFs {};
    let parent_path = FusioPath::from_filesystem_path(parent).map_err(|err| {
        BenchError::Message(format!(
            "failed to convert artifact directory `{}` to fusio path: {err}",
            parent.display()
        ))
    })?;
    LocalFs::create_dir_all(&parent_path).await.map_err(|err| {
        BenchError::Message(format!(
            "failed to create artifact directory `{}`: {err}",
            parent.display()
        ))
    })?;

    let file_part = PathPart::parse(file_name).map_err(|err| {
        BenchError::Message(format!("invalid artifact filename `{file_name}`: {err}"))
    })?;
    let file_path = parent_path.child(file_part);
    let mut file = fs
        .open_options(
            &file_path,
            OpenOptions::default()
                .create(true)
                .truncate(true)
                .write(true)
                .read(false),
        )
        .await
        .map_err(|err| {
            BenchError::Message(format!("open artifact file `{file_path}` failed: {err}"))
        })?;
    let (write_result, _) = file.write_all(bytes).await;
    write_result.map_err(|err| {
        BenchError::Message(format!("write artifact file `{file_path}` failed: {err}"))
    })?;
    file.close().await.map_err(|err| {
        BenchError::Message(format!("close artifact file `{file_path}` failed: {err}"))
    })?;
    Ok(())
}

fn absolutize(path: &Path) -> Result<PathBuf, BenchError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = env::current_dir().map_err(|err| {
        BenchError::Message(format!("failed to resolve current directory: {err}"))
    })?;
    Ok(cwd.join(path))
}

fn env_usize(name: &'static str, default: usize) -> Result<usize, BenchError> {
    match env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .map_err(|_| BenchError::InvalidEnv { name, value: raw }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn env_u64(name: &'static str, default: u64) -> Result<u64, BenchError> {
    match env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .map_err(|_| BenchError::InvalidEnv { name, value: raw }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn env_usize_list(name: &'static str, default: &[usize]) -> Result<Vec<usize>, BenchError> {
    match env::var(name) {
        Ok(raw) => {
            let mut values = Vec::new();
            for token in raw.split(',') {
                let trimmed = token.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value = trimmed
                    .parse::<usize>()
                    .map_err(|_| BenchError::InvalidEnv {
                        name,
                        value: raw.clone(),
                    })?;
                if value == 0 {
                    return Err(BenchError::InvalidEnv { name, value: raw });
                }
                if !values.contains(&value) {
                    values.push(value);
                }
            }
            if values.is_empty() {
                return Err(BenchError::InvalidEnv { name, value: raw });
            }
            Ok(values)
        }
        Err(env::VarError::NotPresent) => Ok(default.to_vec()),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn env_u64_list(name: &'static str, default: &[u64]) -> Result<Vec<u64>, BenchError> {
    match env::var(name) {
        Ok(raw) => {
            let mut values = Vec::new();
            for token in raw.split(',') {
                let trimmed = token.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value = trimmed.parse::<u64>().map_err(|_| BenchError::InvalidEnv {
                    name,
                    value: raw.clone(),
                })?;
                if value == 0 {
                    return Err(BenchError::InvalidEnv { name, value: raw });
                }
                if !values.contains(&value) {
                    values.push(value);
                }
            }
            if values.is_empty() {
                return Err(BenchError::InvalidEnv { name, value: raw });
            }
            Ok(values)
        }
        Err(env::VarError::NotPresent) => Ok(default.to_vec()),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn env_optional_usize_list(
    name: &'static str,
    default: &[Option<usize>],
) -> Result<Vec<Option<usize>>, BenchError> {
    match env::var(name) {
        Ok(raw) => {
            let mut values = Vec::new();
            for token in raw.split(',') {
                let trimmed = token.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let parsed = if matches_ignore_ascii_case(trimmed, &["none", "off", "null"])
                    || trimmed == "0"
                {
                    None
                } else {
                    let value = trimmed
                        .parse::<usize>()
                        .map_err(|_| BenchError::InvalidEnv {
                            name,
                            value: raw.clone(),
                        })?;
                    if value == 0 {
                        return Err(BenchError::InvalidEnv { name, value: raw });
                    }
                    Some(value)
                };
                if !values.contains(&parsed) {
                    values.push(parsed);
                }
            }
            if values.is_empty() {
                return Err(BenchError::InvalidEnv { name, value: raw });
            }
            Ok(values)
        }
        Err(env::VarError::NotPresent) => Ok(default.to_vec()),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn env_bool(name: &'static str, default: bool) -> Result<bool, BenchError> {
    match env::var(name) {
        Ok(raw) => {
            parse_bool(raw.trim()).ok_or_else(|| BenchError::InvalidEnv { name, value: raw })
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn env_backend(name: &'static str, default: BenchBackend) -> Result<BenchBackend, BenchError> {
    match env::var(name) {
        Ok(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "local" => Ok(BenchBackend::Local),
                "object_store" => Ok(BenchBackend::ObjectStore),
                _ => Err(BenchError::InvalidEnv { name, value: raw }),
            }
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    if matches_ignore_ascii_case(raw, &["1", "true", "yes", "on"]) {
        return Some(true);
    }
    if matches_ignore_ascii_case(raw, &["0", "false", "no", "off"]) {
        return Some(false);
    }
    None
}

fn matches_ignore_ascii_case(input: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| input.eq_ignore_ascii_case(candidate))
}

fn env_wal_sync_policy() -> Result<(WalSyncPolicy, String), BenchError> {
    let name = "TONBO_COMPACTION_BENCH_WAL_SYNC";
    let raw = match env::var(name) {
        Ok(raw) => raw,
        Err(env::VarError::NotPresent) => "disabled".to_string(),
        Err(err) => {
            return Err(BenchError::Message(format!(
                "failed reading environment variable `{name}`: {err}"
            )));
        }
    };
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "always" => Ok((WalSyncPolicy::Always, normalized)),
        "disabled" => Ok((WalSyncPolicy::Disabled, normalized)),
        _ => Err(BenchError::InvalidEnv { name, value: raw }),
    }
}

fn pct_drop(baseline: u64, current: u64) -> Option<f64> {
    if baseline == 0 {
        return None;
    }
    Some(((baseline as f64 - current as f64) / baseline as f64) * 100.0)
}

fn pct_drop_float(baseline: f64, current: f64) -> Option<f64> {
    if baseline <= f64::EPSILON {
        return None;
    }
    Some(((baseline - current) / baseline) * 100.0)
}

fn fmt_pct_opt(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{v:+.2}%"),
        None => "n/a".to_string(),
    }
}

fn fmt_ratio_opt(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("{value:.2}"),
        None => "n/a".to_string(),
    }
}

fn build_interpretation(
    backend: BenchBackend,
    read_ops_drop: Option<f64>,
    bytes_drop: Option<f64>,
    mean_improve: Option<f64>,
    p99_improve: Option<f64>,
) -> String {
    let io_drop_large = read_ops_drop.is_some_and(|value| value > 50.0)
        || bytes_drop.is_some_and(|value| value > 50.0);
    let mean_small = mean_improve.is_some_and(|value| value < 10.0);
    let p99_small = p99_improve.is_some_and(|value| value < 10.0);
    let tail_benefit = match (p99_improve, mean_improve) {
        (Some(p99), Some(mean)) => p99 > mean + 10.0,
        (Some(_), None) => true,
        _ => false,
    };
    let remote_latency_delta_large = backend == BenchBackend::ObjectStore
        && (mean_improve.is_some_and(|value| value > 20.0)
            || p99_improve.is_some_and(|value| value > 20.0));

    if io_drop_large && mean_small && p99_small {
        return "latency is not strongly correlated with reduced I/O at this setting; workload \
                may still be CPU/cache-bound."
            .to_string();
    }
    if remote_latency_delta_large {
        return "object-store run shows a larger latency delta, suggesting stronger remote I/O \
                sensitivity in this profile."
            .to_string();
    }
    if tail_benefit {
        return "p99 improves notably more than mean, indicating compaction benefits are \
                concentrated in tail latency."
            .to_string();
    }
    "signals are mixed; gather larger-scale and cross-backend runs before concluding CPU-bound vs \
     I/O-bound."
        .to_string()
}

fn duration_ns_u64(duration: Duration) -> u64 {
    let nanos = duration.as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

fn ratio_per_ms(value: u64, duration_ms: u64) -> Option<f64> {
    if duration_ms == 0 {
        return None;
    }
    Some(value as f64 / duration_ms as f64)
}

fn pct_of_elapsed_ms(duration_ms: u64, elapsed_ns: u64) -> Option<f64> {
    if elapsed_ns == 0 {
        return None;
    }
    Some((duration_ms as f64 / (elapsed_ns as f64 / 1_000_000.0)) * 100.0)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn unix_epoch_ms() -> u64 {
    let duration = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration,
        Err(_) => Duration::from_secs(0),
    };
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    #[test]
    fn physical_stale_estimate_is_physical_minus_logical_delta() {
        let physical = super::StorageVolumeArtifact {
            sst_object_count: 7,
            sst_bytes: 700,
            ..super::StorageVolumeArtifact::default()
        };
        let logical = super::LogicalVolumeArtifact {
            sst_count: 3,
            sst_bytes: 250,
            wal_bytes: None,
            manifest_bytes: None,
            total_bytes: 250,
        };

        let estimate = super::PhysicalStaleEstimateArtifact::from_volume(&physical, &logical);

        assert_eq!(
            estimate,
            super::PhysicalStaleEstimateArtifact {
                candidate_sst_objects: 4,
                candidate_sst_bytes: 450,
                live_sst_objects: 3,
                live_sst_bytes: 250,
                physical_sst_objects: 7,
                physical_sst_bytes: 700,
                stale_sst_byte_amplification_pct: 180.0,
            }
        );
    }

    #[test]
    fn interpretation_calls_out_empty_persisted_plan_vs_physical_estimate() {
        let gc = super::GcObservationArtifact {
            volume_before_gc: super::StorageVolumeArtifact::default(),
            volume_after_gc: super::StorageVolumeArtifact::default(),
            physical_stale_estimate_before_explicit_sweep: super::PhysicalStaleEstimateArtifact {
                candidate_sst_objects: 6,
                candidate_sst_bytes: 600,
                live_sst_objects: 3,
                live_sst_bytes: 300,
                physical_sst_objects: 9,
                physical_sst_bytes: 900,
                stale_sst_byte_amplification_pct: 200.0,
            },
            physical_stale_estimate_after_explicit_sweep: super::PhysicalStaleEstimateArtifact {
                candidate_sst_objects: 6,
                candidate_sst_bytes: 600,
                live_sst_objects: 3,
                live_sst_bytes: 300,
                physical_sst_objects: 9,
                physical_sst_bytes: 900,
                stale_sst_byte_amplification_pct: 200.0,
            },
            reclaimed_sst_objects: 0,
            reclaimed_sst_bytes: 0,
            persisted_plan_before_explicit_sweep: None,
            explicit_sweep_result: super::GcSweepArtifact {
                deleted_objects: 0,
                deleted_bytes: 0,
                delete_failures: 0,
                duration_ms: 1,
            },
            cumulative_before_explicit_sweep: super::GcCountersArtifact {
                sweep_runs: 1,
                deleted_objects: 0,
                deleted_bytes: 0,
                delete_failures: 0,
                sweep_duration_ms_total: 1,
                gc_plan_write_runs: 1,
                gc_plan_overwrite_non_empty: 0,
                gc_plan_previous_sst_candidates: 0,
                gc_plan_written_sst_candidates: 6,
                gc_plan_take_runs: 1,
                gc_plan_taken_sst_candidates: 6,
                gc_plan_authorized_sst_candidates: 6,
                gc_plan_blocked_sst_candidates: 0,
                gc_plan_requeued_sst_candidates: 0,
            },
            cumulative_after_explicit_sweep: super::GcCountersArtifact {
                sweep_runs: 2,
                deleted_objects: 0,
                deleted_bytes: 0,
                delete_failures: 0,
                sweep_duration_ms_total: 2,
                gc_plan_write_runs: 1,
                gc_plan_overwrite_non_empty: 0,
                gc_plan_previous_sst_candidates: 0,
                gc_plan_written_sst_candidates: 6,
                gc_plan_take_runs: 2,
                gc_plan_taken_sst_candidates: 6,
                gc_plan_authorized_sst_candidates: 6,
                gc_plan_blocked_sst_candidates: 0,
                gc_plan_requeued_sst_candidates: 0,
            },
            persisted_plan_after_explicit_sweep: None,
        };

        let interpretation =
            super::explain_gc_observation(&gc).expect("interpretation should exist");

        assert!(interpretation.contains("physical estimate is non-zero"));
        assert!(interpretation.contains("persisted plan is empty"));
        assert!(interpretation.contains("earlier compaction-triggered sweeps"));
    }

    #[test]
    fn gc_derived_artifact_reports_timing_shares_and_reclaim_rates() {
        let assert_close = |actual: Option<f64>, expected: f64| {
            let actual = actual.expect("value should be present");
            assert!((actual - expected).abs() < 0.000_001);
        };
        let gc = super::GcObservationArtifact {
            volume_before_gc: super::StorageVolumeArtifact {
                sst_object_count: 16,
                sst_bytes: 1_600,
                ..super::StorageVolumeArtifact::default()
            },
            volume_after_gc: super::StorageVolumeArtifact {
                sst_object_count: 10,
                sst_bytes: 1_000,
                ..super::StorageVolumeArtifact::default()
            },
            physical_stale_estimate_before_explicit_sweep: super::PhysicalStaleEstimateArtifact {
                candidate_sst_objects: 6,
                candidate_sst_bytes: 600,
                live_sst_objects: 10,
                live_sst_bytes: 1_000,
                physical_sst_objects: 16,
                physical_sst_bytes: 1_600,
                stale_sst_byte_amplification_pct: 60.0,
            },
            physical_stale_estimate_after_explicit_sweep: super::PhysicalStaleEstimateArtifact {
                candidate_sst_objects: 0,
                candidate_sst_bytes: 0,
                live_sst_objects: 10,
                live_sst_bytes: 1_000,
                physical_sst_objects: 10,
                physical_sst_bytes: 1_000,
                stale_sst_byte_amplification_pct: 0.0,
            },
            reclaimed_sst_objects: 6,
            reclaimed_sst_bytes: 600,
            persisted_plan_before_explicit_sweep: None,
            explicit_sweep_result: super::GcSweepArtifact {
                deleted_objects: 1,
                deleted_bytes: 100,
                delete_failures: 0,
                duration_ms: 2,
            },
            cumulative_before_explicit_sweep: super::GcCountersArtifact {
                sweep_runs: 3,
                deleted_objects: 5,
                deleted_bytes: 500,
                delete_failures: 0,
                sweep_duration_ms_total: 8,
                gc_plan_write_runs: 1,
                gc_plan_overwrite_non_empty: 0,
                gc_plan_previous_sst_candidates: 0,
                gc_plan_written_sst_candidates: 6,
                gc_plan_take_runs: 1,
                gc_plan_taken_sst_candidates: 6,
                gc_plan_authorized_sst_candidates: 6,
                gc_plan_blocked_sst_candidates: 0,
                gc_plan_requeued_sst_candidates: 0,
            },
            cumulative_after_explicit_sweep: super::GcCountersArtifact {
                sweep_runs: 4,
                deleted_objects: 6,
                deleted_bytes: 600,
                delete_failures: 0,
                sweep_duration_ms_total: 10,
                gc_plan_write_runs: 1,
                gc_plan_overwrite_non_empty: 0,
                gc_plan_previous_sst_candidates: 0,
                gc_plan_written_sst_candidates: 6,
                gc_plan_take_runs: 2,
                gc_plan_taken_sst_candidates: 6,
                gc_plan_authorized_sst_candidates: 6,
                gc_plan_blocked_sst_candidates: 0,
                gc_plan_requeued_sst_candidates: 0,
            },
            persisted_plan_after_explicit_sweep: None,
        };

        let derived = super::GcDerivedArtifact::from_gc_observation(&gc, 40_000_000, 200_000_000);

        assert_eq!(derived.sst_sweep_runs, 4);
        assert_eq!(derived.sst_sweep_duration_ms_total, 10);
        assert_eq!(derived.sst_deleted_objects_total, 6);
        assert_eq!(derived.sst_deleted_bytes_total, 600);
        assert_eq!(derived.sst_sweep_runs_before_explicit_sweep, 3);
        assert_eq!(derived.sst_sweep_duration_ms_before_explicit_sweep, 8);
        assert_eq!(derived.sst_deleted_objects_before_explicit_sweep, 5);
        assert_eq!(derived.sst_deleted_bytes_before_explicit_sweep, 500);
        assert_eq!(derived.explicit_sweep_runs, 1);
        assert_eq!(derived.explicit_sweep_duration_ms, 2);
        assert_eq!(derived.explicit_sweep_deleted_objects, 1);
        assert_eq!(derived.explicit_sweep_deleted_bytes, 100);
        assert_close(derived.sst_deleted_bytes_per_ms, 60.0);
        assert_close(derived.sst_deleted_objects_per_ms, 0.6);
        assert_close(derived.gc_time_share_pct_of_setup, 25.0);
        assert_close(derived.gc_time_share_pct_of_total_elapsed, 5.0);
    }
}
