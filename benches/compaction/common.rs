use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
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
    AwsCreds, CompactionOptions, CompactionStrategy, DB, DBError, DbBuildError, DbBuilder, Expr,
    LeveledPlannerConfig, ObjectSpec, S3Spec, ScalarValue, ScanSetupProfile,
    Snapshot as DbSnapshot, Version as DbVersion, WalSyncPolicy,
};

pub(crate) const BENCH_ID: &str = "compaction_local";
pub(crate) const BENCH_SCHEMA_VERSION: u32 = 11;

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
const SWMR_APPEND_SHARE_PCT: u64 = 50;
const SWMR_OVERWRITE_SHARE_PCT: u64 = 35;
const SWMR_FINGERPRINT_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const SWMR_FINGERPRINT_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone)]
pub(crate) enum BenchmarkDb {
    Local(DB<ProbedFs<LocalFs>, TokioExecutor>),
    ObjectStore(DB<AmazonS3, TokioExecutor>),
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
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    sst_paths: Mutex<HashSet<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IoCountersArtifact {
    read_ops: u64,
    write_ops: u64,
    bytes_read: u64,
    bytes_written: u64,
    ssts_touched: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct StorageVolumeArtifact {
    object_count: u64,
    total_bytes: u64,
    sst_bytes: u64,
    wal_bytes: u64,
    manifest_bytes: u64,
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

impl IoProbe {
    pub(crate) fn snapshot(&self) -> IoCountersArtifact {
        let ssts_touched = match self.inner.sst_paths.lock() {
            Ok(paths) => paths.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };
        IoCountersArtifact {
            read_ops: self.inner.read_ops.load(Ordering::Relaxed),
            write_ops: self.inner.write_ops.load(Ordering::Relaxed),
            bytes_read: self.inner.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.inner.bytes_written.load(Ordering::Relaxed),
            ssts_touched,
        }
    }

    pub(crate) fn reset(&self) {
        self.inner.read_ops.store(0, Ordering::Relaxed);
        self.inner.write_ops.store(0, Ordering::Relaxed);
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
        self.record_sst(path);
    }

    fn record_write(&self, path: &FusioPath, bytes: u64) {
        saturating_add(&self.inner.write_ops, 1);
        saturating_add(&self.inner.bytes_written, bytes);
        self.record_sst(path);
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
        self.inner.list(path).await
    }

    async fn remove(&self, path: &FusioPath) -> Result<(), FusioError> {
        self.inner.remove(path).await
    }

    async fn copy(&self, from: &FusioPath, to: &FusioPath) -> Result<(), FusioError> {
        self.inner.copy(from, to).await
    }

    async fn link(&self, from: &FusioPath, to: &FusioPath) -> Result<(), FusioError> {
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
    SwmrMixed,
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

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SwmrReaderClass {
    HeadLight,
    HeadHeavy,
    PinnedLight,
    PinnedHeavy,
}

impl SwmrReaderClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::HeadLight => "head_light",
            Self::HeadHeavy => "head_heavy",
            Self::PinnedLight => "pinned_light",
            Self::PinnedHeavy => "pinned_heavy",
        }
    }

    fn is_pinned(self) -> bool {
        matches!(self, Self::PinnedLight | Self::PinnedHeavy)
    }

    fn is_light(self) -> bool {
        matches!(self, Self::HeadLight | Self::PinnedLight)
    }

    pub(crate) fn key_band(self) -> SwmrReaderKeyBand {
        match self {
            Self::HeadLight | Self::PinnedLight => SwmrReaderKeyBand::Hot,
            Self::HeadHeavy | Self::PinnedHeavy => SwmrReaderKeyBand::Warm,
        }
    }

    pub(crate) fn validation_model(self) -> SwmrReaderValidationModel {
        match self {
            Self::HeadLight => SwmrReaderValidationModel::CountAndKeyBand,
            Self::HeadHeavy | Self::PinnedLight | Self::PinnedHeavy => {
                SwmrReaderValidationModel::ExactShapeStable
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SwmrReaderKeyBand {
    Hot,
    Warm,
}

impl SwmrReaderKeyBand {
    fn prefix(self) -> &'static str {
        match self {
            Self::Hot => "hot-",
            Self::Warm => "warm-",
        }
    }

    fn contains_key(self, key: &str) -> bool {
        key.starts_with(self.prefix())
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SwmrReaderValidationModel {
    ExactShapeStable,
    CountAndKeyBand,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SwmrSetupDescriptor {
    pub(crate) logical_target_bytes: u64,
    pub(crate) estimated_row_bytes: u64,
    pub(crate) estimated_preload_logical_bytes: u64,
    pub(crate) estimated_steady_logical_bytes: u64,
    pub(crate) payload_bytes: usize,
    pub(crate) preload_batches: usize,
    pub(crate) steady_batches: usize,
    pub(crate) writer_batches_per_step: usize,
    pub(crate) pinned_snapshot_mode: SwmrPinnedSnapshotMode,
    pub(crate) pinned_snapshot_ts: u64,
    pub(crate) pinned_manifest_version_ts: u64,
    pub(crate) reader_count: usize,
    pub(crate) light_scan_limit: usize,
    pub(crate) heavy_scan_limit: usize,
    pub(crate) held_snapshot_expectations: Vec<SwmrReaderExpectationArtifact>,
    pub(crate) manifest_reconstruction_observations: Vec<SwmrReaderObservationArtifact>,
}

#[derive(Clone)]
pub(crate) struct SwmrWorkloadState {
    schema: SchemaRef,
    light_projection: SchemaRef,
    rows_per_batch: usize,
    payload_bytes: usize,
    preload_batches: usize,
    steady_batches: usize,
    writer_batches_per_step: usize,
    pinned_snapshot: DbSnapshot,
    pinned_manifest_version: DbVersion,
    seed: u64,
    next_step: Arc<AtomicU64>,
    next_append_key: Arc<AtomicU64>,
    light_scan_limit: usize,
    heavy_scan_limit: usize,
    estimated_row_bytes: u64,
    logical_target_bytes: u64,
    reader_expectations: Vec<SwmrReaderExpectationArtifact>,
    manifest_reconstruction_observations: Vec<SwmrReaderObservationArtifact>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SwmrPinnedSnapshotMode {
    HeldSnapshot,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SwmrReaderExpectationArtifact {
    pub(crate) class: SwmrReaderClass,
    pub(crate) key_band: SwmrReaderKeyBand,
    pub(crate) validation_model: SwmrReaderValidationModel,
    pub(crate) expected_rows_per_scan: usize,
    pub(crate) expected_first_key: Option<String>,
    pub(crate) expected_last_key: Option<String>,
    pub(crate) expected_key_fingerprint: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SwmrReaderObservationArtifact {
    pub(crate) class: SwmrReaderClass,
    pub(crate) rows_per_scan: usize,
    pub(crate) first_key: Option<String>,
    pub(crate) last_key: Option<String>,
    pub(crate) key_fingerprint: u64,
    pub(crate) rows_match_expected: bool,
    pub(crate) all_keys_in_expected_band: bool,
    pub(crate) first_key_matches_expected: bool,
    pub(crate) last_key_matches_expected: bool,
    pub(crate) fingerprint_matches_expected: bool,
    pub(crate) valid: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct SwmrWorkloadParams {
    pub(crate) rows_per_batch: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) preload_batches: usize,
    pub(crate) steady_batches: usize,
    pub(crate) writer_batches_per_step: usize,
    pub(crate) seed: u64,
}

impl SwmrWorkloadState {
    pub(crate) fn new(
        schema: SchemaRef,
        light_projection: SchemaRef,
        pinned_snapshot: DbSnapshot,
        pinned_manifest_version: DbVersion,
        reader_expectations: Vec<SwmrReaderExpectationArtifact>,
        manifest_reconstruction_observations: Vec<SwmrReaderObservationArtifact>,
        params: SwmrWorkloadParams,
    ) -> Self {
        let estimated_row_bytes = estimate_swmr_row_bytes(params.payload_bytes);
        let next_append_key = params
            .preload_batches
            .checked_mul(params.rows_per_batch)
            .and_then(|rows| u64::try_from(rows).ok())
            .unwrap_or(u64::MAX);
        let total_rows = params
            .preload_batches
            .saturating_add(params.steady_batches)
            .saturating_mul(params.rows_per_batch);
        Self {
            schema,
            light_projection,
            rows_per_batch: params.rows_per_batch,
            payload_bytes: params.payload_bytes,
            preload_batches: params.preload_batches,
            steady_batches: params.steady_batches,
            writer_batches_per_step: params.writer_batches_per_step.max(1),
            pinned_snapshot,
            pinned_manifest_version,
            seed: params.seed,
            next_step: Arc::new(AtomicU64::new(0)),
            next_append_key: Arc::new(AtomicU64::new(next_append_key)),
            light_scan_limit: params.rows_per_batch.max(1),
            heavy_scan_limit: params.rows_per_batch.saturating_mul(8).max(1),
            estimated_row_bytes,
            logical_target_bytes: estimated_row_bytes
                .saturating_mul(u64::try_from(total_rows).unwrap_or(u64::MAX)),
            reader_expectations,
            manifest_reconstruction_observations,
        }
    }

    pub(crate) fn with_scan_limits(
        mut self,
        light_scan_limit: usize,
        heavy_scan_limit: usize,
    ) -> Self {
        self.light_scan_limit = light_scan_limit.max(1);
        self.heavy_scan_limit = heavy_scan_limit.max(1);
        self
    }

    pub(crate) fn with_logical_target_bytes(mut self, logical_target_bytes: u64) -> Self {
        self.logical_target_bytes = logical_target_bytes;
        self
    }

    pub(crate) fn setup_descriptor(&self) -> SwmrSetupDescriptor {
        let preload_rows = self.preload_batches.saturating_mul(self.rows_per_batch);
        let steady_rows = self.steady_batches.saturating_mul(self.rows_per_batch);
        SwmrSetupDescriptor {
            logical_target_bytes: self.logical_target_bytes,
            estimated_row_bytes: self.estimated_row_bytes,
            estimated_preload_logical_bytes: self
                .estimated_row_bytes
                .saturating_mul(u64::try_from(preload_rows).unwrap_or(u64::MAX)),
            estimated_steady_logical_bytes: self
                .estimated_row_bytes
                .saturating_mul(u64::try_from(steady_rows).unwrap_or(u64::MAX)),
            payload_bytes: self.payload_bytes,
            preload_batches: self.preload_batches,
            steady_batches: self.steady_batches,
            writer_batches_per_step: self.writer_batches_per_step,
            pinned_snapshot_mode: SwmrPinnedSnapshotMode::HeldSnapshot,
            pinned_snapshot_ts: self.pinned_snapshot.read_timestamp().get(),
            pinned_manifest_version_ts: self.pinned_manifest_version.timestamp.get(),
            reader_count: 4,
            light_scan_limit: self.light_scan_limit,
            heavy_scan_limit: self.heavy_scan_limit,
            held_snapshot_expectations: self.reader_expectations.clone(),
            manifest_reconstruction_observations: self.manifest_reconstruction_observations.clone(),
        }
    }

    fn reader_classes(&self) -> [SwmrReaderClass; 4] {
        [
            SwmrReaderClass::HeadLight,
            SwmrReaderClass::HeadHeavy,
            SwmrReaderClass::PinnedLight,
            SwmrReaderClass::PinnedHeavy,
        ]
    }

    fn expectation_for(&self, class: SwmrReaderClass) -> Option<&SwmrReaderExpectationArtifact> {
        self.reader_expectations
            .iter()
            .find(|expectation| expectation.class == class)
    }

    fn observation_for(
        &self,
        class: SwmrReaderClass,
        observation: SwmrReaderScanObservation,
    ) -> Result<SwmrReaderObservationArtifact, BenchError> {
        let Some(expectation) = self.expectation_for(class) else {
            return Err(BenchError::Message(format!(
                "missing swmr expectation for reader `{}`",
                class.as_str()
            )));
        };
        let rows_match_expected = observation.rows_per_scan == expectation.expected_rows_per_scan;
        let first_key_matches_expected =
            observation.first_key.as_ref() == expectation.expected_first_key.as_ref();
        let last_key_matches_expected =
            observation.last_key.as_ref() == expectation.expected_last_key.as_ref();
        let fingerprint_matches_expected =
            observation.key_fingerprint == expectation.expected_key_fingerprint;
        let valid = match expectation.validation_model {
            SwmrReaderValidationModel::ExactShapeStable => {
                rows_match_expected
                    && observation.all_keys_in_expected_band
                    && first_key_matches_expected
                    && last_key_matches_expected
                    && fingerprint_matches_expected
            }
            SwmrReaderValidationModel::CountAndKeyBand => {
                rows_match_expected && observation.all_keys_in_expected_band
            }
        };

        Ok(SwmrReaderObservationArtifact {
            class,
            rows_per_scan: observation.rows_per_scan,
            first_key: observation.first_key,
            last_key: observation.last_key,
            key_fingerprint: observation.key_fingerprint,
            rows_match_expected,
            all_keys_in_expected_band: observation.all_keys_in_expected_band,
            first_key_matches_expected,
            last_key_matches_expected,
            fingerprint_matches_expected,
            valid,
        })
    }

    fn validate_reader_observation(
        &self,
        class: SwmrReaderClass,
        observation: &SwmrReaderObservationArtifact,
    ) -> Result<(), BenchError> {
        if observation.valid {
            return Ok(());
        }

        let Some(expectation) = self.expectation_for(class) else {
            return Err(BenchError::Message(format!(
                "missing swmr expectation for reader `{}`",
                class.as_str()
            )));
        };

        Err(BenchError::Message(format!(
            "swmr reader `{}` violated {:?} validity: rows={} expected_rows={} first_key={:?} \
             expected_first_key={:?} last_key={:?} expected_last_key={:?} fingerprint={} \
             expected_fingerprint={} all_keys_in_expected_band={} pinned_snapshot_ts={} \
             pinned_manifest_version_ts={}",
            class.as_str(),
            expectation.validation_model,
            observation.rows_per_scan,
            expectation.expected_rows_per_scan,
            observation.first_key,
            expectation.expected_first_key,
            observation.last_key,
            expectation.expected_last_key,
            observation.key_fingerprint,
            expectation.expected_key_fingerprint,
            observation.all_keys_in_expected_band,
            self.pinned_snapshot.read_timestamp().get(),
            self.pinned_manifest_version.timestamp.get()
        )))
    }
}

#[derive(Clone)]
pub(crate) struct ScenarioState {
    pub(crate) scenario_id: &'static str,
    pub(crate) scenario_name: String,
    pub(crate) scenario_variant_id: String,
    pub(crate) benchmark_id: String,
    pub(crate) workload: ScenarioWorkload,
    pub(crate) dimensions: ScenarioDimensionsArtifact,
    pub(crate) db: BenchmarkDb,
    pub(crate) io_probe: IoProbe,
    pub(crate) setup_io: IoCountersArtifact,
    pub(crate) rows_per_op_hint: usize,
    pub(crate) version_before_compaction: VersionSummary,
    pub(crate) version_ready: VersionSummary,
    pub(crate) volume_before_compaction: StorageVolumeArtifact,
    pub(crate) volume_ready: StorageVolumeArtifact,
    pub(crate) write_state: Option<WriteWorkloadState>,
    pub(crate) swmr_state: Option<SwmrWorkloadState>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkArtifact {
    pub(crate) schema_version: u32,
    pub(crate) benchmark_id: &'static str,
    pub(crate) run_id: String,
    pub(crate) generated_at_unix_ms: u64,
    pub(crate) topology: BenchmarkTopologyArtifact,
    pub(crate) config: ResolvedConfigArtifact,
    pub(crate) scenarios: Vec<ScenarioArtifact>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BenchmarkTopologyArtifact {
    runner_env: Option<String>,
    runner_region: Option<String>,
    runner_az: Option<String>,
    runner_instance_type: Option<String>,
    bucket_region: Option<String>,
    bucket_az: Option<String>,
    object_store_flavor: Option<String>,
    endpoint_kind: Option<String>,
    network_path: Option<String>,
    median_rtt_ms: Option<f64>,
    same_region: Option<bool>,
    same_az: Option<bool>,
}

impl BenchmarkTopologyArtifact {
    pub(crate) fn from_env() -> Result<Self, BenchError> {
        let runner_region = env_optional_trimmed("TONBO_BENCH_RUNNER_REGION")?;
        let runner_az = env_optional_trimmed("TONBO_BENCH_RUNNER_AZ")?;
        let bucket_region = env_optional_trimmed("TONBO_BENCH_BUCKET_REGION")?;
        let bucket_az = env_optional_trimmed("TONBO_BENCH_BUCKET_AZ")?;
        let same_region = match (&runner_region, &bucket_region) {
            (Some(runner), Some(bucket)) => Some(runner == bucket),
            _ => None,
        };
        let same_az = match (&runner_az, &bucket_az) {
            (Some(runner), Some(bucket)) => Some(runner == bucket),
            _ => None,
        };

        Ok(Self {
            runner_env: env_optional_trimmed("TONBO_BENCH_RUNNER_ENV")?,
            runner_region,
            runner_az,
            runner_instance_type: env_optional_trimmed("TONBO_BENCH_RUNNER_INSTANCE_TYPE")?,
            bucket_region,
            bucket_az,
            object_store_flavor: env_optional_trimmed("TONBO_BENCH_OBJECT_STORE_FLAVOR")?,
            endpoint_kind: env_optional_trimmed("TONBO_BENCH_ENDPOINT_KIND")?,
            network_path: env_optional_trimmed("TONBO_BENCH_NETWORK_PATH")?,
            median_rtt_ms: env_optional_f64("TONBO_BENCH_MEDIAN_RTT_MS")?,
            same_region,
            same_az,
        })
    }

    fn has_any_signal(&self) -> bool {
        self.runner_env.is_some()
            || self.runner_region.is_some()
            || self.runner_az.is_some()
            || self.runner_instance_type.is_some()
            || self.bucket_region.is_some()
            || self.bucket_az.is_some()
            || self.object_store_flavor.is_some()
            || self.endpoint_kind.is_some()
            || self.network_path.is_some()
            || self.median_rtt_ms.is_some()
            || self.same_region.is_some()
            || self.same_az.is_some()
    }

    fn summary_line(&self) -> Option<String> {
        if !self.has_any_signal() {
            return None;
        }
        let mut fields = Vec::new();
        push_topology_field(&mut fields, "runner_env", self.runner_env.as_deref());
        push_topology_field(&mut fields, "runner_region", self.runner_region.as_deref());
        push_topology_field(&mut fields, "runner_az", self.runner_az.as_deref());
        push_topology_field(
            &mut fields,
            "runner_instance_type",
            self.runner_instance_type.as_deref(),
        );
        push_topology_field(&mut fields, "bucket_region", self.bucket_region.as_deref());
        push_topology_field(&mut fields, "bucket_az", self.bucket_az.as_deref());
        push_topology_field(
            &mut fields,
            "object_store_flavor",
            self.object_store_flavor.as_deref(),
        );
        push_topology_field(&mut fields, "endpoint_kind", self.endpoint_kind.as_deref());
        push_topology_field(&mut fields, "network_path", self.network_path.as_deref());
        if let Some(rtt) = self.median_rtt_ms {
            fields.push(format!("median_rtt_ms={rtt:.2}"));
        }
        if let Some(same_region) = self.same_region {
            fields.push(format!("same_region={same_region}"));
        }
        if let Some(same_az) = self.same_az {
            fields.push(format!("same_az={same_az}"));
        }
        Some(fields.join(", "))
    }
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
    rows_per_scan: usize,
    sst_count_before_compaction: usize,
    level_count_before_compaction: usize,
    sst_count_ready: usize,
    level_count_ready: usize,
    logical_before_compaction: LogicalVolumeArtifact,
    logical_ready: LogicalVolumeArtifact,
    volume_before_compaction: StorageVolumeArtifact,
    volume_ready: StorageVolumeArtifact,
    io: IoCountersArtifact,
    swmr: Option<SwmrSetupDescriptor>,
}

#[derive(Debug, Serialize)]
struct ScenarioSummaryArtifact {
    iterations: usize,
    total_elapsed_ns: u64,
    rows_processed: u64,
    throughput: ThroughputSummary,
    latency_ns: LatencySummary,
    read_path_latency_ns: Option<ReadPathLatencySummary>,
    read_path_internal_ns: Option<ReadPathInternalSummary>,
    io: IoCountersArtifact,
    swmr: Option<SwmrSummaryArtifact>,
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
    io: IoCountersArtifact,
    swmr: Option<SwmrAggregate>,
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

#[derive(Debug, Serialize)]
struct SwmrSummaryArtifact {
    writer_rows_processed: u64,
    writer_latency_ns: LatencySummary,
    readers: Vec<SwmrReaderSummaryArtifact>,
}

#[derive(Debug, Serialize)]
struct SwmrReaderSummaryArtifact {
    class: SwmrReaderClass,
    key_band: SwmrReaderKeyBand,
    validation_model: SwmrReaderValidationModel,
    expected_rows_per_scan: usize,
    expected_first_key: Option<String>,
    expected_last_key: Option<String>,
    expected_key_fingerprint: u64,
    rows_processed: u64,
    mean_rows_per_scan: f64,
    min_rows_per_scan: usize,
    max_rows_per_scan: usize,
    unique_key_fingerprints: usize,
    all_rows_match_expected: bool,
    all_keys_in_expected_band: bool,
    all_first_key_matches_expected: bool,
    all_last_key_matches_expected: bool,
    all_fingerprints_match_expected: bool,
    valid: bool,
    latency_ns: LatencySummary,
    read_path_latency_ns: ReadPathLatencySummary,
    read_path_internal_ns: ReadPathInternalSummary,
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

#[derive(Default)]
struct SwmrAggregate {
    writer_rows_processed: u64,
    writer_latencies_ns: Vec<u64>,
    readers: Vec<SwmrReaderAggregate>,
}

impl SwmrAggregate {
    fn record(&mut self, result: SwmrOperationResult) {
        self.writer_rows_processed = self
            .writer_rows_processed
            .saturating_add(u64::try_from(result.writer_rows).unwrap_or(u64::MAX));
        self.writer_latencies_ns.push(result.writer_latency_ns);
        for reader in result.readers {
            if let Some(existing) = self
                .readers
                .iter_mut()
                .find(|aggregate| aggregate.class == reader.class)
            {
                existing.record(reader);
            } else {
                let mut aggregate = SwmrReaderAggregate::new(reader.class);
                aggregate.record(reader);
                self.readers.push(aggregate);
            }
        }
        self.readers.sort_by_key(|reader| match reader.class {
            SwmrReaderClass::HeadLight => 0,
            SwmrReaderClass::HeadHeavy => 1,
            SwmrReaderClass::PinnedLight => 2,
            SwmrReaderClass::PinnedHeavy => 3,
        });
    }

    fn to_summary(&self, state: &SwmrWorkloadState) -> Option<SwmrSummaryArtifact> {
        if self.writer_latencies_ns.is_empty() && self.readers.is_empty() {
            return None;
        }
        Some(SwmrSummaryArtifact {
            writer_rows_processed: self.writer_rows_processed,
            writer_latency_ns: latency_summary(&self.writer_latencies_ns),
            readers: self
                .readers
                .iter()
                .filter_map(|reader| reader.to_summary(state))
                .collect(),
        })
    }
}

struct SwmrReaderAggregate {
    class: SwmrReaderClass,
    samples: usize,
    rows_processed: u64,
    min_rows_per_scan: usize,
    max_rows_per_scan: usize,
    latencies_ns: Vec<u64>,
    read_path: ReadPathAggregate,
    all_rows_match_expected: bool,
    all_keys_in_expected_band: bool,
    all_first_key_matches_expected: bool,
    all_last_key_matches_expected: bool,
    all_fingerprints_match_expected: bool,
    valid: bool,
    key_fingerprints: BTreeSet<u64>,
}

impl SwmrReaderAggregate {
    fn new(class: SwmrReaderClass) -> Self {
        Self {
            class,
            samples: 0,
            rows_processed: 0,
            min_rows_per_scan: usize::MAX,
            max_rows_per_scan: 0,
            latencies_ns: Vec::new(),
            read_path: ReadPathAggregate::default(),
            all_rows_match_expected: true,
            all_keys_in_expected_band: true,
            all_first_key_matches_expected: true,
            all_last_key_matches_expected: true,
            all_fingerprints_match_expected: true,
            valid: true,
            key_fingerprints: BTreeSet::new(),
        }
    }

    fn record(&mut self, result: SwmrReaderOperationResult) {
        self.samples = self.samples.saturating_add(1);
        self.rows_processed = self
            .rows_processed
            .saturating_add(u64::try_from(result.rows).unwrap_or(u64::MAX));
        self.min_rows_per_scan = self.min_rows_per_scan.min(result.rows);
        self.max_rows_per_scan = self.max_rows_per_scan.max(result.rows);
        self.latencies_ns.push(result.latency_ns);
        self.read_path.record(result.read_path);
        self.all_rows_match_expected &= result.observation.rows_match_expected;
        self.all_keys_in_expected_band &= result.observation.all_keys_in_expected_band;
        self.all_first_key_matches_expected &= result.observation.first_key_matches_expected;
        self.all_last_key_matches_expected &= result.observation.last_key_matches_expected;
        self.all_fingerprints_match_expected &= result.observation.fingerprint_matches_expected;
        self.valid &= result.observation.valid;
        let _ = self
            .key_fingerprints
            .insert(result.observation.key_fingerprint);
    }

    fn to_summary(&self, state: &SwmrWorkloadState) -> Option<SwmrReaderSummaryArtifact> {
        let expectation = state.expectation_for(self.class)?;
        let scans = self.samples.max(1) as f64;
        let mean_rows_per_scan = self.rows_processed as f64 / scans;
        Some(SwmrReaderSummaryArtifact {
            class: self.class,
            key_band: expectation.key_band,
            validation_model: expectation.validation_model,
            expected_rows_per_scan: expectation.expected_rows_per_scan,
            expected_first_key: expectation.expected_first_key.clone(),
            expected_last_key: expectation.expected_last_key.clone(),
            expected_key_fingerprint: expectation.expected_key_fingerprint,
            rows_processed: self.rows_processed,
            mean_rows_per_scan,
            min_rows_per_scan: if self.samples == 0 {
                0
            } else {
                self.min_rows_per_scan
            },
            max_rows_per_scan: self.max_rows_per_scan,
            unique_key_fingerprints: self.key_fingerprints.len(),
            all_rows_match_expected: self.all_rows_match_expected,
            all_keys_in_expected_band: self.all_keys_in_expected_band,
            all_first_key_matches_expected: self.all_first_key_matches_expected,
            all_last_key_matches_expected: self.all_last_key_matches_expected,
            all_fingerprints_match_expected: self.all_fingerprints_match_expected,
            valid: self.valid,
            latency_ns: latency_summary(&self.latencies_ns),
            read_path_latency_ns: self.read_path.to_summary()?,
            read_path_internal_ns: self.read_path.to_internal_summary()?,
        })
    }
}

struct OperationResult {
    rows: usize,
    read_path: Option<ReadPathBreakdown>,
    swmr: Option<SwmrOperationResult>,
}

struct SwmrOperationResult {
    writer_rows: usize,
    writer_latency_ns: u64,
    readers: Vec<SwmrReaderOperationResult>,
}

struct SwmrReaderOperationResult {
    class: SwmrReaderClass,
    rows: usize,
    observation: SwmrReaderObservationArtifact,
    latency_ns: u64,
    read_path: ReadPathBreakdown,
}

pub(crate) struct SwmrReaderScanObservation {
    key_band: SwmrReaderKeyBand,
    pub(crate) rows_per_scan: usize,
    pub(crate) first_key: Option<String>,
    pub(crate) last_key: Option<String>,
    pub(crate) key_fingerprint: u64,
    pub(crate) all_keys_in_expected_band: bool,
}

impl SwmrReaderScanObservation {
    fn new(key_band: SwmrReaderKeyBand) -> Self {
        Self {
            key_band,
            rows_per_scan: 0,
            first_key: None,
            last_key: None,
            key_fingerprint: swmr_fingerprint_seed(key_band),
            all_keys_in_expected_band: true,
        }
    }

    fn observe_batch(&mut self, batch: &RecordBatch) -> Result<(), BenchError> {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                BenchError::Message(
                    "swmr reader expected Utf8 id column at projection index 0".to_string(),
                )
            })?;
        for row_idx in 0..batch.num_rows() {
            let key = ids.value(row_idx);
            self.all_keys_in_expected_band &= self.key_band.contains_key(key);
            if self.first_key.is_none() {
                self.first_key = Some(key.to_string());
            }
            self.last_key = Some(key.to_string());
            self.key_fingerprint = swmr_fingerprint_update(self.key_fingerprint, key.as_bytes());
            self.rows_per_scan = self.rows_per_scan.saturating_add(1);
        }
        Ok(())
    }
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
                swmr: None,
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
                swmr: None,
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
                swmr: None,
            })
        }
        ScenarioWorkload::SwmrMixed => {
            let swmr_state = scenario.swmr_state.as_ref().ok_or_else(|| {
                BenchError::Message(format!(
                    "scenario `{}` missing swmr workload state",
                    scenario.benchmark_id
                ))
            })?;
            let result = run_swmr_mixed_operation(&scenario.db, swmr_state).await?;
            let reader_rows = result
                .readers
                .iter()
                .fold(0usize, |acc, reader| acc.saturating_add(reader.rows));
            let rows = result.writer_rows.saturating_add(reader_rows);
            Ok(OperationResult {
                rows,
                read_path: None,
                swmr: Some(result),
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

async fn run_swmr_mixed_operation(
    db: &BenchmarkDb,
    state: &SwmrWorkloadState,
) -> Result<SwmrOperationResult, BenchError> {
    let step_idx = state.next_step.fetch_add(1, Ordering::Relaxed);
    let writer_started = Instant::now();
    let mut writer_rows = 0usize;
    for batch_offset in 0..state.writer_batches_per_step {
        let batch_idx = step_idx
            .saturating_mul(u64::try_from(state.writer_batches_per_step).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(batch_offset).unwrap_or(u64::MAX));
        let batch = build_swmr_mixed_batch(state, batch_idx)?;
        let ingest_result = match db {
            BenchmarkDb::Local(inner) => inner.ingest_with_tombstones(batch.0, batch.1).await,
            BenchmarkDb::ObjectStore(inner) => inner.ingest_with_tombstones(batch.0, batch.1).await,
        };
        ingest_result?;
        writer_rows = writer_rows.saturating_add(state.rows_per_batch);
    }
    let writer_latency_ns = duration_ns_u64(writer_started.elapsed());

    let mut readers = Vec::with_capacity(4);
    for class in state.reader_classes() {
        readers.push(run_swmr_reader(db, state, class).await?);
    }

    Ok(SwmrOperationResult {
        writer_rows,
        writer_latency_ns,
        readers,
    })
}

async fn run_swmr_reader(
    db: &BenchmarkDb,
    state: &SwmrWorkloadState,
    class: SwmrReaderClass,
) -> Result<SwmrReaderOperationResult, BenchError> {
    match db {
        BenchmarkDb::Local(inner) => run_swmr_reader_local(inner, state, class).await,
        BenchmarkDb::ObjectStore(inner) => run_swmr_reader_object_store(inner, state, class).await,
    }
}

async fn run_swmr_reader_local(
    db: &DB<ProbedFs<LocalFs>, TokioExecutor>,
    state: &SwmrWorkloadState,
    class: SwmrReaderClass,
) -> Result<SwmrReaderOperationResult, BenchError> {
    let started = Instant::now();
    let predicate = swmr_predicate(class);
    let key_band = class.key_band();
    let projection = if class.is_light() {
        Some(Arc::clone(&state.light_projection))
    } else {
        None
    };
    let limit = if class.is_light() {
        state.light_scan_limit
    } else {
        state.heavy_scan_limit
    };
    let (observation, read_path) = if class.is_pinned() {
        let plan_started = Instant::now();
        let (mut stream, profile) = {
            let mut builder = state
                .pinned_snapshot
                .scan(db)
                .filter(predicate)
                .limit(limit);
            if let Some(projection) = projection {
                builder = builder.projection(projection);
            }
            builder.stream_with_profile().await
        }
        .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
        let prepare_ns = duration_ns_u64(plan_started.elapsed());
        let consume_started = Instant::now();
        let mut observation = SwmrReaderScanObservation::new(class.key_band());
        let mut batch_count = 0usize;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
            observation.observe_batch(&batch)?;
            batch_count = batch_count.saturating_add(1);
        }
        (
            observation,
            ReadPathBreakdown {
                prepare_ns,
                consume_ns: duration_ns_u64(consume_started.elapsed()),
                batch_count,
                internal: internal_breakdown(profile),
            },
        )
    } else {
        execute_scan_profiled_local(db, predicate, projection, limit, key_band).await?
    };
    let artifact = state.observation_for(class, observation)?;
    state.validate_reader_observation(class, &artifact)?;
    Ok(SwmrReaderOperationResult {
        class,
        rows: artifact.rows_per_scan,
        observation: artifact,
        latency_ns: duration_ns_u64(started.elapsed()),
        read_path,
    })
}

async fn run_swmr_reader_object_store(
    db: &DB<AmazonS3, TokioExecutor>,
    state: &SwmrWorkloadState,
    class: SwmrReaderClass,
) -> Result<SwmrReaderOperationResult, BenchError> {
    let started = Instant::now();
    let predicate = swmr_predicate(class);
    let key_band = class.key_band();
    let projection = if class.is_light() {
        Some(Arc::clone(&state.light_projection))
    } else {
        None
    };
    let limit = if class.is_light() {
        state.light_scan_limit
    } else {
        state.heavy_scan_limit
    };
    let (observation, read_path) = if class.is_pinned() {
        let plan_started = Instant::now();
        let (mut stream, profile) = {
            let mut builder = state
                .pinned_snapshot
                .scan(db)
                .filter(predicate)
                .limit(limit);
            if let Some(projection) = projection {
                builder = builder.projection(projection);
            }
            builder.stream_with_profile().await
        }
        .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
        let prepare_ns = duration_ns_u64(plan_started.elapsed());
        let consume_started = Instant::now();
        let mut observation = SwmrReaderScanObservation::new(class.key_band());
        let mut batch_count = 0usize;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
            observation.observe_batch(&batch)?;
            batch_count = batch_count.saturating_add(1);
        }
        (
            observation,
            ReadPathBreakdown {
                prepare_ns,
                consume_ns: duration_ns_u64(consume_started.elapsed()),
                batch_count,
                internal: internal_breakdown(profile),
            },
        )
    } else {
        execute_scan_profiled_object_store(db, predicate, projection, limit, key_band).await?
    };
    let artifact = state.observation_for(class, observation)?;
    state.validate_reader_observation(class, &artifact)?;
    Ok(SwmrReaderOperationResult {
        class,
        rows: artifact.rows_per_scan,
        observation: artifact,
        latency_ns: duration_ns_u64(started.elapsed()),
        read_path,
    })
}

async fn execute_scan_profiled_local(
    db: &DB<ProbedFs<LocalFs>, TokioExecutor>,
    predicate: Expr,
    projection: Option<SchemaRef>,
    limit: usize,
    key_band: SwmrReaderKeyBand,
) -> Result<(SwmrReaderScanObservation, ReadPathBreakdown), BenchError> {
    let plan_started = Instant::now();
    let (mut stream, profile) = {
        let mut builder = db.scan().filter(predicate).limit(limit);
        if let Some(projection) = projection {
            builder = builder.projection(projection);
        }
        builder.stream_with_profile().await
    }
    .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
    let prepare_ns = duration_ns_u64(plan_started.elapsed());
    let consume_started = Instant::now();
    let mut observation = SwmrReaderScanObservation::new(key_band);
    let mut batch_count = 0usize;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
        observation.observe_batch(&batch)?;
        batch_count = batch_count.saturating_add(1);
    }
    Ok((
        observation,
        ReadPathBreakdown {
            prepare_ns,
            consume_ns: duration_ns_u64(consume_started.elapsed()),
            batch_count,
            internal: internal_breakdown(profile),
        },
    ))
}

async fn execute_scan_profiled_object_store(
    db: &DB<AmazonS3, TokioExecutor>,
    predicate: Expr,
    projection: Option<SchemaRef>,
    limit: usize,
    key_band: SwmrReaderKeyBand,
) -> Result<(SwmrReaderScanObservation, ReadPathBreakdown), BenchError> {
    let plan_started = Instant::now();
    let (mut stream, profile) = {
        let mut builder = db.scan().filter(predicate).limit(limit);
        if let Some(projection) = projection {
            builder = builder.projection(projection);
        }
        builder.stream_with_profile().await
    }
    .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
    let prepare_ns = duration_ns_u64(plan_started.elapsed());
    let consume_started = Instant::now();
    let mut observation = SwmrReaderScanObservation::new(key_band);
    let mut batch_count = 0usize;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
        observation.observe_batch(&batch)?;
        batch_count = batch_count.saturating_add(1);
    }
    Ok((
        observation,
        ReadPathBreakdown {
            prepare_ns,
            consume_ns: duration_ns_u64(consume_started.elapsed()),
            batch_count,
            internal: internal_breakdown(profile),
        },
    ))
}

fn swmr_predicate(class: SwmrReaderClass) -> Expr {
    match class {
        SwmrReaderClass::HeadLight | SwmrReaderClass::PinnedLight => Expr::and(vec![
            Expr::gt_eq("id", ScalarValue::from("hot-00000000")),
            Expr::lt("id", ScalarValue::from("hot-99999999")),
        ]),
        SwmrReaderClass::HeadHeavy | SwmrReaderClass::PinnedHeavy => Expr::and(vec![
            Expr::gt_eq("id", ScalarValue::from("warm-00000000")),
            Expr::lt("id", ScalarValue::from("zzzzzzzz")),
        ]),
    }
}

pub(crate) async fn swmr_reader_observation_for_snapshot(
    db: &BenchmarkDb,
    snapshot: &DbSnapshot,
    class: SwmrReaderClass,
    light_projection: &SchemaRef,
    light_scan_limit: usize,
    heavy_scan_limit: usize,
) -> Result<SwmrReaderScanObservation, BenchError> {
    let predicate = swmr_predicate(class);
    let key_band = class.key_band();
    let projection = if class.is_light() {
        Some(Arc::clone(light_projection))
    } else {
        None
    };
    let limit = if class.is_light() {
        light_scan_limit
    } else {
        heavy_scan_limit
    };

    match db {
        BenchmarkDb::Local(inner) => {
            execute_snapshot_scan_row_count_local(
                inner, snapshot, predicate, projection, limit, key_band,
            )
            .await
        }
        BenchmarkDb::ObjectStore(inner) => {
            execute_snapshot_scan_row_count_object_store(
                inner, snapshot, predicate, projection, limit, key_band,
            )
            .await
        }
    }
}

async fn execute_snapshot_scan_row_count_local(
    db: &DB<ProbedFs<LocalFs>, TokioExecutor>,
    snapshot: &DbSnapshot,
    predicate: Expr,
    projection: Option<SchemaRef>,
    limit: usize,
    key_band: SwmrReaderKeyBand,
) -> Result<SwmrReaderScanObservation, BenchError> {
    let mut stream = {
        let mut builder = snapshot.scan(db).filter(predicate).limit(limit);
        if let Some(projection) = projection {
            builder = builder.projection(projection);
        }
        builder.stream().await
    }
    .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
    let mut observation = SwmrReaderScanObservation::new(key_band);
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
        observation.observe_batch(&batch)?;
    }
    Ok(observation)
}

async fn execute_snapshot_scan_row_count_object_store(
    db: &DB<AmazonS3, TokioExecutor>,
    snapshot: &DbSnapshot,
    predicate: Expr,
    projection: Option<SchemaRef>,
    limit: usize,
    key_band: SwmrReaderKeyBand,
) -> Result<SwmrReaderScanObservation, BenchError> {
    let mut stream = {
        let mut builder = snapshot.scan(db).filter(predicate).limit(limit);
        if let Some(projection) = projection {
            builder = builder.projection(projection);
        }
        builder.stream().await
    }
    .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
    let mut observation = SwmrReaderScanObservation::new(key_band);
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|err| BenchError::Message(format!("swmr scan stream failed: {err}")))?;
        observation.observe_batch(&batch)?;
    }
    Ok(observation)
}

fn build_swmr_mixed_batch(
    state: &SwmrWorkloadState,
    batch_idx: u64,
) -> Result<(RecordBatch, Vec<bool>), BenchError> {
    let mut ids = Vec::with_capacity(state.rows_per_batch);
    let mut values = Vec::with_capacity(state.rows_per_batch);
    let mut payloads = Vec::with_capacity(state.rows_per_batch);
    let mut tombstones = Vec::with_capacity(state.rows_per_batch);

    for row_idx in 0..state.rows_per_batch {
        let ordinal = batch_idx
            .saturating_mul(u64::try_from(state.rows_per_batch).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(row_idx).unwrap_or(u64::MAX));
        let selector = deterministic_mix_u64(ordinal, state.seed) % 100;
        let (id, tombstone) = if selector < SWMR_APPEND_SHARE_PCT {
            let key = state.next_append_key.fetch_add(1, Ordering::Relaxed);
            (format!("append-{key:08}"), false)
        } else if selector < SWMR_APPEND_SHARE_PCT + SWMR_OVERWRITE_SHARE_PCT {
            (swmr_existing_key(ordinal, state.seed, true), false)
        } else {
            (swmr_existing_key(ordinal, state.seed, false), true)
        };
        ids.push(id);
        values.push(
            i64::try_from(deterministic_mix_u64(ordinal ^ 0x51_4D_57_52, state.seed))
                .unwrap_or(i64::MAX),
        );
        payloads.push(swmr_payload(state.payload_bytes, ordinal));
        tombstones.push(tombstone);
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&state.schema),
        vec![
            Arc::new(StringArray::from(ids)) as _,
            Arc::new(Int64Array::from(values)) as _,
            Arc::new(StringArray::from(payloads)) as _,
        ],
    )?;
    Ok((batch, tombstones))
}

fn swmr_existing_key(ordinal: u64, seed: u64, allow_warm: bool) -> String {
    let mixed = deterministic_mix_u64(ordinal ^ 0xC0_11_4D, seed);
    let hot_slot = mixed % 16_384;
    if allow_warm && mixed % 10 >= 7 {
        let warm_slot = mixed % 32_768;
        format!("warm-{warm_slot:08}")
    } else {
        format!("hot-{hot_slot:08}")
    }
}

fn swmr_payload(payload_bytes: usize, ordinal: u64) -> String {
    if payload_bytes == 0 {
        return String::new();
    }
    let prefix = format!("{ordinal:016x}");
    if payload_bytes <= prefix.len() {
        return prefix[..payload_bytes].to_string();
    }
    let mut payload = String::with_capacity(payload_bytes);
    payload.push_str(&prefix);
    while payload.len() < payload_bytes {
        payload.push('p');
    }
    payload.truncate(payload_bytes);
    payload
}

fn swmr_fingerprint_seed(key_band: SwmrReaderKeyBand) -> u64 {
    swmr_fingerprint_update(SWMR_FINGERPRINT_OFFSET_BASIS, key_band.prefix().as_bytes())
}

fn swmr_fingerprint_update(mut fingerprint: u64, bytes: &[u8]) -> u64 {
    for &byte in bytes {
        fingerprint ^= u64::from(byte);
        fingerprint = fingerprint.wrapping_mul(SWMR_FINGERPRINT_PRIME);
    }
    fingerprint = fingerprint.wrapping_mul(SWMR_FINGERPRINT_PRIME);
    fingerprint
}

fn estimate_swmr_row_bytes(payload_bytes: usize) -> u64 {
    let fixed = 32u64;
    fixed.saturating_add(u64::try_from(payload_bytes).unwrap_or(u64::MAX))
}

fn deterministic_mix_u64(ordinal: u64, seed: u64) -> u64 {
    ordinal
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(seed.rotate_left(13))
        .wrapping_add(0xA11C_E5D5)
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
        topology: BenchmarkTopologyArtifact::from_env()?,
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

    let observed = format!(
        "read_ops_delta={}, bytes_delta={}, mean_delta={}, p99_delta={}",
        fmt_pct_opt(read_ops_drop),
        fmt_pct_opt(bytes_drop),
        fmt_pct_opt(mean_improve),
        fmt_pct_opt(p99_improve)
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
    if let Some(summary) = artifact.topology.summary_line() {
        eprintln!("  Topology: {summary}");
    }
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
}

pub(crate) fn benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

pub(crate) fn swmr_benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

pub(crate) fn swmr_light_projection_schema() -> SchemaRef {
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
    let mut builder = DbBuilder::from_schema_key_name(Arc::clone(schema), "id")?
        .on_durable_fs(fs, root_string)?
        .wal_sync_policy(config.wal_sync_policy.clone())
        .with_minor_compaction(1, 0);

    match compaction_profile {
        CompactionProfile::Disabled => {}
        CompactionProfile::Default { periodic_tick_ms } => {
            let options =
                CompactionOptions::new().periodic_tick(Duration::from_millis(*periodic_tick_ms));
            builder = builder.with_compaction_options(options);
        }
        CompactionProfile::Swept(tuning) => {
            let options = build_swept_compaction_options(tuning);
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
    _io_probe: &IoProbe,
) -> Result<BenchmarkDb, BenchError> {
    let mut builder = DbBuilder::from_schema_key_name(Arc::clone(schema), "id")?
        .object_store(object_spec.clone())
        .map_err(BenchError::from)?
        .wal_sync_policy(config.wal_sync_policy.clone())
        .with_minor_compaction(1, 0);

    match compaction_profile {
        CompactionProfile::Disabled => {}
        CompactionProfile::Default { periodic_tick_ms } => {
            let options =
                CompactionOptions::new().periodic_tick(Duration::from_millis(*periodic_tick_ms));
            builder = builder.with_compaction_options(options);
        }
        CompactionProfile::Swept(tuning) => {
            let options = build_swept_compaction_options(tuning);
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

pub(crate) async fn cleanup_local_storage_volume(root: &Path) -> Result<(), BenchError> {
    let fs = LocalFs {};
    let absolute = absolutize(root)?;
    let root_path = FusioPath::from_filesystem_path(&absolute).map_err(|err| {
        BenchError::Message(format!(
            "failed to convert benchmark root `{}` to fusio path: {err}",
            absolute.display()
        ))
    })?;
    cleanup_storage_root_with_fs(&fs, &root_path).await
}

pub(crate) async fn cleanup_object_store_storage_volume(
    object_spec: &ObjectSpec,
) -> Result<(), BenchError> {
    let probe = IoProbe::default();
    let (fs, root) = build_probed_object_store_fs(object_spec, &probe)?;
    cleanup_storage_root_with_fs(fs.as_ref(), &root).await
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
                volume.sst_bytes = volume.sst_bytes.saturating_add(size);
            } else if is_wal_path(path) {
                volume.wal_bytes = volume.wal_bytes.saturating_add(size);
            } else if is_manifest_path(path) {
                volume.manifest_bytes = volume.manifest_bytes.saturating_add(size);
            } else {
                volume.other_bytes = volume.other_bytes.saturating_add(size);
            }
        }
    }
    Ok(volume)
}

async fn cleanup_storage_root_with_fs<FS: Fs>(fs: &FS, root: &FusioPath) -> Result<(), BenchError> {
    let mut pending = VecDeque::new();
    let mut visited = HashSet::new();
    let mut files = Vec::new();
    pending.push_back(root.clone());
    while let Some(dir) = pending.pop_front() {
        if !visited.insert(dir.to_string()) {
            continue;
        }
        let stream = fs.list(&dir).await.map_err(|err| {
            BenchError::Message(format!(
                "failed to list storage for cleanup under `{}`: {err}",
                dir.as_ref()
            ))
        })?;
        futures::pin_mut!(stream);
        while let Some(meta_result) = stream.next().await {
            let meta = meta_result.map_err(|err| {
                BenchError::Message(format!(
                    "failed to read storage metadata for cleanup under `{}`: {err}",
                    dir.as_ref()
                ))
            })?;
            if is_probably_file_path(meta.path.as_ref()) {
                files.push(meta.path);
            } else {
                pending.push_back(meta.path);
            }
        }
    }

    files.sort_by(|left, right| right.as_ref().cmp(left.as_ref()));
    for path in files {
        fs.remove(&path).await.map_err(|err| {
            BenchError::Message(format!(
                "failed to remove benchmark artifact `{}`: {err}",
                path.as_ref()
            ))
        })?;
    }

    Ok(())
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

pub(crate) async fn preload_swmr_workload(
    db: &BenchmarkDb,
    schema: &SchemaRef,
    rows_per_batch: usize,
    payload_bytes: usize,
    preload_batches: usize,
    seed: u64,
) -> Result<(), BenchError> {
    for batch_idx in 0..preload_batches {
        let batch = build_swmr_preload_batch(
            schema,
            rows_per_batch,
            payload_bytes,
            u64::try_from(batch_idx).unwrap_or(u64::MAX),
            seed,
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

fn build_swmr_preload_batch(
    schema: &SchemaRef,
    rows_per_batch: usize,
    payload_bytes: usize,
    batch_idx: u64,
    seed: u64,
) -> Result<RecordBatch, BenchError> {
    let mut ids = Vec::with_capacity(rows_per_batch);
    let mut values = Vec::with_capacity(rows_per_batch);
    let mut payloads = Vec::with_capacity(rows_per_batch);

    for row_idx in 0..rows_per_batch {
        let ordinal = batch_idx
            .saturating_mul(u64::try_from(rows_per_batch).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(row_idx).unwrap_or(u64::MAX));
        let key_idx = usize::try_from(ordinal).unwrap_or(usize::MAX);
        ids.push(swmr_preload_key(key_idx));
        values.push(i64::try_from(deterministic_mix_u64(ordinal, seed)).unwrap_or(i64::MAX));
        payloads.push(swmr_payload(payload_bytes, ordinal));
    }

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(ids)) as _,
            Arc::new(Int64Array::from(values)) as _,
            Arc::new(StringArray::from(payloads)) as _,
        ],
    )
    .map_err(BenchError::from)
}

fn swmr_preload_key(index: usize) -> String {
    if index < 16_384 {
        format!("hot-{index:08}")
    } else if index < 49_152 {
        format!("warm-{:08}", index - 16_384)
    } else {
        format!("cold-{:08}", index - 49_152)
    }
}

pub(crate) fn deterministic_key_slot(global_idx: usize, key_space: usize, seed: u64) -> usize {
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
    db: &DB<AmazonS3, TokioExecutor>,
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
        let mut swmr = SwmrAggregate::default();
        let started = Instant::now();
        for _ in 0..iterations {
            let op_started = Instant::now();
            let result = run_scenario_operation(scenario).await?;
            rows_processed = rows_processed.saturating_add(usize_to_u64(result.rows));
            if let Some(breakdown) = result.read_path {
                read_path.record(breakdown);
            }
            if let Some(swmr_result) = result.swmr {
                swmr.record(swmr_result);
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
            io: scenario.io_probe.snapshot(),
            swmr: if !swmr.writer_latencies_ns.is_empty() || !swmr.readers.is_empty() {
                Some(swmr)
            } else {
                None
            },
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

    ScenarioArtifact {
        scenario_id: scenario.scenario_id.to_string(),
        scenario_name: scenario.scenario_name.clone(),
        scenario_variant_id: scenario.scenario_variant_id.clone(),
        dimensions: scenario.dimensions.clone(),
        setup: ScenarioSetupArtifact {
            rows_per_scan: scenario.rows_per_op_hint,
            sst_count_before_compaction: scenario.version_before_compaction.sst_count,
            level_count_before_compaction: scenario.version_before_compaction.level_count,
            sst_count_ready: scenario.version_ready.sst_count,
            level_count_ready: scenario.version_ready.level_count,
            logical_before_compaction: LogicalVolumeArtifact::from_version(
                scenario.version_before_compaction,
            ),
            logical_ready: LogicalVolumeArtifact::from_version(scenario.version_ready),
            volume_before_compaction: scenario.volume_before_compaction.clone(),
            volume_ready: scenario.volume_ready.clone(),
            io: scenario.setup_io.clone(),
            swmr: scenario
                .swmr_state
                .as_ref()
                .map(SwmrWorkloadState::setup_descriptor),
        },
        summary: ScenarioSummaryArtifact {
            iterations: measurement.iterations,
            total_elapsed_ns: measurement.total_elapsed_ns,
            rows_processed: measurement.rows_processed,
            throughput,
            latency_ns: latency,
            read_path_latency_ns: read_path_latency,
            read_path_internal_ns: read_path_internal,
            io: measurement.io,
            swmr: measurement.swmr.as_ref().and_then(|swmr| {
                scenario
                    .swmr_state
                    .as_ref()
                    .and_then(|state| swmr.to_summary(state))
            }),
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

fn env_optional_trimmed(name: &'static str) -> Result<Option<String>, BenchError> {
    match env::var(name) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn env_optional_f64(name: &'static str) -> Result<Option<f64>, BenchError> {
    match env::var(name) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let value = trimmed.parse::<f64>().map_err(|_| BenchError::InvalidEnv {
                name,
                value: raw.clone(),
            })?;
            Ok(Some(value))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

fn push_topology_field(fields: &mut Vec<String>, label: &str, value: Option<&str>) {
    if let Some(value) = value {
        fields.push(format!("{label}={value}"));
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
