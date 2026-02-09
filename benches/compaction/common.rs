use std::{
    collections::{HashMap, HashSet},
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
    path::{Path as FusioPath, PathPart},
};
use fusio_manifest::ObjectHead;
use serde::Serialize;
use thiserror::Error;
use tokio::{runtime::Runtime, time::sleep};
use tonbo::db::{CompactionOptions, DB, DBError, DbBuildError, DbBuilder, WalSyncPolicy};

pub(crate) const BENCH_ID: &str = "compaction_local";
pub(crate) const BENCH_SCHEMA_VERSION: u32 = 2;

const DEFAULT_INGEST_BATCHES: usize = 640;
const DEFAULT_ROWS_PER_BATCH: usize = 64;
const DEFAULT_KEY_SPACE: usize = 2_048;
const DEFAULT_ARTIFACT_ITERATIONS: usize = 48;
const DEFAULT_CRITERION_SAMPLE_SIZE: usize = 20;
const DEFAULT_COMPACTION_WAIT_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_COMPACTION_POLL_INTERVAL_MS: u64 = 50;
const DEFAULT_COMPACTION_PERIODIC_TICK_MS: u64 = 200;
const DEFAULT_SEED: u64 = 584;

pub(crate) type BenchmarkDb = DB<ProbedLocalFs, TokioExecutor>;

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

#[derive(Clone, Default)]
pub(crate) struct ProbedLocalFs {
    inner: LocalFs,
    probe: IoProbe,
}

impl ProbedLocalFs {
    pub(crate) fn new(inner: LocalFs, probe: IoProbe) -> Self {
        Self { inner, probe }
    }
}

pub(crate) struct ProbedLocalFile {
    inner: <LocalFs as Fs>::File,
    path: FusioPath,
    probe: IoProbe,
}

impl Read for ProbedLocalFile {
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

impl Write for ProbedLocalFile {
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

impl FileCommit for ProbedLocalFile {
    async fn commit(&mut self) -> Result<(), FusioError> {
        self.inner.commit().await
    }
}

impl Fs for ProbedLocalFs {
    type File = ProbedLocalFile;

    fn file_system(&self) -> FileSystemTag {
        self.inner.file_system()
    }

    async fn open_options(
        &self,
        path: &FusioPath,
        options: OpenOptions,
    ) -> Result<Self::File, FusioError> {
        let file = self.inner.open_options(path, options).await?;
        Ok(ProbedLocalFile {
            inner: file,
            path: path.clone(),
            probe: self.probe.clone(),
        })
    }

    async fn create_dir_all(path: &FusioPath) -> Result<(), FusioError> {
        LocalFs::create_dir_all(path).await
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

impl FsCas for ProbedLocalFs {
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

impl ObjectHead for ProbedLocalFs {
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
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedConfig {
    pub(crate) ingest_batches: usize,
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
}

impl ResolvedConfig {
    pub(crate) fn from_env() -> Result<Self, BenchError> {
        let ingest_batches = env_usize(
            "TONBO_COMPACTION_BENCH_INGEST_BATCHES",
            DEFAULT_INGEST_BATCHES,
        )?;
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

        if rows_per_batch == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_COMPACTION_BENCH_ROWS_PER_BATCH",
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

        Ok(Self {
            ingest_batches,
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
        })
    }

    pub(crate) fn artifact(&self) -> ResolvedConfigArtifact {
        ResolvedConfigArtifact {
            ingest_batches: self.ingest_batches,
            rows_per_batch: self.rows_per_batch,
            key_space: self.key_space,
            artifact_iterations: self.artifact_iterations,
            criterion_sample_size: self.criterion_sample_size,
            compaction_wait_timeout_ms: self.compaction_wait_timeout_ms,
            compaction_poll_interval_ms: self.compaction_poll_interval_ms,
            compaction_periodic_tick_ms: self.compaction_periodic_tick_ms,
            seed: self.seed,
            wal_sync_policy: self.wal_sync_policy_name.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VersionSummary {
    pub(crate) sst_count: usize,
    pub(crate) level_count: usize,
}

#[derive(Clone)]
pub(crate) struct ScenarioState {
    pub(crate) scenario_id: &'static str,
    pub(crate) scenario_name: &'static str,
    pub(crate) db: BenchmarkDb,
    pub(crate) io_probe: IoProbe,
    pub(crate) setup_io: IoCountersArtifact,
    pub(crate) rows_per_scan: usize,
    pub(crate) version_before_compaction: VersionSummary,
    pub(crate) version_ready: VersionSummary,
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
    rows_per_batch: usize,
    key_space: usize,
    artifact_iterations: usize,
    criterion_sample_size: usize,
    compaction_wait_timeout_ms: u64,
    compaction_poll_interval_ms: u64,
    compaction_periodic_tick_ms: u64,
    seed: u64,
    wal_sync_policy: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ScenarioArtifact {
    scenario_id: String,
    scenario_name: String,
    setup: ScenarioSetupArtifact,
    summary: ScenarioSummaryArtifact,
}

#[derive(Debug, Serialize)]
struct ScenarioSetupArtifact {
    rows_per_scan: usize,
    sst_count_before_compaction: usize,
    level_count_before_compaction: usize,
    sst_count_ready: usize,
    level_count_ready: usize,
    io: IoCountersArtifact,
}

#[derive(Debug, Serialize)]
struct ScenarioSummaryArtifact {
    iterations: usize,
    total_elapsed_ns: u64,
    throughput: ThroughputSummary,
    latency_ns: LatencySummary,
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
    max: u64,
    mean: f64,
}

struct ScenarioMeasurement {
    iterations: usize,
    total_elapsed_ns: u64,
    latencies_ns: Vec<u64>,
    io: IoCountersArtifact,
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
        group.throughput(Throughput::Elements(usize_to_u64(scenario.rows_per_scan)));

        let db = scenario.db.clone();
        let runtime = Arc::clone(runtime);
        let scenario_id = scenario.scenario_id;
        group.bench_function(scenario_id, move |b| {
            let db = db.clone();
            let runtime = Arc::clone(&runtime);
            b.iter_custom(move |iters| {
                let started = Instant::now();
                runtime.block_on(async {
                    for _ in 0..iters {
                        match read_all_rows(&db).await {
                            Ok(rows) => {
                                black_box(rows);
                            }
                            Err(err) => {
                                panic!("scenario `{scenario_id}` read failed: {err}");
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
    enable_major_compaction: bool,
    io_probe: &IoProbe,
) -> Result<BenchmarkDb, BenchError> {
    let root_string = absolutize(root)?.to_string_lossy().into_owned();
    let fs = Arc::new(ProbedLocalFs::new(LocalFs {}, io_probe.clone()));
    let mut builder = DbBuilder::from_schema_key_name(Arc::clone(schema), "id")?
        .on_durable_fs(fs, root_string)?
        .wal_sync_policy(config.wal_sync_policy.clone())
        .with_minor_compaction(1, 0);

    if enable_major_compaction {
        let options = CompactionOptions::new()
            .periodic_tick(Duration::from_millis(config.compaction_periodic_tick_ms));
        builder = builder.with_compaction_options(options);
    }

    builder.open().await.map_err(BenchError::from)
}

pub(crate) async fn ingest_workload(
    db: &BenchmarkDb,
    schema: &SchemaRef,
    config: &ResolvedConfig,
) -> Result<(), BenchError> {
    for batch_idx in 0..config.ingest_batches {
        let batch = build_batch(schema, config, batch_idx)?;
        db.ingest(batch).await?;
    }
    Ok(())
}

fn build_batch(
    schema: &SchemaRef,
    config: &ResolvedConfig,
    batch_idx: usize,
) -> Result<RecordBatch, BenchError> {
    let mut ids = Vec::with_capacity(config.rows_per_batch);
    let mut values = Vec::with_capacity(config.rows_per_batch);

    for row_idx in 0..config.rows_per_batch {
        let global_idx = batch_idx
            .saturating_mul(config.rows_per_batch)
            .saturating_add(row_idx);
        let key_idx = deterministic_key_slot(global_idx, config.key_space, config.seed);
        ids.push(format!("k{key_idx:08}"));
        values.push(deterministic_value(global_idx, config.seed));
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

pub(crate) async fn latest_version_summary(db: &BenchmarkDb) -> Result<VersionSummary, BenchError> {
    let versions = db
        .list_versions(1)
        .await
        .map_err(|err| BenchError::Message(format!("list_versions failed: {err}")))?;
    let Some(version) = versions.into_iter().next() else {
        return Err(BenchError::Message(
            "list_versions returned no versions".to_string(),
        ));
    };
    Ok(VersionSummary {
        sst_count: version.sst_count,
        level_count: version.level_count,
    })
}

pub(crate) async fn wait_for_compaction(
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

pub(crate) async fn read_all_rows(db: &BenchmarkDb) -> Result<usize, BenchError> {
    let batches = db
        .scan()
        .collect()
        .await
        .map_err(|err| BenchError::Message(format!("scan collect failed: {err}")))?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
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
        let started = Instant::now();
        for _ in 0..iterations {
            let op_started = Instant::now();
            let rows = read_all_rows(&scenario.db).await?;
            black_box(rows);
            latencies_ns.push(duration_ns_u64(op_started.elapsed()));
        }
        Ok(ScenarioMeasurement {
            iterations,
            total_elapsed_ns: duration_ns_u64(started.elapsed()),
            latencies_ns,
            io: scenario.io_probe.snapshot(),
        })
    })
}

fn to_scenario_artifact(
    scenario: &ScenarioState,
    measurement: ScenarioMeasurement,
) -> ScenarioArtifact {
    let latency = latency_summary(&measurement.latencies_ns);
    let total_rows = scenario
        .rows_per_scan
        .saturating_mul(measurement.iterations);
    let total_elapsed_secs = if measurement.total_elapsed_ns == 0 {
        0.0
    } else {
        measurement.total_elapsed_ns as f64 / 1_000_000_000.0
    };
    let throughput = if total_elapsed_secs > 0.0 {
        ThroughputSummary {
            ops_per_sec: measurement.iterations as f64 / total_elapsed_secs,
            rows_per_sec: total_rows as f64 / total_elapsed_secs,
        }
    } else {
        ThroughputSummary {
            ops_per_sec: 0.0,
            rows_per_sec: 0.0,
        }
    };

    ScenarioArtifact {
        scenario_id: scenario.scenario_id.to_string(),
        scenario_name: scenario.scenario_name.to_string(),
        setup: ScenarioSetupArtifact {
            rows_per_scan: scenario.rows_per_scan,
            sst_count_before_compaction: scenario.version_before_compaction.sst_count,
            level_count_before_compaction: scenario.version_before_compaction.level_count,
            sst_count_ready: scenario.version_ready.sst_count,
            level_count_ready: scenario.version_ready.level_count,
            io: scenario.setup_io.clone(),
        },
        summary: ScenarioSummaryArtifact {
            iterations: measurement.iterations,
            total_elapsed_ns: measurement.total_elapsed_ns,
            throughput,
            latency_ns: latency,
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
