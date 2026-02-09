#![cfg(feature = "tokio")]

use std::{
    env,
    path::{Path, PathBuf},
    process,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use fusio::{
    Write,
    disk::LocalFs,
    executor::tokio::TokioExecutor,
    fs::{Fs, OpenOptions},
    path::{Path as FusioPath, PathPart},
};
use serde::Serialize;
use thiserror::Error;
use tokio::{runtime::Runtime, time::sleep};
use tonbo::db::{CompactionOptions, DB, DBError, DbBuildError, DbBuilder, WalSyncPolicy};

const BENCH_ID: &str = "compaction_local";
const BENCH_SCHEMA_VERSION: u32 = 1;

const DEFAULT_INGEST_BATCHES: usize = 640;
const DEFAULT_ROWS_PER_BATCH: usize = 64;
const DEFAULT_KEY_SPACE: usize = 2_048;
const DEFAULT_ARTIFACT_ITERATIONS: usize = 48;
const DEFAULT_CRITERION_SAMPLE_SIZE: usize = 20;
const DEFAULT_COMPACTION_WAIT_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_COMPACTION_POLL_INTERVAL_MS: u64 = 50;
const DEFAULT_COMPACTION_PERIODIC_TICK_MS: u64 = 200;
const DEFAULT_SEED: u64 = 584;

type BenchmarkDb = DB<LocalFs, TokioExecutor>;

#[derive(Debug, Error)]
enum BenchError {
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
struct ResolvedConfig {
    ingest_batches: usize,
    rows_per_batch: usize,
    key_space: usize,
    artifact_iterations: usize,
    criterion_sample_size: usize,
    compaction_wait_timeout_ms: u64,
    compaction_poll_interval_ms: u64,
    compaction_periodic_tick_ms: u64,
    seed: u64,
    wal_sync_policy: WalSyncPolicy,
    wal_sync_policy_name: String,
}

impl ResolvedConfig {
    fn from_env() -> Result<Self, BenchError> {
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

    fn artifact(&self) -> ResolvedConfigArtifact {
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
struct VersionSummary {
    sst_count: usize,
    level_count: usize,
}

#[derive(Clone)]
struct ScenarioState {
    scenario_id: &'static str,
    scenario_name: &'static str,
    db: BenchmarkDb,
    rows_per_scan: usize,
    version_before_compaction: VersionSummary,
    version_ready: VersionSummary,
}

#[derive(Debug, Serialize)]
struct BenchmarkArtifact {
    schema_version: u32,
    benchmark_id: &'static str,
    run_id: String,
    generated_at_unix_ms: u64,
    config: ResolvedConfigArtifact,
    scenarios: Vec<ScenarioArtifact>,
}

#[derive(Debug, Serialize)]
struct ResolvedConfigArtifact {
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
struct ScenarioArtifact {
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
}

#[derive(Debug, Serialize)]
struct ScenarioSummaryArtifact {
    iterations: usize,
    total_elapsed_ns: u64,
    throughput: ThroughputSummary,
    latency_ns: LatencySummary,
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
}

fn compaction_local(c: &mut Criterion) {
    let runtime = match build_runtime() {
        Ok(runtime) => Arc::new(runtime),
        Err(err) => panic!("failed to build tokio runtime for benchmark: {err}"),
    };
    let config = match ResolvedConfig::from_env() {
        Ok(config) => config,
        Err(err) => panic!("failed to resolve benchmark config: {err}"),
    };
    let run_id = format!("{}-{}", unix_epoch_ms(), process::id());

    let scenarios = match runtime.block_on(prepare_scenarios(&config, &run_id)) {
        Ok(scenarios) => scenarios,
        Err(err) => panic!("scenario preparation failed: {err}"),
    };

    let scenario_artifacts =
        match measure_for_artifact(runtime.as_ref(), &scenarios, config.artifact_iterations) {
            Ok(metrics) => metrics,
            Err(err) => panic!("artifact measurement failed: {err}"),
        };

    let artifact = BenchmarkArtifact {
        schema_version: BENCH_SCHEMA_VERSION,
        benchmark_id: BENCH_ID,
        run_id: run_id.clone(),
        generated_at_unix_ms: unix_epoch_ms(),
        config: config.artifact(),
        scenarios: scenario_artifacts,
    };
    let artifact_path = PathBuf::from("target")
        .join("tonbo-bench")
        .join(format!("{BENCH_ID}-{run_id}.json"));
    if let Err(err) = runtime.block_on(write_artifact_json(&artifact_path, &artifact)) {
        panic!("failed to persist benchmark artifact: {err}");
    }
    eprintln!("tonbo benchmark artifact: {}", artifact_path.display());

    run_criterion(c, &runtime, &scenarios, config.criterion_sample_size);
}

fn build_runtime() -> Result<Runtime, BenchError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| BenchError::Message(format!("tokio runtime build failed: {err}")))
}

fn run_criterion(
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

async fn prepare_scenarios(
    config: &ResolvedConfig,
    run_id: &str,
) -> Result<Vec<ScenarioState>, BenchError> {
    let baseline = prepare_read_baseline(config, run_id).await?;
    let post_compaction = prepare_read_post_compaction(config, run_id).await?;
    Ok(vec![baseline, post_compaction])
}

async fn prepare_read_baseline(
    config: &ResolvedConfig,
    run_id: &str,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "read_baseline";
    let scenario_name = "Read Baseline";
    let root = scenario_root(run_id, scenario_id);
    let schema = benchmark_schema();
    let db = open_benchmark_db(&schema, &root, config, false).await?;

    ingest_workload(&db, &schema, config).await?;

    let version_ready = latest_version_summary(&db).await?;
    if version_ready.sst_count == 0 {
        return Err(BenchError::Message(
            "read_baseline setup produced no SSTs; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES"
                .to_string(),
        ));
    }
    let rows_per_scan = read_all_rows(&db).await?;

    Ok(ScenarioState {
        scenario_id,
        scenario_name,
        db,
        rows_per_scan,
        version_before_compaction: version_ready,
        version_ready,
    })
}

async fn prepare_read_post_compaction(
    config: &ResolvedConfig,
    run_id: &str,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "read_post_compaction";
    let scenario_name = "Read Post Compaction";
    let root = scenario_root(run_id, scenario_id);
    let schema = benchmark_schema();

    let ingest_only_db = open_benchmark_db(&schema, &root, config, false).await?;
    ingest_workload(&ingest_only_db, &schema, config).await?;
    let version_before_compaction = latest_version_summary(&ingest_only_db).await?;
    if version_before_compaction.sst_count == 0 {
        return Err(BenchError::Message(
            "read_post_compaction setup produced no SSTs; increase \
             TONBO_COMPACTION_BENCH_INGEST_BATCHES"
                .to_string(),
        ));
    }
    drop(ingest_only_db);

    let db = open_benchmark_db(&schema, &root, config, true).await?;
    wait_for_compaction(&db, version_before_compaction, config).await?;
    let version_ready = latest_version_summary(&db).await?;
    let rows_per_scan = read_all_rows(&db).await?;

    Ok(ScenarioState {
        scenario_id,
        scenario_name,
        db,
        rows_per_scan,
        version_before_compaction,
        version_ready,
    })
}

fn scenario_root(run_id: &str, scenario_id: &str) -> PathBuf {
    PathBuf::from("target")
        .join("tonbo-bench")
        .join("workspaces")
        .join(run_id)
        .join(scenario_id)
}

fn benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

async fn open_benchmark_db(
    schema: &SchemaRef,
    root: &Path,
    config: &ResolvedConfig,
    enable_major_compaction: bool,
) -> Result<BenchmarkDb, BenchError> {
    let root_string = absolutize(root)?.to_string_lossy().into_owned();
    let mut builder = DbBuilder::from_schema_key_name(Arc::clone(schema), "id")?
        .on_disk(root_string)?
        .wal_sync_policy(config.wal_sync_policy.clone())
        .with_minor_compaction(1, 0);

    if enable_major_compaction {
        let options = CompactionOptions::new()
            .periodic_tick(Duration::from_millis(config.compaction_periodic_tick_ms));
        builder = builder.with_compaction_options(options);
    }

    builder.open().await.map_err(BenchError::from)
}

async fn ingest_workload(
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
    let rotated = global_idx.rotate_left((seed as u32) % usize::BITS);
    rotated % key_space
}

fn deterministic_value(global_idx: usize, seed: u64) -> i64 {
    let mixed = (global_idx as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(seed ^ 0x9E37_79B9_7F4A_7C15);
    (mixed >> 1) as i64
}

async fn latest_version_summary(db: &BenchmarkDb) -> Result<VersionSummary, BenchError> {
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

async fn wait_for_compaction(
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

async fn read_all_rows(db: &BenchmarkDb) -> Result<usize, BenchError> {
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
        },
        summary: ScenarioSummaryArtifact {
            iterations: measurement.iterations,
            total_elapsed_ns: measurement.total_elapsed_ns,
            throughput,
            latency_ns: latency,
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

async fn write_artifact_json(path: &Path, artifact: &BenchmarkArtifact) -> Result<(), BenchError> {
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

criterion_group!(benches, compaction_local);
criterion_main!(benches);
