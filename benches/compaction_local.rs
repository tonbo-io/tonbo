#![cfg(feature = "tokio")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "compaction/common.rs"]
mod common;

use common::{
    BenchBackend, BenchError, CompactionProfile, CompactionSweepPoint, CompactionTuning,
    ObjectStoreBenchConfig, ResolvedConfig, ScenarioDimensionsArtifact, ScenarioState,
    ScenarioWorkload, VersionSummary, WriteWorkloadState, artifact_path, benchmark_schema,
    build_artifact, build_run_id, build_runtime, ingest_workload, latest_version_summary,
    latest_version_summary_if_any, open_benchmark_db, open_object_store_benchmark_db,
    print_directional_report, read_all_rows, run_criterion, scenario_root,
    wait_for_compaction_quiesced, wait_for_first_compaction_observed, write_artifact_json,
};

#[derive(Clone)]
struct ScenarioStorage {
    backend: BenchBackend,
    object_store: Option<ObjectStoreBenchConfig>,
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
    let run_id = build_run_id();
    let storage = match config.backend {
        BenchBackend::Local => ScenarioStorage {
            backend: BenchBackend::Local,
            object_store: None,
        },
        BenchBackend::ObjectStore => match ObjectStoreBenchConfig::from_env() {
            Ok(object_store) => ScenarioStorage {
                backend: BenchBackend::ObjectStore,
                object_store: Some(object_store),
            },
            Err(reason) => {
                eprintln!(
                    "tonbo benchmark: backend `object_store` requested but unsupported in this \
                     environment: {reason}"
                );
                eprintln!(
                    "tonbo benchmark: skipping run (set TONBO_BENCH_BACKEND=local or configure \
                     TONBO_S3_* variables)"
                );
                return;
            }
        },
    };

    let scenarios = match runtime.block_on(prepare_scenarios(&config, &run_id, &storage)) {
        Ok(scenarios) => scenarios,
        Err(err) => panic!("scenario preparation failed: {err}"),
    };
    if scenarios.is_empty() {
        eprintln!("tonbo benchmark: no scenarios were prepared; skipping run");
        return;
    }

    let artifact = match build_artifact(&config, &run_id, runtime.as_ref(), &scenarios) {
        Ok(artifact) => artifact,
        Err(err) => panic!("artifact measurement failed: {err}"),
    };
    let artifact_path = artifact_path(&run_id);
    if let Err(err) = runtime.block_on(write_artifact_json(&artifact_path, &artifact)) {
        panic!("failed to persist benchmark artifact: {err}");
    }
    eprintln!("tonbo benchmark artifact: {}", artifact_path.display());
    print_directional_report(&artifact, &config);

    run_criterion(c, &runtime, &scenarios, config.criterion_sample_size);
}

async fn prepare_scenarios(
    config: &ResolvedConfig,
    run_id: &str,
    storage: &ScenarioStorage,
) -> Result<Vec<ScenarioState>, BenchError> {
    let selected = selected_scenario_filter();
    let mut scenarios = Vec::new();
    if should_prepare_scenario(&selected, "read_baseline") {
        push_prepared_scenario(
            &mut scenarios,
            prepare_read_baseline(config, run_id, storage).await,
        )?;
    }
    if should_prepare_scenario(&selected, "read_after_first_compaction_observed") {
        push_prepared_scenario(
            &mut scenarios,
            prepare_read_after_first_compaction_observed(config, run_id, storage).await,
        )?;
    }
    if should_prepare_scenario(&selected, "read_compaction_quiesced") {
        push_prepared_scenario(
            &mut scenarios,
            prepare_read_compaction_quiesced(config, run_id, storage).await,
        )?;
    }

    let sweep_points = config.compaction_sweep_points();
    if config.enable_read_while_compaction {
        for point in &sweep_points {
            push_prepared_scenario(
                &mut scenarios,
                prepare_read_while_compaction(config, run_id, point, storage).await,
            )?;
        }
    }
    if config.enable_write_throughput_vs_compaction_frequency {
        for &periodic_tick_ms in &config.write_frequency_periodic_ticks_ms {
            for point in &sweep_points {
                push_prepared_scenario(
                    &mut scenarios,
                    prepare_write_throughput_vs_compaction_frequency(
                        config,
                        run_id,
                        point,
                        periodic_tick_ms,
                        storage,
                    )
                    .await,
                )?;
            }
        }
    }

    Ok(scenarios)
}

fn selected_scenario_filter() -> Option<HashSet<&'static str>> {
    const KNOWN_SCENARIOS: [&str; 5] = [
        "read_baseline",
        "read_after_first_compaction_observed",
        "read_compaction_quiesced",
        "read_while_compaction",
        "write_throughput_vs_compaction_frequency",
    ];
    let mut selected = HashSet::new();
    for arg in std::env::args().skip(1) {
        for scenario in KNOWN_SCENARIOS {
            if arg.contains(scenario) {
                selected.insert(scenario);
            }
        }
    }
    if selected.is_empty() {
        None
    } else {
        eprintln!(
            "tonbo benchmark: scenario filter active: {}",
            selected.iter().copied().collect::<Vec<_>>().join(", ")
        );
        Some(selected)
    }
}

fn should_prepare_scenario(
    selected: &Option<HashSet<&'static str>>,
    scenario_id: &'static str,
) -> bool {
    match selected {
        Some(selected) => selected.contains(scenario_id),
        None => true,
    }
}

#[derive(Default)]
struct PrepTimingReport {
    phases: Vec<(&'static str, u64)>,
}

impl PrepTimingReport {
    fn record(&mut self, phase: &'static str, started: Instant) {
        self.phases
            .push((phase, started.elapsed().as_nanos().min(u64::MAX as u128) as u64));
    }

    fn emit(&self, scenario_id: &'static str) {
        let total_ns: u128 = self.phases.iter().map(|(_, ns)| u128::from(*ns)).sum();
        let total_ms = total_ns as f64 / 1_000_000.0;
        eprintln!(
            "tonbo benchmark prep timing scenario={} total_ms={total_ms:.3}",
            scenario_id
        );
        for (phase, elapsed_ns) in &self.phases {
            let elapsed_ms = *elapsed_ns as f64 / 1_000_000.0;
            let share_pct = if total_ns > 0 {
                (*elapsed_ns as f64 / total_ns as f64) * 100.0
            } else {
                0.0
            };
            eprintln!(
                "tonbo benchmark prep timing scenario={} phase={} elapsed_ms={elapsed_ms:.3} share_pct={share_pct:.2}",
                scenario_id, phase
            );
        }
    }
}

fn push_prepared_scenario(
    scenarios: &mut Vec<ScenarioState>,
    prepared: Result<ScenarioState, BenchError>,
) -> Result<(), BenchError> {
    match prepared {
        Ok(scenario) => scenarios.push(scenario),
        Err(BenchError::ScenarioSkipped {
            scenario_id,
            reason,
        }) => {
            eprintln!("tonbo benchmark: skipping scenario `{scenario_id}`: {reason}");
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

async fn prepare_read_baseline(
    config: &ResolvedConfig,
    run_id: &str,
    storage: &ScenarioStorage,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "read_baseline";
    let scenario_variant_id = "default".to_string();
    let benchmark_id = scenario_id.to_string();
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();
    let mut timing = PrepTimingReport::default();
    let phase_started = Instant::now();
    let db = open_scenario_db(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;
    timing.record("open_db", phase_started);

    let phase_started = Instant::now();
    ingest_workload(&db, &schema, config).await?;
    timing.record("ingest_workload", phase_started);

    let Some(version_ready) = latest_version_summary_if_any(&db).await? else {
        return scenario_skipped(
            scenario_id,
            "ingest produced no manifest versions; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES",
        );
    };
    ensure_ssts_present(scenario_id, version_ready)?;
    let phase_started = Instant::now();
    let (rows_per_scan, _) = read_all_rows(&db).await?;
    timing.record("read_all_rows", phase_started);
    let setup_io = io_probe.snapshot();
    timing.emit(scenario_id);

    Ok(ScenarioState {
        scenario_id,
        scenario_name: "Read Baseline".to_string(),
        scenario_variant_id: scenario_variant_id.clone(),
        benchmark_id,
        workload: ScenarioWorkload::ReadOnly,
        dimensions: ScenarioDimensionsArtifact::baseline(
            scenario_variant_id,
            ScenarioWorkload::ReadOnly,
        ),
        db,
        io_probe,
        setup_io,
        rows_per_op_hint: rows_per_scan,
        version_before_compaction: version_ready,
        version_ready,
        write_state: None,
    })
}

async fn prepare_read_after_first_compaction_observed(
    config: &ResolvedConfig,
    run_id: &str,
    storage: &ScenarioStorage,
) -> Result<ScenarioState, BenchError> {
    prepare_read_compaction_state(
        config,
        run_id,
        storage,
        "read_after_first_compaction_observed",
        "Read After First Compaction Observed",
        false,
    )
    .await
}

async fn prepare_read_compaction_quiesced(
    config: &ResolvedConfig,
    run_id: &str,
    storage: &ScenarioStorage,
) -> Result<ScenarioState, BenchError> {
    prepare_read_compaction_state(
        config,
        run_id,
        storage,
        "read_compaction_quiesced",
        "Read Compaction Quiesced",
        true,
    )
    .await
}

async fn prepare_read_compaction_state(
    config: &ResolvedConfig,
    run_id: &str,
    storage: &ScenarioStorage,
    scenario_id: &'static str,
    scenario_name: &'static str,
    wait_for_quiesced: bool,
) -> Result<ScenarioState, BenchError> {
    let scenario_variant_id = "default".to_string();
    let benchmark_id = scenario_id.to_string();
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();
    let mut timing = PrepTimingReport::default();

    let phase_started = Instant::now();
    let ingest_only_db = open_scenario_db(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;
    timing.record("open_db_ingest_only", phase_started);
    let phase_started = Instant::now();
    ingest_workload(&ingest_only_db, &schema, config).await?;
    timing.record("ingest_workload", phase_started);
    let Some(version_before_compaction) = latest_version_summary_if_any(&ingest_only_db).await?
    else {
        return scenario_skipped(
            scenario_id,
            "ingest produced no manifest versions; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES",
        );
    };
    ensure_ssts_present(scenario_id, version_before_compaction)?;
    drop(ingest_only_db);

    let phase_started = Instant::now();
    let db = open_scenario_db(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &CompactionProfile::Default {
            periodic_tick_ms: config.compaction_periodic_tick_ms,
        },
        &io_probe,
    )
    .await?;
    timing.record("open_db_for_compaction", phase_started);
    let phase_started = Instant::now();
    if wait_for_quiesced {
        wait_for_compaction_quiesced(&db, version_before_compaction, config).await?;
    } else {
        wait_for_first_compaction_observed(&db, version_before_compaction, config).await?;
    }
    timing.record("wait_for_compaction_state", phase_started);
    let version_ready = latest_version_summary(&db).await?;
    let phase_started = Instant::now();
    let (rows_per_scan, _) = read_all_rows(&db).await?;
    timing.record("read_all_rows", phase_started);
    let setup_io = io_probe.snapshot();
    timing.emit(scenario_id);

    Ok(ScenarioState {
        scenario_id,
        scenario_name: scenario_name.to_string(),
        scenario_variant_id: scenario_variant_id.clone(),
        benchmark_id,
        workload: ScenarioWorkload::ReadOnly,
        dimensions: ScenarioDimensionsArtifact::baseline(
            scenario_variant_id,
            ScenarioWorkload::ReadOnly,
        ),
        db,
        io_probe,
        setup_io,
        rows_per_op_hint: rows_per_scan,
        version_before_compaction,
        version_ready,
        write_state: None,
    })
}

async fn prepare_read_while_compaction(
    config: &ResolvedConfig,
    run_id: &str,
    point: &CompactionSweepPoint,
    storage: &ScenarioStorage,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "read_while_compaction";
    let tuning = compaction_tuning(point, config.compaction_periodic_tick_ms);
    let scenario_variant_id = sweep_variant_id(point, tuning.periodic_tick_ms);
    let benchmark_id = format!("{scenario_id}__{scenario_variant_id}");
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();

    let ingest_only_db = open_scenario_db(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;
    ingest_workload(&ingest_only_db, &schema, config).await?;
    let Some(version_before_compaction) = latest_version_summary_if_any(&ingest_only_db).await?
    else {
        return scenario_skipped(
            scenario_id,
            "ingest produced no manifest versions; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES",
        );
    };
    ensure_ssts_present(scenario_id, version_before_compaction)?;
    drop(ingest_only_db);

    let db = open_scenario_db(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &CompactionProfile::Swept(tuning.clone()),
        &io_probe,
    )
    .await?;
    let Some(version_ready) = latest_version_summary_if_any(&db).await? else {
        return scenario_skipped(
            scenario_id,
            "reopened database has no manifest versions to benchmark",
        );
    };
    let (rows_per_scan, _) = read_all_rows(&db).await?;
    let setup_io = io_probe.snapshot();
    let write_state = WriteWorkloadState::new(
        Arc::clone(&schema),
        config.rows_per_batch,
        config.key_space,
        config.seed ^ 0xA11CE,
        u64::try_from(config.ingest_batches).unwrap_or(u64::MAX),
    );

    Ok(ScenarioState {
        scenario_id,
        scenario_name: format!(
            "Read While Compaction [l0_trigger={}, max_inputs={}, max_task_bytes={}, tick_ms={}]",
            tuning.l0_trigger,
            tuning.max_inputs,
            format_max_task_bytes(tuning.max_task_bytes),
            tuning.periodic_tick_ms
        ),
        scenario_variant_id: scenario_variant_id.clone(),
        benchmark_id,
        workload: ScenarioWorkload::ReadWhileCompaction,
        dimensions: ScenarioDimensionsArtifact::swept(
            scenario_variant_id,
            ScenarioWorkload::ReadWhileCompaction,
            &tuning,
        ),
        db,
        io_probe,
        setup_io,
        rows_per_op_hint: rows_per_scan,
        version_before_compaction,
        version_ready,
        write_state: Some(write_state),
    })
}

async fn prepare_write_throughput_vs_compaction_frequency(
    config: &ResolvedConfig,
    run_id: &str,
    point: &CompactionSweepPoint,
    periodic_tick_ms: u64,
    storage: &ScenarioStorage,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "write_throughput_vs_compaction_frequency";
    let tuning = compaction_tuning(point, periodic_tick_ms);
    let scenario_variant_id = sweep_variant_id(point, periodic_tick_ms);
    let benchmark_id = format!("{scenario_id}__{scenario_variant_id}");
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();

    let ingest_only_db = open_scenario_db(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;
    ingest_workload(&ingest_only_db, &schema, config).await?;
    let Some(version_before_compaction) = latest_version_summary_if_any(&ingest_only_db).await?
    else {
        return scenario_skipped(
            scenario_id,
            "ingest produced no manifest versions; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES",
        );
    };
    ensure_ssts_present(scenario_id, version_before_compaction)?;
    drop(ingest_only_db);

    let db = open_scenario_db(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &CompactionProfile::Swept(tuning.clone()),
        &io_probe,
    )
    .await?;
    let Some(version_ready) = latest_version_summary_if_any(&db).await? else {
        return scenario_skipped(
            scenario_id,
            "reopened database has no manifest versions to benchmark",
        );
    };
    let setup_io = io_probe.snapshot();
    let write_state = WriteWorkloadState::new(
        Arc::clone(&schema),
        config.rows_per_batch,
        config.key_space,
        config.seed ^ 0xBEE5,
        u64::try_from(config.ingest_batches).unwrap_or(u64::MAX),
    );

    Ok(ScenarioState {
        scenario_id,
        scenario_name: format!(
            "Write Throughput vs Compaction Frequency [l0_trigger={}, max_inputs={}, \
             max_task_bytes={}, tick_ms={}]",
            tuning.l0_trigger,
            tuning.max_inputs,
            format_max_task_bytes(tuning.max_task_bytes),
            tuning.periodic_tick_ms
        ),
        scenario_variant_id: scenario_variant_id.clone(),
        benchmark_id,
        workload: ScenarioWorkload::WriteThroughput,
        dimensions: ScenarioDimensionsArtifact::swept(
            scenario_variant_id,
            ScenarioWorkload::WriteThroughput,
            &tuning,
        ),
        db,
        io_probe,
        setup_io,
        rows_per_op_hint: config.rows_per_batch,
        version_before_compaction,
        version_ready,
        write_state: Some(write_state),
    })
}

async fn open_scenario_db(
    storage: &ScenarioStorage,
    schema: &arrow_schema::SchemaRef,
    run_id: &str,
    benchmark_id: &str,
    config: &ResolvedConfig,
    profile: &CompactionProfile,
    io_probe: &common::IoProbe,
) -> Result<common::BenchmarkDb, BenchError> {
    match storage.backend {
        BenchBackend::Local => {
            let root = scenario_root(run_id, benchmark_id);
            open_benchmark_db(schema, &root, config, profile, io_probe).await
        }
        BenchBackend::ObjectStore => {
            let Some(object_store) = &storage.object_store else {
                return scenario_skipped(
                    benchmark_id,
                    "object_store backend selected without resolved object-store config",
                );
            };
            let spec = object_store.object_spec(run_id, benchmark_id);
            open_object_store_benchmark_db(schema, &spec, config, profile, io_probe).await
        }
    }
}

fn compaction_tuning(point: &CompactionSweepPoint, periodic_tick_ms: u64) -> CompactionTuning {
    CompactionTuning {
        l0_trigger: point.l0_trigger,
        max_inputs: point.max_inputs,
        max_task_bytes: point.max_task_bytes,
        periodic_tick_ms,
    }
}

fn sweep_variant_id(point: &CompactionSweepPoint, periodic_tick_ms: u64) -> String {
    format!(
        "l0{}_max{}_task{}_tick{}ms",
        point.l0_trigger,
        point.max_inputs,
        format_max_task_bytes(point.max_task_bytes),
        periodic_tick_ms
    )
}

fn format_max_task_bytes(value: Option<usize>) -> String {
    match value {
        Some(bytes) => bytes.to_string(),
        None => "none".to_string(),
    }
}

fn ensure_ssts_present(scenario_id: &str, summary: VersionSummary) -> Result<(), BenchError> {
    if summary.sst_count == 0 {
        return scenario_skipped(
            scenario_id,
            "setup produced no SSTs; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES",
        );
    }
    Ok(())
}

fn scenario_skipped<T>(scenario_id: &str, reason: impl Into<String>) -> Result<T, BenchError> {
    Err(BenchError::ScenarioSkipped {
        scenario_id: scenario_id.to_string(),
        reason: reason.into(),
    })
}

criterion_group!(benches, compaction_local);
criterion_main!(benches);
