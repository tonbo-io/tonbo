#![cfg(feature = "tokio")]

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "compaction/common.rs"]
mod common;

use common::{
    BenchBackend, BenchError, CompactionProfile, CompactionSweepPoint, CompactionTuning,
    ObjectStoreBenchConfig, ResolvedConfig, ScenarioDimensionsArtifact, ScenarioState,
    ScenarioWorkload, StorageVolumeArtifact, SwmrReaderClass, SwmrReaderExpectationArtifact,
    SwmrWorkloadParams, SwmrWorkloadState, VersionSummary, WriteWorkloadState, artifact_path,
    benchmark_schema, build_artifact, build_run_id, build_runtime, cleanup_local_storage_volume,
    cleanup_object_store_storage_volume, ingest_workload, latest_version_summary,
    latest_version_summary_if_any, open_benchmark_db, open_object_store_benchmark_db,
    preload_swmr_workload, print_directional_report, read_all_rows, run_criterion, scenario_root,
    snapshot_local_storage_volume, snapshot_object_store_storage_volume, swmr_benchmark_schema,
    swmr_light_projection_schema, swmr_reader_observation_for_snapshot,
    wait_for_compaction_quiesced, wait_for_first_compaction_observed, write_artifact_json,
};

#[derive(Clone)]
struct ScenarioStorage {
    backend: BenchBackend,
    object_store: Option<ObjectStoreBenchConfig>,
}

#[derive(Clone, Copy)]
struct SwmrBenchConfig {
    logical_target_bytes: u64,
    rows_per_batch: usize,
    payload_bytes: usize,
    preload_fraction_pct: usize,
    light_scan_limit: usize,
    heavy_scan_limit: usize,
}

impl SwmrBenchConfig {
    fn from_env() -> Result<Self, BenchError> {
        let logical_target_bytes = match std::env::var("TONBO_SWMR_BENCH_LOGICAL_BYTES") {
            Ok(raw) => raw.parse::<u64>().map_err(|_| BenchError::InvalidEnv {
                name: "TONBO_SWMR_BENCH_LOGICAL_BYTES",
                value: raw,
            })?,
            Err(std::env::VarError::NotPresent) => {
                let gb_raw = std::env::var("TONBO_SWMR_BENCH_LOGICAL_GB")
                    .unwrap_or_else(|_| "1".to_string());
                let gb = gb_raw.parse::<u64>().map_err(|_| BenchError::InvalidEnv {
                    name: "TONBO_SWMR_BENCH_LOGICAL_GB",
                    value: gb_raw,
                })?;
                gb.saturating_mul(1024 * 1024 * 1024)
            }
            Err(err) => {
                return Err(BenchError::Message(format!(
                    "failed reading TONBO_SWMR_BENCH_LOGICAL_BYTES: {err}"
                )));
            }
        };
        let rows_per_batch = env_usize_local("TONBO_SWMR_BENCH_ROWS_PER_BATCH", 256)?;
        let payload_bytes = env_usize_local("TONBO_SWMR_BENCH_PAYLOAD_BYTES", 4096)?;
        let preload_fraction_pct = env_usize_local("TONBO_SWMR_BENCH_PRELOAD_FRACTION_PCT", 90)?;
        let light_scan_limit = env_usize_local("TONBO_SWMR_BENCH_LIGHT_SCAN_LIMIT", 256)?;
        let heavy_scan_limit = env_usize_local("TONBO_SWMR_BENCH_HEAVY_SCAN_LIMIT", 4096)?;
        if rows_per_batch == 0 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_SWMR_BENCH_ROWS_PER_BATCH",
                value: "must be > 0".to_string(),
            });
        }
        if preload_fraction_pct > 99 {
            return Err(BenchError::InvalidEnv {
                name: "TONBO_SWMR_BENCH_PRELOAD_FRACTION_PCT",
                value: "must be in 0..=99".to_string(),
            });
        }
        Ok(Self {
            logical_target_bytes,
            rows_per_batch,
            payload_bytes,
            preload_fraction_pct,
            light_scan_limit,
            heavy_scan_limit,
        })
    }

    fn estimated_row_bytes(self) -> u64 {
        32u64.saturating_add(u64::try_from(self.payload_bytes).unwrap_or(u64::MAX))
    }

    fn total_rows(self) -> usize {
        let row_bytes = self.estimated_row_bytes().max(1);
        let rows = self.logical_target_bytes.saturating_add(row_bytes - 1) / row_bytes;
        usize::try_from(rows).unwrap_or(usize::MAX)
    }

    fn total_batches(self) -> usize {
        let total_rows = self.total_rows();
        total_rows.saturating_add(self.rows_per_batch - 1) / self.rows_per_batch
    }

    fn preload_batches(self) -> usize {
        let total_batches = self.total_batches().max(1);
        let preload = total_batches.saturating_mul(self.preload_fraction_pct) / 100;
        preload.clamp(1, total_batches.saturating_sub(1).max(1))
    }

    fn steady_batches(self) -> usize {
        self.total_batches()
            .saturating_sub(self.preload_batches())
            .max(1)
    }
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

    if let Err(err) = runtime.block_on(cleanup_scenarios(&scenarios, &storage, &run_id)) {
        eprintln!("tonbo benchmark cleanup warning: {err}");
    }
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
    if should_prepare_scenario(&selected, "swmr_gb_scale_mixed") {
        push_prepared_scenario(
            &mut scenarios,
            prepare_swmr_gb_scale_mixed(config, run_id, storage).await,
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
    const KNOWN_SCENARIOS: [&str; 6] = [
        "read_baseline",
        "read_after_first_compaction_observed",
        "read_compaction_quiesced",
        "read_while_compaction",
        "write_throughput_vs_compaction_frequency",
        "swmr_gb_scale_mixed",
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
        self.phases.push((
            phase,
            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        ));
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
                "tonbo benchmark prep timing scenario={} phase={} elapsed_ms={elapsed_ms:.3} \
                 share_pct={share_pct:.2}",
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
    let volume_before_compaction = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;

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
    let volume_ready = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
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
        volume_before_compaction,
        volume_ready,
        write_state: None,
        swmr_state: None,
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
    let volume_before_compaction = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
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
    drop(db);

    let phase_started = Instant::now();
    let expected_rows_per_scan = expected_benchmark_visible_rows(config);
    let (db, rows_per_scan) = reopen_measurement_db_until_visible(
        storage,
        &schema,
        run_id,
        &benchmark_id,
        config,
        &io_probe,
        expected_rows_per_scan,
    )
    .await?;
    timing.record("read_all_rows", phase_started);
    let volume_ready = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
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
        volume_before_compaction,
        volume_ready,
        write_state: None,
        swmr_state: None,
    })
}

fn expected_benchmark_visible_rows(config: &ResolvedConfig) -> usize {
    let total_rows = config.ingest_batches.saturating_mul(config.rows_per_batch);
    let mut keys = HashSet::with_capacity(config.key_space.min(total_rows));
    for global_idx in 0..total_rows {
        keys.insert(common::deterministic_key_slot(
            global_idx,
            config.key_space,
            config.seed,
        ));
    }
    keys.len()
}

async fn reopen_measurement_db_until_visible(
    storage: &ScenarioStorage,
    schema: &arrow_schema::SchemaRef,
    run_id: &str,
    benchmark_id: &str,
    config: &ResolvedConfig,
    io_probe: &common::IoProbe,
    expected_rows_per_scan: usize,
) -> Result<(common::BenchmarkDb, usize), BenchError> {
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(config.compaction_wait_timeout_ms.max(5_000));
    let poll = Duration::from_millis(config.compaction_poll_interval_ms.max(50));
    loop {
        let db = open_scenario_db(
            storage,
            schema,
            run_id,
            benchmark_id,
            config,
            &CompactionProfile::Disabled,
            io_probe,
        )
        .await?;
        let (rows_per_scan, _) = read_all_rows(&db).await?;
        if rows_per_scan == expected_rows_per_scan {
            return Ok((db, rows_per_scan));
        }
        drop(db);

        if tokio::time::Instant::now() >= deadline {
            return Err(BenchError::Message(format!(
                "reopened measurement db never reached expected visibility: rows={} \
                 expected_rows={}",
                rows_per_scan, expected_rows_per_scan
            )));
        }
        tokio::time::sleep(poll).await;
    }
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
    let volume_before_compaction = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
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
    let volume_ready = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
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
        volume_before_compaction,
        volume_ready,
        write_state: Some(write_state),
        swmr_state: None,
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
    let volume_before_compaction = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
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
    let volume_ready = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
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
        volume_before_compaction,
        volume_ready,
        write_state: Some(write_state),
        swmr_state: None,
    })
}

async fn prepare_swmr_gb_scale_mixed(
    config: &ResolvedConfig,
    run_id: &str,
    storage: &ScenarioStorage,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "swmr_gb_scale_mixed";
    let swmr = SwmrBenchConfig::from_env()?;
    let scenario_variant_id = format!(
        "logical{}gb_payload{}_rpb{}",
        swmr.logical_target_bytes / (1024 * 1024 * 1024),
        swmr.payload_bytes,
        swmr.rows_per_batch
    );
    let benchmark_id = format!("{scenario_id}__{scenario_variant_id}");
    let schema = swmr_benchmark_schema();
    let light_projection = swmr_light_projection_schema();
    let io_probe = common::IoProbe::default();
    let mut timing = PrepTimingReport::default();

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
    timing.record("open_db", phase_started);

    let preload_batches = swmr.preload_batches();
    let steady_batches = swmr.steady_batches();
    let writer_batches_per_step = steady_batches
        .saturating_add(config.artifact_iterations.saturating_sub(1))
        / config.artifact_iterations.max(1);

    let phase_started = Instant::now();
    preload_swmr_workload(
        &db,
        &schema,
        swmr.rows_per_batch,
        swmr.payload_bytes,
        preload_batches,
        config.seed ^ 0x0055_7A11,
    )
    .await?;
    timing.record("preload", phase_started);

    let volume_before_compaction = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
    let Some(version_before_compaction) = latest_version_summary_if_any(&db).await? else {
        return scenario_skipped(
            scenario_id,
            "preload produced no manifest versions; increase TONBO_SWMR_BENCH_LOGICAL_GB or \
             payload",
        );
    };
    ensure_ssts_present(scenario_id, version_before_compaction)?;
    let pinned_manifest_version = latest_version_info(&db).await?;
    let pinned_snapshot = match &db {
        common::BenchmarkDb::Local(inner) => inner.begin_snapshot().await,
        common::BenchmarkDb::ObjectStore(inner) => inner.begin_snapshot().await,
    }
    .map_err(|err| BenchError::Message(format!("begin_snapshot failed: {err}")))?;
    let mut reader_expectations = Vec::new();
    let mut manifest_reconstruction_observations = Vec::new();
    let reconstructed_snapshot = match &db {
        common::BenchmarkDb::Local(inner) => {
            inner.snapshot_at(pinned_manifest_version.timestamp).await
        }
        common::BenchmarkDb::ObjectStore(inner) => {
            inner.snapshot_at(pinned_manifest_version.timestamp).await
        }
    }
    .map_err(|err| {
        BenchError::Message(format!(
            "swmr pinned manifest reconstruction failed at ts {}: {err}",
            pinned_manifest_version.timestamp.get()
        ))
    })?;
    for class in [
        SwmrReaderClass::HeadLight,
        SwmrReaderClass::HeadHeavy,
        SwmrReaderClass::PinnedLight,
        SwmrReaderClass::PinnedHeavy,
    ] {
        let expected_observation = swmr_reader_observation_for_snapshot(
            &db,
            &pinned_snapshot,
            class,
            &light_projection,
            swmr.light_scan_limit,
            swmr.heavy_scan_limit,
        )
        .await?;
        reader_expectations.push(SwmrReaderExpectationArtifact {
            class,
            key_band: class.key_band(),
            validation_model: class.validation_model(),
            expected_rows_per_scan: expected_observation.rows_per_scan,
            expected_first_key: expected_observation.first_key,
            expected_last_key: expected_observation.last_key,
            expected_key_fingerprint: expected_observation.key_fingerprint,
        });

        let reconstructed_observation = swmr_reader_observation_for_snapshot(
            &db,
            &reconstructed_snapshot,
            class,
            &light_projection,
            swmr.light_scan_limit,
            swmr.heavy_scan_limit,
        )
        .await?;
        let expectation = reader_expectations.last().ok_or_else(|| {
            BenchError::Message(format!(
                "missing setup expectation immediately after building `{}`",
                class.as_str()
            ))
        })?;
        let rows_match_expected =
            reconstructed_observation.rows_per_scan == expectation.expected_rows_per_scan;
        let first_key_matches_expected =
            reconstructed_observation.first_key.as_ref() == expectation.expected_first_key.as_ref();
        let last_key_matches_expected =
            reconstructed_observation.last_key.as_ref() == expectation.expected_last_key.as_ref();
        let fingerprint_matches_expected =
            reconstructed_observation.key_fingerprint == expectation.expected_key_fingerprint;
        let valid = match expectation.validation_model {
            common::SwmrReaderValidationModel::ExactShapeStable => {
                rows_match_expected
                    && reconstructed_observation.all_keys_in_expected_band
                    && first_key_matches_expected
                    && last_key_matches_expected
                    && fingerprint_matches_expected
            }
            common::SwmrReaderValidationModel::CountAndKeyBand => {
                rows_match_expected && reconstructed_observation.all_keys_in_expected_band
            }
        };
        manifest_reconstruction_observations.push(common::SwmrReaderObservationArtifact {
            class,
            rows_per_scan: reconstructed_observation.rows_per_scan,
            first_key: reconstructed_observation.first_key,
            last_key: reconstructed_observation.last_key,
            key_fingerprint: reconstructed_observation.key_fingerprint,
            rows_match_expected,
            all_keys_in_expected_band: reconstructed_observation.all_keys_in_expected_band,
            first_key_matches_expected,
            last_key_matches_expected,
            fingerprint_matches_expected,
            valid,
        });
    }
    for expectation in &reader_expectations {
        if expectation.expected_rows_per_scan == 0 {
            return Err(BenchError::Message(format!(
                "swmr setup invalid: reader `{}` matched 0 rows in the held pinned snapshot at ts \
                 {}",
                expectation.class.as_str(),
                pinned_snapshot.read_timestamp().get()
            )));
        }
    }
    let version_ready = latest_version_summary(&db).await?;
    let volume_ready = snapshot_scenario_volume(storage, run_id, &benchmark_id).await?;
    let setup_io = io_probe.snapshot();
    timing.emit(scenario_id);

    let swmr_state = SwmrWorkloadState::new(
        Arc::clone(&schema),
        Arc::clone(&light_projection),
        pinned_snapshot,
        pinned_manifest_version,
        reader_expectations,
        manifest_reconstruction_observations,
        SwmrWorkloadParams {
            rows_per_batch: swmr.rows_per_batch,
            payload_bytes: swmr.payload_bytes,
            preload_batches,
            steady_batches,
            writer_batches_per_step: writer_batches_per_step.max(1),
            seed: config.seed ^ 0x5157_4D52,
        },
    )
    .with_scan_limits(swmr.light_scan_limit, swmr.heavy_scan_limit)
    .with_logical_target_bytes(swmr.logical_target_bytes);
    let rows_per_op_hint = swmr
        .rows_per_batch
        .saturating_mul(writer_batches_per_step.max(1))
        .saturating_add(swmr.light_scan_limit.saturating_mul(2))
        .saturating_add(swmr.heavy_scan_limit.saturating_mul(2));

    Ok(ScenarioState {
        scenario_id,
        scenario_name: format!(
            "SWMR GB Scale Mixed [logical_target_gb={}, preload_batches={}, steady_batches={}]",
            swmr.logical_target_bytes / (1024 * 1024 * 1024),
            preload_batches,
            steady_batches
        ),
        scenario_variant_id: scenario_variant_id.clone(),
        benchmark_id,
        workload: ScenarioWorkload::SwmrMixed,
        dimensions: ScenarioDimensionsArtifact::baseline(
            scenario_variant_id,
            ScenarioWorkload::SwmrMixed,
        ),
        db,
        io_probe,
        setup_io,
        rows_per_op_hint,
        version_before_compaction,
        version_ready,
        volume_before_compaction,
        volume_ready,
        write_state: None,
        swmr_state: Some(swmr_state),
    })
}

async fn latest_version_info(db: &common::BenchmarkDb) -> Result<tonbo::db::Version, BenchError> {
    let versions = match db {
        common::BenchmarkDb::Local(inner) => inner.list_versions(1).await,
        common::BenchmarkDb::ObjectStore(inner) => inner.list_versions(1).await,
    }
    .map_err(|err| BenchError::Message(format!("list_versions failed: {err}")))?;
    versions
        .into_iter()
        .next()
        .ok_or_else(|| BenchError::Message("list_versions returned no versions".to_string()))
}

async fn cleanup_scenarios(
    scenarios: &[ScenarioState],
    storage: &ScenarioStorage,
    run_id: &str,
) -> Result<(), BenchError> {
    for scenario in scenarios {
        match storage.backend {
            BenchBackend::Local => {
                let root = scenario_root(run_id, &scenario.benchmark_id);
                cleanup_local_storage_volume(&root).await?;
            }
            BenchBackend::ObjectStore => {
                let Some(object_store) = &storage.object_store else {
                    return Err(BenchError::Message(
                        "object_store backend selected without resolved object-store config for \
                         cleanup"
                            .to_string(),
                    ));
                };
                let spec = object_store.object_spec(run_id, &scenario.benchmark_id);
                cleanup_object_store_storage_volume(&spec).await?;
            }
        }
    }
    Ok(())
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

async fn snapshot_scenario_volume(
    storage: &ScenarioStorage,
    run_id: &str,
    benchmark_id: &str,
) -> Result<StorageVolumeArtifact, BenchError> {
    match storage.backend {
        BenchBackend::Local => {
            let root = scenario_root(run_id, benchmark_id);
            snapshot_local_storage_volume(&root).await
        }
        BenchBackend::ObjectStore => {
            let Some(object_store) = &storage.object_store else {
                return Err(BenchError::Message(format!(
                    "object_store backend selected without resolved object-store config for \
                     benchmark `{benchmark_id}`"
                )));
            };
            let spec = object_store.object_spec(run_id, benchmark_id);
            snapshot_object_store_storage_volume(&spec).await
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

fn env_usize_local(name: &'static str, default: usize) -> Result<usize, BenchError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .map_err(|_| BenchError::InvalidEnv { name, value: raw }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(BenchError::Message(format!(
            "failed reading environment variable `{name}`: {err}"
        ))),
    }
}

criterion_group!(benches, compaction_local);
criterion_main!(benches);
