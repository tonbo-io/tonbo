#![cfg(feature = "tokio")]

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "compaction/common.rs"]
mod common;

use common::{
    BenchError, CompactionProfile, CompactionSweepPoint, CompactionTuning, ResolvedConfig,
    ScenarioDimensionsArtifact, ScenarioState, ScenarioWorkload, VersionSummary,
    WriteWorkloadState, artifact_path, benchmark_schema, build_artifact, build_run_id,
    build_runtime, ingest_workload, latest_version_summary, open_benchmark_db, read_all_rows,
    run_criterion, scenario_root, wait_for_compaction, write_artifact_json,
};

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

    let scenarios = match runtime.block_on(prepare_scenarios(&config, &run_id)) {
        Ok(scenarios) => scenarios,
        Err(err) => panic!("scenario preparation failed: {err}"),
    };

    let artifact = match build_artifact(&config, &run_id, runtime.as_ref(), &scenarios) {
        Ok(artifact) => artifact,
        Err(err) => panic!("artifact measurement failed: {err}"),
    };
    let artifact_path = artifact_path(&run_id);
    if let Err(err) = runtime.block_on(write_artifact_json(&artifact_path, &artifact)) {
        panic!("failed to persist benchmark artifact: {err}");
    }
    eprintln!("tonbo benchmark artifact: {}", artifact_path.display());

    run_criterion(c, &runtime, &scenarios, config.criterion_sample_size);
}

async fn prepare_scenarios(
    config: &ResolvedConfig,
    run_id: &str,
) -> Result<Vec<ScenarioState>, BenchError> {
    let mut scenarios = Vec::new();
    scenarios.push(prepare_read_baseline(config, run_id).await?);
    scenarios.push(prepare_read_post_compaction(config, run_id).await?);

    let sweep_points = config.compaction_sweep_points();
    if config.enable_read_while_compaction {
        for point in &sweep_points {
            let scenario = prepare_read_while_compaction(config, run_id, point).await?;
            scenarios.push(scenario);
        }
    }
    if config.enable_write_throughput_vs_compaction_frequency {
        for &periodic_tick_ms in &config.write_frequency_periodic_ticks_ms {
            for point in &sweep_points {
                let scenario = prepare_write_throughput_vs_compaction_frequency(
                    config,
                    run_id,
                    point,
                    periodic_tick_ms,
                )
                .await?;
                scenarios.push(scenario);
            }
        }
    }

    Ok(scenarios)
}

async fn prepare_read_baseline(
    config: &ResolvedConfig,
    run_id: &str,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "read_baseline";
    let scenario_variant_id = "default".to_string();
    let benchmark_id = scenario_id.to_string();
    let root = scenario_root(run_id, &benchmark_id);
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();
    let db = open_benchmark_db(
        &schema,
        &root,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;

    ingest_workload(&db, &schema, config).await?;

    let version_ready = latest_version_summary(&db).await?;
    ensure_ssts_present(scenario_id, version_ready)?;
    let rows_per_scan = read_all_rows(&db).await?;
    let setup_io = io_probe.snapshot();

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

async fn prepare_read_post_compaction(
    config: &ResolvedConfig,
    run_id: &str,
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "read_post_compaction";
    let scenario_variant_id = "default".to_string();
    let benchmark_id = scenario_id.to_string();
    let root = scenario_root(run_id, &benchmark_id);
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();

    let ingest_only_db = open_benchmark_db(
        &schema,
        &root,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;
    ingest_workload(&ingest_only_db, &schema, config).await?;
    let version_before_compaction = latest_version_summary(&ingest_only_db).await?;
    ensure_ssts_present(scenario_id, version_before_compaction)?;
    drop(ingest_only_db);

    let db = open_benchmark_db(
        &schema,
        &root,
        config,
        &CompactionProfile::Default {
            periodic_tick_ms: config.compaction_periodic_tick_ms,
        },
        &io_probe,
    )
    .await?;
    wait_for_compaction(&db, version_before_compaction, config).await?;
    let version_ready = latest_version_summary(&db).await?;
    let rows_per_scan = read_all_rows(&db).await?;
    let setup_io = io_probe.snapshot();

    Ok(ScenarioState {
        scenario_id,
        scenario_name: "Read Post Compaction".to_string(),
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
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "read_while_compaction";
    let tuning = compaction_tuning(point, config.compaction_periodic_tick_ms);
    let scenario_variant_id = sweep_variant_id(point, tuning.periodic_tick_ms);
    let benchmark_id = format!("{scenario_id}__{scenario_variant_id}");
    let root = scenario_root(run_id, &benchmark_id);
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();

    let ingest_only_db = open_benchmark_db(
        &schema,
        &root,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;
    ingest_workload(&ingest_only_db, &schema, config).await?;
    let version_before_compaction = latest_version_summary(&ingest_only_db).await?;
    ensure_ssts_present(scenario_id, version_before_compaction)?;
    drop(ingest_only_db);

    let db = open_benchmark_db(
        &schema,
        &root,
        config,
        &CompactionProfile::Swept(tuning.clone()),
        &io_probe,
    )
    .await?;
    let version_ready = latest_version_summary(&db).await?;
    let rows_per_scan = read_all_rows(&db).await?;
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
) -> Result<ScenarioState, BenchError> {
    let scenario_id = "write_throughput_vs_compaction_frequency";
    let tuning = compaction_tuning(point, periodic_tick_ms);
    let scenario_variant_id = sweep_variant_id(point, periodic_tick_ms);
    let benchmark_id = format!("{scenario_id}__{scenario_variant_id}");
    let root = scenario_root(run_id, &benchmark_id);
    let schema = benchmark_schema();
    let io_probe = common::IoProbe::default();

    let ingest_only_db = open_benchmark_db(
        &schema,
        &root,
        config,
        &CompactionProfile::Disabled,
        &io_probe,
    )
    .await?;
    ingest_workload(&ingest_only_db, &schema, config).await?;
    let version_before_compaction = latest_version_summary(&ingest_only_db).await?;
    ensure_ssts_present(scenario_id, version_before_compaction)?;
    drop(ingest_only_db);

    let db = open_benchmark_db(
        &schema,
        &root,
        config,
        &CompactionProfile::Swept(tuning.clone()),
        &io_probe,
    )
    .await?;
    let version_ready = latest_version_summary(&db).await?;
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
        return Err(BenchError::Message(format!(
            "{scenario_id} setup produced no SSTs; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES"
        )));
    }
    Ok(())
}

criterion_group!(benches, compaction_local);
criterion_main!(benches);
