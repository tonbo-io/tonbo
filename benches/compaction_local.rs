#![cfg(feature = "tokio")]

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "compaction/common.rs"]
mod common;

use common::{
    BenchError, ResolvedConfig, ScenarioState, artifact_path, benchmark_schema, build_artifact,
    build_run_id, build_runtime, ingest_workload, latest_version_summary, open_benchmark_db,
    read_all_rows, run_criterion, scenario_root, wait_for_compaction, write_artifact_json,
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
    let io_probe = common::IoProbe::default();
    let db = open_benchmark_db(&schema, &root, config, false, &io_probe).await?;

    ingest_workload(&db, &schema, config).await?;

    let version_ready = latest_version_summary(&db).await?;
    if version_ready.sst_count == 0 {
        return Err(BenchError::Message(
            "read_baseline setup produced no SSTs; increase TONBO_COMPACTION_BENCH_INGEST_BATCHES"
                .to_string(),
        ));
    }
    let rows_per_scan = read_all_rows(&db).await?;
    let setup_io = io_probe.snapshot();

    Ok(ScenarioState {
        scenario_id,
        scenario_name,
        db,
        io_probe,
        setup_io,
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
    let io_probe = common::IoProbe::default();

    let ingest_only_db = open_benchmark_db(&schema, &root, config, false, &io_probe).await?;
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

    let db = open_benchmark_db(&schema, &root, config, true, &io_probe).await?;
    wait_for_compaction(&db, version_before_compaction, config).await?;
    let version_ready = latest_version_summary(&db).await?;
    let rows_per_scan = read_all_rows(&db).await?;
    let setup_io = io_probe.snapshot();

    Ok(ScenarioState {
        scenario_id,
        scenario_name,
        db,
        io_probe,
        setup_io,
        rows_per_scan,
        version_before_compaction,
        version_ready,
    })
}

criterion_group!(benches, compaction_local);
criterion_main!(benches);
