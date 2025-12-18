//! Scenario benchmarks entry point (Phase 2).
//! See docs/rfcs/0012-performance-benchmarking.md for design constraints.

use std::{path::PathBuf, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use clap::Parser;

mod harness;
use harness::diagnostics::diagnostics_config;
#[path = "scenarios/mixed.rs"]
mod mixed;
#[path = "scenarios/read_only.rs"]
mod read_only;
#[path = "scenarios/write_only.rs"]
mod write_only;

use harness::{BackendRun, BenchConfig, BenchResult, BenchResultWriter, Workload, WorkloadKind};

#[derive(Debug, Parser)]
struct Args {
    /// Path to YAML config.
    #[arg(long)]
    config: PathBuf,
}

fn bench_target_from_path(path: &PathBuf) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "bench".to_string())
}

pub struct ScenarioContext<'a> {
    pub config: &'a BenchConfig,
    pub workload: &'a Workload,
    pub backend: &'a BackendRun,
    pub bench_target: &'a str,
    pub schema: arrow_schema::SchemaRef,
    pub diagnostics: harness::DiagnosticsCollector,
}

pub struct ScenarioReport {
    pub workload_type: &'static str,
    pub parameters: serde_json::Value,
    pub metrics: serde_json::Value,
    pub diagnostics: Option<harness::DiagnosticsOutput>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    let config = BenchConfig::from_yaml_file(&args.config)?;
    let bench_target = bench_target_from_path(&args.config);
    let workload = Workload::from_config(&config)
        .ok_or_else(|| anyhow::anyhow!("unsupported workload: {}", config.workload.name))?;

    if workload.concurrency != 1 {
        anyhow::bail!("concurrency > 1 not implemented");
    }

    let backend_run = BackendRun::new(&config).await?;
    let diagnostics_cfg = diagnostics_config(&config);
    let diagnostics = harness::DiagnosticsCollector::from_config(diagnostics_cfg);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Binary, false),
    ]));
    let ctx = ScenarioContext {
        config: &config,
        workload: &workload,
        backend: &backend_run,
        bench_target: &bench_target,
        schema,
        diagnostics,
    };

    let run_result = match workload.kind {
        WorkloadKind::SequentialWrite => write_only::run(ctx).await,
        WorkloadKind::ReadOnly => read_only::run(ctx).await,
        WorkloadKind::Mixed => mixed::run(ctx).await,
    };

    let cleanup_result = backend_run.cleanup().await;
    if let Err(err) = cleanup_result {
        eprintln!("bench backend cleanup failed: {err}");
    }

    run_result
}

fn parse_args() -> Args {
    let filtered: Vec<String> = std::env::args().filter(|arg| arg != "--bench").collect();
    Args::parse_from(filtered)
}

pub fn emit_result(
    backend_run: &BackendRun,
    config: &BenchConfig,
    bench_target: &str,
    report: ScenarioReport,
) -> anyhow::Result<()> {
    let git_commit = std::env::var("GIT_COMMIT").ok();
    let storage_substrate = backend_run.storage_substrate();
    let result = BenchResult {
        run_id: backend_run.run_id().to_string(),
        bench_target: bench_target.to_string(),
        storage_substrate: storage_substrate.clone(),
        benchmark_name: config.workload.name.clone(),
        benchmark_type: "scenario".into(),
        backend: backend_run.backend_kind().into(),
        backend_details: Some(backend_run.backend_details()),
        workload_type: Some(report.workload_type.into()),
        parameters: report.parameters,
        metrics: report.metrics,
        git_commit,
        diagnostics: report.diagnostics,
    };

    let writer = BenchResultWriter::new(
        &backend_run.result_root,
        backend_run.run_id(),
        bench_target,
        &storage_substrate,
    )?;
    let path = writer.write(&config.workload.name, &result)?;
    println!("wrote results to {}", path.display());
    Ok(())
}
