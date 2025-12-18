//! Component benchmark: manifest/metadata write throughput by issuing small edits.

use std::time::Instant;

use crate::harness::{
    BackendRun, BenchConfig, BenchResult, BenchResultWriter, Workload, WorkloadKind,
    default_results_root,
};

pub async fn run_manifest_bench(
    backend: &BackendRun,
    _config: &BenchConfig,
    workload: &Workload,
    bench_target: &str,
) -> anyhow::Result<()> {
    if workload.kind != WorkloadKind::SequentialWrite {
        anyhow::bail!("manifest bench only supports sequential_write workload");
    }

    println!(
        "manifest bench is experimental (placeholder); replace with real manifest edits when APIs \
         are available"
    );

    // Use a small synthetic loop to simulate manifest edits; currently a placeholder until
    // manifest APIs are exposed for component benches. Here we just measure the loop overhead.
    let iterations = workload.num_records;
    let start = Instant::now();
    let mut checksum: u64 = 0;
    for i in 0..iterations {
        checksum = checksum.wrapping_add(i);
    }
    let elapsed = start.elapsed();

    let wall_time_ms = elapsed.as_millis() as u64;
    let wall_time_secs = elapsed.as_secs_f64().max(0.000_001);
    let ops_per_sec = iterations as f64 / wall_time_secs;

    let parameters = serde_json::json!({
        "iterations": iterations,
        "note": "placeholder manifest benchmark (synthetic); replace with real manifest edits",
    });
    let metrics = serde_json::json!({
        "ops_per_sec": ops_per_sec,
        "wall_time_ms": wall_time_ms,
        "checksum": checksum,
    });
    let git_commit = std::env::var("GIT_COMMIT").ok();
    let run_id = backend.run_id().to_string();
    let storage_substrate = backend.storage_substrate();
    let result = BenchResult {
        run_id: run_id.clone(),
        bench_target: bench_target.to_string(),
        storage_substrate: storage_substrate.clone(),
        benchmark_name: "manifest_edit".into(),
        benchmark_type: "component".into(),
        backend: backend.backend_kind().into(),
        backend_details: Some(backend.backend_details()),
        workload_type: None,
        parameters,
        metrics,
        git_commit,
        diagnostics: None,
    };

    let writer = BenchResultWriter::new(
        default_results_root(),
        &run_id,
        bench_target,
        &storage_substrate,
    )?;
    let path = writer.write("manifest_edit", &result)?;
    println!(
        "component manifest bench wrote results to {}",
        path.display()
    );

    Ok(())
}
