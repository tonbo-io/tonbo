//! Component benchmark: SST encode/write throughput using staged Arrow batches to Parquet.

use std::{sync::Arc, time::Instant};

use arrow_array::{
    RecordBatch,
    builder::{BinaryBuilder, UInt64Builder},
};
use arrow_schema::{DataType, Field, Schema};

use crate::harness::{
    BackendRun, BenchConfig, BenchResult, BenchResultWriter, DiagnosticsCollector,
    DiagnosticsOutput, Workload, WorkloadKind, default_results_root,
    diagnostics::diagnostics_config,
};

pub async fn run_sst_encode_bench(
    backend: &BackendRun,
    _config: &BenchConfig,
    workload: &Workload,
    bench_target: &str,
) -> anyhow::Result<()> {
    if workload.kind != WorkloadKind::SequentialWrite {
        anyhow::bail!("sst_encode bench only supports sequential_write workload");
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Binary, false),
    ]));

    let diag_cfg = diagnostics_config(_config);
    let diagnostics = DiagnosticsCollector::from_config(diag_cfg.clone());

    // Use component WAL policy helper to reuse disk/object storage handling; wal sync policy
    // defaults to crate settings.
    let db = backend.open_db(schema.clone(), "id").await?;

    let target_bytes_per_batch: usize = 4 * 1024 * 1024;
    let row_bytes = workload.value_size_bytes as u64 + std::mem::size_of::<u64>() as u64;
    let chunk_size: u64 = std::cmp::max(
        1,
        target_bytes_per_batch
            .saturating_div(workload.value_size_bytes)
            .max(1),
    ) as u64;
    let payload = vec![b's'; workload.value_size_bytes];
    let mut inserted: u64 = 0;

    let start = Instant::now();
    while inserted < workload.num_records {
        let remaining = workload.num_records - inserted;
        let this_chunk = remaining.min(chunk_size) as usize;
        let mut key_builder = UInt64Builder::with_capacity(this_chunk);
        let mut value_builder =
            BinaryBuilder::with_capacity(this_chunk, this_chunk * payload.len());

        for i in 0..this_chunk {
            key_builder.append_value(inserted + i as u64);
            value_builder.append_value(&payload);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )?;

        db.ingest(batch).await?;
        inserted += this_chunk as u64;
        diagnostics.record_logical_bytes(row_bytes.saturating_mul(this_chunk as u64));
    }
    let elapsed = start.elapsed();

    let wall_time_ms = elapsed.as_millis() as u64;
    let wall_time_secs = elapsed.as_secs_f64().max(0.000_001);
    let ops_per_sec = workload.num_records as f64 / wall_time_secs;
    let bytes_total = workload.num_records as f64 * workload.value_size_bytes as f64;
    let bytes_per_sec = bytes_total / wall_time_secs;

    let parameters = serde_json::json!({
        "num_records": workload.num_records,
        "value_size_bytes": workload.value_size_bytes,
        "concurrency": workload.concurrency,
        "note": "encodes batches via ingest to exercise SST write path",
    });
    let metrics = serde_json::json!({
        "ops_per_sec": ops_per_sec,
        "bytes_per_sec": bytes_per_sec,
        "wall_time_ms": wall_time_ms,
    });
    let git_commit = std::env::var("GIT_COMMIT").ok();
    let run_id = backend.run_id().to_string();
    let storage_substrate = backend.storage_substrate();
    let result = BenchResult {
        run_id: run_id.clone(),
        bench_target: bench_target.to_string(),
        storage_substrate: storage_substrate.clone(),
        benchmark_name: "sst_encode".into(),
        benchmark_type: "component".into(),
        backend: backend.backend_kind().into(),
        backend_details: Some(backend.backend_details()),
        workload_type: None,
        parameters,
        metrics,
        git_commit,
        diagnostics: finalize_component_diagnostics(&diagnostics, backend, &db).await?,
    };

    let writer = BenchResultWriter::new(
        default_results_root(),
        &run_id,
        bench_target,
        &storage_substrate,
    )?;
    let path = writer.write("sst_encode", &result)?;
    println!(
        "component sst_encode bench wrote results to {}",
        path.display()
    );

    Ok(())
}

async fn finalize_component_diagnostics(
    diag: &DiagnosticsCollector,
    backend: &BackendRun,
    db: &crate::harness::BenchDb,
) -> anyhow::Result<Option<DiagnosticsOutput>> {
    if diag.enabled() {
        let snapshot = db.metrics_snapshot().await;
        diag.record_engine_snapshot(snapshot);
    }
    Ok(diag.finalize(backend).await?)
}
