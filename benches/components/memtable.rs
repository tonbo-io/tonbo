//! Component benchmark: approximate memtable insert throughput via in-memory DB ingest.
//! Direct memtable APIs are crate-private; this uses the public ingestion path and is
//! marked TODO for a narrower component harness.

use std::{sync::Arc, time::Instant};

use arrow_array::{
    RecordBatch,
    builder::{BinaryBuilder, UInt64Builder},
};
use arrow_schema::{DataType, Field, Schema};
use tonbo::db::DbBuilder;

use crate::harness::{BenchConfig, BenchResult, BenchResultWriter, Workload, WorkloadKind};

pub fn run_memtable_bench(_config: &BenchConfig, workload: &Workload) -> anyhow::Result<()> {
    if workload.kind != WorkloadKind::SequentialWrite {
        anyhow::bail!("memtable bench only supports sequential_write workload");
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Binary, false),
    ]));

    let rt = tokio::runtime::Runtime::new()?;
    let db = rt.block_on(async {
        DbBuilder::from_schema_key_name(schema.clone(), "id")?
            .in_memory("memtable-bench")
            .map_err(anyhow::Error::from)?
            .open()
            .await
            .map_err(anyhow::Error::from)
    })?;

    let target_bytes_per_batch: usize = 4 * 1024 * 1024;
    let chunk_size: u64 = std::cmp::max(
        1,
        target_bytes_per_batch
            .saturating_div(workload.value_size_bytes)
            .max(1),
    ) as u64;
    let payload = vec![b'x'; workload.value_size_bytes];
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

        rt.block_on(db.ingest(batch))?;
        inserted += this_chunk as u64;
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
        "note": "uses public ingest; direct memtable API is crate-private (TODO tighten scope)",
    });
    let metrics = serde_json::json!({
        "ops_per_sec": ops_per_sec,
        "bytes_per_sec": bytes_per_sec,
        "wall_time_ms": wall_time_ms,
    });
    let git_commit = std::env::var("GIT_COMMIT").ok();
    let result = BenchResult {
        benchmark_name: "memtable_insert".into(),
        benchmark_type: "component".into(),
        backend: "in_memory".into(),
        backend_details: None,
        workload_type: None,
        parameters,
        metrics,
        git_commit,
        diagnostics: None,
    };

    let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
    let writer = BenchResultWriter::new("target/bench-results")?;
    let path = writer.write(&format!("{timestamp}-memtable_insert"), &result)?;
    println!("component bench wrote results to {}", path.display());

    Ok(())
}
