use std::{sync::Arc, time::Instant};

use arrow_array::{
    RecordBatch,
    builder::{BinaryBuilder, UInt64Builder},
};
use tonbo::db::DbBuilder;

use crate::{ScenarioContext, ScenarioReport, emit_result};

const KEY_FIELD: &str = "id";
const TARGET_BYTES_PER_BATCH: usize = 4 * 1024 * 1024;

/// Compaction scenario: write enough data to seal multiple immutables and let minor compaction
/// merge them. Uses `with_minor_compaction` to keep the harness stable while exercising the
/// compaction path.
pub async fn run(ctx: ScenarioContext<'_>) -> anyhow::Result<()> {
    if ctx.workload.num_records == 0 {
        anyhow::bail!("num_records must be > 0 for compaction workload");
    }

    // Enable minor compaction with a small segment threshold so we see compaction activity during
    // the run without exposing engine internals beyond the bench-only knob.
    let mut builder = DbBuilder::from_schema_key_name(ctx.schema.clone(), KEY_FIELD)?;
    builder = builder.with_minor_compaction(3, 0, 1);
    let db = ctx.backend.open_db_with_builder(builder).await?;

    let diag = ctx.diagnostics.clone();
    let payload = vec![b'c'; ctx.workload.value_size_bytes];
    let row_bytes = ctx.workload.value_size_bytes as u64 + std::mem::size_of::<u64>() as u64;
    let chunk_size: u64 = std::cmp::max(
        1,
        TARGET_BYTES_PER_BATCH
            .saturating_div(ctx.workload.value_size_bytes)
            .max(1),
    ) as u64;

    let start = Instant::now();
    let mut inserted: u64 = 0;
    while inserted < ctx.workload.num_records {
        let remaining = ctx.workload.num_records - inserted;
        let this_chunk = remaining.min(chunk_size) as usize;
        let mut key_builder = UInt64Builder::with_capacity(this_chunk);
        let mut value_builder =
            BinaryBuilder::with_capacity(this_chunk, this_chunk * payload.len());

        for i in 0..this_chunk {
            key_builder.append_value(inserted + i as u64);
            value_builder.append_value(&payload);
        }

        let batch = RecordBatch::try_new(
            ctx.schema.clone(),
            vec![
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )?;

        db.ingest(batch).await?;
        inserted += this_chunk as u64;
        diag.record_logical_bytes(row_bytes.saturating_mul(this_chunk as u64));
    }
    let elapsed = start.elapsed();

    let wall_time_ms = elapsed.as_millis() as u64;
    let wall_time_secs = elapsed.as_secs_f64().max(0.000_001);
    let ops_per_sec = ctx.workload.num_records as f64 / wall_time_secs;
    let bytes_total = ctx.workload.num_records as f64 * ctx.workload.value_size_bytes as f64;
    let bytes_per_sec = bytes_total / wall_time_secs;

    if diag.enabled() {
        if let Ok(snapshot) =
            tokio::time::timeout(std::time::Duration::from_secs(10), db.metrics_snapshot()).await
        {
            diag.record_engine_snapshot(snapshot);
        }
    }
    let diagnostics = diag.finalize(ctx.backend).await?;

    let parameters = serde_json::json!({
        "num_records": ctx.workload.num_records,
        "value_size_bytes": ctx.workload.value_size_bytes,
        "concurrency": ctx.workload.concurrency,
        "minor_compaction": {
            "segment_threshold": 3,
            "target_level": 0,
            "start_id": 1,
        },
    });
    let metrics = serde_json::json!({
        "ops_per_sec": ops_per_sec,
        "bytes_per_sec": bytes_per_sec,
        "wall_time_ms": wall_time_ms,
    });

    let report = ScenarioReport {
        workload_type: "compaction",
        parameters,
        metrics,
        diagnostics,
    };
    emit_result(ctx.backend, ctx.config, ctx.bench_target, report)
}
