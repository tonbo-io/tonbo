use std::{sync::Arc, time::Instant};

use arrow_array::{
    RecordBatch,
    builder::{BinaryBuilder, UInt64Builder},
};
use tonbo::prelude::{ColumnRef, Predicate, ScalarValue};

use crate::{ScenarioContext, ScenarioReport, emit_result};

const KEY_FIELD: &str = "id";
const TARGET_BYTES_PER_BATCH: usize = 4 * 1024 * 1024;

pub async fn run(ctx: ScenarioContext<'_>) -> anyhow::Result<()> {
    if ctx.workload.num_records == 0 {
        anyhow::bail!("num_records must be > 0 for read workloads");
    }

    let db = ctx.backend.open_db(ctx.schema.clone(), KEY_FIELD).await?;
    let diag = ctx.diagnostics.clone();
    let row_bytes = ctx.workload.value_size_bytes as u64 + std::mem::size_of::<u64>() as u64;
    preload(&db, &ctx).await?;
    diag.record_logical_bytes(row_bytes.saturating_mul(ctx.workload.num_records));

    if ctx.workload.warmup_records > 0 {
        let warmup = ctx.workload.warmup_records.min(ctx.workload.num_records);
        for key in 0..warmup {
            let _ = read_one(&db, key).await?;
        }
    }

    let start = Instant::now();
    for key in 0..ctx.workload.num_records {
        let _ = read_one(&db, key).await?;
    }
    let elapsed = start.elapsed();

    let wall_time_ms = elapsed.as_millis() as u64;
    let wall_time_secs = elapsed.as_secs_f64().max(0.000_001);
    let ops_per_sec = ctx.workload.num_records as f64 / wall_time_secs;
    let bytes_total = ctx.workload.num_records as f64 * ctx.workload.value_size_bytes as f64;
    let bytes_per_sec = bytes_total / wall_time_secs;

    let parameters = serde_json::json!({
        "num_records": ctx.workload.num_records,
        "value_size_bytes": ctx.workload.value_size_bytes,
        "warmup_records": ctx.workload.warmup_records,
        "concurrency": ctx.workload.concurrency,
    });
    let metrics = serde_json::json!({
        "ops_per_sec": ops_per_sec,
        "bytes_per_sec": bytes_per_sec,
        "wall_time_ms": wall_time_ms,
    });

    #[cfg(any(test, tonbo_bench))]
    if diag.enabled() {
        let snapshot = db.bench_diagnostics().await;
        diag.record_engine_snapshot(snapshot);
    }
    let diagnostics = diag.finalize(ctx.backend).await?;

    let report = ScenarioReport {
        workload_type: "read_only",
        parameters,
        metrics,
        diagnostics,
    };
    emit_result(ctx.backend, ctx.config, ctx.bench_target, report)
}

async fn preload(db: &crate::harness::BenchDb, ctx: &ScenarioContext<'_>) -> anyhow::Result<()> {
    let chunk_size: u64 = std::cmp::max(
        1,
        TARGET_BYTES_PER_BATCH
            .saturating_div(ctx.workload.value_size_bytes)
            .max(1),
    ) as u64;
    let payload = vec![b'x'; ctx.workload.value_size_bytes];
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
    }

    Ok(())
}

async fn read_one(db: &crate::harness::BenchDb, key: u64) -> anyhow::Result<usize> {
    let predicate = Predicate::eq(ColumnRef::new(KEY_FIELD), ScalarValue::from(key));
    let batches = db.scan_with_predicate(predicate).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if rows == 0 {
        anyhow::bail!("expected at least one row for key {key}");
    }
    Ok(rows)
}
