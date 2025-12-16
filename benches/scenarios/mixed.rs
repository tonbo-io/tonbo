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
        anyhow::bail!("num_records must be > 0 for mixed workload");
    }

    let db = ctx.backend.open_db(ctx.schema.clone(), KEY_FIELD).await?;
    let payload = vec![b'x'; ctx.workload.value_size_bytes];
    let diag = ctx.diagnostics.clone();
    let row_bytes = ctx.workload.value_size_bytes as u64 + std::mem::size_of::<u64>() as u64;
    preload(&db, &ctx).await?;
    diag.record_logical_bytes(row_bytes.saturating_mul(ctx.workload.num_records));

    let mut read_ops = (ctx.workload.read_ratio * ctx.workload.num_records as f64).round() as u64;
    let write_ops = (ctx.workload.write_ratio * ctx.workload.num_records as f64).round() as u64;

    let requested_total = read_ops + write_ops;
    if requested_total == 0 {
        anyhow::bail!("mixed workload requires at least one read or write op");
    }

    if requested_total < ctx.workload.num_records {
        read_ops += ctx.workload.num_records - requested_total;
    } else if requested_total > ctx.workload.num_records {
        let excess = requested_total - ctx.workload.num_records;
        read_ops = read_ops.saturating_sub(excess.min(read_ops));
    }

    let schedule = build_schedule(read_ops, write_ops);
    let mut reads_done: u64 = 0;
    let mut writes_done: u64 = 0;
    let mut next_write_key = ctx.workload.num_records;

    let start = Instant::now();
    for op in schedule {
        match op {
            Operation::Read => {
                let max_key = ctx.workload.num_records + writes_done;
                let target = reads_done % max_key;
                let _ = read_one(&db, target).await?;
                reads_done += 1;
            }
            Operation::Write => {
                let batch = single_row_batch(&ctx, next_write_key, &payload)?;
                db.ingest(batch).await?;
                next_write_key += 1;
                writes_done += 1;
                diag.record_logical_bytes(row_bytes);
            }
        }
    }
    let elapsed = start.elapsed();

    let total_ops = reads_done + writes_done;
    let wall_time_ms = elapsed.as_millis() as u64;
    let wall_time_secs = elapsed.as_secs_f64().max(0.000_001);
    let bytes_per_sec = total_ops as f64 * ctx.workload.value_size_bytes as f64 / wall_time_secs;
    let ops_per_sec = total_ops as f64 / wall_time_secs;

    let parameters = serde_json::json!({
        "num_records": ctx.workload.num_records,
        "value_size_bytes": ctx.workload.value_size_bytes,
        "read_ratio": ctx.workload.read_ratio,
        "write_ratio": ctx.workload.write_ratio,
        "read_ops": reads_done,
        "write_ops": writes_done,
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
        workload_type: "mixed",
        parameters,
        metrics,
        diagnostics,
    };
    emit_result(ctx.backend, ctx.config, report)
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

fn single_row_batch(
    ctx: &ScenarioContext<'_>,
    key: u64,
    payload: &[u8],
) -> anyhow::Result<RecordBatch> {
    let mut key_builder = UInt64Builder::with_capacity(1);
    let mut value_builder = BinaryBuilder::with_capacity(1, ctx.workload.value_size_bytes);
    key_builder.append_value(key);
    value_builder.append_value(payload);

    let batch = RecordBatch::try_new(
        ctx.schema.clone(),
        vec![
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ],
    )?;
    Ok(batch)
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

fn build_schedule(read_ops: u64, write_ops: u64) -> Vec<Operation> {
    if write_ops == 0 {
        return vec![Operation::Read; read_ops as usize];
    }
    if read_ops == 0 {
        return vec![Operation::Write; write_ops as usize];
    }

    let gcd = gcd(read_ops, write_ops);
    let read_block = read_ops / gcd;
    let write_block = write_ops / gcd;
    let mut schedule = Vec::with_capacity((read_ops + write_ops) as usize);

    for _ in 0..gcd {
        schedule.extend(std::iter::repeat(Operation::Read).take(read_block as usize));
        schedule.extend(std::iter::repeat(Operation::Write).take(write_block as usize));
    }

    schedule
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a.max(1)
}

#[derive(Clone, Copy)]
enum Operation {
    Read,
    Write,
}
