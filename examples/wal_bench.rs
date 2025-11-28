#![allow(clippy::missing_panics_doc)]

use std::{
    fmt, fs,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use arrow_select::take::take;
use clap::{Parser, ValueEnum};
use fusio::executor::tokio::TokioExecutor;
use tonbo::{
    db::{DB, DbBuilder, DynMode},
    mvcc::{MVCC_COMMIT_COL, Timestamp},
    wal::{WalExt, WalSyncPolicy},
};
use ulid::Ulid;

#[tokio::main]
async fn main() {
    let cfg = Config::parse();
    let schema = Arc::new(build_schema(cfg.value_columns));

    match cfg.mode {
        Mode::Wal => {
            let (duration, bytes) = bench_wal_append(&cfg, schema).await;
            report("wal", &cfg, duration, bytes);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    Wal,
}

#[derive(Parser, Clone)]
#[command(
    name = "wal-bench",
    about = "Benchmark WAL append throughput into the WAL."
)]
struct Config {
    #[arg(long, value_enum, default_value_t = Mode::Wal)]
    mode: Mode,
    #[arg(long, default_value_t = 4096)]
    rows: usize,
    #[arg(long, default_value_t = 64)]
    batches: usize,
    #[arg(long = "density", default_value_t = 0.2)]
    tombstone_density: f64,
    #[arg(long, value_enum, default_value_t = SyncMode::Disabled)]
    sync: SyncMode,
    #[arg(long = "columns", default_value_t = 2)]
    value_columns: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SyncMode {
    Disabled,
    Always,
}

impl Config {
    fn sync_policy(&self) -> WalSyncPolicy {
        match self.sync {
            SyncMode::Disabled => WalSyncPolicy::Disabled,
            SyncMode::Always => WalSyncPolicy::Always,
        }
    }
}

fn report(mode: &str, cfg: &Config, duration: Duration, bytes: usize) {
    let secs = duration.as_secs_f64();
    let throughput = if secs > 0.0 {
        bytes as f64 / secs / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let ops = if secs > 0.0 {
        cfg.batches as f64 / secs
    } else {
        0.0
    };

    println!(
        "mode={mode} rows={} batches={} density={:.2} sync={:?} duration_ms={:.2} bytes={} \
         mb_per_s={:.2} ops_per_s={:.2}",
        cfg.rows,
        cfg.batches,
        cfg.tombstone_density,
        cfg.sync,
        secs * 1000.0,
        bytes,
        throughput,
        ops,
    );
}

async fn bench_wal_append(cfg: &Config, schema: Arc<Schema>) -> (Duration, usize) {
    let db = setup_db(Arc::clone(&schema), cfg.sync_policy()).await;
    let batch = build_batch(Arc::clone(&schema), cfg.rows);
    let tombstones = build_tombstones(cfg.rows, cfg.tombstone_density);
    let bytes_per_batch = batch_size_bytes(&batch, &tombstones);
    let wal = db.wal().expect("wal").clone();
    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
    ]));
    let partitioned = partition_batch(&batch, &tombstones).expect("partition batch");
    let upserts = partitioned.upsert_batch;
    let delete_keys = partitioned.delete_keys;

    let start = Instant::now();
    for n in 0..cfg.batches {
        let commit_ts = Timestamp::new(n as u64);
        if delete_keys.is_empty() {
            if let Some(ref upsert_batch) = upserts {
                let ticket = wal.append(upsert_batch, commit_ts).await.expect("append");
                ticket.durable().await.expect("durable");
            }
        } else {
            let provisional_id = wal.next_provisional_id();
            let mut append_tickets = Vec::new();
            if let Some(ref upsert_batch) = upserts {
                append_tickets.push(
                    wal.txn_append(provisional_id, upsert_batch, commit_ts)
                        .await
                        .expect("txn append"),
                );
            }
            if !delete_keys.is_empty() {
                let delete_batch = build_delete_batch(&delete_schema, &delete_keys, commit_ts);
                append_tickets.push(
                    wal.txn_append_delete(provisional_id, delete_batch)
                        .await
                        .expect("delete append"),
                );
            }
            let commit_ticket = wal
                .txn_commit(provisional_id, commit_ts)
                .await
                .expect("commit");
            for ticket in append_tickets {
                ticket.durable().await.expect("append durable");
            }
            commit_ticket.durable().await.expect("durable commit");
        }
    }
    let elapsed = start.elapsed();

    (elapsed, bytes_per_batch * cfg.batches)
}

async fn setup_db(schema: Arc<Schema>, sync: WalSyncPolicy) -> DB<DynMode, TokioExecutor> {
    let label = format!("wal-bench-{}", Ulid::new());
    let root = std::env::temp_dir().join(&label);
    fs::create_dir_all(&root).expect("wal bench dir");
    let db: DB<DynMode, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .expect("config")
        .on_disk(&root)
        .expect("on_disk")
        .wal_sync_policy(sync)
        .build()
        .await
        .expect("db");
    db
}

fn build_schema(value_columns: usize) -> Schema {
    let mut fields = Vec::with_capacity(value_columns + 1);
    fields.push(Field::new("id", DataType::Utf8, false));
    for idx in 0..value_columns {
        fields.push(Field::new(format!("v{idx}"), DataType::Int32, false));
    }
    Schema::new(fields)
}

fn build_batch(schema: Arc<Schema>, rows: usize) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let ids = StringArray::from_iter_values((0..rows).map(|i| format!("id-{i}")));
    columns.push(Arc::new(ids) as ArrayRef);
    for idx in 1..schema.fields().len() {
        let values: Vec<i32> = (0..rows).map(|row| row as i32 + idx as i32).collect();
        columns.push(Arc::new(Int32Array::from(values)) as ArrayRef);
    }
    RecordBatch::try_new(schema, columns).expect("batch")
}

fn build_tombstones(rows: usize, density: f64) -> Vec<bool> {
    if density <= f64::EPSILON {
        return vec![false; rows];
    }
    if (density - 1.0).abs() < f64::EPSILON {
        return vec![true; rows];
    }
    let mut bitmap = vec![false; rows];
    let interval = (1.0 / density.max(1e-6)).round() as usize;
    for (idx, slot) in bitmap.iter_mut().enumerate() {
        if idx % interval == 0 {
            *slot = true;
        }
    }
    bitmap
}

fn batch_size_bytes(batch: &RecordBatch, tombstones: &[bool]) -> usize {
    let column_bytes: usize = batch
        .columns()
        .iter()
        .map(|array| array.get_buffer_memory_size())
        .sum();
    let tombstone_bytes = (tombstones.len() + 7) / 8;
    column_bytes + tombstone_bytes
}

struct PartitionedWalBatch {
    upsert_batch: Option<RecordBatch>,
    delete_keys: Vec<String>,
}

fn partition_batch(
    batch: &RecordBatch,
    tombstones: &[bool],
) -> Result<PartitionedWalBatch, ArrowError> {
    if batch.num_rows() != tombstones.len() {
        return Err(ArrowError::ComputeError(
            "tombstone bitmap length mismatch".to_string(),
        ));
    }
    let key_column = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::ComputeError("id column not utf8".to_string()))?;
    let mut upsert_indices = Vec::new();
    let mut delete_keys = Vec::new();
    for (idx, tombstone) in tombstones.iter().enumerate() {
        if *tombstone {
            delete_keys.push(key_column.value(idx).to_string());
        } else {
            upsert_indices.push(idx as u32);
        }
    }
    let upsert_batch = if upsert_indices.is_empty() {
        None
    } else if upsert_indices.len() == batch.num_rows() {
        Some(batch.clone())
    } else {
        Some(take_full_batch(batch, &upsert_indices)?)
    };
    Ok(PartitionedWalBatch {
        upsert_batch,
        delete_keys,
    })
}

fn take_full_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ArrowError> {
    let idx_array = UInt32Array::from(indices.to_vec());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), &idx_array, None)?);
    }
    RecordBatch::try_new(batch.schema(), columns)
}

fn build_delete_batch(schema: &SchemaRef, keys: &[String], commit_ts: Timestamp) -> RecordBatch {
    let ids = Arc::new(StringArray::from(keys.to_vec())) as ArrayRef;
    let commits = Arc::new(UInt64Array::from(vec![commit_ts.get(); keys.len()])) as ArrayRef;
    RecordBatch::try_new(schema.clone(), vec![ids, commits]).expect("delete batch")
}

impl fmt::Debug for SyncMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncMode::Disabled => f.write_str("disabled"),
            SyncMode::Always => f.write_str("always"),
        }
    }
}
