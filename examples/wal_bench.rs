#![allow(clippy::missing_panics_doc)]

use std::{
    collections::HashMap,
    env,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{impls::mem::fs::InMemoryFs, path::Path};
use tokio::runtime::Runtime;
use ulid::Ulid;

use tonbo::{
    db::DB,
    mode::{DynMode, DynModeConfig},
    mvcc::Timestamp,
    wal::{WalConfig, WalSyncPolicy, WalExt},
};
use fusio::executor::tokio::TokioExecutor;

fn main() {
    let cfg = Config::from_args(env::args());
    let runtime = Runtime::new().expect("tokio runtime");
    let schema = Arc::new(build_schema(cfg.value_columns));

    match cfg.mode {
        Mode::Wal => {
            let (duration, bytes) = runtime.block_on(bench_wal_append(&cfg, schema));
            report("wal", &cfg, duration, bytes);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Wal,
}

#[derive(Clone)]
struct Config {
    mode: Mode,
    rows: usize,
    batches: usize,
    tombstone_density: f64,
    sync: SyncMode,
    value_columns: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SyncMode {
    Disabled,
    Always,
}

impl Config {
    fn from_args<I>(iter: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        let mut map: HashMap<String, String> = HashMap::new();
        for arg in iter.into_iter().map(Into::into).skip(1) {
            if let Some((k, v)) = arg.split_once('=') {
                map.insert(k.trim_start_matches("--").to_string(), v.to_string());
            }
        }

        let mode = Mode::Wal;

        let rows = map
            .get("rows")
            .and_then(|s| s.parse().ok())
            .unwrap_or(4096);
        let batches = map
            .get("batches")
            .and_then(|s| s.parse().ok())
            .unwrap_or(64);
        let density = map
            .get("density")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.2);
        let sync = match map.get("sync").map(|s| s.as_str()) {
            Some("always") => SyncMode::Always,
            _ => SyncMode::Disabled,
        };
        let value_columns = map
            .get("columns")
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);

        Self {
            mode,
            rows,
            batches,
            tombstone_density: density,
            sync,
            value_columns,
        }
    }

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
        "mode={mode} rows={} batches={} density={:.2} sync={:?} duration_ms={:.2} bytes={} mb_per_s={:.2} ops_per_s={:.2}",
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

async fn bench_wal_append(
    cfg: &Config,
    schema: Arc<Schema>,
) -> (Duration, usize) {
    let db = setup_db(Arc::clone(&schema), cfg.sync_policy()).await;
    let batch = build_batch(Arc::clone(&schema), cfg.rows);
    let tombstones = build_tombstones(cfg.rows, cfg.tombstone_density);
    let bytes_per_batch = batch_size_bytes(&batch, &tombstones);
    let wal = db.wal().expect("wal").clone();

    let start = Instant::now();
    for n in 0..cfg.batches {
        let ticket = wal
            .append(&batch, &tombstones, Timestamp::new(n as u64))
            .await
            .expect("append");
        ticket.durable().await.expect("durable");
    }
    let elapsed = start.elapsed();

    (elapsed, bytes_per_batch * cfg.batches)
}

async fn setup_db(schema: Arc<Schema>, sync: WalSyncPolicy) -> DB<DynMode, TokioExecutor> {
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::new(config, executor).expect("db");
    let cfg = wal_config(sync);
    db.enable_wal(cfg).expect("enable wal");
    db
}

fn wal_config(sync: WalSyncPolicy) -> WalConfig {
    let mut cfg = WalConfig::default();
    cfg.filesystem = Arc::new(InMemoryFs::new());
    cfg.dir = Path::parse(format!("wal-bench-{}", Ulid::new())).expect("path");
    cfg.sync = sync;
    cfg
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

impl fmt::Debug for SyncMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncMode::Disabled => f.write_str("disabled"),
            SyncMode::Always => f.write_str("always"),
        }
    }
}
