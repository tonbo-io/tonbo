#![cfg(feature = "tokio")]

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{executor::NoopExecutor, impls::mem::fs::InMemoryFs};

use crate::{BatchesThreshold, ColumnRef, Predicate, WalSyncPolicy, db::DB};

#[path = "common/mod.rs"]
mod common;
use common::config_with_pk;

/// Ensure a wasm-like config (in-memory FS + no-op executor) can ingest and scan end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_like_in_memory_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();

    let exec = Arc::new(NoopExecutor);
    let mut inner: crate::db::DbInner<InMemoryFs, NoopExecutor> =
        DB::<InMemoryFs, NoopExecutor>::builder(config)
            .in_memory("wasm-compat-e2e")?
            .wal_sync_policy(WalSyncPolicy::Always)
            .open_with_executor(exec)
            .await?
            .into_inner();

    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as _,
            Arc::new(Int32Array::from(vec![1, 2])) as _,
        ],
    )?;
    inner.ingest(first).await?;

    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["c"])) as _,
            Arc::new(Int32Array::from(vec![3])) as _,
        ],
    )?;
    inner.ingest(second).await?;

    let db = DB::from_inner(Arc::new(inner));

    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
    let mut rows: Vec<(String, i32)> = db
        .begin_snapshot()
        .await?
        .scan(&db)
        .filter(predicate)
        .collect()
        .await?
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("id col");
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .expect("v col");
            ids.iter()
                .zip(vals.iter())
                .filter_map(|(id, v)| Some((id?.to_string(), v?)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)]
    );

    Ok(())
}
