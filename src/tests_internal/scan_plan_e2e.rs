#![cfg(feature = "tokio")]

use std::{fs, path::PathBuf, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor, mem::fs::InMemoryFs};
use futures::TryStreamExt;

use crate::{
    db::{BatchesThreshold, ColumnRef, DB, NeverSeal, Predicate, ScalarValue},
    test_support::{execute_scan_plan, plan_scan_snapshot},
};

#[path = "common/mod.rs"]
mod common;
use common::config_with_pk;

fn workspace_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    let dir = base.join("target").join("tmp").join(format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("create workspace temp dir");
    dir
}

/// Plan + execute a scan that spans SSTs and mutable rows, applies residual predicates, respects
/// limits, and prunes tombstones. This exercises the two-phase read path end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_execute_scan_merges_layers_with_residuals() -> Result<(), Box<dyn std::error::Error>>
{
    let temp_root = workspace_temp_dir("plan-execute-scan");
    let root_str = temp_root.to_string_lossy().into_owned();

    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();

    // Force each ingest to seal so the first batch flushes to SST (minor compaction), then keep
    // subsequent writes mutable to exercise merge ordering.
    let executor = Arc::new(TokioExecutor::default());
    let mut db = DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(root_str.clone())?
        .with_minor_compaction(1, 0, 1)
        .open_with_executor(Arc::clone(&executor))
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    // Immutable batch -> SST via minor compaction.
    let immutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "z"])) as _,
            Arc::new(Int32Array::from(vec![1, 9])) as _,
        ],
    )?;
    db.ingest(immutable_batch).await?;

    // Leave the next writes mutable and include a tombstone to ensure deletes are filtered.
    db.set_seal_policy(Arc::new(NeverSeal::default()));
    let mutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["m-live", "m-drop", "z"])) as _,
            Arc::new(Int32Array::from(vec![3, 4, 0])) as _,
        ],
    )?;
    let tombstones = vec![false, true, true];
    db.ingest_with_tombstones(mutable_batch, tombstones).await?;

    // Plan/execute through the snapshot API to exercise manifest pinning + merge stream.
    let snapshot = db.begin_snapshot().await?;
    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(0i64));
    let plan = plan_scan_snapshot(&snapshot, &db, &predicate, None, Some(2)).await?;
    let stream = execute_scan_plan(&db, plan).await?;
    let batches = stream.try_collect::<Vec<_>>().await?;

    let mut rows: Vec<(String, i32)> = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value column");
        for (id, v) in ids.iter().zip(values.iter()) {
            if let (Some(id), Some(v)) = (id, v) {
                rows.push((id.to_string(), v));
            }
        }
    }

    // MergeStream should yield globally ordered rows and drop tombstoned/deleted entries.
    assert_eq!(rows.len(), 2, "limit should apply after residual filtering");
    assert_eq!(rows, vec![("a".into(), 1), ("m-live".into(), 3)]);

    // Cleanup best-effort.
    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }

    Ok(())
}

/// Large-package scan still honors the global limit and ordering across immutable + mutable layers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_execute_applies_limit_after_merge_ordering() -> Result<(), Box<dyn std::error::Error>>
{
    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();

    let noop_exec = Arc::new(fusio::executor::NoopExecutor);
    let mut db: crate::db::DbInner<InMemoryFs, fusio::executor::NoopExecutor> =
        DB::<InMemoryFs, fusio::executor::NoopExecutor>::builder(config)
            .in_memory("plan-execute-limit")?
            .open_with_executor(noop_exec)
            .await?
            .into_inner();

    // Seal the first batch into an immutable segment, then keep subsequent writes mutable to force
    // MergeStream to merge across layers while respecting the limit after packaging.
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let mut ids = Vec::new();
    let mut values = Vec::new();
    // First batch: 1200 rows to exceed DEFAULT_SCAN_BATCH_ROWS.
    for idx in 0..1200 {
        ids.push(format!("k{idx:04}"));
        values.push(idx as i32);
    }
    let immutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)) as _,
            Arc::new(Int32Array::from(values)) as _,
        ],
    )?;
    db.ingest(immutable_batch).await?;

    db.set_seal_policy(Arc::new(NeverSeal::default()));
    let mutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["k2000", "k2001"])) as _,
            Arc::new(Int32Array::from(vec![2000, 2001])) as _,
        ],
    )?;
    db.ingest(mutable_batch).await?;

    let snapshot = db.begin_snapshot().await?;
    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
    let plan = plan_scan_snapshot(&snapshot, &db, &predicate, None, Some(100)).await?;
    let stream = execute_scan_plan(&db, plan).await?;
    let batches = stream.try_collect::<Vec<_>>().await?;

    let mut seen = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        for id in ids.iter().flatten() {
            seen.push(id.to_string());
        }
    }

    assert_eq!(
        seen.len(),
        100,
        "limit should cap results after merge ordering"
    );
    assert_eq!(seen.first().map(String::as_str), Some("k0000"));
    assert_eq!(seen.last().map(String::as_str), Some("k0099"));

    Ok(())
}
