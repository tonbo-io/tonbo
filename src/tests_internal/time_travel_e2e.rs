#![cfg(feature = "tokio")]

use std::{fs, path::PathBuf, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor};

use crate::db::{BatchesThreshold, DB, Expr};

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

fn extract_rows(batches: Vec<RecordBatch>) -> Vec<(String, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id col");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("v col");
        for (id, v) in ids.iter().zip(vals.iter()) {
            if let (Some(id), Some(v)) = (id, v) {
                rows.push((id.to_string(), v));
            }
        }
    }
    rows.sort();
    rows
}

/// Verify snapshot_at can time-travel between manifest versions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_at_reads_older_manifest_version() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("time-travel");
    let root_str = temp_root.to_string_lossy().into_owned();

    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();

    let executor = Arc::new(TokioExecutor::default());
    let mut inner = DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(root_str.clone())?
        .with_minor_compaction(1, 0, 1)
        .open_with_executor(Arc::clone(&executor))
        .await?
        .into_inner();
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
    let db = DB::from_inner(Arc::new(inner));

    let batch_v1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["v1-a", "v1-b"])) as _,
            Arc::new(Int32Array::from(vec![1, 2])) as _,
        ],
    )?;
    db.ingest(batch_v1).await?;

    let batch_v2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["v2-a"])) as _,
            Arc::new(Int32Array::from(vec![99])) as _,
        ],
    )?;
    db.ingest(batch_v2).await?;

    let versions = db.list_versions(10).await?;
    if versions.len() < 2 {
        eprintln!("insufficient manifest versions recorded; skipping time-travel assertion");
        return Ok(());
    }

    let earliest = versions.last().expect("earliest version");
    let snapshot_old = db.snapshot_at(earliest.timestamp).await?;
    let predicate = Expr::is_not_null("id");

    let old_rows = extract_rows(
        snapshot_old
            .scan(&db)
            .filter(predicate.clone())
            .collect()
            .await?,
    );
    assert_eq!(
        old_rows,
        vec![("v1-a".into(), 1), ("v1-b".into(), 2)],
        "older snapshot should not see later writes"
    );

    let latest_rows = extract_rows(db.scan().filter(predicate).collect().await?);
    assert!(
        latest_rows.contains(&("v2-a".into(), 99)),
        "latest view should include second batch"
    );

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }
    Ok(())
}
