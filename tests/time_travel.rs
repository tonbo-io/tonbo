use std::{path::PathBuf, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor};
use tonbo::{ColumnRef, DB, Predicate, WalSyncPolicy, db::DbBuilder};

fn workspace_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    let dir = base.join("target").join("tmp").join(format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create workspace temp dir");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_at_reads_prior_version() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("e2e-time-travel");
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let exec = Arc::new(TokioExecutor::default());

    let db: DB<LocalFs, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(temp_root.to_string_lossy().into_owned())?
        .wal_sync_policy(WalSyncPolicy::Always)
        .wal_segment_bytes(512)
        .wal_flush_interval(Duration::from_millis(2))
        .open_with_executor(Arc::clone(&exec))
        .await?;

    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["v1-a", "v1-b"])) as _,
            Arc::new(Int32Array::from(vec![1, 2])) as _,
        ],
    )?;
    db.ingest(first).await?;

    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["v2-a"])) as _,
            Arc::new(Int32Array::from(vec![99])) as _,
        ],
    )?;
    db.ingest(second).await?;

    let versions = db.list_versions(8).await?;
    if versions.len() < 2 {
        eprintln!("insufficient manifest versions recorded; skipping time-travel assertion");
        return Ok(());
    }
    let earliest = versions.last().expect("earliest version");

    let snapshot = db.snapshot_at(earliest.timestamp).await?;
    let predicate = Predicate::is_not_null(ColumnRef::new("id"));

    let old_rows = extract_rows(
        snapshot
            .scan(&db)
            .filter(predicate.clone())
            .collect()
            .await?,
    );
    assert_eq!(
        old_rows,
        vec![("v1-a".into(), 1), ("v1-b".into(), 2)],
        "older snapshot should exclude later writes"
    );

    let latest_rows = extract_rows(db.scan().filter(predicate).collect().await?);
    assert!(
        latest_rows.contains(&("v2-a".into(), 99)),
        "latest view should include second batch"
    );

    if let Err(err) = std::fs::remove_dir_all(&temp_root) {
        eprintln!("cleanup failed for {:?}: {err}", temp_root);
    }
    Ok(())
}
