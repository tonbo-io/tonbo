use std::{path::PathBuf, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor, path::Path as FusioPath};
use tonbo::{
    ColumnRef, DB, Predicate, WalSyncPolicy,
    db::DbBuilder,
};

fn workspace_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    let dir = base
        .join("target")
        .join("tmp")
        .join(format!("{prefix}-{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()));
    std::fs::create_dir_all(&dir).expect("create workspace temp dir");
    dir
}

async fn collect_rows(
    db: &DB<LocalFs, TokioExecutor>,
) -> Result<Vec<(String, i32)>, Box<dyn std::error::Error>> {
    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
    let batches = db.scan().filter(predicate).collect().await?;
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
            .expect("value col");
        for (id, v) in ids.iter().zip(vals.iter()) {
            if let (Some(id), Some(v)) = (id, v) {
                rows.push((id.to_string(), v));
            }
        }
    }
    rows.sort();
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durability_restart_recovers_via_wal() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("e2e-durability-wal");
    let wal_dir = temp_root.join("wal");
    let wal_path = FusioPath::from_filesystem_path(&wal_dir)?;
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let exec = Arc::new(TokioExecutor::default());

    let mut builder = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(temp_root.to_string_lossy().into_owned())?
        .wal_dir(wal_path)
        .wal_sync_policy(WalSyncPolicy::Always)
        .wal_segment_bytes(512)
        .wal_flush_interval(Duration::from_millis(2));

    let db: DB<LocalFs, TokioExecutor> = builder
        .open_with_executor(Arc::clone(&exec))
        .await?;

    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ],
    )?;
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b"])) as _,
            Arc::new(Int32Array::from(vec![2])) as _,
        ],
    )?;
    db.ingest(first).await?;
    db.ingest(second).await?;

    drop(db);

    // Rebuild the same DB purely through the public builder + WAL config.
    builder = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(temp_root.to_string_lossy().into_owned())?
        .wal_dir(FusioPath::from_filesystem_path(&wal_dir)?)
        .wal_sync_policy(WalSyncPolicy::Always)
        .wal_segment_bytes(512)
        .wal_flush_interval(Duration::from_millis(2));

    let reopened: DB<LocalFs, TokioExecutor> = builder
        .open_with_executor(exec)
        .await?;

    let rows = collect_rows(&reopened).await?;
    assert_eq!(rows, vec![("a".into(), 1), ("b".into(), 2)]);

    // Best-effort cleanup.
    if let Err(err) = std::fs::remove_dir_all(&temp_root) {
        eprintln!("cleanup failed for {:?}: {err}", temp_root);
    }
    Ok(())
}
