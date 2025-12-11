use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor, path::Path as FusioPath};
use tonbo::{DB, WalSyncPolicy, db::DbBuilder};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_rotation_happens_with_small_segments() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("e2e-wal-rotation");
    let wal_dir = temp_root.join("wal");
    let wal_path = FusioPath::from_filesystem_path(&wal_dir)?;
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let exec = Arc::new(TokioExecutor::default());

    let db: DB<LocalFs, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(temp_root.to_string_lossy().into_owned())?
        .wal_dir(wal_path)
        .wal_sync_policy(WalSyncPolicy::Always)
        .wal_segment_bytes(256) // force frequent rotation
        .wal_flush_interval(Duration::from_millis(1))
        .open_with_executor(Arc::clone(&exec))
        .await?;

    for batch_idx in 0..4 {
        let ids: Vec<String> = (0..64)
            .map(|n| format!("user-{batch_idx}-{n:02}"))
            .collect();
        let vals: Vec<i32> = (0..64)
            .map(|n| (batch_idx as i32) * 100 + n as i32)
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(Int32Array::from(vals)) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    // Allow the WAL writer to drain to disk.
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(db);

    let wal_files: Vec<_> = fs::read_dir(&wal_dir)?
        .flatten()
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.starts_with("wal-") && name.ends_with(".tonwal")
        })
        .collect();

    assert!(
        wal_files.len() >= 2,
        "small segments should produce multiple WAL files, saw {}",
        wal_files.len()
    );

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("cleanup failed for {:?}: {err}", temp_root);
    }
    Ok(())
}
