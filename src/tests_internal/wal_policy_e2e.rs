#![cfg(feature = "tokio")]

use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{DynFs, disk::LocalFs, executor::tokio::TokioExecutor, path::Path as FusioPath};
use tokio::time::sleep;

use crate::{
    BatchesThreshold, WalSyncPolicy,
    test_support::{TestFsWalStateStore, TestWalExt as WalExt},
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

fn wal_cfg_with_backend(wal_dir: &PathBuf, policy: WalSyncPolicy) -> crate::db::WalConfig {
    fs::create_dir_all(wal_dir).expect("wal dir");
    let wal_path = FusioPath::from_filesystem_path(wal_dir).expect("wal path");
    let wal_fs = Arc::new(LocalFs {});
    let wal_backend: Arc<dyn DynFs> = wal_fs.clone();
    let wal_state = Arc::new(TestFsWalStateStore::new(wal_fs));
    crate::db::WalConfig::default()
        .wal_dir(wal_path)
        .segment_backend(wal_backend)
        .state_store(Some(wal_state))
        .segment_max_bytes(256)
        .flush_interval(Duration::from_millis(1))
        .sync_policy(policy)
}

async fn write_rows(
    db: &mut crate::db::DbInner<LocalFs, TokioExecutor>,
    schema: &arrow_schema::SchemaRef,
    offset: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let ids: Vec<String> = (0..32).map(|n| format!("row-{offset}-{n:02}")).collect();
    let vals: Vec<i32> = (0..32).map(|n| offset + n as i32).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)) as _,
            Arc::new(Int32Array::from(vals)) as _,
        ],
    )?;
    db.ingest(batch).await?;
    Ok(())
}

/// IntervalBytes policy should trigger syncs after crossing byte threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_interval_bytes_syncs() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-policy-bytes");
    let wal_dir = temp_root.join("wal");
    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();
    let wal_cfg = wal_cfg_with_backend(&wal_dir, WalSyncPolicy::IntervalBytes(1));
    let executor = Arc::new(TokioExecutor::default());

    let mut db = crate::db::DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(temp_root.to_string_lossy().into_owned())?
        .wal_config(wal_cfg)
        .with_minor_compaction(1, 0, 1)
        .open_with_executor(Arc::clone(&executor))
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    write_rows(&mut db, &schema, 0).await?;
    write_rows(&mut db, &schema, 100).await?;

    let wal_handle = db
        .wal()
        .cloned()
        .expect("wal handle available before shutdown");

    db.disable_wal().await?;
    let metrics = wal_handle.metrics();
    let guard = metrics.read().await;
    assert!(
        guard.sync_operations > 0,
        "expected sync operations to be recorded"
    );

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("cleanup failed: {err}");
    }
    Ok(())
}

/// IntervalTime policy should also emit syncs even with small batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_interval_time_syncs() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-policy-time");
    let wal_dir = temp_root.join("wal");
    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();
    let wal_cfg = wal_cfg_with_backend(
        &wal_dir,
        WalSyncPolicy::IntervalTime(Duration::from_millis(0)),
    );
    let executor = Arc::new(TokioExecutor::default());

    let mut db = crate::db::DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(temp_root.to_string_lossy().into_owned())?
        .wal_config(wal_cfg)
        .with_minor_compaction(1, 0, 1)
        .open_with_executor(Arc::clone(&executor))
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    write_rows(&mut db, &schema, 0).await?;

    let wal_handle = db
        .wal()
        .cloned()
        .expect("wal handle available before shutdown");

    // allow timer to tick
    sleep(Duration::from_millis(5)).await;

    db.disable_wal().await?;
    let metrics = wal_handle.metrics();
    let guard = metrics.read().await;
    assert!(
        guard.sync_operations > 0,
        "expected sync operations to be recorded"
    );

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("cleanup failed: {err}");
    }
    Ok(())
}
