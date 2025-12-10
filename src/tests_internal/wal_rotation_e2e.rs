#![cfg(feature = "tokio")]

use std::{fs, path::PathBuf, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{DynFs, disk::LocalFs, executor::tokio::TokioExecutor, path::Path as FusioPath};
use serde::Deserialize;

use crate::{
    db::{BatchesThreshold, WalSyncPolicy},
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

fn wal_cfg_with_backend(wal_dir: &PathBuf) -> crate::db::WalConfig {
    use std::time::Duration;
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
        .sync_policy(WalSyncPolicy::Always)
}

#[derive(Debug, Deserialize)]
struct WalStateDisk {
    last_segment_seq: Option<u64>,
}

/// WAL with sync policy `Always` should rotate segments and persist state before shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_rotation_and_state_persisted() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-rotation-e2e");
    let root_str = temp_root.to_string_lossy().into_owned();

    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();

    let wal_dir = temp_root.join("wal");
    let wal_cfg = wal_cfg_with_backend(&wal_dir);
    let executor = Arc::new(TokioExecutor::default());

    let mut db = crate::db::DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(root_str.clone())?
        .wal_config(wal_cfg.clone())
        .with_minor_compaction(1, 0, 1)
        .open_with_executor(Arc::clone(&executor))
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    // Two batches large enough to force multiple WAL frames and at least one rotation.
    for idx in 0..3 {
        let ids: Vec<String> = (0..64).map(|n| format!("user-{idx}-{n:02}")).collect();
        let vals: Vec<i32> = (0..64).map(|n| idx as i32 * 100 + n as i32).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(Int32Array::from(vals)) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    // Ensure writer drains and state is flushed.
    db.disable_wal().await?;

    // Expect multiple WAL segment files on disk.
    let wal_files: Vec<_> = fs::read_dir(&wal_dir)?
        .flatten()
        .filter(|entry| {
            entry.file_name().to_string_lossy().starts_with("wal-")
                && entry.file_name().to_string_lossy().ends_with(".tonwal")
        })
        .collect();
    assert!(
        wal_files.len() >= 2,
        "expected wal rotation to produce multiple segments"
    );

    // State store should record the latest segment sequence to allow recovery.
    let state_path = wal_dir.join("state.json");
    let state: WalStateDisk = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert!(
        state.last_segment_seq.unwrap_or(0) >= 1,
        "state should capture last wal segment"
    );

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }

    Ok(())
}
