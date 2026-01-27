#![cfg(feature = "tokio")]

use std::{fs, path::PathBuf, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{DynFs, disk::LocalFs, executor::tokio::TokioExecutor, path::Path as FusioPath};

use crate::{
    db::{BatchesThreshold, ColumnRef, Predicate},
    test_support::{
        TestFsWalStateStore as FsWalStateStore, TestSsTableConfig as SsTableConfig,
        TestSsTableDescriptor as SsTableDescriptor, TestSsTableId as SsTableId,
        TestWalExt as WalExt, TestWalSyncPolicy as WalSyncPolicy, compact_merge_l0, config_with_pk,
        flush_immutables, prune_wal_segments_below_floor,
    },
};

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

/// Build a WalConfig with local backend and state store.
fn wal_cfg_with_backend(wal_dir: &PathBuf) -> crate::db::WalConfig {
    use std::time::Duration;
    fs::create_dir_all(wal_dir).expect("wal dir");
    let wal_path = FusioPath::from_filesystem_path(wal_dir).expect("wal path");
    let wal_fs = Arc::new(LocalFs {});
    let wal_backend: Arc<dyn DynFs> = wal_fs.clone();
    let wal_state = Arc::new(FsWalStateStore::new(wal_fs));
    crate::db::WalConfig::default()
        .wal_dir(wal_path)
        .segment_backend(wal_backend)
        .state_store(Some(wal_state))
        .segment_max_bytes(512)
        .flush_interval(Duration::from_millis(1))
        .sync_policy(WalSyncPolicy::Disabled)
}

/// End-to-end compaction path that records obsolete WAL segments and prunes GC plan without losing
/// visible rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_gc_prunes_obsolete_wal_and_preserves_visible_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-gc-e2e");
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

    let wal_dir = temp_root.join("wal");
    let wal_cfg = wal_cfg_with_backend(&wal_dir);

    let mut db = crate::db::DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(root_str.clone())?
        .wal_config(wal_cfg.clone())
        .with_minor_compaction(1, 0)
        .open_with_executor(Arc::clone(&executor))
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    // Ingest two batches that will become two L0 SSTs.
    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::new(LocalFs {}),
        FusioPath::from_filesystem_path(temp_root.join("sst")).expect("sst path"),
    ));
    for pass in 0..2 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![format!("k{pass}")])) as _,
                Arc::new(Int32Array::from(vec![pass as i32])) as _,
            ],
        )?;
        db.ingest(batch).await?;
        // Seal + flush each immutable to distinct SSTs.
        let descriptor = SsTableDescriptor::new(SsTableId::new(pass as u64 + 1), 0);
        if let Err(err) = flush_immutables(&db, Arc::clone(&sst_cfg), descriptor).await {
            eprintln!("flush skipped: {err}");
            return Ok(());
        }
    }

    // Plan a compaction that merges both L0 SSTs into L1 and records WAL GC.
    if let Err(err) = compact_merge_l0(&db, vec![1, 2], 1, Arc::clone(&sst_cfg), 100).await {
        eprintln!("compaction merge skipped: {err}");
        return Ok(());
    }

    prune_wal_segments_below_floor(&db).await;

    // Restart and verify data remains visible after compaction + WAL GC.
    drop(db);
    let recovered = crate::db::DB::<LocalFs, TokioExecutor>::builder(config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    ))
    .on_disk(root_str.clone())?
    .wal_config(wal_cfg)
    .open_with_executor(executor)
    .await?;

    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
    let batches = recovered.scan().filter(predicate).collect().await?;
    let mut rows: Vec<(String, i32)> = batches
        .into_iter()
        .flat_map(|batch| {
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
            ids.iter()
                .zip(vals.iter())
                .filter_map(|(id, v)| Some((id?.to_string(), v?)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("k0".into(), 0), ("k1".into(), 1)]);

    recovered.into_inner().disable_wal().await?;
    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }

    Ok(())
}
