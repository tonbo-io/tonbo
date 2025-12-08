use std::{fs, sync::Arc, time::Duration};

use arrow_schema::{DataType, Field, Schema};
use fusio::{
    DynFs, disk::LocalFs, executor::tokio::TokioExecutor, fs::FsCas, mem::fs::InMemoryFs,
    path::Path,
};
use typed_arrow_dyn::{DynCell, DynRow};

use super::common::{wal_segment_paths, workspace_temp_dir};
use crate::{
    db::{DB, DbInner},
    inmem::policy::BatchesThreshold,
    mode::DynModeConfig,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableId},
    schema::SchemaBuilder,
    test::build_batch,
    wal::{WalConfig as RuntimeWalConfig, WalExt, WalSyncPolicy, state::FsWalStateStore},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_gc_smoke_prunes_segments_after_flush() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-gc-smoke");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let executor = Arc::new(TokioExecutor::default());
    let namespace = temp_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("wal-gc-smoke");
    let mut db: DbInner<InMemoryFs, TokioExecutor> =
        DB::<InMemoryFs, TokioExecutor>::builder(mode_config)
            .in_memory(namespace.to_string())?
            .open_with_executor(Arc::clone(&executor))
            .await?
            .into_inner();

    let wal_local_fs = Arc::new(LocalFs {});
    let wal_dyn_fs: Arc<dyn DynFs> = wal_local_fs.clone();
    let wal_cas: Arc<dyn FsCas> = wal_local_fs.clone();

    let wal_dir = temp_root.join("wal");
    fs::create_dir_all(&wal_dir)?;
    let wal_path = Path::from_filesystem_path(&wal_dir)?;

    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = wal_path;
    wal_cfg.segment_backend = wal_dyn_fs;
    wal_cfg.state_store = Some(Arc::new(FsWalStateStore::new(wal_cas)));
    // Force extremely small segments so each batch append rotates the WAL; this
    // guarantees multiple sealed segments exist between the two flushes and
    // exercises the GC path we care about.
    wal_cfg.segment_max_bytes = 1;
    wal_cfg.flush_interval = Duration::from_millis(1);
    wal_cfg.sync = WalSyncPolicy::Disabled;

    db.enable_wal(wal_cfg.clone()).await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let sst_dir = temp_root.join("sst");
    fs::create_dir_all(&sst_dir)?;
    let sst_root = Path::from_filesystem_path(&sst_dir)?;
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));

    // First ingest/flush establishes the initial WAL floor.
    for idx in 0..4 {
        let rows = vec![DynRow(vec![
            Some(DynCell::Str(format!("user-{idx}").into())),
            Some(DynCell::I32(idx as i32)),
        ])];
        let batch = build_batch(schema.clone(), rows)?;
        db.ingest(batch).await?;
    }
    assert!(
        db.num_immutable_segments() >= 1,
        "expected first seal to produce immutables"
    );
    let descriptor_a = SsTableDescriptor::new(SsTableId::new(99), 0);
    db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_a)
        .await?;
    let floor_after_first = db
        .manifest_wal_floor()
        .await
        .map(|ref_| ref_.seq())
        .expect("manifest floor after first flush");
    let first_prune_view = wal_segment_paths(&wal_dir);
    assert!(
        first_prune_view.len() >= 2,
        "expected multiple WAL segments present after first flush"
    );

    // Second ingest/flush should advance the floor and delete older segments.
    for idx in 4..8 {
        let rows = vec![DynRow(vec![
            Some(DynCell::Str(format!("user-{idx}").into())),
            Some(DynCell::I32(idx as i32)),
        ])];
        let batch = build_batch(schema.clone(), rows)?;
        db.ingest(batch).await?;
    }
    assert!(
        db.num_immutable_segments() >= 1,
        "expected second seal to produce immutables"
    );
    let descriptor_b = SsTableDescriptor::new(SsTableId::new(100), 0);
    db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_b)
        .await?;
    let floor_after_second = db
        .manifest_wal_floor()
        .await
        .map(|ref_| ref_.seq())
        .expect("manifest floor after second flush");
    assert!(
        floor_after_second > floor_after_first,
        "floor should advance after second flush"
    );
    let after = wal_segment_paths(&wal_dir);
    let removed: Vec<_> = first_prune_view
        .iter()
        .filter(|path| !after.contains(path))
        .collect();
    assert!(
        !removed.is_empty(),
        "second flush should prune at least one WAL segment"
    );

    if let Some(handle) = db.wal().cloned() {
        let metrics = handle.metrics();
        let guard = metrics.read().await;
        assert!(
            guard.wal_segments_pruned >= removed.len() as u64,
            "pruned segment count should be reflected in metrics"
        );
        assert!(
            guard.wal_floor_advancements >= 2,
            "floor should advance at least once per flush"
        );
        assert_eq!(
            guard.wal_prune_dry_runs, 0,
            "regular pruning should not increment dry-run counters"
        );
    }

    db.disable_wal().await?;
    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_gc_dry_run_reports_only() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-gc-dry-run");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = SchemaBuilder::from_schema(schema)
        .primary_key("id")
        .with_metadata()
        .build()
        .expect("key field");
    let schema = Arc::clone(&mode_config.schema);
    let executor = Arc::new(TokioExecutor::default());
    let namespace = temp_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("wal-gc-dry-run");
    let mut db: DbInner<InMemoryFs, TokioExecutor> =
        DB::<InMemoryFs, TokioExecutor>::builder(mode_config)
            .in_memory(namespace.to_string())?
            .open_with_executor(Arc::clone(&executor))
            .await?
            .into_inner();

    let wal_local_fs = Arc::new(LocalFs {});
    let wal_dyn_fs: Arc<dyn DynFs> = wal_local_fs.clone();
    let wal_cas: Arc<dyn FsCas> = wal_local_fs.clone();
    let wal_dir = temp_root.join("wal");
    fs::create_dir_all(&wal_dir)?;
    let wal_path = Path::from_filesystem_path(&wal_dir)?;

    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = wal_path;
    wal_cfg.segment_backend = wal_dyn_fs;
    wal_cfg.state_store = Some(Arc::new(FsWalStateStore::new(wal_cas)));
    wal_cfg.segment_max_bytes = 1;
    wal_cfg.prune_dry_run = true;

    db.enable_wal(wal_cfg.clone()).await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let sst_dir = temp_root.join("sst");
    fs::create_dir_all(&sst_dir)?;
    let sst_root = Path::from_filesystem_path(&sst_dir)?;
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));

    for idx in 0..4 {
        let rows = vec![vec![
            Some(DynCell::Str(format!("dry-{idx}").into())),
            Some(DynCell::I32(idx as i32)),
        ]];
        let batch = build_batch(schema.clone(), rows)?;
        db.ingest(batch).await?;
    }
    let descriptor_a = SsTableDescriptor::new(SsTableId::new(991), 0);
    db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_a)
        .await?;
    let before = wal_segment_paths(&wal_dir);

    for idx in 4..8 {
        let rows = vec![vec![
            Some(DynCell::Str(format!("dry-{idx}").into())),
            Some(DynCell::I32(idx as i32)),
        ]];
        let batch = build_batch(schema.clone(), rows)?;
        db.ingest(batch).await?;
    }
    let descriptor_b = SsTableDescriptor::new(SsTableId::new(992), 0);
    db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_b)
        .await?;
    let after = wal_segment_paths(&wal_dir);
    assert!(
        after.len() >= before.len(),
        "dry-run pruning should not delete WAL segments"
    );

    if let Some(handle) = db.wal().cloned() {
        let metrics = handle.metrics();
        let guard = metrics.read().await;
        assert_eq!(guard.wal_segments_pruned, 0);
        assert!(
            guard.wal_prune_dry_runs > 0,
            "dry-run pruning should report would-be deletions"
        );
    }

    db.disable_wal().await?;
    fs::remove_dir_all(&temp_root)?;
    Ok(())
}
