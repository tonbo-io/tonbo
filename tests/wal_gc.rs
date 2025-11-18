use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{
    DynFs,
    disk::LocalFs,
    executor::tokio::TokioExecutor,
    path::{Path as FusioPath, path_to_local},
};
use tokio::time::sleep;
use tonbo::{
    BatchesThreshold, CommitAckMode, DB, NeverSeal,
    key::KeyOwned,
    mode::{DynMode, DynModeConfig},
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableId},
    scan::RangeSet,
    wal::{WalConfig as RuntimeWalConfig, WalExt, WalSyncPolicy},
};
use typed_arrow_dyn::{DynCell, DynRow};

fn workspace_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let dir = base
        .join("target")
        .join("tmp")
        .join(format!("{prefix}-{unique}"));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn wal_segment_paths(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("tonwal") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn wal_file_for_seq(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal-{seq:020}.tonwal"))
}

fn single_row_batch(schema: Arc<Schema>, id: &str, value: i32) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![id.to_string()])) as _,
            Arc::new(Int32Array::from(vec![value])) as _,
        ],
    )
    .expect("batch")
}

fn rows_from_db(db: &DB<DynMode, TokioExecutor>) -> Vec<(String, i32)> {
    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = db
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id = match cells.next().expect("id cell") {
                Some(DynCell::Str(value)) => value,
                other => panic!("unexpected id cell {other:?}"),
            };
            let value = match cells.next().expect("value cell") {
                Some(DynCell::I32(v)) => v,
                other => panic!("unexpected value cell {other:?}"),
            };
            (id, value)
        })
        .collect();
    rows.sort();
    rows
}

/// End-to-end regression that keeps mutable WAL frames pinned across segment
/// rotation. We seal an initial batch (so flushes advance the manifest), keep a
/// second batch mutable while the WAL writer rotates, flush + GC, and assert
/// the manifest floor still references the pinned segment so the file stays on
/// disk. We then recover from the WAL to prove the pinned file is still
/// required before finally flushing that mutable state and verifying GC removes
/// the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_gc_respects_pinned_segments() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-gc-regression");
    let root_str = temp_root.to_string_lossy().into_owned();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let executor = Arc::new(TokioExecutor::default());
    let build_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let sst_dir = temp_root.join("sst");
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_path = FusioPath::from_filesystem_path(&sst_dir)?;
    let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_path));

    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(build_config)
        .on_disk(root_str.clone())
        .create_dirs(true)
        .wal_segment_bytes(512)
        .wal_flush_interval(Duration::from_millis(1))
        .wal_sync_policy(WalSyncPolicy::Disabled)
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;

    let wal_dir_local = path_to_local(&db.wal_config().expect("wal").dir).expect("local wal dir");

    db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));

    let mut ingested_rows: Vec<(String, i32)> = Vec::new();
    for idx in 0..2 {
        let id = format!("sealed-{idx}");
        let value = idx as i32;
        ingested_rows.push((id.clone(), value));
        db.ingest(single_row_batch(schema.clone(), &id, value))
            .await?;
    }
    assert!(
        db.num_immutable_segments() >= 1,
        "expected initial seal to create immutables"
    );

    db.set_seal_policy(Box::new(NeverSeal::default()));
    for idx in 0..4 {
        let id = format!("pinned-{idx}");
        let value = 100 + idx as i32;
        ingested_rows.push((id.clone(), value));
        db.ingest(single_row_batch(schema.clone(), &id, value))
            .await?;
    }

    let before_flush = wal_segment_paths(&wal_dir_local);
    assert!(
        before_flush.len() >= 2,
        "tiny wal segments should create rotations"
    );

    let descriptor_a = SsTableDescriptor::new(SsTableId::new(1), 0);
    db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_a)
        .await?;
    db.prune_wal_segments_below_floor().await;

    let after_flush = wal_segment_paths(&wal_dir_local);
    assert!(after_flush.len() <= before_flush.len());
    let pinned_floor = db
        .wal_floor_seq()
        .await
        .expect("manifest floor should exist after flush");
    let pinned_file = wal_file_for_seq(&wal_dir_local, pinned_floor);
    assert!(pinned_file.exists(), "pinned WAL segment must stay on disk");

    let recovery_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
    // Recovery happens before releasing the pinned WAL range: older GC logic
    // would have dropped the segment here which meant WAL replay lost data.
    let recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(recovery_config)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .wal_segment_bytes(512)
            .wal_flush_interval(Duration::from_millis(1))
            .wal_sync_policy(WalSyncPolicy::Disabled)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;
    assert_eq!(rows_from_db(&recovered), {
        let mut rows = ingested_rows.clone();
        rows.sort();
        rows
    });
    drop(recovered);

    db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));
    let final_id = "flushed".to_string();
    ingested_rows.push((final_id.clone(), 999));
    db.ingest(single_row_batch(schema.clone(), &final_id, 999))
        .await?;
    assert!(
        db.num_immutable_segments() >= 1,
        "expected pending mutable to seal before final flush"
    );
    let descriptor_b = SsTableDescriptor::new(SsTableId::new(2), 0);
    db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_b)
        .await?;
    db.prune_wal_segments_below_floor().await;
    let after_final_gc = wal_segment_paths(&wal_dir_local);
    assert!(
        !after_final_gc.contains(&pinned_file),
        "pinned WAL file should be deleted after mutable flush"
    );

    db.disable_wal().await?;
    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strict_transaction_updates_wal_floor() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("txn-wal-floor-strict");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let executor = Arc::new(TokioExecutor::default());
    let config = DynModeConfig::from_key_name(schema.clone(), "id")?
        .with_commit_ack_mode(CommitAckMode::Strict);

    let wal_dir = temp_root.join("wal");
    fs::create_dir_all(&wal_dir)?;
    let wal_path = FusioPath::from_filesystem_path(&wal_dir)?;
    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = wal_path;
    wal_cfg.segment_max_bytes = 256;
    wal_cfg.flush_interval = Duration::from_millis(1);
    wal_cfg.sync = WalSyncPolicy::Always;

    let mut db: DB<DynMode, TokioExecutor> = DB::new(config, Arc::clone(&executor)).await?;
    db.enable_wal(wal_cfg.clone()).await?;
    assert!(db.wal_floor_seq().await.is_none());

    let mut tx = db.begin_transaction().await?;
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("strict".into())),
        Some(DynCell::I32(1)),
    ]))?;
    tx.commit(&mut db).await?;

    let floor = db
        .wal_floor_seq()
        .await
        .expect("transaction commit should publish wal floor");
    db.prune_wal_segments_below_floor().await;

    let wal_dir_local = path_to_local(&db.wal_config().expect("wal").dir)?;
    let pinned = wal_file_for_seq(&wal_dir_local, floor);
    assert!(pinned.exists(), "floor segment must remain on disk");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_transaction_updates_wal_floor() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("txn-wal-floor-fast");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let executor = Arc::new(TokioExecutor::default());
    let config = DynModeConfig::from_key_name(schema.clone(), "id")?
        .with_commit_ack_mode(CommitAckMode::Fast);

    let wal_dir = temp_root.join("wal");
    fs::create_dir_all(&wal_dir)?;
    let wal_path = FusioPath::from_filesystem_path(&wal_dir)?;
    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = wal_path;
    wal_cfg.segment_max_bytes = 256;
    wal_cfg.flush_interval = Duration::from_millis(1);
    wal_cfg.sync = WalSyncPolicy::Disabled;

    let mut db: DB<DynMode, TokioExecutor> = DB::new(config, Arc::clone(&executor)).await?;
    db.enable_wal(wal_cfg.clone()).await?;

    let mut tx = db.begin_transaction().await?;
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("fast".into())),
        Some(DynCell::I32(7)),
    ]))?;
    tx.commit(&mut db).await?;

    let start = Instant::now();
    let timeout = Duration::from_secs(5);
    while db.wal_floor_seq().await.is_none() {
        if start.elapsed() > timeout {
            panic!("wal floor never published for fast commit");
        }
        sleep(Duration::from_millis(10)).await;
    }

    db.prune_wal_segments_below_floor().await;
    Ok(())
}
