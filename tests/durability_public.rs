use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{DynFs, disk::LocalFs, executor::tokio::TokioExecutor, path::Path as FusioPath};
use futures::TryStreamExt;
use tonbo::{
    BatchesThreshold, DB, NeverSeal,
    compaction::MinorCompactor,
    db::WalConfig as BuilderWalConfig,
    ondisk::sstable::SsTableConfig,
    query::{ColumnRef, Predicate},
    wal::{WalExt, WalSyncPolicy, state::FsWalStateStore},
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

fn wal_cfg_with_backend(
    wal_dir: &PathBuf,
    with_state_store: bool,
) -> Result<BuilderWalConfig, Box<dyn std::error::Error>> {
    fs::create_dir_all(wal_dir)?;
    let wal_path = FusioPath::from_filesystem_path(wal_dir)?;
    let wal_fs = Arc::new(LocalFs {});
    let wal_backend: Arc<dyn DynFs> = wal_fs.clone();
    let wal_state = with_state_store.then(|| Arc::new(FsWalStateStore::new(wal_fs)));

    let mut cfg = BuilderWalConfig::default()
        .wal_dir(wal_path)
        .segment_backend(wal_backend)
        .segment_max_bytes(512)
        .flush_interval(Duration::from_millis(1))
        .sync_policy(WalSyncPolicy::Disabled);
    if let Some(state) = wal_state {
        cfg = cfg.state_store(Some(state));
    }
    Ok(cfg)
}

async fn rows_from_db(
    db: &DB<LocalFs, TokioExecutor>,
) -> Result<Vec<(String, i32)>, Box<dyn std::error::Error>> {
    let predicate = Predicate::is_not_null(ColumnRef::new("id", None));
    let snapshot = db.begin_snapshot().await?;
    let plan = snapshot.plan_scan(db, &predicate, None, None).await?;
    let batches = db.execute_scan(plan).await?.try_collect::<Vec<_>>().await?;
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
                .expect("value col");
            ids.iter()
                .zip(vals.iter())
                .filter_map(|(id, v)| Some((id?.to_string(), v?)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durability_restart_via_public_compaction_path() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("durability-public");
    let root_str = temp_root.to_string_lossy().into_owned();

    let build_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = build_config.schema();
    let executor = Arc::new(TokioExecutor::default());

    // WAL config with local fs backend and state store.
    let wal_dir = temp_root.join("wal");
    let wal_cfg = wal_cfg_with_backend(&wal_dir, true)?;

    // SST config for compaction flush.
    let sst_dir = temp_root.join("sst");
    fs::create_dir_all(&sst_dir)?;
    let sst_root = FusioPath::from_filesystem_path(&sst_dir)?;
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));

    let mut db: DB<LocalFs, TokioExecutor> = DB::<LocalFs, TokioExecutor>::builder(build_config)
        .on_disk(root_str.clone())?
        .create_dirs(true)
        .wal_config(wal_cfg.clone())
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let expected_rows = vec![
        ("alpha".to_string(), 10),
        ("bravo".to_string(), 20),
        ("charlie".to_string(), 30),
    ];
    for (id, value) in &expected_rows {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id.clone()])) as _,
                Arc::new(Int32Array::from(vec![*value])) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    // Use the public MinorCompactor to flush immutables to SSTs.
    let compactor = MinorCompactor::new(1, 0, 1);
    let flushed = compactor
        .maybe_compact(&mut db, Arc::clone(&sst_cfg))
        .await?
        .expect("compaction should flush immutables");
    assert_eq!(flushed.descriptor().level(), 0);

    drop(db);

    // Restart and rely on manifest+WAL replay only through public builder.
    let recover_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let mut recovered: DB<LocalFs, TokioExecutor> =
        DB::<LocalFs, TokioExecutor>::builder(recover_config)
            .on_disk(root_str.clone())?
            .create_dirs(true)
            .wal_config(wal_cfg)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let rows = rows_from_db(&recovered).await?;
    assert_eq!(rows, expected_rows);

    recovered.disable_wal().await?;
    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean test dir {:?}: {err}", &temp_root);
    }

    Ok(())
}

/// End-user path: ingest only (no flush), restart, and recover purely from WAL replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durability_restart_via_wal_only() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("durability-wal-only");
    let root_str = temp_root.to_string_lossy().into_owned();

    let build_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = build_config.schema();
    let executor = Arc::new(TokioExecutor::default());

    // WAL config with local fs backend and state store.
    let wal_dir = temp_root.join("wal");
    let wal_cfg = wal_cfg_with_backend(&wal_dir, true)?;

    let mut db: DB<LocalFs, TokioExecutor> = DB::<LocalFs, TokioExecutor>::builder(build_config)
        .on_disk(root_str.clone())?
        .create_dirs(true)
        .wal_config(wal_cfg.clone())
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let expected_rows = vec![("delta".to_string(), 100), ("echo".to_string(), 200)];
    for (id, value) in &expected_rows {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id.clone()])) as _,
                Arc::new(Int32Array::from(vec![*value])) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    // Do not flush; rely solely on WAL replay.
    drop(db);

    let recover_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let mut recovered: DB<LocalFs, TokioExecutor> =
        DB::<LocalFs, TokioExecutor>::builder(recover_config)
            .on_disk(root_str.clone())?
            .create_dirs(true)
            .wal_config(wal_cfg)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let rows = rows_from_db(&recovered).await?;
    assert_eq!(rows, expected_rows);

    recovered.disable_wal().await?;
    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean test dir {:?}: {err}", &temp_root);
    }

    Ok(())
}

/// Mixed immutable (flushed) + mutable (wal-only) recovery through public APIs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durability_restart_mixed_sst_and_wal() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("durability-mixed");
    let root_str = temp_root.to_string_lossy().into_owned();

    let build_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = build_config.schema();
    let executor = Arc::new(TokioExecutor::default());

    // WAL config with state store.
    let wal_dir = temp_root.join("wal");
    let wal_cfg = wal_cfg_with_backend(&wal_dir, true)?;

    // SST config.
    let sst_dir = temp_root.join("sst");
    fs::create_dir_all(&sst_dir)?;
    let sst_root = FusioPath::from_filesystem_path(&sst_dir)?;
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));

    let mut db: DB<LocalFs, TokioExecutor> = DB::<LocalFs, TokioExecutor>::builder(build_config)
        .on_disk(root_str.clone())?
        .create_dirs(true)
        .wal_config(wal_cfg.clone())
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    // First batch: will be sealed + flushed.
    let flushed_rows = vec![("f1".to_string(), 1), ("f2".to_string(), 2)];
    for (id, value) in &flushed_rows {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id.clone()])) as _,
                Arc::new(Int32Array::from(vec![*value])) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }
    let compactor = MinorCompactor::new(1, 0, 10);
    compactor
        .maybe_compact(&mut db, Arc::clone(&sst_cfg))
        .await?
        .expect("compaction should flush immutables");

    // Second batch: stays mutable/WAL only.
    let wal_only_rows = vec![("w1".to_string(), 100), ("w2".to_string(), 200)];
    db.set_seal_policy(Arc::new(NeverSeal::default()));
    for (id, value) in &wal_only_rows {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id.clone()])) as _,
                Arc::new(Int32Array::from(vec![*value])) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    drop(db);

    let recover_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let recovered: DB<LocalFs, TokioExecutor> =
        DB::<LocalFs, TokioExecutor>::builder(recover_config)
            .on_disk(root_str.clone())?
            .create_dirs(true)
            .wal_config(wal_cfg)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let mut rows = rows_from_db(&recovered).await?;
    rows.sort();
    let mut expected = flushed_rows;
    expected.extend_from_slice(&wal_only_rows);
    expected.sort();
    assert_eq!(rows, expected);

    Ok(())
}

/// Multiple restarts remain idempotent and keep commit clock monotonic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durability_multi_restart_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("durability-multi");
    let root_str = temp_root.to_string_lossy().into_owned();
    let wal_dir = temp_root.join("wal");
    let wal_cfg = wal_cfg_with_backend(&wal_dir, true)?;
    let build_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = build_config.schema();
    let executor = Arc::new(TokioExecutor::default());

    async fn reopen_once(
        root: &str,
        wal_cfg: &BuilderWalConfig,
        executor: Arc<TokioExecutor>,
        schema: Arc<Schema>,
        rows: Vec<(String, i32)>,
    ) -> Result<DB<LocalFs, TokioExecutor>, Box<dyn std::error::Error>> {
        let reopen_config = config_with_pk(
            vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ],
            &["id"],
        );
        let mut db: DB<LocalFs, TokioExecutor> =
            DB::<LocalFs, TokioExecutor>::builder(reopen_config)
                .on_disk(root.to_string())?
                .create_dirs(true)
                .wal_config(wal_cfg.clone())
                .recover_or_init_with_executor(Arc::clone(&executor))
                .await?;
        db.set_seal_policy(Arc::new(NeverSeal::default()));
        for (id, value) in rows {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![id.clone()])) as _,
                    Arc::new(Int32Array::from(vec![value])) as _,
                ],
            )?;
            db.ingest(batch).await?;
        }
        Ok(db)
    }

    let db = reopen_once(
        &root_str,
        &wal_cfg,
        Arc::clone(&executor),
        Arc::clone(&schema),
        vec![("a".into(), 1), ("b".into(), 2)],
    )
    .await?;
    drop(db);

    let db = reopen_once(
        &root_str,
        &wal_cfg,
        Arc::clone(&executor),
        Arc::clone(&schema),
        vec![("c".into(), 3)],
    )
    .await?;
    drop(db);

    let db = reopen_once(
        &root_str,
        &wal_cfg,
        Arc::clone(&executor),
        Arc::clone(&schema),
        vec![("d".into(), 4)],
    )
    .await?;

    let mut rows = rows_from_db(&db).await?;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4)
        ]
    );

    Ok(())
}

/// WAL-only restart still works without a WAL state store; replay advances commit clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durability_wal_only_no_state_store() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("durability-wal-no-state");
    let root_str = temp_root.to_string_lossy().into_owned();
    let build_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = build_config.schema();
    let executor = Arc::new(TokioExecutor::default());

    let wal_dir = temp_root.join("wal");
    let wal_cfg = wal_cfg_with_backend(&wal_dir, false)?;

    let mut db: DB<LocalFs, TokioExecutor> = DB::<LocalFs, TokioExecutor>::builder(build_config)
        .on_disk(root_str.clone())?
        .create_dirs(true)
        .wal_config(wal_cfg.clone())
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;
    db.set_seal_policy(Arc::new(NeverSeal::default()));

    let expected_rows = vec![("ns1".to_string(), 7), ("ns2".to_string(), 8)];
    for (id, value) in &expected_rows {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id.clone()])) as _,
                Arc::new(Int32Array::from(vec![*value])) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }
    drop(db);

    let recover_config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let recovered: DB<LocalFs, TokioExecutor> =
        DB::<LocalFs, TokioExecutor>::builder(recover_config)
            .on_disk(root_str.clone())?
            .create_dirs(true)
            .wal_config(wal_cfg)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let mut rows = rows_from_db(&recovered).await?;
    rows.sort();
    assert_eq!(rows, expected_rows);

    Ok(())
}
