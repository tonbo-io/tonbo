use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use fusio::{DynFs, Write, executor::tokio::TokioExecutor, path::Path as FusioPath};
use tonbo::{
    DB,
    db::WalConfig as BuilderWalConfig,
    key::KeyOwned,
    mode::{DynMode, DynModeConfig},
    mvcc::Timestamp,
    scan::RangeSet,
    wal::{
        DynBatchPayload, WalCommand, WalConfig as RuntimeWalConfig, WalExt, WalRecoveryMode,
        WalSyncPolicy,
        frame::{INITIAL_FRAME_SEQ, WalEvent, encode_autocommit_frames, encode_command},
        replay::Replayer,
        storage::WalStorage,
    },
};
use typed_arrow_dyn::DynCell;

#[path = "common/mod.rs"]
mod common;

use common::schema_and_config;

fn workspace_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    let dir = base.join("target").join("tmp").join(format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("create workspace temp dir");
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovers_rows_across_restart() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-e2e");
    let root_str = root_dir.to_string_lossy().into_owned();

    let (schema, mode_config) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );

    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .on_disk(root_str.clone())
        .create_dirs(true)
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-1", "user-2"])) as _,
            Arc::new(Int32Array::from(vec![10, 20])) as _,
        ],
    )?;
    db.ingest(batch).await?;

    db.disable_wal().await?;
    drop(db);

    let (_, mode_config_recover) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config_recover)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("user-1".into(), 10), ("user-2".into(), 20)]);

    recovered.disable_wal().await?;
    drop(recovered);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovers_composite_keys_in_order() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-composite");
    let root_str = root_dir.to_string_lossy().into_owned();

    let (schema, mode_config) = schema_and_config(
        vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("bucket", DataType::Int64, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["tenant", "bucket"],
    );

    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .on_disk(root_str.clone())
        .create_dirs(true)
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["tenant2", "tenant1", "tenant1"])) as _,
            Arc::new(Int64Array::from(vec![5_i64, 7, 3])) as _,
            Arc::new(Int32Array::from(vec![20, 10, 15])) as _,
        ],
    )?;
    db.ingest(batch).await?;

    db.disable_wal().await?;
    drop(db);

    let mode_config_recover = DynModeConfig::from_metadata(schema.clone())?;
    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config_recover)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let rows: Vec<((String, i64), i32)> = recovered
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let tenant = match cells.next().expect("tenant cell") {
                Some(DynCell::Str(value)) => value,
                other => panic!("unexpected tenant cell {other:?}"),
            };
            let bucket = match cells.next().expect("bucket cell") {
                Some(DynCell::I64(value)) => value,
                other => panic!("unexpected bucket cell {other:?}"),
            };
            let value = match cells.next().expect("value cell") {
                Some(DynCell::I32(v)) => v,
                other => panic!("unexpected value cell {other:?}"),
            };
            ((tenant, bucket), value)
        })
        .collect();

    assert_eq!(
        rows,
        vec![
            (("tenant1".into(), 3), 15),
            (("tenant1".into(), 7), 10),
            (("tenant2".into(), 5), 20),
        ]
    );

    recovered.disable_wal().await?;
    drop(recovered);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_replay_emits_delete_frames_for_tombstones() -> Result<(), Box<dyn std::error::Error>> {
    let dir = workspace_temp_dir("tonbo-wal-delete-frame");
    let wal_path = dir.join("wal");
    fs::create_dir_all(&wal_path)?;

    let backend = Arc::new(fusio::mem::fs::InMemoryFs::new());
    let fs_dyn: Arc<dyn DynFs> = backend.clone();
    let wal_root = FusioPath::from_filesystem_path(&wal_path).expect("wal path");
    let storage = WalStorage::new(Arc::clone(&fs_dyn), wal_root.clone());
    storage
        .ensure_dir(storage.root())
        .await
        .expect("ensure dir");

    let upsert_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("_commit_ts", DataType::UInt64, false),
    ]));

    let upsert_batch = RecordBatch::try_new(
        upsert_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["keep", "remove"])) as _,
            Arc::new(Int32Array::from(vec![1, 2])) as _,
        ],
    )?;
    let delete_batch = RecordBatch::try_new(
        delete_schema,
        vec![
            Arc::new(StringArray::from(vec!["remove"])) as _,
            Arc::new(UInt64Array::from(vec![99_u64])) as _,
        ],
    )?;

    let provisional_id = 33;
    let commit_ts = Timestamp::new(99);
    let commit_array: ArrayRef = Arc::new(UInt64Array::from(vec![
        commit_ts.get();
        upsert_batch.num_rows()
    ])) as ArrayRef;
    let payload_upsert = DynBatchPayload::Row {
        batch: upsert_batch,
        commit_ts_column: commit_array,
    };
    let payload_delete = DynBatchPayload::Delete {
        batch: delete_batch,
    };

    let mut frames = Vec::new();
    frames.extend(encode_command(WalCommand::TxnAppend {
        provisional_id,
        payload: payload_upsert,
    })?);
    frames.extend(encode_command(WalCommand::TxnAppend {
        provisional_id,
        payload: payload_delete,
    })?);
    frames.extend(encode_command(WalCommand::TxnCommit {
        provisional_id,
        commit_ts,
    })?);

    let mut seq = INITIAL_FRAME_SEQ;
    let mut bytes = Vec::new();
    for frame in frames {
        bytes.extend_from_slice(&frame.into_bytes(seq));
        seq += 1;
    }

    let mut segment = storage.open_segment(0).await.expect("segment");
    let (write_res, _) = segment.file_mut().write_all(bytes).await;
    write_res.expect("write wal");
    segment.file_mut().flush().await.expect("flush");

    let mut cfg = RuntimeWalConfig::default();
    cfg.dir = wal_root.clone();
    cfg.segment_backend = fs_dyn;
    cfg.state_store = None;

    let replayer = Replayer::new(cfg);
    let events = replayer.scan().await.expect("scan");
    assert_eq!(events.len(), 3);

    match &events[0] {
        WalEvent::DynAppend { payload, .. } => {
            assert_eq!(payload.batch.num_rows(), 2);
        }
        other => panic!("unexpected first event: {other:?}"),
    }
    match &events[1] {
        WalEvent::DynDelete { payload, .. } => {
            assert_eq!(payload.batch.num_rows(), 1);
            let ids = payload
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column");
            assert_eq!(ids.value(0), "remove");
        }
        other => panic!("unexpected second event: {other:?}"),
    }
    assert!(matches!(events[2], WalEvent::TxnCommit { .. }));

    if let Err(err) = fs::remove_dir_all(&dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &dir);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_ignores_truncated_commit() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-truncated");
    let root_str = root_dir.to_string_lossy().into_owned();
    let wal_dir = root_dir.join("wal");

    let (schema, mode_config) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );

    let executor = Arc::new(TokioExecutor::default());

    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = FusioPath::from_filesystem_path(&wal_dir)?;

    let storage = WalStorage::new(Arc::clone(&wal_cfg.segment_backend), wal_cfg.dir.clone());
    storage.ensure_dir(storage.root()).await?;
    let mut segment = storage.open_segment(1).await?;

    let committed_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["committed"])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ],
    )?;
    let committed_frames =
        encode_autocommit_frames(committed_batch.clone(), 11, Timestamp::new(100))?;

    let partial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["partial"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    let partial_frames = encode_autocommit_frames(partial_batch.clone(), 17, Timestamp::new(200))?;

    let mut seq = INITIAL_FRAME_SEQ;
    for frame in committed_frames {
        let bytes = frame.into_bytes(seq);
        let (write_res, _) = segment.file_mut().write_all(bytes).await;
        write_res?;
        seq += 1;
    }

    let append_bytes = partial_frames[0].clone().into_bytes(seq);
    let (write_res, _) = segment.file_mut().write_all(append_bytes).await;
    write_res?;
    seq += 1;

    let mut commit_bytes = partial_frames[1].clone().into_bytes(seq);
    commit_bytes.truncate(commit_bytes.len().saturating_sub(4));
    let (write_res, _) = segment.file_mut().write_all(commit_bytes).await;
    write_res?;
    segment.file_mut().flush().await?;
    drop(segment);
    drop(storage);

    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("committed".into(), 1)]);

    recovered.disable_wal().await?;
    drop(recovered);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_tolerates_corrupted_tail() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-tolerate");
    let root_str = root_dir.to_string_lossy().into_owned();
    let wal_dir = root_dir.join("wal");

    let (schema, mode_config) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );

    let executor = Arc::new(TokioExecutor::default());

    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = FusioPath::from_filesystem_path(&wal_dir)?;
    wal_cfg.recovery = WalRecoveryMode::TolerateCorruptedTail;

    let storage = WalStorage::new(Arc::clone(&wal_cfg.segment_backend), wal_cfg.dir.clone());
    storage.ensure_dir(storage.root()).await?;
    let mut segment = storage.open_segment(1).await?;

    let committed_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["committed"])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ],
    )?;
    let committed_frames =
        encode_autocommit_frames(committed_batch.clone(), 11, Timestamp::new(100))?;

    let partial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["partial"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    let partial_frames = encode_autocommit_frames(partial_batch.clone(), 17, Timestamp::new(200))?;

    let mut seq = INITIAL_FRAME_SEQ;
    for frame in committed_frames {
        let bytes = frame.into_bytes(seq);
        let (write_res, _) = segment.file_mut().write_all(bytes).await;
        write_res?;
        seq += 1;
    }

    let append_bytes = partial_frames[0].clone().into_bytes(seq);
    let (write_res, _) = segment.file_mut().write_all(append_bytes).await;
    write_res?;
    seq += 1;

    let mut commit_bytes = partial_frames[1].clone().into_bytes(seq);
    commit_bytes.truncate(commit_bytes.len().saturating_sub(4));
    let (write_res, _) = segment.file_mut().write_all(commit_bytes).await;
    write_res?;
    segment.file_mut().flush().await?;
    drop(segment);

    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .wal_config(
                BuilderWalConfig::default().recovery_mode(WalRecoveryMode::TolerateCorruptedTail),
            )
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("committed".into(), 1)]);

    recovered.disable_wal().await?;
    drop(recovered);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_rewrite_after_truncated_tail() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-rewrite");
    let root_str = root_dir.to_string_lossy().into_owned();
    let wal_dir = root_dir.join("wal");

    let (schema, mode_config) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );

    let executor = Arc::new(TokioExecutor::default());

    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = FusioPath::from_filesystem_path(&wal_dir)?;

    let storage = WalStorage::new(Arc::clone(&wal_cfg.segment_backend), wal_cfg.dir.clone());
    storage.ensure_dir(storage.root()).await?;
    let mut segment = storage.open_segment(1).await?;

    let committed_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["committed"])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ],
    )?;
    let committed_frames =
        encode_autocommit_frames(committed_batch.clone(), 11, Timestamp::new(100))?;

    let partial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["partial"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    let partial_frames = encode_autocommit_frames(partial_batch.clone(), 17, Timestamp::new(200))?;

    let mut seq = INITIAL_FRAME_SEQ;
    for frame in committed_frames {
        let bytes = frame.into_bytes(seq);
        let (write_res, _) = segment.file_mut().write_all(bytes).await;
        write_res?;
        seq += 1;
    }

    let append_bytes = partial_frames[0].clone().into_bytes(seq);
    let (write_res, _) = segment.file_mut().write_all(append_bytes).await;
    write_res?;
    seq += 1;

    let mut commit_bytes = partial_frames[1].clone().into_bytes(seq);
    commit_bytes.truncate(commit_bytes.len().saturating_sub(4));
    let (write_res, _) = segment.file_mut().write_all(commit_bytes).await;
    write_res?;
    segment.file_mut().flush().await?;
    drop(segment);

    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    assert_eq!(rows, vec![("committed".into(), 1)]);

    let runtime_cfg = recovered
        .wal_config()
        .cloned()
        .expect("wal config available");
    recovered.disable_wal().await?;
    recovered.enable_wal(runtime_cfg.clone()).await?;
    let rewrite_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["rewrite"])) as _,
            Arc::new(Int32Array::from(vec![2])) as _,
        ],
    )?;
    recovered.ingest(rewrite_batch).await?;
    recovered.disable_wal().await?;
    drop(recovered);

    let (_, final_config) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let mut recovered_again: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(final_config)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let mut rows_after: Vec<(String, i32)> = recovered_again
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    rows_after.sort();
    assert!(rows_after.contains(&("rewrite".into(), 2)));

    recovered_again.disable_wal().await?;
    drop(recovered_again);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_ignores_aborted_transactions() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-abort");
    let root_str = root_dir.to_string_lossy().into_owned();
    let wal_dir = root_dir.join("wal");
    fs::create_dir_all(&wal_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let executor = Arc::new(TokioExecutor::default());

    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = FusioPath::from_filesystem_path(&wal_dir)?;

    let storage = WalStorage::new(Arc::clone(&wal_cfg.segment_backend), wal_cfg.dir.clone());
    storage.ensure_dir(storage.root()).await?;
    let mut segment = storage.open_segment(1).await?;

    let committed_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["live"])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ],
    )?;
    let committed_frames =
        encode_autocommit_frames(committed_batch.clone(), 41, Timestamp::new(100))?;

    let aborted_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["aborted"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    let mut aborted_frames =
        encode_autocommit_frames(aborted_batch.clone(), 55, Timestamp::new(150))?;
    let aborted_append = aborted_frames.remove(0);
    drop(aborted_frames);

    let aborted_begin = encode_command(WalCommand::TxnBegin { provisional_id: 55 })?;
    let aborted_abort = encode_command(WalCommand::TxnAbort { provisional_id: 55 })?;

    let mut seq = INITIAL_FRAME_SEQ;
    for frame in committed_frames {
        let bytes = frame.into_bytes(seq);
        let (write_res, _) = segment.file_mut().write_all(bytes).await;
        write_res?;
        seq += 1;
    }

    for frame in aborted_begin {
        let bytes = frame.into_bytes(seq);
        let (write_res, _) = segment.file_mut().write_all(bytes).await;
        write_res?;
        seq += 1;
    }

    let bytes = aborted_append.into_bytes(seq);
    let (write_res, _) = segment.file_mut().write_all(bytes).await;
    write_res?;
    seq += 1;

    for frame in aborted_abort {
        let bytes = frame.into_bytes(seq);
        let (write_res, _) = segment.file_mut().write_all(bytes).await;
        write_res?;
        seq += 1;
    }

    segment.file_mut().flush().await?;
    drop(segment);

    let recovered: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .on_disk(root_str.clone())
        .create_dirs(true)
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row projection");
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
    assert_eq!(rows, vec![("live".into(), 1)]);

    drop(recovered);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_survives_segment_rotations() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-rotate");
    let root_str = root_dir.to_string_lossy().into_owned();

    let (schema, mode_config) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );

    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .on_disk(root_str.clone())
        .create_dirs(true)
        .wal_segment_bytes(1)
        .wal_flush_interval(Duration::from_millis(0))
        .wal_sync_policy(WalSyncPolicy::Always)
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;

    for chunk in 0..3 {
        let ids = vec![
            format!("user-{}", chunk * 2),
            format!("user-{}", chunk * 2 + 1),
        ];
        let values = vec![(chunk as i32) * 10, (chunk as i32) * 10 + 1];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(Int32Array::from(values)) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    db.disable_wal().await?;
    drop(db);

    let (_, mode_config_recover) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config_recover)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .wal_segment_bytes(1)
            .wal_flush_interval(Duration::from_millis(0))
            .wal_sync_policy(WalSyncPolicy::Always)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    rows.sort();
    let expected = vec![
        ("user-0".into(), 0),
        ("user-1".into(), 1),
        ("user-2".into(), 10),
        ("user-3".into(), 11),
        ("user-4".into(), 20),
        ("user-5".into(), 21),
    ];
    assert_eq!(rows, expected);

    let runtime_cfg = recovered
        .wal_config()
        .cloned()
        .expect("wal config available");
    recovered.disable_wal().await?;
    recovered.enable_wal(runtime_cfg.clone()).await?;
    let extra_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-extra"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    recovered.ingest(extra_batch).await?;
    recovered.disable_wal().await?;
    drop(recovered);

    let (_, mode_config_final) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let mut recovered_again: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config_final)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .wal_segment_bytes(1)
            .wal_flush_interval(Duration::from_millis(0))
            .wal_sync_policy(WalSyncPolicy::Always)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let mut rows_final: Vec<(String, i32)> = recovered_again
        .scan_mutable_rows(&ranges, None)
        .expect("scan rows")
        .map(|row| {
            let row = row.expect("row scan failed");
            let mut cells = row.0.into_iter();
            let id_cell = cells.next().expect("id cell");
            let value_cell = cells.next().expect("value cell");

            let id = match id_cell {
                Some(DynCell::Str(value)) => value,
                _ => panic!("unexpected id cell"),
            };
            let value = match value_cell {
                Some(DynCell::I32(v)) => v,
                _ => panic!("unexpected value cell"),
            };
            (id, value)
        })
        .collect();
    rows_final.sort();
    let mut expected_final = expected.clone();
    expected_final.push(("user-extra".into(), 999));
    expected_final.sort();
    assert_eq!(rows_final, expected_final);

    recovered_again.disable_wal().await?;
    drop(recovered_again);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_reenable_seeds_provisional_sequence() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = workspace_temp_dir("tonbo-wal-seq");
    let root_str = root_dir.to_string_lossy().into_owned();

    let (schema, mode_config) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );

    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .on_disk(root_str.clone())
        .create_dirs(true)
        .recover_or_init_with_executor(Arc::clone(&executor))
        .await?;

    let initial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-initial"])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ],
    )?;
    db.ingest(initial_batch).await?;

    db.disable_wal().await?;
    drop(db);

    let (_, mode_config_recover) = schema_and_config(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::<DynMode, TokioExecutor>::builder(mode_config_recover)
            .on_disk(root_str.clone())
            .create_dirs(true)
            .recover_or_init_with_executor(Arc::clone(&executor))
            .await?;

    let runtime_cfg = recovered
        .wal_config()
        .cloned()
        .expect("wal config available");
    recovered.disable_wal().await?;
    recovered.enable_wal(runtime_cfg.clone()).await?;

    let wal_handle = recovered.wal().cloned().expect("wal handle");
    let append_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["user-new"])) as _,
            Arc::new(Int32Array::from(vec![5])) as _,
        ],
    )?;
    let append = wal_handle
        .append(&append_batch, Timestamp::new(777))
        .await?;
    assert_eq!(
        append.commit_ticket.seq,
        INITIAL_FRAME_SEQ.saturating_add(1)
    );
    let (append_ack, commit_ack) = append.durable().await?;
    assert!(append_ack.first_seq <= append_ack.last_seq);
    assert!(commit_ack.first_seq <= commit_ack.last_seq);

    recovered.disable_wal().await?;
    drop(recovered);
    if let Err(err) = fs::remove_dir_all(&root_dir) {
        eprintln!("failed to clean test dir {:?}: {err}", &root_dir);
    }

    Ok(())
}
