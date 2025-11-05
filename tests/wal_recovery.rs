use std::{
    fs,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{Write, executor::tokio::TokioExecutor, path::Path as FusioPath};
use tonbo::{
    DB,
    key::KeyOwned,
    mode::{DynMode, DynModeConfig},
    mvcc::Timestamp,
    scan::RangeSet,
    wal::{
        WalConfig, WalExt, WalRecoveryMode, WalSyncPolicy,
        frame::{INITIAL_FRAME_SEQ, encode_autocommit_frames},
        storage::WalStorage,
    },
};
use typed_arrow_dyn::DynCell;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovers_rows_across_restart() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = std::env::temp_dir().join(format!(
        "tonbo-wal-e2e-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&root_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let executor = Arc::new(TokioExecutor::default());
    let root_str = root_dir.to_string_lossy().into_owned();
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .on_disk(root_str.clone())
        .build_with_executor(Arc::clone(&executor))
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    let wal_cfg = db
        .wal_config()
        .cloned()
        .expect("builder should set wal config");
    let recovery_cfg = wal_cfg.clone();

    db.enable_wal(wal_cfg)?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-1", "user-2"])) as _,
            Arc::new(Int32Array::from(vec![10, 20])) as _,
        ],
    )?;
    db.ingest(batch).await?;

    db.disable_wal()?;
    drop(db);

    let mode_config_recover = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let recovered: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config_recover, Arc::clone(&executor), recovery_cfg).await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.into_iter();
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

    drop(recovered);
    fs::remove_dir_all(&root_dir)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_ignores_truncated_commit() -> Result<(), Box<dyn std::error::Error>> {
    let wal_dir = std::env::temp_dir().join(format!(
        "tonbo-wal-truncated-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&wal_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let executor = Arc::new(TokioExecutor::default());

    let mut wal_cfg = WalConfig::default();
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
    let committed_frames = encode_autocommit_frames(
        committed_batch.clone(),
        vec![false],
        11,
        Timestamp::new(100),
    )?;

    let partial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["partial"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    let partial_frames =
        encode_autocommit_frames(partial_batch.clone(), vec![false], 17, Timestamp::new(200))?;

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

    let recovered: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config, Arc::clone(&executor), wal_cfg).await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.into_iter();
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

    drop(recovered);
    fs::remove_dir_all(&wal_dir)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_tolerates_corrupted_tail() -> Result<(), Box<dyn std::error::Error>> {
    let wal_dir = std::env::temp_dir().join(format!(
        "tonbo-wal-tolerate-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&wal_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let executor = Arc::new(TokioExecutor::default());

    let mut wal_cfg = WalConfig::default();
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
    let committed_frames = encode_autocommit_frames(
        committed_batch.clone(),
        vec![false],
        11,
        Timestamp::new(100),
    )?;

    let partial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["partial"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    let partial_frames =
        encode_autocommit_frames(partial_batch.clone(), vec![false], 17, Timestamp::new(200))?;

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

    let recovered: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config, Arc::clone(&executor), wal_cfg).await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.into_iter();
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

    drop(recovered);
    fs::remove_dir_all(&wal_dir)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_rewrite_after_truncated_tail() -> Result<(), Box<dyn std::error::Error>> {
    let wal_dir = std::env::temp_dir().join(format!(
        "tonbo-wal-rewrite-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&wal_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let executor = Arc::new(TokioExecutor::default());

    let mut wal_cfg = WalConfig::default();
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
    let committed_frames = encode_autocommit_frames(
        committed_batch.clone(),
        vec![false],
        11,
        Timestamp::new(100),
    )?;

    let partial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["partial"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    let partial_frames =
        encode_autocommit_frames(partial_batch.clone(), vec![false], 17, Timestamp::new(200))?;

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

    let recovered: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config, Arc::clone(&executor), wal_cfg.clone()).await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.into_iter();
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

    let mut recovered = recovered;
    recovered.enable_wal(wal_cfg.clone())?;
    let rewrite_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["rewrite"])) as _,
            Arc::new(Int32Array::from(vec![2])) as _,
        ],
    )?;
    recovered.ingest(rewrite_batch).await?;
    recovered.disable_wal()?;
    drop(recovered);

    let final_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let recovered_again: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(final_config, Arc::clone(&executor), wal_cfg.clone()).await?;

    let mut rows_after: Vec<(String, i32)> = recovered_again
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.into_iter();
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

    drop(recovered_again);
    fs::remove_dir_all(&wal_dir)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_recovery_survives_segment_rotations() -> Result<(), Box<dyn std::error::Error>> {
    let wal_dir = std::env::temp_dir().join(format!(
        "tonbo-wal-rotate-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&wal_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::new(mode_config, Arc::clone(&executor))?;

    let mut wal_cfg = WalConfig::default();
    wal_cfg.dir = FusioPath::from_filesystem_path(&wal_dir)?;
    wal_cfg.segment_max_bytes = 1;
    wal_cfg.flush_interval = Duration::from_millis(0);
    wal_cfg.sync = WalSyncPolicy::Always;

    db.enable_wal(wal_cfg.clone())?;

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

    db.disable_wal()?;
    drop(db);

    let mode_config_recover = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config_recover, Arc::clone(&executor), wal_cfg.clone()).await?;

    let ranges = RangeSet::<KeyOwned>::all();
    let mut rows: Vec<(String, i32)> = recovered
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.into_iter();
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

    recovered.enable_wal(wal_cfg.clone())?;
    let extra_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-extra"])) as _,
            Arc::new(Int32Array::from(vec![999])) as _,
        ],
    )?;
    recovered.ingest(extra_batch).await?;
    recovered.disable_wal()?;
    drop(recovered);

    let mode_config_final = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let recovered_again: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config_final, Arc::clone(&executor), wal_cfg.clone()).await?;

    let mut rows_final: Vec<(String, i32)> = recovered_again
        .scan_mutable_rows(&ranges)
        .map(|row| {
            let mut cells = row.into_iter();
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

    drop(recovered_again);
    fs::remove_dir_all(&wal_dir)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_reenable_seeds_provisional_sequence() -> Result<(), Box<dyn std::error::Error>> {
    let wal_dir = std::env::temp_dir().join(format!(
        "tonbo-wal-seq-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&wal_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::new(mode_config, Arc::clone(&executor))?;

    let mut wal_cfg = WalConfig::default();
    wal_cfg.dir = FusioPath::from_filesystem_path(&wal_dir)?;
    let recovery_cfg = wal_cfg.clone();

    db.enable_wal(wal_cfg.clone())?;

    let initial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-initial"])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ],
    )?;
    db.ingest(initial_batch).await?;

    db.disable_wal()?;
    drop(db);

    let mode_config_recover = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let mut recovered: DB<DynMode, TokioExecutor> =
        DB::recover_with_wal(mode_config_recover, Arc::clone(&executor), recovery_cfg).await?;

    recovered.enable_wal(wal_cfg.clone())?;

    let wal_handle = recovered.wal().cloned().expect("wal handle");
    let append_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["user-new"])) as _,
            Arc::new(Int32Array::from(vec![5])) as _,
        ],
    )?;
    let ticket = wal_handle
        .append(&append_batch, &[false], Timestamp::new(777))
        .await?;
    assert_eq!(ticket.seq, INITIAL_FRAME_SEQ.saturating_add(1));
    ticket.durable().await?;

    recovered.disable_wal()?;
    drop(recovered);
    fs::remove_dir_all(&wal_dir)?;

    Ok(())
}
