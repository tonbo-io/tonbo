use std::{fs, sync::Arc, time::Duration};

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{
    DynFs,
    executor::{Executor, tokio::TokioExecutor},
    fs::FsCas,
    mem::fs::InMemoryFs,
    path::Path,
};
use futures::{
    StreamExt, TryStreamExt,
    channel::{mpsc, oneshot as futures_oneshot},
};
use predicate::{ColumnRef, Predicate};
use tokio::sync::{Mutex, oneshot};
use typed_arrow_dyn::{DynCell, DynRow};

use super::common::workspace_temp_dir;
use crate::{
    db::{DB, DbInner},
    inmem::policy::BatchesThreshold,
    mode::DynModeConfig,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableId},
    schema::SchemaBuilder,
    test::build_batch,
    wal::{
        WalAck, WalCommand, WalConfig as RuntimeWalConfig, WalExt, WalHandle, WalResult,
        WalSnapshot, WalSyncPolicy, frame, metrics::WalMetrics, state::FsWalStateStore, writer,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_waits_for_wal_durable_ack() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

    let executor = Arc::new(TokioExecutor::default());
    let (sender, mut receiver) = mpsc::channel(1);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ack_slot = Arc::new(Mutex::new(None));
    let (ack_ready_tx, ack_ready_rx) = oneshot::channel();
    let (release_ack_tx, release_ack_rx) = oneshot::channel();

    let ack_slot_clone = Arc::clone(&ack_slot);
    let join = executor.spawn(async move {
        let mut release_ack_rx = Some(release_ack_rx);
        while let Some(msg) = receiver.next().await {
            match msg {
                writer::WriterMsg::Enqueue {
                    command, ack_tx, ..
                } => match command {
                    WalCommand::TxnAppend { .. } => {
                        let ack = WalAck {
                            first_seq: frame::INITIAL_FRAME_SEQ,
                            last_seq: frame::INITIAL_FRAME_SEQ,
                            bytes_flushed: 0,
                            elapsed: Duration::from_millis(0),
                        };
                        let _ = ack_tx.send(Ok(ack));
                    }
                    WalCommand::TxnCommit { .. } => {
                        {
                            let mut slot = ack_slot_clone.lock().await;
                            *slot = Some(ack_tx);
                        }
                        let _ = ack_ready_tx.send(());
                        if let Some(rx) = release_ack_rx.take() {
                            let _ = rx.await;
                        }
                        let ack = WalAck {
                            first_seq: frame::INITIAL_FRAME_SEQ + 1,
                            last_seq: frame::INITIAL_FRAME_SEQ + 1,
                            bytes_flushed: 0,
                            elapsed: Duration::from_millis(0),
                        };
                        let mut slot = ack_slot_clone.lock().await;
                        if let Some(sender) = slot.take() {
                            let _ = sender.send(Ok(ack));
                        }
                        break;
                    }
                    _ => {
                        let ack = WalAck {
                            first_seq: frame::INITIAL_FRAME_SEQ,
                            last_seq: frame::INITIAL_FRAME_SEQ,
                            bytes_flushed: 0,
                            elapsed: Duration::from_millis(0),
                        };
                        let _ = ack_tx.send(Ok(ack));
                    }
                },
                writer::WriterMsg::Rotate { ack_tx } => {
                    let _ = ack_tx.send(Ok(()));
                }
                writer::WriterMsg::Snapshot { ack_tx } => {
                    let snapshot = WalSnapshot {
                        sealed_segments: Vec::new(),
                        active_segment: None,
                    };
                    let _ = ack_tx.send(Ok(snapshot));
                }
            }
        }
        Ok(())
    });

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

    let mut db: DbInner<InMemoryFs, TokioExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();
    let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
    let handle =
        WalHandle::test_from_parts(sender, queue_depth, join, frame::INITIAL_FRAME_SEQ, metrics);
    db.set_wal_handle(Some(handle));

    let mut ingest_future = Box::pin(db.ingest(batch));
    tokio::select! {
        _ = ack_ready_rx => {}
        res = &mut ingest_future => panic!("ingest finished early: {:?}", res),
    }

    release_ack_tx.send(()).expect("release ack");
    ingest_future.await.expect("ingest after ack");

    let pred = Predicate::is_not_null(ColumnRef::new("id", None));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &pred, None, None)
        .await
        .expect("plan");
    let stream = db.execute_scan(plan).await.expect("execute");
    let rows: Vec<_> = stream
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .into_iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id col")
                .iter()
                .flatten()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(rows, vec!["k".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_live_frame_floor_tracks_multi_frame_append() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

    let executor = Arc::new(TokioExecutor::default());
    let (sender, mut receiver) = mpsc::channel(4);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let join = executor.spawn(async move {
        let mut next_seq = frame::INITIAL_FRAME_SEQ;
        while let Some(msg) = receiver.next().await {
            match msg {
                writer::WriterMsg::Enqueue {
                    command, ack_tx, ..
                } => {
                    let (first_seq, last_seq, advance) = match command {
                        WalCommand::TxnAppend { .. } => (next_seq, next_seq.saturating_add(1), 2),
                        WalCommand::TxnCommit { .. } => (next_seq, next_seq, 1),
                        _ => (next_seq, next_seq, 1),
                    };
                    next_seq = next_seq.saturating_add(advance);
                    let ack = WalAck {
                        first_seq,
                        last_seq,
                        bytes_flushed: 0,
                        elapsed: Duration::from_millis(0),
                    };
                    let _ = ack_tx.send(Ok(ack));
                }
                writer::WriterMsg::Rotate { ack_tx } => {
                    let _ = ack_tx.send(Ok(()));
                }
                writer::WriterMsg::Snapshot { ack_tx } => {
                    let snapshot = WalSnapshot {
                        sealed_segments: Vec::new(),
                        active_segment: None,
                    };
                    let _ = ack_tx.send(Ok(snapshot));
                }
            }
        }
        Ok(())
    });

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

    let mut db: DbInner<InMemoryFs, TokioExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();
    let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
    let handle =
        WalHandle::test_from_parts(sender, queue_depth, join, frame::INITIAL_FRAME_SEQ, metrics);
    db.set_wal_handle(Some(handle));
    assert!(db.wal_handle().is_some(), "wal handle should be installed");

    db.ingest_with_tombstones(batch, vec![false])
        .await
        .expect("ingest");

    let observed_range = db
        .mutable_wal_range_snapshot()
        .or_else(|| {
            db.seal_state_lock()
                .immutable_wal_ranges
                .first()
                .copied()
                .flatten()
        })
        .expect("wal range populated after ingest");
    assert_eq!(observed_range.first, frame::INITIAL_FRAME_SEQ);
    assert_eq!(observed_range.last, frame::INITIAL_FRAME_SEQ + 2);
    assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

    if db.mutable_wal_range_snapshot().is_some() {
        if let Some(sealed) = db.seal_mutable() {
            let wal_range = db.take_mutable_wal_range();
            db.add_immutable(sealed, wal_range);
        }
    }

    assert!(db.mutable_wal_range_snapshot().is_none());
    assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

    {
        let mut seal = db.seal_state_lock();
        seal.immutables.clear();
        seal.immutable_wal_ranges.clear();
    }
    assert_eq!(db.wal_live_frame_floor(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_live_frame_floor_tracks_multi_frame_append_via_insert() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

    let executor = Arc::new(TokioExecutor::default());
    let (sender, mut receiver) = mpsc::channel(4);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let join = executor.spawn(async move {
        let mut next_seq = frame::INITIAL_FRAME_SEQ;
        while let Some(msg) = receiver.next().await {
            match msg {
                writer::WriterMsg::Enqueue {
                    command, ack_tx, ..
                } => {
                    let (first_seq, last_seq, advance) = match command {
                        WalCommand::TxnAppend { .. } => (next_seq, next_seq.saturating_add(1), 2),
                        WalCommand::TxnCommit { .. } => (next_seq, next_seq, 1),
                        _ => (next_seq, next_seq, 1),
                    };
                    next_seq = next_seq.saturating_add(advance);
                    let ack = WalAck {
                        first_seq,
                        last_seq,
                        bytes_flushed: 0,
                        elapsed: Duration::from_millis(0),
                    };
                    let _ = ack_tx.send(Ok(ack));
                }
                writer::WriterMsg::Rotate { ack_tx } => {
                    let _ = ack_tx.send(Ok(()));
                }
                writer::WriterMsg::Snapshot { ack_tx } => {
                    let snapshot = WalSnapshot {
                        sealed_segments: Vec::new(),
                        active_segment: None,
                    };
                    let _ = ack_tx.send(Ok(snapshot));
                }
            }
        }
        Ok(())
    });

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

    let mut db: DbInner<InMemoryFs, TokioExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();
    let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
    let handle =
        WalHandle::test_from_parts(sender, queue_depth, join, frame::INITIAL_FRAME_SEQ, metrics);
    db.set_wal_handle(Some(handle));
    assert!(db.wal_handle().is_some(), "wal handle should be installed");

    db.ingest(batch.clone()).await.expect("ingest");

    let observed_range = db
        .mutable_wal_range_snapshot()
        .or_else(|| {
            db.seal_state_lock()
                .immutable_wal_ranges
                .first()
                .copied()
                .flatten()
        })
        .expect("wal range populated after ingest");
    assert_eq!(observed_range.first, frame::INITIAL_FRAME_SEQ);
    assert_eq!(observed_range.last, frame::INITIAL_FRAME_SEQ + 2);
    assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

    if db.mutable_wal_range_snapshot().is_some() {
        if let Some(sealed) = db.seal_mutable() {
            let wal_range = db.take_mutable_wal_range();
            db.add_immutable(sealed, wal_range);
        }
    }

    assert!(db.mutable_wal_range_snapshot().is_none());
    assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

    {
        let mut seal = db.seal_state_lock();
        seal.immutables.clear();
        seal.immutable_wal_ranges.clear();
    }
    assert_eq!(db.wal_live_frame_floor(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dyn_insert_enqueues_commit_before_append_ack() {
    use tokio::time;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

    let executor = Arc::new(TokioExecutor::default());
    let (sender, mut receiver) = mpsc::channel(4);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let (append_seen_tx, append_seen_rx) = oneshot::channel();
    let (commit_seen_tx, commit_seen_rx) = oneshot::channel();
    let (release_append_ack_tx, release_append_ack_rx) = oneshot::channel();
    let (release_commit_ack_tx, release_commit_ack_rx) = oneshot::channel();

    let join = executor.spawn(async move {
        let mut next_seq = frame::INITIAL_FRAME_SEQ;
        let mut pending_append_ack: Option<futures_oneshot::Sender<WalResult<WalAck>>> = None;
        let mut release_append_ack_rx = Some(release_append_ack_rx);
        let mut release_commit_ack_rx = Some(release_commit_ack_rx);
        let mut append_seen_tx = Some(append_seen_tx);
        let mut commit_seen_tx = Some(commit_seen_tx);
        while let Some(msg) = receiver.next().await {
            match msg {
                writer::WriterMsg::Enqueue {
                    command, ack_tx, ..
                } => match command {
                    WalCommand::TxnAppend { .. } => {
                        pending_append_ack = Some(ack_tx);
                        if let Some(tx) = append_seen_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    WalCommand::TxnCommit { .. } => {
                        if let Some(tx) = commit_seen_tx.take() {
                            let _ = tx.send(());
                        }
                        if let Some(rx) = release_append_ack_rx.take() {
                            let _ = rx.await;
                        }
                        if let Some(append_ack_tx) = pending_append_ack.take() {
                            let ack = WalAck {
                                first_seq: next_seq,
                                last_seq: next_seq,
                                bytes_flushed: 0,
                                elapsed: Duration::from_millis(0),
                            };
                            let _ = append_ack_tx.send(Ok(ack));
                            next_seq = next_seq.saturating_add(1);
                        }
                        if let Some(rx) = release_commit_ack_rx.take() {
                            let _ = rx.await;
                        }
                        let ack = WalAck {
                            first_seq: next_seq,
                            last_seq: next_seq,
                            bytes_flushed: 0,
                            elapsed: Duration::from_millis(0),
                        };
                        let _ = ack_tx.send(Ok(ack));
                        next_seq = next_seq.saturating_add(1);
                    }
                    _ => {}
                },
                writer::WriterMsg::Rotate { ack_tx } => {
                    let _ = ack_tx.send(Ok(()));
                }
                writer::WriterMsg::Snapshot { ack_tx } => {
                    let snapshot = WalSnapshot {
                        sealed_segments: Vec::new(),
                        active_segment: None,
                    };
                    let _ = ack_tx.send(Ok(snapshot));
                }
            }
        }
        Ok(())
    });

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

    let mut db: DbInner<InMemoryFs, TokioExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();
    let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
    let handle =
        WalHandle::test_from_parts(sender, queue_depth, join, frame::INITIAL_FRAME_SEQ, metrics);
    db.set_wal_handle(Some(handle));

    let mut ingest_future = Box::pin(db.ingest(batch));
    tokio::select! {
        _ = append_seen_rx => {}
        res = &mut ingest_future => panic!("ingest finished early: {:?}", res),
    }
    tokio::select! {
        res = commit_seen_rx => {
            res.expect("commit notification");
        }
        _ = time::sleep(Duration::from_millis(50)) => {
            panic!("commit not enqueued before append ack release");
        }
        res = &mut ingest_future => panic!("ingest finished before commit ack gating: {:?}", res),
    }

    release_append_ack_tx.send(()).expect("release append ack");
    release_commit_ack_tx.send(()).expect("release commit ack");

    ingest_future.await.expect("ingest complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_records_manifest_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-manifest-metadata");
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
        .unwrap_or("wal-manifest-metadata");
    let mut db: DbInner<InMemoryFs, TokioExecutor> =
        DB::<InMemoryFs, TokioExecutor>::builder(mode_config)
            .in_memory(namespace.to_string())?
            .open_with_executor(Arc::clone(&executor))
            .await?
            .into_inner();

    let wal_local_fs = Arc::new(fusio::disk::LocalFs {});
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
    wal_cfg.flush_interval = Duration::from_millis(1);
    wal_cfg.sync = WalSyncPolicy::Disabled;

    db.enable_wal(wal_cfg.clone()).await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let rows = vec![
        vec![Some(DynCell::Str("alpha".into())), Some(DynCell::I32(7))],
        vec![Some(DynCell::Str("beta".into())), Some(DynCell::I32(9))],
    ];
    let batch = build_batch(schema.clone(), rows)?;
    db.ingest(batch).await?;
    assert!(db.num_immutable_segments() >= 1);

    let sst_dir = temp_root.join("sst");
    fs::create_dir_all(&sst_dir)?;
    let sst_root = Path::from_filesystem_path(&sst_dir)?;
    let sst_fs: Arc<dyn DynFs> = Arc::new(fusio::disk::LocalFs {});
    let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));
    let descriptor = SsTableDescriptor::new(SsTableId::new(555), 0);
    db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor.clone())
        .await?;

    let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
    let latest = snapshot
        .latest_version
        .expect("latest version should exist after flush");
    assert!(
        !latest.wal_segments().is_empty(),
        "manifest should track wal segments for the version"
    );
    assert!(
        latest.wal_floor().is_some(),
        "wal floor should be derived from recorded segments"
    );
    let recorded = &latest.ssts()[0][0];
    let stats = recorded.stats().expect("sst stats should be recorded");
    assert_eq!(stats.rows, 2);
    assert!(stats.min_key.is_some() && stats.max_key.is_some());
    assert!(stats.min_commit_ts.is_some() && stats.max_commit_ts.is_some());
    let watermark = latest
        .tombstone_watermark()
        .expect("tombstone watermark should be populated");
    assert_eq!(
        watermark,
        stats
            .max_commit_ts
            .expect("max commit timestamp should be recorded")
            .get()
    );

    if let Some(handle) = db.wal().cloned() {
        let metrics = handle.metrics();
        let guard = metrics.read().await;
        assert!(guard.wal_floor_advancements >= 1);
    }

    db.disable_wal().await?;
    fs::remove_dir_all(&temp_root)?;
    Ok(())
}
