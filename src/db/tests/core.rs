use std::{
    collections::HashSet,
    fs,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow_array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use fusio::{
    disk::LocalFs,
    dynamic::{DynFs, MaybeSendFuture},
    executor::{NoopExecutor, tokio::TokioExecutor},
    fs::FsCas,
    mem::fs::InMemoryFs,
    path::{Path, PathPart},
};
use futures::{
    StreamExt, TryStreamExt,
    channel::{mpsc, oneshot as futures_oneshot},
    executor::block_on,
};
use predicate::{ColumnRef, Predicate, ScalarValue};
use tokio::sync::{Mutex, oneshot};
use typed_arrow_dyn::{DynCell, DynRow};

use super::*;
use crate::{
    compaction::{
        executor::{
            CompactionError, CompactionExecutor, CompactionJob, CompactionOutcome,
            LocalCompactionExecutor,
        },
        planner::{
            CompactionInput, CompactionPlanner, CompactionSnapshot, CompactionTask,
            LeveledCompactionPlanner, LeveledPlannerConfig,
        },
    },
    db::wal::apply_dyn_wal_batch,
    extractor::{KeyProjection, projection_for_columns},
    inmem::{
        immutable::memtable::{MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL},
        mutable::memtable::DynMem,
        policy::{BatchesThreshold, NeverSeal},
    },
    manifest::{SstEntry, TableId, TonboManifest, VersionEdit, VersionState, init_fs_manifest},
    mvcc::Timestamp,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
    test::build_batch,
    wal::{
        DynBatchPayload, WalCommand, WalExt, WalResult, WalSyncPolicy,
        frame::{INITIAL_FRAME_SEQ, encode_command},
        metrics::WalMetrics,
        state::FsWalStateStore,
    },
};

fn build_dyn_components(config: DynModeConfig) -> Result<(DynMode, DynMem), KeyExtractError> {
    let DynModeConfig {
        schema,
        extractor,
        commit_ack_mode,
    } = config;
    extractor.validate_schema(&schema)?;

    let key_schema = extractor.key_schema();
    let mut delete_fields = key_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    delete_fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false));
    let delete_schema = Arc::new(Schema::new(delete_fields));

    let key_columns = key_schema.fields().len();
    let delete_projection =
        projection_for_columns(delete_schema.clone(), (0..key_columns).collect())?;
    let delete_projection: Arc<dyn KeyProjection> = delete_projection.into();

    let mutable = DynMem::new(schema.clone());
    let mode = DynMode {
        schema,
        delete_schema,
        extractor,
        delete_projection,
        commit_ack_mode,
    };
    Ok((mode, mutable))
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_tombstone_length_mismatch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let executor = Arc::new(NoopExecutor);
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::clone(&executor)).await.expect("db");

    let err = db
        .ingest_with_tombstones(batch, vec![])
        .await
        .expect_err("length mismatch");
    assert!(matches!(
        err,
        KeyExtractError::TombstoneLengthMismatch {
            expected: 1,
            actual: 0
        }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_batch_with_tombstones_marks_versions_and_visibility() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::clone(&executor)).await.expect("db");

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("k1".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("k2".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
    let result = db.ingest_with_tombstones(batch, vec![false, true]).await;
    result.expect("ingest");

    let chain_k1 = db
        .mem_read()
        .inspect_versions(&KeyOwned::from("k1"))
        .expect("chain k1");
    assert_eq!(chain_k1.len(), 1);
    assert!(!chain_k1[0].1);

    let chain_k2 = db
        .mem_read()
        .inspect_versions(&KeyOwned::from("k2"))
        .expect("chain k2");
    assert_eq!(chain_k2.len(), 1);
    assert!(chain_k2[0].1);

    let pred = Predicate::is_not_null(ColumnRef::new("id", None));
    let snapshot = block_on(db.begin_snapshot()).expect("snapshot");
    let plan = block_on(snapshot.plan_scan(&db, &pred, None, None)).expect("plan");
    let stream = block_on(db.execute_scan(plan)).expect("exec");
    let visible: Vec<String> = block_on(stream.try_collect::<Vec<_>>())
        .expect("collect")
        .into_iter()
        .flat_map(|batch| {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 col");
            col.iter()
                .flatten()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(visible, vec!["k1".to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn apply_dyn_wal_batch_inserts_live_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let executor = Arc::new(NoopExecutor);
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::clone(&executor)).await.expect("db");

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["k1", "k2"])) as _,
            Arc::new(Int32Array::from(vec![1, 2])) as _,
        ],
    )
    .expect("batch");
    let commit_ts = Timestamp::new(5);
    let commit_array: ArrayRef = Arc::new(UInt64Array::from(vec![commit_ts.get(); 2]));

    apply_dyn_wal_batch(&db, batch, commit_array, commit_ts).expect("apply");

    let live = db
        .mem_read()
        .inspect_versions(&KeyOwned::from("k1"))
        .expect("live key");
    assert_eq!(live, vec![(commit_ts, false)]);

    let deleted = db
        .mem_read()
        .inspect_versions(&KeyOwned::from("k2"))
        .expect("second key");
    assert_eq!(deleted, vec![(commit_ts, false)]);
}

#[tokio::test(flavor = "current_thread")]
async fn apply_dyn_wal_batch_rejects_tombstone_payloads() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, true),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let executor = Arc::new(NoopExecutor);
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::clone(&executor)).await.expect("db");

    let wal_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, true),
        Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false),
    ]));
    let wal_batch = RecordBatch::try_new(
        wal_schema,
        vec![
            Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef,
        ],
    )
    .expect("wal batch");
    let commit_ts = Timestamp::new(22);
    let commit_array: ArrayRef = Arc::new(UInt64Array::from(vec![commit_ts.get()]));

    let err = apply_dyn_wal_batch(&db, wal_batch, commit_array, commit_ts)
        .expect_err("apply should fail");
    match err {
        KeyExtractError::SchemaMismatch { .. } | KeyExtractError::Wal(_) => {}
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn apply_dyn_wal_batch_allows_user_tombstone_column() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("_tombstone", DataType::Utf8, false),
        Field::new("v", DataType::Int32, true),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let executor = Arc::new(NoopExecutor);
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::clone(&executor)).await.expect("db");

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("flag")])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef,
        ],
    )
    .expect("batch");
    let commit_ts = Timestamp::new(30);
    let commit_array: ArrayRef = Arc::new(UInt64Array::from(vec![commit_ts.get()]));

    apply_dyn_wal_batch(&db, batch, commit_array, commit_ts).expect("apply");

    let versions = db
        .mem_read()
        .inspect_versions(&KeyOwned::from("k"))
        .expect("versions");
    assert_eq!(versions, vec![(commit_ts, false)]);
}

#[tokio::test(flavor = "current_thread")]
async fn begin_snapshot_tracks_commit_clock() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::clone(&executor)).await.expect("db");

    let snapshot = db.begin_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.read_view().read_ts(), Timestamp::MIN);
    assert!(snapshot.head().last_manifest_txn.is_none());
    assert!(snapshot.latest_version().is_none());

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k1".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest_with_tombstones(batch, vec![false])
        .await
        .expect("ingest");

    let snapshot_after = db.begin_snapshot().await.expect("snapshot after ingest");
    assert_eq!(snapshot_after.read_view().read_ts(), Timestamp::new(0));
    assert!(snapshot_after.head().last_manifest_txn.is_none());
    assert!(snapshot_after.latest_version().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_seal_on_batches_threshold() {
    // Build a simple schema: id: Utf8 (key), v: Int32
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    // Build one batch with two rows
    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");

    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name");
    let mut db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("schema ok");
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
    assert_eq!(db.num_immutable_segments(), 0);
    db.ingest(batch).await.expect("insert batch");
    assert_eq!(db.num_immutable_segments(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn auto_seals_when_memtable_hits_capacity() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name");
    let mut db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("schema ok");

    // Force minimal capacity and disable policy-based sealing to exercise MemtableFull
    // recovery.
    {
        let mut mem = db.mem_write();
        *mem = DynMem::with_capacity(db.mode.schema.clone(), 1);
    }
    db.set_seal_policy(Arc::new(NeverSeal));

    let make_batch = |val: i32| {
        build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(val)),
            ])],
        )
        .expect("batch")
    };

    db.ingest(make_batch(1)).await.expect("ingest 1");
    db.ingest(make_batch(2)).await.expect("ingest 2");
    db.ingest(make_batch(3)).await.expect("ingest 3");

    // Each time capacity is hit we should seal and continue inserting.
    assert_eq!(db.num_immutable_segments(), 2);
    assert_eq!(db.mem_read().batch_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_with_manifest_preserves_table_id() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let build_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let executor = Arc::new(TokioExecutor::default());

    let fs = InMemoryFs::new();
    let manifest_root = Path::parse("durable-test").expect("path");
    let wal_dir = manifest_root.child(PathPart::parse("wal").expect("wal dir component"));

    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = wal_dir;
    let dyn_fs: Arc<dyn DynFs> = Arc::new(fs.clone());
    wal_cfg.segment_backend = dyn_fs;
    let cas_backend: Arc<dyn FsCas> = Arc::new(fs.clone());
    wal_cfg.state_store = Some(Arc::new(FsWalStateStore::new(cas_backend)));

    let manifest = init_fs_manifest(fs.clone(), &manifest_root)
        .await
        .expect("init manifest");
    let file_ids = FileIdGenerator::default();
    let table_definition = DynMode::table_definition(&build_config, builder::DEFAULT_TABLE_NAME);
    let manifest_table = manifest
        .register_table(&file_ids, &table_definition)
        .await
        .expect("register table")
        .table_id;

    let (mode, mem) = build_dyn_components(build_config)?;
    let db_fs: Arc<dyn DynFs> = Arc::new(fs.clone());
    let mut db = DB::from_components(
        mode,
        mem,
        db_fs,
        manifest,
        manifest_table,
        Some(wal_cfg.clone()),
        Arc::clone(&executor),
    );
    db.enable_wal(wal_cfg.clone()).await.expect("enable wal");

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user-1"])) as _,
            Arc::new(Int32Array::from(vec![7])) as _,
        ],
    )?;
    db.ingest_with_tombstones(batch, vec![false])
        .await
        .expect("ingest");
    drop(db);

    let manifest = init_fs_manifest(fs.clone(), &manifest_root)
        .await
        .expect("reopen manifest");
    let recover_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
    let recover_fs: Arc<dyn DynFs> = Arc::new(fs.clone());
    let mut recovered = DB::recover_with_wal_with_manifest(
        recover_config,
        Arc::clone(&executor),
        recover_fs,
        wal_cfg.clone(),
        manifest,
        manifest_table,
    )
    .await
    .expect("recover");
    recovered.enable_wal(wal_cfg).await.expect("re-enable wal");

    assert_eq!(recovered.manifest_table_id(), manifest_table);

    let pred = Predicate::is_not_null(ColumnRef::new("id", None));
    let snapshot = block_on(recovered.begin_snapshot()).expect("snapshot");
    let plan = block_on(snapshot.plan_scan(&recovered, &pred, None, None)).expect("plan");
    let stream = block_on(recovered.execute_scan(plan)).expect("execute");
    let rows: Vec<(String, i32)> = block_on(stream.try_collect::<Vec<_>>())
        .expect("collect")
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
    assert_eq!(rows, vec![("user-1".into(), 7)]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_filters_immutable_segments() {
    let db = db_with_immutable_keys(&["k1", "z1"]).await;
    let predicate = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k1"));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    // Pruning is currently disabled; expect to scan all immutables and retain the predicate
    // for residual evaluation.
    assert_eq!(plan.immutable_indexes, vec![0, 1]);
    assert!(plan.residual_predicate.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_preserves_residual_predicate() {
    let db = db_with_immutable_keys(&["k1"]).await;
    let key_pred = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k1"));
    let value_pred = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(5i64));
    let predicate = Predicate::and(vec![key_pred, value_pred]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert!(plan.residual_predicate.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_marks_empty_range() {
    let db = db_with_immutable_keys(&["k1"]).await;
    let pred_a = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k1"));
    let pred_b = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k2"));
    let predicate = Predicate::and(vec![pred_a, pred_b]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    // Pruning is currently disabled; even contradictory predicates scan all immutables.
    assert_eq!(plan.immutable_indexes, vec![0]);
}

async fn db_with_immutable_keys(keys: &[&str]) -> DB<DynMode, NoopExecutor> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let mut db: DB<DynMode, NoopExecutor> =
        DB::new(config, Arc::clone(&executor)).await.expect("db");
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
    for (idx, key) in keys.iter().enumerate() {
        let rows = vec![DynRow(vec![
            Some(DynCell::Str((*key).into())),
            Some(DynCell::I32(idx as i32)),
        ])];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        db.ingest_with_tombstones(batch, vec![false])
            .await
            .expect("ingest");
    }
    db
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_waits_for_wal_durable_ack() {
    use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

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

    let mut db: DB<DynMode, TokioExecutor> =
        DB::new(config, Arc::clone(&executor)).await.expect("db");
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
    use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

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

    let mut db: DB<DynMode, TokioExecutor> =
        DB::new(config, Arc::clone(&executor)).await.expect("db");
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
        let sealed = {
            let mut mem = db.mem_write();
            mem.seal_into_immutable(&db.mode.schema, db.mode.extractor.as_ref())
                .expect("seal mutable")
                .expect("mutable contained rows")
        };
        let wal_range = db.take_mutable_wal_range();
        db.add_immutable(sealed, wal_range);
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
    use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

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

    let mut db: DB<DynMode, TokioExecutor> =
        DB::new(config, Arc::clone(&executor)).await.expect("db");
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
        let sealed = {
            let mut mem = db.mem_write();
            mem.seal_into_immutable(&db.mode.schema, db.mode.extractor.as_ref())
                .expect("seal mutable")
                .expect("mutable contained rows")
        };
        let wal_range = db.take_mutable_wal_range();
        db.add_immutable(sealed, wal_range);
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

    use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

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

    let mut db: DB<DynMode, TokioExecutor> =
        DB::new(config, Arc::clone(&executor)).await.expect("db");
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
#[tokio::test(flavor = "current_thread")]
async fn dynamic_new_from_metadata_field_marker() {
    use std::collections::HashMap;
    // Schema: mark id with field-level metadata tonbo.key = true
    let mut fm = HashMap::new();
    fm.insert("tonbo.key".to_string(), "true".to_string());
    let f_id = Field::new("id", DataType::Utf8, false).with_metadata(fm);
    let f_v = Field::new("v", DataType::Int32, false);
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata key config");
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("metadata key");

    // Build one batch and insert to ensure extractor wired
    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert via metadata");
    assert_eq!(db.num_immutable_segments(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_new_from_metadata_schema_level() {
    use std::collections::HashMap;
    let f_id = Field::new("id", DataType::Utf8, false);
    let f_v = Field::new("v", DataType::Int32, false);
    let mut sm = HashMap::new();
    sm.insert("tonbo.keys".to_string(), "id".to_string());
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]).with_metadata(sm));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("schema metadata config");
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("schema metadata key");

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("x".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("y".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert via metadata");
    assert_eq!(db.num_immutable_segments(), 0);
}

#[test]
fn dynamic_new_from_metadata_conflicts_and_missing() {
    use std::collections::HashMap;
    // Conflict: two fields marked as key
    let mut fm1 = HashMap::new();
    fm1.insert("tonbo.key".to_string(), "true".to_string());
    let mut fm2 = HashMap::new();
    fm2.insert("tonbo.key".to_string(), "1".to_string());
    let f1 = Field::new("id1", DataType::Utf8, false).with_metadata(fm1);
    let f2 = Field::new("id2", DataType::Utf8, false).with_metadata(fm2);
    let schema_conflict = std::sync::Arc::new(Schema::new(vec![f1, f2]));
    assert!(DynModeConfig::from_metadata(schema_conflict).is_err());

    // Missing: no markers at field or schema level
    let schema_missing = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    assert!(DynModeConfig::from_metadata(schema_missing).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn flush_without_immutables_errors() {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name config");
    let executor = Arc::new(NoopExecutor);
    let mut db: DB<DynMode, NoopExecutor> = DB::new(config, executor).await.expect("db init");

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sstable_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        fs,
        Path::from("/tmp/tonbo-flush-test"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(1), 0);

    let result = db
        .flush_immutables_with_descriptor(sstable_cfg, descriptor.clone())
        .await;
    assert!(matches!(result, Err(SsTableError::NoImmutableSegments)));
    assert_eq!(db.num_immutable_segments(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_publishes_manifest_version() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let mut db: DB<DynMode, NoopExecutor> =
        DB::new(config, Arc::clone(&executor)).await.expect("db");
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("ingest triggers seal");
    assert_eq!(db.num_immutable_segments(), 1);

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sstable_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        fs,
        Path::from("/tmp/tonbo-flush-ok"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(7), 0);

    let table = db
        .flush_immutables_with_descriptor(sstable_cfg, descriptor.clone())
        .await
        .expect("flush succeeds");
    assert_eq!(db.num_immutable_segments(), 0);

    let snapshot = db
        .manifest
        .snapshot_latest(db.manifest_table)
        .await
        .expect("manifest snapshot");
    assert_eq!(
        snapshot.head.last_manifest_txn,
        Some(Timestamp::new(1)),
        "first flush should publish manifest txn 1"
    );
    let latest = snapshot
        .latest_version
        .expect("latest version must exist after flush");
    assert_eq!(
        latest.commit_timestamp(),
        Timestamp::new(1),
        "latest version should reflect manifest txn 1"
    );
    assert_eq!(latest.ssts().len(), 1);
    assert_eq!(latest.ssts()[0].len(), 1);
    let recorded = &latest.ssts()[0][0];
    assert_eq!(recorded.sst_id(), descriptor.id());
    assert!(
        recorded.stats().is_some() || table.descriptor().stats().is_none(),
        "stats should propagate when available"
    );
    assert!(
        recorded.wal_segments().is_none(),
        "no WAL segments recorded since none were attached"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_compaction_returns_task() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = crate::schema::SchemaBuilder::from_schema(schema.clone())
        .primary_key("id")
        .with_metadata()
        .build()
        .expect("schema builder");
    let schema = Arc::clone(&config.schema);
    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::new(config, Arc::clone(&executor)).await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(SsTableConfig::new(
        Arc::clone(&schema),
        fs,
        Path::from("/tmp/plan-compaction"),
    ));

    for pass in 0..2 {
        let rows = vec![vec![
            Some(DynCell::Str(format!("comp-{pass}").into())),
            Some(DynCell::I32(pass as i32)),
        ]];
        let batch = build_batch(Arc::clone(&schema), rows).expect("batch");
        db.ingest(batch).await.expect("ingest");
        let descriptor = SsTableDescriptor::new(SsTableId::new(pass as u64 + 1), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor)
            .await
            .expect("flush");
    }

    let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
        l0_trigger: 1,
        l0_max_inputs: 2,
        l0_max_bytes: None,
        level_thresholds: vec![usize::MAX],
        level_max_bytes: Vec::new(),
        max_inputs_per_task: 2,
        max_task_bytes: None,
    });
    let task = db
        .plan_compaction_task(&planner)
        .await?
        .expect("compaction task");
    assert_eq!(task.source_level, 0);
    assert_eq!(task.target_level, 1);
    assert_eq!(task.input.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_compaction_empty_manifest_is_none() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = crate::schema::SchemaBuilder::from_schema(schema.clone())
        .primary_key("id")
        .with_metadata()
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let db: DB<DynMode, TokioExecutor> = DB::new(config, Arc::clone(&executor)).await?;
    let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig::default());
    let plan = db.plan_compaction_task(&planner).await?;
    assert!(plan.is_none());
    Ok(())
}

#[test]
fn wal_segments_after_compaction_preserves_manifest_when_metadata_missing() {
    let generator = FileIdGenerator::default();
    let table_id = TableId::new(&generator);
    let mut version = VersionState::empty(table_id);

    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

    let entry_missing_wal = SstEntry::new(SsTableId::new(1), None, None, Path::default(), None);
    let entry_with_wal = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_b.file_id().clone()]),
        Path::default(),
        None,
    );

    version
        .apply_edits(&[
            VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry_missing_wal, entry_with_wal],
            },
            VersionEdit::SetWalSegments {
                segments: vec![wal_a.clone(), wal_b.clone()],
            },
        ])
        .expect("apply edits");

    let wal_ids =
        DB::<DynMode, NoopExecutor>::wal_ids_for_remaining_ssts(&version, &HashSet::new(), &[]);
    assert!(
        wal_ids.is_none(),
        "missing wal metadata on a remaining SST should preserve the manifest wal set"
    );

    let filtered = DB::<DynMode, NoopExecutor>::wal_segments_after_compaction(&version, &[], &[]);
    assert!(
        filtered.is_none(),
        "compaction should not rewrite wal segments when metadata is absent"
    );
}

#[test]
fn wal_segments_after_compaction_filters_and_tracks_obsolete() {
    let generator = FileIdGenerator::default();
    let table_id = TableId::new(&generator);
    let mut version = VersionState::empty(table_id);

    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);
    let wal_c = WalSegmentRef::new(3, generator.generate(), 0, 0);

    let entry_a = SstEntry::new(
        SsTableId::new(1),
        None,
        Some(vec![wal_a.file_id().clone(), wal_b.file_id().clone()]),
        Path::default(),
        None,
    );
    let entry_b = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_c.file_id().clone()]),
        Path::default(),
        None,
    );

    version
        .apply_edits(&[
            VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry_a, entry_b],
            },
            VersionEdit::SetWalSegments {
                segments: vec![wal_a.clone(), wal_b.clone(), wal_c.clone()],
            },
        ])
        .expect("apply edits");

    let removed = vec![
        SsTableDescriptor::new(SsTableId::new(1), 0)
            .with_storage_paths(Path::from("L0/1.parquet"), None),
    ];
    let added = vec![SsTableDescriptor::new(SsTableId::new(3), 0)];

    assert_eq!(version.wal_segments().len(), 3);
    assert!(
        version.ssts()[0][0].wal_segments().is_some(),
        "entry should carry wal segments"
    );
    assert!(
        version.ssts()[0][1].wal_segments().is_some(),
        "second entry should carry wal segments"
    );
    let removed_ids: HashSet<SsTableId> = removed.iter().map(|d| d.id().clone()).collect();
    let wal_ids =
        DB::<DynMode, NoopExecutor>::wal_ids_for_remaining_ssts(&version, &removed_ids, &added);
    assert!(
        wal_ids.is_none(),
        "wal ids should be None when any SST lacks wal metadata"
    );
    let filtered =
        DB::<DynMode, NoopExecutor>::wal_segments_after_compaction(&version, &removed, &added);
    assert!(
        filtered.is_none(),
        "filtered wal segments should be None when any SST lacks wal metadata"
    );

    let outcome = CompactionOutcome {
        add_ssts: Vec::new(),
        remove_ssts: removed,
        target_level: 0,
        wal_segments: None,
        tombstone_watermark: None,
        outputs: added.clone(),
        obsolete_sst_ids: Vec::new(),
        wal_floor: None,
        obsolete_wal_segments: vec![wal_a.clone(), wal_b.clone()],
    };
    let plan = DB::<DynMode, NoopExecutor>::gc_plan_from_outcome(&outcome)
        .expect("gc plan")
        .expect("plan present");
    assert_eq!(plan.obsolete_wal_segments.len(), 2);
    assert!(plan.obsolete_wal_segments.contains(&wal_a));
    assert!(plan.obsolete_wal_segments.contains(&wal_b));
}

#[derive(Clone)]
struct StaticPlanner {
    task: CompactionTask,
}

impl CompactionPlanner for StaticPlanner {
    fn plan(&self, _snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        Some(self.task.clone())
    }
}

#[derive(Clone)]
struct StaticExecutor {
    outputs: Vec<SsTableDescriptor>,
    wal_segments: Vec<WalSegmentRef>,
    target_level: u32,
}

impl CompactionExecutor for StaticExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        let outputs = self.outputs.clone();
        let wal_segments = self.wal_segments.clone();
        let target_level = self.target_level;
        Box::pin(async move {
            let mut outcome = CompactionOutcome::from_outputs(
                outputs.clone(),
                job.inputs.clone(),
                target_level,
                Some(wal_segments),
            )?;
            outcome.outputs = outputs;
            Ok(outcome)
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        _outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_updates_manifest_wal_and_records_gc_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let generator = FileIdGenerator::default();
    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

    let entry_a = SstEntry::new(
        SsTableId::new(1),
        None,
        Some(vec![wal_a.file_id().clone(), wal_b.file_id().clone()]),
        Path::from("L0/1.parquet"),
        None,
    );
    let entry_b = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_b.file_id().clone()]),
        Path::from("L0/2.parquet"),
        None,
    );

    let db: DB<DynMode, NoopExecutor> = DB::new(
        crate::schema::SchemaBuilder::from_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ])))
        .primary_key("id")
        .build()
        .expect("schema builder"),
        Arc::new(NoopExecutor),
    )
    .await
    .expect("db init");

    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_a, entry_b],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone()],
                },
            ],
        )
        .await
        .expect("apply edits");

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(1),
            },
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(2),
            },
        ],
        key_range: None,
    };
    let planner = StaticPlanner { task };

    let output_desc = SsTableDescriptor::new(SsTableId::new(3), 1)
        .with_wal_ids(Some(vec![wal_b.file_id().clone()]))
        .with_storage_paths(Path::from("L1/3.parquet"), None);
    let executor = StaticExecutor {
        outputs: vec![output_desc],
        wal_segments: vec![wal_b.clone()],
        target_level: 1,
    };

    let outcome = db
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");

    let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
    let latest = snapshot.latest_version.expect("latest version");
    assert_eq!(latest.wal_segments().len(), 1);
    assert_eq!(latest.wal_segments()[0].file_id(), wal_b.file_id());
    assert_eq!(
        outcome.obsolete_wal_segments,
        vec![wal_a.clone()],
        "should surface obsolete wal segments"
    );

    let plan = db
        .manifest
        .take_gc_plan(db.manifest_table)
        .await?
        .expect("gc plan recorded");
    assert_eq!(plan.obsolete_wal_segments.len(), 1);
    assert_eq!(plan.obsolete_wal_segments[0].file_id(), wal_a.file_id());
    assert_eq!(plan.obsolete_ssts.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_preserves_manifest_wal_when_metadata_missing()
-> Result<(), Box<dyn std::error::Error>> {
    let generator = FileIdGenerator::default();
    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

    let entry_missing_wal = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/1.parquet"),
        None,
    );
    let entry_with_wal = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_b.file_id().clone()]),
        Path::from("L0/2.parquet"),
        None,
    );

    let db: DB<DynMode, NoopExecutor> = DB::new(
        crate::schema::SchemaBuilder::from_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ])))
        .primary_key("id")
        .build()
        .expect("schema builder"),
        Arc::new(NoopExecutor),
    )
    .await
    .expect("db init");

    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_missing_wal, entry_with_wal],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone()],
                },
            ],
        )
        .await
        .expect("apply edits");

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![CompactionInput {
            level: 0,
            sst_id: SsTableId::new(2),
        }],
        key_range: None,
    };
    let planner = StaticPlanner { task };

    let output_desc = SsTableDescriptor::new(SsTableId::new(3), 1)
        .with_wal_ids(Some(vec![wal_b.file_id().clone()]))
        .with_storage_paths(Path::from("L1/3.parquet"), None);
    let executor = StaticExecutor {
        outputs: vec![output_desc],
        wal_segments: vec![wal_b.clone()],
        target_level: 1,
    };

    let outcome = db
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");

    let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
    let latest = snapshot.latest_version.expect("latest version");
    assert_eq!(
        latest.wal_segments(),
        &[wal_a.clone(), wal_b.clone()],
        "manifest wal set should remain unchanged when wal metadata is missing",
    );
    assert!(
        outcome.obsolete_wal_segments.is_empty(),
        "should not surface obsolete wal segments when manifest set is preserved"
    );

    let plan = db
        .manifest
        .take_gc_plan(db.manifest_table)
        .await?
        .expect("gc plan recorded");
    assert!(
        plan.obsolete_wal_segments.is_empty(),
        "gc plan should not mark wal segments obsolete"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_e2e_merges_and_advances_wal_floor() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-e2e-wal");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor =
        crate::extractor::projection_for_field(Arc::clone(&schema), 0).expect("extractor");
    let mode_cfg = crate::schema::SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_root = temp_root.join("sst");
    fs::create_dir_all(&sst_root)?;
    let sst_root = Path::from_filesystem_path(&sst_root)?;
    let sst_cfg = Arc::new(
        SsTableConfig::new(Arc::clone(&schema), fs, sst_root).with_key_extractor(extractor.into()),
    );

    // Build two SSTs with WAL ids.
    let wal_gen = FileIdGenerator::default();
    let wal_a = WalSegmentRef::new(10, wal_gen.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(11, wal_gen.generate(), 0, 0);

    let batch_a = build_batch(
        Arc::clone(&schema),
        vec![DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I32(1)),
        ])],
    )?;
    let imm_a = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_a, "id")?;
    let mut builder_a = SsTableBuilder::<DynMode>::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(1), 0),
    );
    builder_a.add_immutable(&imm_a)?;
    let sst_a = builder_a.finish().await?;
    let desc_a = sst_a
        .descriptor()
        .clone()
        .with_wal_ids(Some(vec![wal_a.file_id().clone()]));

    let batch_b = build_batch(
        Arc::clone(&schema),
        vec![DynRow(vec![
            Some(DynCell::Str("b".into())),
            Some(DynCell::I32(2)),
        ])],
    )?;
    let imm_b = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_b, "id")?;
    let mut builder_b = SsTableBuilder::<DynMode>::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(2), 0),
    );
    builder_b.add_immutable(&imm_b)?;
    let sst_b = builder_b.finish().await?;
    let desc_b = sst_b
        .descriptor()
        .clone()
        .with_wal_ids(Some(vec![wal_b.file_id().clone()]));

    // Seed manifest with the two inputs and WAL set.
    let db: DB<DynMode, NoopExecutor> = DB::new(mode_cfg, Arc::new(NoopExecutor)).await?;
    let entry_a = SstEntry::new(
        desc_a.id().clone(),
        desc_a.stats().cloned(),
        desc_a.wal_ids().map(|ids| ids.to_vec()),
        desc_a
            .data_path()
            .expect("input descriptor missing data path")
            .clone(),
        desc_a.delete_path().cloned(),
    );
    let entry_b = SstEntry::new(
        desc_b.id().clone(),
        desc_b.stats().cloned(),
        desc_b.wal_ids().map(|ids| ids.to_vec()),
        desc_b
            .data_path()
            .expect("input descriptor missing data path")
            .clone(),
        desc_b.delete_path().cloned(),
    );
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_a, entry_b],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone()],
                },
            ],
        )
        .await?;

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: desc_a.id().clone(),
            },
            CompactionInput {
                level: 0,
                sst_id: desc_b.id().clone(),
            },
        ],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100)
        .with_max_output_bytes(8 * 1024 * 1024);

    let outcome = db
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");
    assert_eq!(outcome.remove_ssts.len(), 2);
    assert_eq!(outcome.add_ssts.len(), 1);
    assert_eq!(
        outcome
            .add_ssts
            .first()
            .and_then(|e| e.wal_segments())
            .map(|ids| ids.len()),
        Some(2),
        "output should aggregate wal ids"
    );
    assert_eq!(
        outcome.wal_segments.as_ref().map(|segments| segments.len()),
        Some(2),
        "manifest wal set should retain gap when inputs have discontinuity"
    );
    assert_eq!(
        outcome.wal_floor.as_ref().map(|w| w.seq()),
        Some(wal_a.seq()),
        "wal floor should reflect first retained segment when gaps exist"
    );
    assert_eq!(
        outcome.obsolete_wal_segments.len(),
        0,
        "no wal segments should be obsolete when gaps are retained"
    );

    let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
    let latest = snapshot.latest_version.expect("latest version");
    assert_eq!(latest.wal_segments().len(), 2);
    let wal_ids: HashSet<_> = latest
        .wal_segments()
        .iter()
        .map(|seg| seg.file_id().clone())
        .collect();
    assert!(wal_ids.contains(wal_a.file_id()));
    assert!(wal_ids.contains(wal_b.file_id()));
    assert_eq!(
        latest.wal_floor().as_ref().map(|w| w.file_id()),
        Some(wal_a.file_id()),
        "wal floor should align to first retained segment"
    );

    let plan = db
        .manifest
        .take_gc_plan(db.manifest_table)
        .await?
        .expect("gc plan recorded");
    assert_eq!(plan.obsolete_wal_segments.len(), 0);
    assert!(
        plan.obsolete_ssts.len() >= 1,
        "gc plan should record obsolete ssts"
    );

    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[derive(Clone)]
struct ConflictingExecutor {
    inner: LocalCompactionExecutor,
    manifest: TonboManifest,
    table: TableId,
    outputs: Arc<Mutex<Vec<SsTableDescriptor>>>,
}

impl ConflictingExecutor {
    fn new(
        inner: LocalCompactionExecutor,
        manifest: TonboManifest,
        table: TableId,
        outputs: Arc<Mutex<Vec<SsTableDescriptor>>>,
    ) -> Self {
        Self {
            inner,
            manifest,
            table,
            outputs,
        }
    }
}

impl CompactionExecutor for ConflictingExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        let inner = self.inner.clone();
        let manifest = self.manifest.clone();
        let table = self.table;
        let outputs = Arc::clone(&self.outputs);
        Box::pin(async move {
            let outcome = inner.execute(job).await?;
            {
                let mut guard = outputs.lock().await;
                guard.clear();
                guard.extend(outcome.outputs.iter().cloned());
            }
            manifest
                .apply_version_edits(
                    table,
                    &[VersionEdit::SetWalSegments {
                        segments: Vec::new(),
                    }],
                )
                .await
                .map_err(CompactionError::Manifest)?;
            Ok(outcome)
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        self.inner.cleanup_outputs(outputs)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_cas_conflict_cleans_outputs() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-cas-cleanup");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mode_cfg = crate::schema::SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let extractor = Arc::clone(&mode_cfg.extractor);
    let schema = Arc::clone(&mode_cfg.schema);
    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::new(mode_cfg, Arc::clone(&executor)).await?;
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let sst_root = temp_root.join("sst");
    fs::create_dir_all(&sst_root)?;
    let sst_root = Path::from_filesystem_path(&sst_root)?;
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(
        SsTableConfig::new(Arc::clone(&schema), sst_fs.clone(), sst_root)
            .with_key_extractor(extractor),
    );

    for idx in 0..2 {
        let rows = vec![vec![
            Some(DynCell::Str(format!("ck-{idx}").into())),
            Some(DynCell::I32(idx as i32)),
        ]];
        let batch = build_batch(Arc::clone(&schema), rows)?;
        db.ingest(batch).await?;
        let descriptor = SsTableDescriptor::new(SsTableId::new(idx as u64 + 1), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor)
            .await?;
    }

    let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
        l0_trigger: 1,
        l0_max_inputs: 2,
        l0_max_bytes: None,
        level_thresholds: vec![usize::MAX],
        level_max_bytes: Vec::new(),
        max_inputs_per_task: 2,
        max_task_bytes: None,
    });
    let recorded_outputs = Arc::new(Mutex::new(Vec::new()));
    let executor = ConflictingExecutor::new(
        LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100),
        db.manifest.clone(),
        db.manifest_table,
        Arc::clone(&recorded_outputs),
    );

    let result = db.run_compaction_task(&planner, &executor).await;
    match result {
        Err(CompactionError::Manifest(ManifestError::CasConflict(_)))
        | Err(CompactionError::CasConflict) => {}
        other => panic!("expected CAS conflict, got {other:?}"),
    }

    let outputs = recorded_outputs.lock().await.clone();
    assert!(
        !outputs.is_empty(),
        "compaction should have produced outputs before CAS conflict"
    );
    for desc in outputs {
        if let Some(path) = desc.data_path() {
            assert!(
                sst_fs.open(path).await.is_err(),
                "data file should be cleaned up"
            );
        }
        if let Some(path) = desc.delete_path() {
            assert!(
                sst_fs.open(path).await.is_err(),
                "delete file should be cleaned up"
            );
        }
    }

    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[test]
fn resolve_compaction_inputs_keeps_levels() {
    let file_ids = FileIdGenerator::default();
    let table_id = TableId::new(&file_ids);
    let mut version = VersionState::empty(table_id);
    let l0_id = SsTableId::new(1);
    let l1_id = SsTableId::new(2);
    let edits = vec![
        VersionEdit::AddSsts {
            level: 0,
            entries: vec![SstEntry::new(
                l0_id.clone(),
                None,
                None,
                Path::from("L0/000.parquet"),
                None,
            )],
        },
        VersionEdit::AddSsts {
            level: 1,
            entries: vec![SstEntry::new(
                l1_id.clone(),
                None,
                None,
                Path::from("L1/001.parquet"),
                None,
            )],
        },
    ];
    version.apply_edits(&edits).expect("apply edits");

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: l0_id,
            },
            CompactionInput {
                level: 1,
                sst_id: l1_id,
            },
        ],
        key_range: None,
    };

    let resolved =
        DB::<DynMode, NoopExecutor>::resolve_compaction_inputs(&version, &task).expect("resolve");
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].level(), 0);
    assert_eq!(resolved[1].level(), 1);
}

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
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .in_memory(namespace.to_string())
        .build_with_executor(Arc::clone(&executor))
        .await?;

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
    let mode_config = crate::schema::SchemaBuilder::from_schema(schema)
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
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .in_memory(namespace.to_string())
        .build_with_executor(Arc::clone(&executor))
        .await?;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_records_manifest_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("wal-manifest-metadata");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let mode_config = crate::schema::SchemaBuilder::from_schema(schema)
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
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
        .in_memory(namespace.to_string())
        .build_with_executor(Arc::clone(&executor))
        .await?;

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
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
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

#[tokio::test(flavor = "current_thread")]
async fn dynamic_composite_from_field_ordinals_and_scan() {
    use std::collections::HashMap;
    // Fields: id (Utf8, ord 1), ts (Int64, ord 2), v (Int32)
    let mut m1 = HashMap::new();
    m1.insert("tonbo.key".to_string(), "1".to_string());
    let mut m2 = HashMap::new();
    m2.insert("tonbo.key".to_string(), "2".to_string());
    let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
    let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
    let f_v = Field::new("v", DataType::Int32, false);
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("composite field metadata");

    let rows = vec![
        DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I64(10)),
            Some(DynCell::I32(1)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I64(5)),
            Some(DynCell::I32(2)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("b".into())),
            Some(DynCell::I64(1)),
            Some(DynCell::I32(3)),
        ]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert batch");

    let pred = Predicate::and(vec![
        Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("a")),
        Predicate::and(vec![
            Predicate::gte(ColumnRef::new("ts", None), ScalarValue::from(5i64)),
            Predicate::lte(ColumnRef::new("ts", None), ScalarValue::from(10i64)),
        ]),
    ]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &pred, None, None)
        .await
        .expect("plan");
    let batches = db
        .execute_scan(plan)
        .await
        .expect("exec")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect");
    let got: Vec<(String, i64)> = batches
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id col");
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ts col");
            ids.iter()
                .zip(ts.iter())
                .filter_map(|(id, t)| Some((id?.to_string(), t?)))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_composite_from_schema_list_and_scan() {
    use std::collections::HashMap;
    let f_id = Field::new("id", DataType::Utf8, false);
    let f_ts = Field::new("ts", DataType::Int64, false);
    let f_v = Field::new("v", DataType::Int32, false);
    let mut sm = HashMap::new();
    sm.insert("tonbo.keys".to_string(), "[\"id\", \"ts\"]".to_string());
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]).with_metadata(sm));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
    let db: DB<DynMode, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("composite schema metadata");

    let rows = vec![
        DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I64(5)),
            Some(DynCell::I32(1)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I64(10)),
            Some(DynCell::I32(2)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("b".into())),
            Some(DynCell::I64(1)),
            Some(DynCell::I32(3)),
        ]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert batch");

    let pred = Predicate::and(vec![
        Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("a")),
        Predicate::and(vec![
            Predicate::gte(ColumnRef::new("ts", None), ScalarValue::from(1i64)),
            Predicate::lte(ColumnRef::new("ts", None), ScalarValue::from(10i64)),
        ]),
    ]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let batches = db
        .execute_scan(
            snapshot
                .plan_scan(&db, &pred, None, None)
                .await
                .expect("plan"),
        )
        .await
        .expect("exec")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect");
    let got: Vec<(String, i64)> = batches
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id col");
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ts col");
            ids.iter()
                .zip(ts.iter())
                .filter_map(|(id, t)| Some((id?.to_string(), t?)))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_replays_commit_timestamps_and_advances_clock() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let clock_nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(delta) => delta.as_nanos(),
        Err(err) => panic!("system clock before unix epoch: {err}"),
    };
    let wal_dir = std::env::temp_dir().join(format!("tonbo-replay-test-{clock_nanos}"));
    fs::create_dir_all(&wal_dir).expect("create wal dir");

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let delete_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
    ]));
    let delete_batch = RecordBatch::try_new(
        delete_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![42])) as ArrayRef,
        ],
    )
    .expect("delete batch");

    let mut frames = Vec::new();
    frames.extend(
        encode_command(WalCommand::TxnAppend {
            provisional_id: 7,
            payload: DynBatchPayload::Delete {
                batch: delete_batch.clone(),
            },
        })
        .expect("encode delete"),
    );
    frames.extend(
        encode_command(WalCommand::TxnCommit {
            provisional_id: 7,
            commit_ts: Timestamp::new(42),
        })
        .expect("encode commit"),
    );

    let mut seq = INITIAL_FRAME_SEQ;
    let mut bytes = Vec::new();
    for frame in frames {
        bytes.extend_from_slice(&frame.into_bytes(seq));
        seq += 1;
    }
    fs::write(wal_dir.join("wal-00000000000000000001.tonwal"), bytes).expect("write wal");

    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let mut cfg = RuntimeWalConfig::default();
    cfg.dir = fusio::path::Path::from_filesystem_path(&wal_dir).expect("wal fusio path");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let table_definition = DynMode::table_definition(&config, builder::DEFAULT_TABLE_NAME);
    let manifest = init_in_memory_manifest().await.expect("init manifest");
    let file_ids = FileIdGenerator::default();
    let manifest_table = manifest
        .register_table(&file_ids, &table_definition)
        .await
        .expect("register table")
        .table_id;
    let test_fs: Arc<dyn DynFs> = Arc::new(fusio::disk::LocalFs {});
    let db: DB<DynMode, NoopExecutor> = DB::recover_with_wal_with_manifest(
        config,
        executor.clone(),
        test_fs,
        cfg,
        manifest,
        manifest_table,
    )
    .await
    .expect("recover");

    // Replayed version retains commit_ts 42 and tombstone state.
    let chain = db
        .mem_read()
        .inspect_versions(&KeyOwned::from("k"))
        .expect("chain");
    assert_eq!(chain, vec![(Timestamp::new(42), true)]);

    let pred = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k"));
    let snapshot = db
        .begin_snapshot_at(Timestamp::new(50))
        .await
        .expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &pred, None, None)
        .await
        .expect("plan");
    let visible_rows: usize = db
        .execute_scan(plan)
        .await
        .expect("execute")
        .try_fold(
            0usize,
            |acc, batch| async move { Ok(acc + batch.num_rows()) },
        )
        .await
        .expect("fold");
    assert_eq!(visible_rows, 0);

    // New ingest should advance to > 42 (next clock tick).
    let new_batch = build_batch(
        schema.clone(),
        vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(2)),
        ])],
    )
    .expect("batch2");
    db.ingest(new_batch).await.expect("ingest new");

    let chain = db
        .mem_read()
        .inspect_versions(&KeyOwned::from("k"))
        .expect("chain");
    assert_eq!(
        chain,
        vec![(Timestamp::new(42), true), (Timestamp::new(43), false)]
    );

    fs::remove_dir_all(&wal_dir).expect("cleanup");
}

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

fn wal_segment_paths(dir: &std::path::Path) -> Vec<PathBuf> {
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
