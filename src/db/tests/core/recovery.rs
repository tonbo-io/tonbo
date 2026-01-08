use std::{
    fs,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fusio::{
    DynFs,
    disk::LocalFs,
    executor::{NoopExecutor, tokio::TokioExecutor},
    fs::FsCas,
    mem::fs::InMemoryFs,
    path::{Path, PathPart},
};
use futures::{TryStreamExt, executor::block_on};
use tonbo_predicate::{ColumnRef, Predicate, ScalarValue};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{
    db::{DbInner, builder},
    extractor::KeyExtractError,
    id::FileIdGenerator,
    manifest::{init_fs_manifest_in_memory, init_in_memory_manifest},
    metrics::ObjectStoreMetrics,
    mode::{DynModeConfig, table_definition},
    mvcc::Timestamp,
    test::build_batch,
    transaction::CommitAckMode,
    wal::{
        DynBatchPayload, WalCommand, WalConfig as RuntimeWalConfig, WalExt,
        frame::{INITIAL_FRAME_SEQ, encode_command},
        state::FsWalStateStore,
    },
};

fn build_dyn_components(
    config: DynModeConfig,
) -> Result<
    (
        SchemaRef,
        SchemaRef,
        CommitAckMode,
        crate::inmem::mutable::DynMem,
    ),
    KeyExtractError,
> {
    config.build()
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

    let manifest = init_fs_manifest_in_memory(fs.clone(), &manifest_root, TokioExecutor::default())
        .await
        .expect("init manifest");
    let file_ids = FileIdGenerator::default();
    let table_def = table_definition(&build_config, builder::DEFAULT_TABLE_NAME);
    let table_meta = manifest
        .register_table(&file_ids, &table_def)
        .await
        .expect("register table");
    let manifest_table = table_meta.table_id;

    let (schema, delete_schema, commit_ack_mode, mem) = build_dyn_components(build_config)?;
    let db_fs: Arc<dyn DynFs> = Arc::new(fs.clone());
    let object_store_metrics = Arc::new(ObjectStoreMetrics::default());
    let mut db = DbInner::from_components(
        schema.clone(),
        delete_schema,
        commit_ack_mode,
        mem,
        db_fs,
        manifest,
        manifest_table,
        table_meta.clone(),
        Some(wal_cfg.clone()),
        Arc::clone(&object_store_metrics),
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

    let manifest = init_fs_manifest_in_memory(fs.clone(), &manifest_root, TokioExecutor::default())
        .await
        .expect("reopen manifest");
    let recover_config = DynModeConfig::from_key_name(schema, "id")?;
    let recover_fs: Arc<dyn DynFs> = Arc::new(fs.clone());
    let mut recovered = DbInner::recover_with_wal_with_manifest(
        recover_config,
        Arc::clone(&executor),
        recover_fs,
        wal_cfg.clone(),
        manifest,
        manifest_table,
        table_meta.clone(),
        object_store_metrics,
    )
    .await
    .expect("recover");
    recovered.enable_wal(wal_cfg).await.expect("re-enable wal");

    assert_eq!(recovered.table_id(), manifest_table);

    let pred = Predicate::is_not_null(ColumnRef::new("id"));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_replays_commit_timestamps_and_advances_clock() {
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
        Field::new(
            crate::inmem::immutable::memtable::MVCC_COMMIT_COL,
            DataType::UInt64,
            false,
        ),
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
    let table_def = table_definition(&config, builder::DEFAULT_TABLE_NAME);
    let manifest = init_in_memory_manifest(NoopExecutor)
        .await
        .expect("init manifest");
    let file_ids = FileIdGenerator::default();
    let table_meta = manifest
        .register_table(&file_ids, &table_def)
        .await
        .expect("register table");
    let manifest_table = table_meta.table_id;
    let test_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let object_store_metrics = Arc::new(ObjectStoreMetrics::default());
    let db: DbInner<InMemoryFs, NoopExecutor> = DbInner::recover_with_wal_with_manifest(
        config,
        executor.clone(),
        test_fs,
        cfg,
        manifest,
        manifest_table,
        table_meta,
        object_store_metrics,
    )
    .await
    .expect("recover");

    // Replayed version retains commit_ts 42 and tombstone state.
    let chain = db
        .mem_read()
        .inspect_versions(&crate::key::KeyOwned::from("k"))
        .expect("chain");
    assert_eq!(chain, vec![(Timestamp::new(42), true)]);

    let pred = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("k"));
    let snapshot = db.snapshot_at(Timestamp::new(50)).await.expect("snapshot");
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
        .inspect_versions(&crate::key::KeyOwned::from("k"))
        .expect("chain");
    assert_eq!(
        chain,
        vec![(Timestamp::new(42), true), (Timestamp::new(43), false)]
    );

    fs::remove_dir_all(&wal_dir).expect("cleanup");
}
