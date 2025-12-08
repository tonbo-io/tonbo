use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
use futures::{TryStreamExt, executor::block_on};
use predicate::{ColumnRef, Predicate};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{
    db::{DB, DbInner, wal::apply_dyn_wal_batch},
    extractor::{self, KeyExtractError},
    inmem::{
        immutable::memtable::MVCC_TOMBSTONE_COL,
        policy::{BatchesThreshold, NeverSeal},
    },
    key::KeyOwned,
    mode::DynModeConfig,
    mvcc::Timestamp,
    test::build_batch,
};

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
    let db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();

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
    let extractor = extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();

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

    let pred = Predicate::is_not_null(ColumnRef::new("id"));
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
    let db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();

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
    let db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();

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
    let db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();

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
    let extractor = extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();

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
    let mut db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("schema ok")
        .into_inner();
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
    let mut db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("schema ok")
        .into_inner();

    // Force minimal capacity and disable policy-based sealing to exercise MemtableFull
    // recovery.
    db.set_mem_capacity(1);
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
