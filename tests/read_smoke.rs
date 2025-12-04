use std::{sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{
    DynFs,
    executor::{NoopExecutor, tokio::TokioExecutor},
    mem::fs::InMemoryFs,
    path::Path as FusioPath,
};
use futures::StreamExt;
use tonbo::{
    BatchesThreshold, DB,
    db::{DynDbHandle, DynDbHandleExt},
    key::KeyOwned,
    mode::{DynMode, DynModeConfig},
    mvcc::Timestamp,
    query::{ColumnRef, Predicate, ScalarValue},
    transaction::Transaction,
    wal::{WalConfig as RuntimeWalConfig, WalExt, WalSyncPolicy},
};
use typed_arrow_dyn::{DynCell, DynRow};

const PACKAGE_ROWS: usize = 1024;

fn build_batch(schema: Arc<Schema>, rows: &[(&str, i32)]) -> RecordBatch {
    let ids: Vec<_> = rows.iter().map(|(id, _)| id.to_string()).collect();
    let values: Vec<_> = rows.iter().map(|(_, v)| *v).collect();
    let columns = vec![
        Arc::new(StringArray::from(ids)) as _,
        Arc::new(Int32Array::from(values)) as _,
    ];
    RecordBatch::try_new(schema, columns).expect("record batch")
}

fn build_batch_owned(schema: Arc<Schema>, ids: Vec<String>, values: Vec<i32>) -> RecordBatch {
    let columns = vec![
        Arc::new(StringArray::from(ids)) as _,
        Arc::new(Int32Array::from(values)) as _,
    ];
    RecordBatch::try_new(schema, columns).expect("record batch")
}

async fn make_db() -> (DynDbHandle<NoopExecutor>, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let mut db = DB::new(config, Arc::new(NoopExecutor)).await.expect("db");
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
    (db.into_shared(), schema)
}

type BlockingTx = Transaction<NoopExecutor>;
type TokioTx = Transaction<TokioExecutor>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_streams_mutable_and_immutable() {
    let (db, schema) = make_db().await;

    let batch = build_batch(schema.clone(), &[("immutable-a", 10), ("immutable-b", 20)]);
    db.ingest(batch).await.expect("ingest first batch");

    let batch = build_batch(schema.clone(), &[("mutable-a", 30), ("mutable-b", 40)]);
    db.ingest(batch).await.expect("ingest second batch");

    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(0i64));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    let mut stream = db.execute_scan(plan).await.expect("execute");

    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ids");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("values");
        for idx in 0..batch.num_rows() {
            rows.push((ids.value(idx).to_string(), values.value(idx)));
        }
    }

    assert_eq!(rows.len(), 4);
    assert!(rows.iter().any(|(id, _)| id == "immutable-a"));
    assert!(rows.iter().any(|(id, _)| id == "mutable-a"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_snapshot_honors_tombstones() {
    let (db, schema) = make_db().await;
    let initially_visible = build_batch(schema.clone(), &[("user-a", 10), ("user-b", 20)]);
    db.ingest_with_tombstones(initially_visible, vec![false, false])
        .await
        .expect("ingest initial rows");

    let txn: BlockingTx = db.begin_transaction().await.expect("snapshot transaction");

    let after_delete = build_batch(schema.clone(), &[("user-a", 99), ("user-b", 0)]);
    db.ingest_with_tombstones(after_delete, vec![false, true])
        .await
        .expect("ingest deletes");

    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(-1i64));

    let snapshot_rows = txn
        .scan(&predicate, None, None)
        .await
        .expect("txn snapshot scan");
    let snapshot_rows: Vec<(String, i32)> = snapshot_rows
        .iter()
        .map(|row| {
            let id = match &row.0[0] {
                Some(DynCell::Str(s)) => s.clone(),
                other => panic!("unexpected id cell {other:?}"),
            };
            let v = match &row.0[1] {
                Some(DynCell::I32(v)) => *v,
                other => panic!("unexpected value cell {other:?}"),
            };
            (id, v)
        })
        .collect();
    assert_eq!(
        snapshot_rows,
        vec![("user-a".to_string(), 10), ("user-b".to_string(), 20)],
        "snapshot should see pre-delete values",
    );

    let latest_rows = collect_rows_for_predicate(&db, &predicate, Timestamp::MAX).await;
    assert_eq!(
        latest_rows,
        vec![("user-a".to_string(), 99)],
        "latest snapshot should hide tombstoned rows",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_streams_large_packages() {
    let (db, schema) = make_db().await;
    let total_rows = PACKAGE_ROWS * 3 + PACKAGE_ROWS / 2;
    let mut ids = Vec::with_capacity(total_rows);
    let mut values = Vec::with_capacity(total_rows);
    for idx in 0..total_rows {
        ids.push(format!("row-{idx}"));
        values.push(idx as i32);
    }
    let batch = build_batch_owned(schema.clone(), ids, values);
    db.ingest(batch).await.expect("ingest rows");

    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(-1i64));

    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    let mut stream = db.execute_scan(plan).await.expect("execute");

    let mut row_count = 0usize;
    let mut batch_count = 0usize;
    let expected_batches = (total_rows + PACKAGE_ROWS - 1) / PACKAGE_ROWS;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch ok");
        batch_count += 1;
        row_count += batch.num_rows();
        if batch_count < expected_batches {
            assert_eq!(batch.num_rows(), PACKAGE_ROWS);
        } else {
            let expected_last = total_rows - PACKAGE_ROWS * (expected_batches - 1);
            assert_eq!(batch.num_rows(), expected_last);
        }
    }

    assert_eq!(row_count, total_rows);
    assert_eq!(batch_count, expected_batches);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_residual_predicate_filters_rows() {
    let (db, schema) = make_db().await;
    let batch = build_batch(schema.clone(), &[("keep", 10), ("drop", -5)]);
    db.ingest(batch).await.expect("ingest rows");
    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(0i64));
    let rows = collect_rows_for_predicate(&db, &predicate, Timestamp::MAX).await;
    assert_eq!(rows, vec![("keep".to_string(), 10)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_projection_retains_predicate_columns() {
    let (db, schema) = make_db().await;
    let batch = build_batch(
        schema.clone(),
        &[("keep", 10), ("drop", -5), ("also-drop", -1)],
    );
    db.ingest(batch).await.expect("ingest rows");

    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(0i64));
    let projected_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&projected_schema), None)
        .await
        .expect("plan");
    let mut stream = db.execute_scan(plan).await.expect("execute");

    let mut ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        assert_eq!(
            batch.num_columns(),
            1,
            "projected schema should only include id"
        );
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ids");
        for idx in 0..batch.num_rows() {
            ids.push(col.value(idx).to_string());
        }
    }

    assert_eq!(ids, vec!["keep".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_transaction_scan() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let executor = Arc::new(TokioExecutor::default());
    let mut db: DB<DynMode, TokioExecutor> = DB::new(config, executor).await.expect("db");
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let mem_fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
    let mut wal_cfg = RuntimeWalConfig::default();
    wal_cfg.dir = FusioPath::parse("mem-wal").expect("wal path");
    wal_cfg.segment_backend = Arc::clone(&mem_fs);
    wal_cfg.state_store = None;
    wal_cfg.flush_interval = Duration::from_millis(1);
    wal_cfg.sync = WalSyncPolicy::Disabled;
    db.enable_wal(wal_cfg).await.expect("enable wal");

    let db = db.into_shared();

    let mut base_tx: TokioTx = db.begin_transaction().await.expect("base tx");
    base_tx
        .upsert(DynRow(vec![
            Some(DynCell::Str("base-a".into())),
            Some(DynCell::I32(111)),
        ]))
        .expect("stage base a");
    base_tx
        .upsert(DynRow(vec![
            Some(DynCell::Str("base-b".into())),
            Some(DynCell::I32(2)),
        ]))
        .expect("stage base b");
    base_tx
        .upsert(DynRow(vec![
            Some(DynCell::Str("delete-me".into())),
            Some(DynCell::I32(3)),
        ]))
        .expect("stage delete target");
    base_tx.commit().await.expect("commit base rows");

    let mut tx: TokioTx = db.begin_transaction().await.expect("overlay tx");
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("base-a".into())),
        Some(DynCell::I32(999)),
    ]))
    .expect("stage update");
    tx.delete(KeyOwned::from("delete-me")).expect("delete");
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("txn-only".into())),
        Some(DynCell::I32(222)),
    ]))
    .expect("stage insert");

    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(-1i64));
    let rows = tx
        .scan(&predicate, None, None)
        .await
        .expect("transaction scan overlays staged rows");
    let mut entries: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let key = match &row.0[0] {
                Some(DynCell::Str(value)) => value.clone(),
                other => panic!("unexpected key cell {other:?}"),
            };
            let value = match row.0[1].as_ref() {
                Some(DynCell::I32(v)) => *v,
                other => panic!("unexpected value cell {other:?}"),
            };
            (key, value)
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        entries,
        vec![
            ("base-a".to_string(), 999),
            ("base-b".to_string(), 2),
            ("txn-only".to_string(), 222)
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_transaction_scan_projection() {
    let (db, schema) = make_db().await;
    let batch = build_batch(schema.clone(), &[("base", 1), ("neg-base", -5)]);
    db.ingest(batch).await.expect("ingest base row");

    let mut tx: BlockingTx = db.begin_transaction().await.expect("tx");
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("base".into())),
        Some(DynCell::I32(5)),
    ]))
    .expect("stage base update");
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("txn-only".into())),
        Some(DynCell::I32(10)),
    ]))
    .expect("stage insert");
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("skip".into())),
        Some(DynCell::I32(-3)),
    ]))
    .expect("stage negative insert");

    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(0i64));
    let projected_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let rows = tx
        .scan(&predicate, Some(&projected_schema), None)
        .await
        .expect("transaction scan projects columns");
    assert_eq!(rows.len(), 2);
    let mut ids: Vec<String> = rows
        .iter()
        .map(|row| match row.0.get(0).and_then(|cell| cell.as_ref()) {
            Some(DynCell::Str(value)) => value.clone(),
            other => panic!("unexpected projected cell {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["base".to_string(), "txn-only".to_string()]);
    assert!(rows.iter().all(|row| row.0.len() == 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_projects_value_column_only() {
    let (db, schema) = make_db().await;
    let batch = build_batch(schema.clone(), &[("p1", 10), ("p2", 20)]);
    db.ingest(batch).await.expect("ingest rows");

    let predicate = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("p1"));
    let projection = Schema::new(vec![Field::new("v", DataType::Int32, false)]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&Arc::new(projection)), None)
        .await
        .expect("plan projection");
    let mut stream = db.execute_scan(plan).await.expect("execute");

    let mut values_seen = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch ok");
        assert_eq!(batch.num_columns(), 1);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value column");
        for idx in 0..values.len() {
            values_seen.push(values.value(idx));
        }
    }
    assert_eq!(
        values_seen,
        vec![10],
        "projection should return only p1 and only the value column"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_key_range_predicate_filters_rows() {
    let (db, schema) = make_db().await;
    let batch = build_batch(
        schema.clone(),
        &[("k1", 1), ("k2", 2), ("k3", 3), ("k4", 4)],
    );
    db.ingest(batch).await.expect("ingest rows");

    let predicate = Predicate::and(vec![
        Predicate::gte(ColumnRef::new("id", None), ScalarValue::from("k2")),
        Predicate::lt(ColumnRef::new("id", None), ScalarValue::from("k4")),
    ]);
    let rows = collect_rows_for_predicate(&db, &predicate, Timestamp::MAX).await;
    assert_eq!(
        rows,
        vec![("k2".to_string(), 2), ("k3".to_string(), 3)],
        "range predicate should prune k1 and k4"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_plan_scan_applies_limit() {
    let (db, schema) = make_db().await;
    let batch = build_batch(schema.clone(), &[("l1", 1), ("l2", 2), ("l3", 3)]);
    db.ingest(batch).await.expect("ingest rows");

    let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(-1i64));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, Some(2))
        .await
        .expect("plan with limit");
    let mut stream = db.execute_scan(plan).await.expect("execute");
    let mut seen = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch ok");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        for idx in 0..batch.num_rows() {
            seen.push(ids.value(idx).to_string());
        }
    }
    assert_eq!(seen.len(), 2, "limit should restrict to two rows");
}

async fn collect_rows_for_predicate(
    db: &DynDbHandle<NoopExecutor>,
    predicate: &tonbo::query::Predicate,
    _read_ts: Timestamp,
) -> Vec<(String, i32)> {
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(db, predicate, None, None)
        .await
        .expect("plan");
    let mut stream = db.execute_scan(plan).await.expect("execute");
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch ok");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ids column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("values column");
        for row_idx in 0..batch.num_rows() {
            rows.push((ids.value(row_idx).to_string(), values.value(row_idx)));
        }
    }
    rows
}
