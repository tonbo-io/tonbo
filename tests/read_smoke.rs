use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
use tonbo::db::{ColumnRef, DB, DbBuilder, Predicate, ScalarValue};
use typed_arrow_dyn::{DynCell, DynRow};
const PACKAGE_ROWS: usize = 1024;

/// Helper to extract (id, value) pairs from scan result batches.
fn extract_rows_from_batches(batches: &[RecordBatch]) -> Vec<(String, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
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
        for (id, v) in ids.iter().zip(vals.iter()) {
            if let (Some(id), Some(v)) = (id, v) {
                rows.push((id.to_string(), v));
            }
        }
    }
    rows
}

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

async fn make_db() -> (DB<InMemoryFs, NoopExecutor>, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .expect("config")
        .in_memory("read-smoke")
        .expect("in memory config")
        .open_with_executor(Arc::new(NoopExecutor))
        .await
        .expect("db");
    (db, schema)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_streams_mutable_and_immutable() {
    let (db, schema) = make_db().await;

    let batch = build_batch(schema.clone(), &[("immutable-a", 10), ("immutable-b", 20)]);
    db.ingest(batch).await.expect("ingest first batch");

    let batch = build_batch(schema.clone(), &[("mutable-a", 30), ("mutable-b", 40)]);
    db.ingest(batch).await.expect("ingest second batch");

    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(0i64));
    let batches = db.scan().filter(predicate).collect().await.expect("scan");

    let rows = extract_rows_from_batches(&batches);

    assert_eq!(rows.len(), 4);
    assert!(rows.iter().any(|(id, _)| id == "immutable-a"));
    assert!(rows.iter().any(|(id, _)| id == "mutable-a"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_snapshot_honors_tombstones() {
    let (db, _schema) = make_db().await;

    // Insert initial rows via transaction
    let mut init_tx = db.begin_transaction().await.expect("init tx");
    init_tx
        .upsert(DynRow(vec![
            Some(DynCell::Str("user-a".into())),
            Some(DynCell::I32(10)),
        ]))
        .expect("upsert user-a");
    init_tx
        .upsert(DynRow(vec![
            Some(DynCell::Str("user-b".into())),
            Some(DynCell::I32(20)),
        ]))
        .expect("upsert user-b");
    init_tx.commit().await.expect("commit init");

    // Start a snapshot transaction before making changes
    let txn = db.begin_transaction().await.expect("snapshot transaction");

    // Update user-a and delete user-b via a new transaction
    let mut update_tx = db.begin_transaction().await.expect("update tx");
    update_tx
        .upsert(DynRow(vec![
            Some(DynCell::Str("user-a".into())),
            Some(DynCell::I32(99)),
        ]))
        .expect("update user-a");
    update_tx.delete("user-b").expect("delete user-b");
    update_tx.commit().await.expect("commit updates");

    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(-1i64));

    let snapshot_batches = txn
        .scan()
        .filter(predicate.clone())
        .collect()
        .await
        .expect("txn snapshot scan");
    let snapshot_rows = extract_rows_from_batches(&snapshot_batches);
    assert_eq!(
        snapshot_rows,
        vec![("user-a".to_string(), 10), ("user-b".to_string(), 20)],
        "snapshot should see pre-delete values",
    );

    let latest_rows = collect_rows_for_predicate(&db, &predicate).await;
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

    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(-1i64));

    let batches = db.scan().filter(predicate).collect().await.expect("scan");

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(row_count, total_rows);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_residual_predicate_filters_rows() {
    let (db, schema) = make_db().await;
    let batch = build_batch(schema.clone(), &[("keep", 10), ("drop", -5)]);
    db.ingest(batch).await.expect("ingest rows");
    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(0i64));
    let rows = collect_rows_for_predicate(&db, &predicate).await;
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

    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(0i64));
    let projected_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batches = db
        .scan()
        .filter(predicate)
        .projection(projected_schema)
        .collect()
        .await
        .expect("scan");

    let mut ids = Vec::new();
    for batch in &batches {
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
    let (db, _schema) = make_db().await;

    let mut base_tx = db.begin_transaction().await.expect("base tx");
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

    let mut tx = db.begin_transaction().await.expect("overlay tx");
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("base-a".into())),
        Some(DynCell::I32(999)),
    ]))
    .expect("stage update");
    tx.delete("delete-me").expect("delete");
    tx.upsert(DynRow(vec![
        Some(DynCell::Str("txn-only".into())),
        Some(DynCell::I32(222)),
    ]))
    .expect("stage insert");

    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(-1i64));
    let batches = tx
        .scan()
        .filter(predicate)
        .collect()
        .await
        .expect("transaction scan overlays staged rows");
    let mut entries = extract_rows_from_batches(&batches);
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

    let mut tx = db.begin_transaction().await.expect("tx");
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

    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(0i64));
    let projected_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batches = tx
        .scan()
        .filter(predicate)
        .projection(projected_schema)
        .collect()
        .await
        .expect("transaction scan projects columns");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    let mut ids: Vec<String> = Vec::new();
    for batch in &batches {
        assert_eq!(
            batch.num_columns(),
            1,
            "projection should have single column"
        );
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id col");
        for id in id_col.iter().flatten() {
            ids.push(id.to_string());
        }
    }
    ids.sort();
    assert_eq!(ids, vec!["base".to_string(), "txn-only".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_smoke_projects_value_column_only() {
    let (db, schema) = make_db().await;
    let batch = build_batch(schema.clone(), &[("p1", 10), ("p2", 20)]);
    db.ingest(batch).await.expect("ingest rows");

    let predicate = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("p1"));
    let projection = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batches = db
        .scan()
        .filter(predicate)
        .projection(projection)
        .collect()
        .await
        .expect("scan");

    let mut values_seen = Vec::new();
    for batch in &batches {
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
        Predicate::gte(ColumnRef::new("id"), ScalarValue::from("k2")),
        Predicate::lt(ColumnRef::new("id"), ScalarValue::from("k4")),
    ]);
    let rows = collect_rows_for_predicate(&db, &predicate).await;
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

    let predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(-1i64));
    let batches = db
        .scan()
        .filter(predicate)
        .limit(2)
        .collect()
        .await
        .expect("scan");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "limit should restrict to two rows");
}

async fn collect_rows_for_predicate(
    db: &DB<InMemoryFs, NoopExecutor>,
    predicate: &Predicate,
) -> Vec<(String, i32)> {
    let batches = db
        .scan()
        .filter(predicate.clone())
        .collect()
        .await
        .expect("scan");
    extract_rows_from_batches(&batches)
}
