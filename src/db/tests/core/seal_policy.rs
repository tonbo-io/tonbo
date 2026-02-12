use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{
    db::{DB, DbBuilder, Expr, ScalarValue},
    inmem::policy::BatchesThreshold,
    mode::DynModeConfig,
    test::build_batch,
};

type TestDb = DB<InMemoryFs, NoopExecutor>;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

fn extract_rows(batches: &[RecordBatch]) -> Vec<(String, i32)> {
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

#[tokio::test(flavor = "current_thread")]
async fn set_seal_policy_succeeds_on_exclusive_handle() {
    let schema = test_schema();
    let config = DynModeConfig::from_key_name(schema, "id").expect("config");
    let executor = Arc::new(NoopExecutor);
    let mut db: TestDb = DB::new(config, executor).await.expect("db");

    let updated = db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
    assert!(updated, "set_seal_policy should succeed on an exclusive Arc");
}

#[tokio::test(flavor = "current_thread")]
async fn set_seal_policy_fails_on_shared_handle() {
    let schema = test_schema();
    let config = DynModeConfig::from_key_name(schema, "id").expect("config");
    let executor = Arc::new(NoopExecutor);
    let mut db: TestDb = DB::new(config, executor).await.expect("db");

    // Clone the inner Arc to simulate a shared handle.
    let _shared = db.inner().clone();

    let updated = db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
    assert!(
        !updated,
        "set_seal_policy should fail when Arc has multiple references"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn set_seal_policy_activates_sealing() {
    let schema = test_schema();
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
    let executor = Arc::new(NoopExecutor);
    let mut db: TestDb = DB::new(config, executor).await.expect("db");

    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k1".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch = build_batch(schema, rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    assert!(
        db.inner().num_immutable_segments() >= 1,
        "sealing should trigger after ingest with BatchesThreshold {{ batches: 1 }}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn builder_with_seal_policy_applies_policy() {
    let schema = test_schema();
    let db: TestDb = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .expect("config")
        .in_memory("seal-policy-builder-test")
        .expect("in memory config")
        .with_seal_policy(Arc::new(BatchesThreshold { batches: 1 }))
        .open_with_executor(Arc::new(NoopExecutor))
        .await
        .expect("db");

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k1".into())),
        Some(DynCell::I32(10)),
    ])];
    let batch = build_batch(schema, rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    assert!(
        db.inner().num_immutable_segments() >= 1,
        "seal policy set via builder should trigger sealing"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn builder_with_seal_policy_data_remains_readable() {
    let schema = test_schema();
    let db: TestDb = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .expect("config")
        .in_memory("seal-policy-read-test")
        .expect("in memory config")
        .with_seal_policy(Arc::new(BatchesThreshold { batches: 1 }))
        .open_with_executor(Arc::new(NoopExecutor))
        .await
        .expect("db");

    let rows = vec![
        DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I32(1)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("b".into())),
            Some(DynCell::I32(2)),
        ]),
    ];
    let batch = build_batch(schema, rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    // Data should still be readable even after being sealed into immutable segments.
    let predicate = Expr::gt("v", ScalarValue::from(0i64));
    let batches = db.scan().filter(predicate).collect().await.expect("scan");
    let rows = extract_rows(&batches);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(id, _)| id == "a"));
    assert!(rows.iter().any(|(id, _)| id == "b"));
}
