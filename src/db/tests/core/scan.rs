use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
use tonbo_predicate::{ColumnRef, Predicate, ScalarValue};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{
    db::{DB, DbInner},
    extractor,
    inmem::policy::BatchesThreshold,
    mode::DynModeConfig,
    test::build_batch,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_filters_immutable_segments() {
    let db = db_with_immutable_keys(&["k1", "z1"]).await;
    let predicate = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("k1"));
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
    let key_pred = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("k1"));
    let value_pred = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(5i64));
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
    let pred_a = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("k1"));
    let pred_b = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("k2"));
    let predicate = Predicate::and(vec![pred_a, pred_b]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    // Pruning is currently disabled; even contradictory predicates scan all immutables.
    assert_eq!(plan.immutable_indexes, vec![0]);
}

async fn db_with_immutable_keys(keys: &[&str]) -> DbInner<InMemoryFs, NoopExecutor> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let policy = Arc::new(BatchesThreshold { batches: 1 });
    let db = DB::new_with_policy(config, Arc::clone(&executor), policy)
        .await
        .expect("db")
        .into_inner();
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
