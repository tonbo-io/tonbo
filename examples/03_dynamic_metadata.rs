// 03: Dynamic (runtime) schema: infer key from Arrow metadata

use std::sync::Arc;

use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
use futures::TryStreamExt;
use tonbo::{
    db::{DB, DbBuilder},
    query::{ColumnRef, Predicate},
};
use typed_arrow::{
    arrow_array::RecordBatch,
    arrow_schema::{DataType, Field, Schema},
};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow};

fn build_batch(schema: Arc<Schema>, rows: Vec<DynRow>) -> RecordBatch {
    let mut builders = DynBuilders::new(schema.clone(), rows.len());
    for row in rows {
        builders.append_option_row(Some(row)).expect("append row");
    }
    builders.try_finish_into_batch().expect("record batch")
}

#[tokio::main]
async fn main() {
    // Schema-level metadata: tonbo.keys = "id"
    let f_id = Field::new("id", DataType::Utf8, false);
    let f_score = Field::new("score", DataType::Int32, false);
    let mut md = std::collections::HashMap::new();
    md.insert("tonbo.keys".to_string(), "id".to_string());
    let schema = Arc::new(Schema::new(vec![f_id, f_score]).with_metadata(md));

    // Build a batch
    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows);

    // Create DB from metadata
    let executor = Arc::new(NoopExecutor);
    let db: DB<InMemoryFs, NoopExecutor> = DbBuilder::from_schema_metadata(schema.clone())
        .expect("metadata config")
        .in_memory("dynamic-metadata")
        .expect("in_memory config")
        .build_with_executor(Arc::clone(&executor))
        .await
        .expect("metadata ok");
    db.ingest(batch).await.expect("insert");

    // Scan all rows using a trivial predicate
    let pred = Predicate::is_not_null(ColumnRef::new("id", None));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &pred, None, None)
        .await
        .expect("plan");
    let rows: Vec<(String, i32)> = db
        .execute_scan(plan)
        .await
        .expect("execute")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<typed_arrow::arrow_array::StringArray>()
                .expect("id col");
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<typed_arrow::arrow_array::Int32Array>()
                .expect("v col");
            ids.iter()
                .zip(vals.iter())
                .filter_map(|(id, v)| Some((id?.to_string(), v?)))
                .collect::<Vec<_>>()
        })
        .collect();
    println!("dynamic (metadata) rows: {:?}", rows);
}
