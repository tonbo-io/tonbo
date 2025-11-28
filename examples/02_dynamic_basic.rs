// 02: Dynamic (runtime) schema: key-by-name, insert a batch, and scan

use std::sync::Arc;

use fusio::executor::tokio::TokioExecutor;
use futures::TryStreamExt;
use tonbo::{
    db::{DB, DbBuilder, DynMode},
    mvcc::Timestamp,
    query::{ColumnRef, Predicate, ScalarValue},
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
    // Define an Arrow schema at runtime (string key)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    // Build a RecordBatch from dynamic rows
    let rows = vec![
        DynRow(vec![
            Some(DynCell::Str("carol".into())),
            Some(DynCell::I32(30)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("dave".into())),
            Some(DynCell::I32(40)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("erin".into())),
            Some(DynCell::I32(50)),
        ]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows);

    // Create a dynamic DB by specifying the key field name
    let mut db: DB<DynMode, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .expect("key col")
        .in_memory("dynamic-basic")
        .build()
        .await
        .expect("schema ok");
    db.ingest(batch).await.expect("insert dynamic batch");

    let key_col = ColumnRef::new("id", Some(0));

    // Scan for a specific key (id == "carol") using predicate
    let carol_pred = Predicate::eq(key_col.clone(), ScalarValue::from("carol"));
    let out = scan_pairs(&db, &carol_pred).await;
    println!("dynamic scan rows (carol): {:?}", out);

    // Query expression: id == "dave"
    let expr: Predicate = Predicate::eq(key_col.clone(), ScalarValue::from("dave"));
    let out_q = scan_pairs(&db, &expr).await;
    println!("dynamic query rows (id == dave): {:?}", out_q);

    // Scan all dynamic rows (id is not null)
    let all_pred = Predicate::is_not_null(key_col.clone());
    let all_rows = scan_pairs(&db, &all_pred).await;
    println!("dynamic rows (all): {:?}", all_rows);
}

async fn scan_pairs(db: &DB<DynMode, TokioExecutor>, predicate: &Predicate) -> Vec<(String, i32)> {
    let plan = db
        .plan_scan(predicate, None, None, Timestamp::MAX)
        .await
        .expect("plan");
    let batches = db
        .execute_scan(plan)
        .await
        .expect("execute")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect");
    batches
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
        .collect()
}
