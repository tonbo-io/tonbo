// 04: Dynamic (runtime) schema: composite keys via metadata ordinals

use std::{collections::HashMap, sync::Arc};

use fusio::executor::BlockingExecutor;
use futures::TryStreamExt;
use tonbo::{
    db::{DB, DynMode},
    mode::DynModeConfig,
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
    // Field-level metadata: tonbo.key ordinals define lexicographic order
    let mut m1 = HashMap::new();
    m1.insert("tonbo.key".to_string(), "1".to_string());
    let mut m2 = HashMap::new();
    m2.insert("tonbo.key".to_string(), "2".to_string());
    let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
    let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
    let f_v = Field::new("v", DataType::Int32, false);
    let schema = Arc::new(Schema::new(vec![f_id, f_ts, f_v]));

    // Create DB from metadata
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
    let executor = Arc::new(BlockingExecutor);
    let mut db: DB<DynMode, BlockingExecutor> = DB::<DynMode, BlockingExecutor>::builder(config)
        .in_memory("dynamic-composite")
        .build_with_executor(Arc::clone(&executor))
        .await
        .expect("composite ok");

    // Build a batch with three rows
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
    let batch: RecordBatch = build_batch(schema.clone(), rows);
    db.ingest(batch).await.expect("insert");

    // Predicate over composite key: id = 'a' AND ts BETWEEN 5 AND 10
    let pred = Predicate::and(vec![
        Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("a")),
        Predicate::and(vec![
            Predicate::gte(ColumnRef::new("ts", None), ScalarValue::from(5i64)),
            Predicate::lte(ColumnRef::new("ts", None), ScalarValue::from(10i64)),
        ]),
    ]);

    let plan = db
        .plan_scan(&pred, None, None, Timestamp::MAX)
        .await
        .expect("plan");
    let got: Vec<(String, i64)> = db
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
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<typed_arrow::arrow_array::Int64Array>()
                .expect("ts col");
            ids.iter()
                .zip(ts.iter())
                .filter_map(|(id, t)| Some((id?.to_string(), t?)))
                .collect::<Vec<_>>()
        })
        .collect();
    println!("dynamic composite range rows: {:?}", got);
}
