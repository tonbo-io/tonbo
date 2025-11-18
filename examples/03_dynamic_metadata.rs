// 03: Dynamic (runtime) schema: infer key from Arrow metadata

use std::sync::Arc;

use fusio::executor::BlockingExecutor;
use tonbo::{
    db::{DB, DynMode},
    key::KeyOwned,
    mode::DynModeConfig,
    scan::RangeSet,
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
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
    let executor = Arc::new(BlockingExecutor);
    let mut db: DB<DynMode, BlockingExecutor> = DB::<DynMode, BlockingExecutor>::builder(config)
        .in_memory("dynamic-metadata")
        .build_with_executor(Arc::clone(&executor))
        .await
        .expect("metadata ok");
    db.ingest(batch).await.expect("insert");

    // Scan all rows
    let all = RangeSet::<KeyOwned>::all();
    let rows: Vec<(String, i32)> = db
        .scan_mutable_rows(&all, None)
        .expect("scan rows")
        .map(|result| match result {
            Ok(r) => match (r.0[0].as_ref(), r.0[1].as_ref()) {
                (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
                _ => unreachable!(),
            },
            Err(_) => unreachable!(),
        })
        .collect();
    println!("dynamic (metadata) rows: {:?}", rows);
}
