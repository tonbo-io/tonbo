// 03: Dynamic (runtime) schema: infer key from Arrow metadata

use std::sync::Arc;

use fusio::executor::BlockingExecutor;
use futures::executor::block_on;
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
use typed_arrow_dyn::{DynCell, DynColumnBuilder, new_dyn_builder, validate_nullability};

fn build_batch(schema: Arc<Schema>, rows: Vec<Vec<Option<DynCell>>>) -> RecordBatch {
    let mut builders: Vec<Box<dyn DynColumnBuilder>> = schema
        .fields()
        .iter()
        .map(|f| new_dyn_builder(f.data_type()))
        .collect();
    for row in rows {
        assert_eq!(row.len(), builders.len(), "row width mismatch");
        for (idx, cell) in row.into_iter().enumerate() {
            let builder = &mut builders[idx];
            match cell {
                None => builder.append_null(),
                Some(cell) => builder.append_dyn(cell).expect("append cell"),
            }
        }
    }

    let mut arrays = Vec::with_capacity(builders.len());
    for builder in builders.iter_mut() {
        arrays.push(builder.try_finish().expect("finish column"));
    }
    validate_nullability(&schema, &arrays).expect("nullability");
    RecordBatch::try_new(schema, arrays).expect("record batch")
}

fn main() {
    // Schema-level metadata: tonbo.keys = "id"
    let f_id = Field::new("id", DataType::Utf8, false);
    let f_score = Field::new("score", DataType::Int32, false);
    let mut md = std::collections::HashMap::new();
    md.insert("tonbo.keys".to_string(), "id".to_string());
    let schema = Arc::new(Schema::new(vec![f_id, f_score]).with_metadata(md));

    // Build a batch
    let rows = vec![
        vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))],
        vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))],
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows);

    // Create DB from metadata
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
    let executor = Arc::new(BlockingExecutor);
    let mut db: DB<DynMode, BlockingExecutor> = DB::builder(config)
        .in_memory("dynamic-metadata")
        .build_with_executor(Arc::clone(&executor))
        .expect("metadata ok");
    block_on(db.ingest(batch)).expect("insert");

    // Scan all rows
    let all = RangeSet::<KeyOwned>::all();
    let rows: Vec<(String, i32)> = db
        .scan_mutable_rows(&all)
        .map(|r| match (&r[0], &r[1]) {
            (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
            _ => unreachable!(),
        })
        .collect();
    println!("dynamic (metadata) rows: {:?}", rows);
}
