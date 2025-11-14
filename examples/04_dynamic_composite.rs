// 04: Dynamic (runtime) schema: composite keys via metadata ordinals

use std::{collections::HashMap, ops::Bound, sync::Arc};

use fusio::executor::BlockingExecutor;
use futures::executor::block_on;
use tonbo::{
    db::{DB, DynMode},
    key::KeyOwned,
    mode::DynModeConfig,
    scan::{KeyRange, RangeSet},
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

fn main() {
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
    block_on(db.ingest(batch)).expect("insert");

    // Range over composite key: ("a", 5) ..= ("a", 10)
    let lo = KeyOwned::tuple(vec![KeyOwned::from("a"), KeyOwned::from(5i64)]);
    let hi = KeyOwned::tuple(vec![KeyOwned::from("a"), KeyOwned::from(10i64)]);
    let rs = RangeSet::from_ranges(vec![KeyRange::new(
        Bound::Included(lo),
        Bound::Included(hi),
    )]);
    let got: Vec<(String, i64)> = db
        .scan_mutable_rows(&rs, None)
        .expect("scan rows")
        .map(|result| match result {
            Ok(r) => match (r.0[0].as_ref(), r.0[1].as_ref()) {
                (Some(DynCell::Str(s)), Some(DynCell::I64(ts))) => (s.clone(), *ts),
                _ => unreachable!(),
            },
            Err(_) => unreachable!(),
        })
        .collect();
    println!("dynamic composite range rows: {:?}", got);
}
