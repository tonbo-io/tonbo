// 02: Dynamic (runtime) schema: key-by-name, insert a batch, and scan

use std::{ops::Bound, sync::Arc};

use fusio::executor::tokio::TokioExecutor;
use futures::executor::block_on;
use tonbo::{
    db::{DB, DynMode},
    key::KeyOwned,
    mode::DynModeConfig,
    query::{ColumnRef, Predicate, PredicateBuilder, ScalarValue},
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
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key col");
    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(config)
        .in_memory("dynamic-basic")
        .build()
        .expect("schema ok");
    block_on(db.ingest(batch)).expect("insert dynamic batch");

    // Scan for a specific key (id == "carol") using KeyOwned
    let carol = RangeSet::from_ranges(vec![KeyRange::new(
        Bound::Included(KeyOwned::from("carol")),
        Bound::Included(KeyOwned::from("carol")),
    )]);
    let out: Vec<(String, i32)> = db
        .scan_mutable_rows(&carol, None)
        .expect("scan rows")
        .map(|result| match result {
            Ok(r) => match (r.0[0].as_ref(), r.0[1].as_ref()) {
                (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
                _ => unreachable!(),
            },
            Err(_) => unreachable!(),
        })
        .collect();
    println!("dynamic scan rows (carol): {:?}", out);

    // Query expression: id == "dave"
    let key_col = ColumnRef::new("id", Some(0));
    let expr: Predicate = PredicateBuilder::leaf()
        .equals(key_col.clone(), ScalarValue::Utf8("dave".into()))
        .build();
    let (rs, residual) = tonbo::query::extract_key_ranges::<KeyOwned>(&expr, &[key_col]);
    assert!(residual.is_none());
    let out_q: Vec<(String, i32)> = db
        .scan_mutable_rows(&rs, None)
        .expect("scan rows")
        .map(|result| match result {
            Ok(r) => match (r.0[0].as_ref(), r.0[1].as_ref()) {
                (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
                _ => unreachable!(),
            },
            Err(_) => unreachable!(),
        })
        .collect();
    println!("dynamic query rows (id == dave): {:?}", out_q);

    // Or scan all dynamic rows
    let all = RangeSet::<KeyOwned>::all();
    let all_rows: Vec<(String, i32)> = db
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
    println!("dynamic rows (all): {:?}", all_rows);
}
