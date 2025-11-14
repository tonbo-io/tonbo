// 02: Dynamic (runtime) schema: key-by-name, insert a batch, and scan

use std::{collections::HashMap, ops::Bound, sync::Arc};

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
    let mut union_null_rows: HashMap<usize, Vec<usize>> = HashMap::new();
    for builder in builders.iter_mut() {
        let finished = builder.try_finish().expect("finish column");
        for (array_key, rows) in finished.union_metadata {
            union_null_rows.entry(array_key).or_default().extend(rows);
        }
        arrays.push(finished.array);
    }
    validate_nullability(schema.as_ref(), &arrays, &union_null_rows).expect("nullability");
    RecordBatch::try_new(schema, arrays).expect("record batch")
}

fn main() {
    // Define an Arrow schema at runtime (string key)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    // Build a RecordBatch from dynamic rows
    let rows = vec![
        vec![Some(DynCell::Str("carol".into())), Some(DynCell::I32(30))],
        vec![Some(DynCell::Str("dave".into())), Some(DynCell::I32(40))],
        vec![Some(DynCell::Str("erin".into())), Some(DynCell::I32(50))],
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
