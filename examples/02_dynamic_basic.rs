// 02: Dynamic (runtime) schema: key-by-name, insert a batch, and scan

use std::{ops::Bound, sync::Arc};

use tonbo::{
    query::{Expr, Predicate},
    record::extract::KeyDyn,
    scan::{KeyRange, RangeSet},
    tonbo::{Tonbo, DynMode},
};
use typed_arrow::{
    arrow_array::RecordBatch,
    arrow_schema::{DataType, Field, Schema},
};
use typed_arrow_dyn::{DynCell, DynRow};
use typed_arrow_unified::SchemaLike;

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
    let batch: RecordBatch = schema.build_batch(rows).expect("valid dynamic rows");

    // Create a dynamic Tonbo instance by specifying the key field name
    let mut tonbo: Tonbo<DynMode> = Tonbo::new_dyn_with_key_name(schema.clone(), "id").expect("schema ok");
    tonbo.ingest(batch).expect("insert dynamic batch");

    // Scan for a specific key (id == "carol") using KeyDyn
    let carol = RangeSet::from_ranges(vec![KeyRange::new(
        Bound::Included(KeyDyn::from("carol")),
        Bound::Included(KeyDyn::from("carol")),
    )]);
    let out: Vec<(String, i32)> = tonbo
        .scan_mutable_rows(&carol)
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
            _ => unreachable!(),
        })
        .collect();
    println!("dynamic scan rows (carol): {:?}", out);

    // Query expression: id == "dave"
    let expr = Expr::Pred(Predicate::Eq {
        value: KeyDyn::from("dave"),
    });
    let rs = tonbo::query::extract_key_ranges(&expr);
    let out_q: Vec<(String, i32)> = tonbo
        .scan_mutable_rows(&rs)
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
            _ => unreachable!(),
        })
        .collect();
    println!("dynamic query rows (id == dave): {:?}", out_q);

    // Or scan all dynamic rows
    let all = RangeSet::<KeyDyn>::all();
    let all_rows: Vec<(String, i32)> = tonbo
        .scan_mutable_rows(&all)
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
            _ => unreachable!(),
        })
        .collect();
    println!("dynamic rows (all): {:?}", all_rows);
}
