// 04: Dynamic (runtime) schema: composite keys via metadata ordinals

use std::{ops::Bound, sync::Arc};

use tonbo::{
    db::{DB, DynMode},
    record::extract::KeyDyn,
    scan::{KeyRange, RangeSet},
};
use typed_arrow::{
    arrow_array::RecordBatch,
    arrow_schema::{DataType, Field, Schema},
};
use typed_arrow_dyn::{DynCell, DynRow};
use typed_arrow_unified::SchemaLike;

fn main() {
    // Field-level metadata: tonbo.key ordinals define lexicographic order
    let mut m1 = std::collections::HashMap::new();
    m1.insert("tonbo.key".to_string(), "1".to_string());
    let mut m2 = std::collections::HashMap::new();
    m2.insert("tonbo.key".to_string(), "2".to_string());
    let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
    let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
    let f_v = Field::new("v", DataType::Int32, false);
    let schema = Arc::new(Schema::new(vec![f_id, f_ts, f_v]));

    // Create DB from metadata
    let mut db: DB<DynMode> = DB::new_dyn_from_metadata(schema.clone()).expect("composite ok");

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
    let batch: RecordBatch = schema.build_batch(rows).expect("ok");
    db.ingest(batch).expect("insert");

    // Range over composite key: ("a", 5) ..= ("a", 10)
    let lo = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(5i64)]);
    let hi = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(10i64)]);
    let rs = RangeSet::from_ranges(vec![KeyRange::new(
        Bound::Included(lo),
        Bound::Included(hi),
    )]);
    let got: Vec<(String, i64)> = db
        .scan_mutable_rows(&rs)
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Some(DynCell::Str(s)), Some(DynCell::I64(ts))) => (s.clone(), *ts),
            _ => unreachable!(),
        })
        .collect();
    println!("dynamic composite range rows: {:?}", got);
}
