// 03: Dynamic (runtime) schema: infer key from Arrow metadata

use std::sync::Arc;

use tonbo::{
    record::extract::KeyDyn,
    scan::RangeSet,
    tonbo::{Tonbo, DynMode},
};
use typed_arrow::{
    arrow_array::RecordBatch,
    arrow_schema::{DataType, Field, Schema},
};
use typed_arrow_dyn::{DynCell, DynRow};
use typed_arrow_unified::SchemaLike;

fn main() {
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
    let batch: RecordBatch = schema.build_batch(rows).expect("ok");

    // Create Tonbo instance from metadata
    let mut tonbo: Tonbo<DynMode> = Tonbo::new_dyn_from_metadata(schema.clone()).expect("metadata ok");
    tonbo.ingest(batch).expect("insert");

    // Scan all rows
    let all = RangeSet::<KeyDyn>::all();
    let rows: Vec<(String, i32)> = tonbo
        .scan_mutable_rows(&all)
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Some(DynCell::Str(s)), Some(DynCell::I32(v))) => (s.clone(), *v),
            _ => unreachable!(),
        })
        .collect();
    println!("dynamic (metadata) rows: {:?}", rows);
}
