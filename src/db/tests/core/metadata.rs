use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{
    db::{DB, Expr, ScalarValue},
    mode::DynModeConfig,
    test::build_batch,
};

#[tokio::test(flavor = "current_thread")]
async fn dynamic_new_from_metadata_field_marker() {
    // Schema: mark id with field-level metadata tonbo.key = true
    let mut fm = HashMap::new();
    fm.insert("tonbo.key".to_string(), "true".to_string());
    let f_id = Field::new("id", DataType::Utf8, false).with_metadata(fm);
    let f_v = Field::new("v", DataType::Int32, false);
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata key config");
    let db: DB<InMemoryFs, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("metadata key");

    // Build one batch and insert to ensure extractor wired
    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert via metadata");
    assert_eq!(db.inner().num_immutable_segments(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_new_from_metadata_schema_level() {
    let f_id = Field::new("id", DataType::Utf8, false);
    let f_v = Field::new("v", DataType::Int32, false);
    let mut sm = HashMap::new();
    sm.insert("tonbo.keys".to_string(), "id".to_string());
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]).with_metadata(sm));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("schema metadata config");
    let db: DB<InMemoryFs, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("schema metadata key");

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("x".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("y".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert via metadata");
    assert_eq!(db.inner().num_immutable_segments(), 0);
}

#[test]
fn dynamic_new_from_metadata_conflicts_and_missing() {
    // Conflict: two fields marked as key
    let mut fm1 = HashMap::new();
    fm1.insert("tonbo.key".to_string(), "true".to_string());
    let mut fm2 = HashMap::new();
    fm2.insert("tonbo.key".to_string(), "1".to_string());
    let f1 = Field::new("id1", DataType::Utf8, false).with_metadata(fm1);
    let f2 = Field::new("id2", DataType::Utf8, false).with_metadata(fm2);
    let schema_conflict = std::sync::Arc::new(Schema::new(vec![f1, f2]));
    assert!(DynModeConfig::from_metadata(schema_conflict).is_err());

    // Missing: no markers at field or schema level
    let schema_missing = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    assert!(DynModeConfig::from_metadata(schema_missing).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_composite_from_field_ordinals_and_scan() {
    // Fields: id (Utf8, ord 1), ts (Int64, ord 2), v (Int32)
    let mut m1 = HashMap::new();
    m1.insert("tonbo.key".to_string(), "1".to_string());
    let mut m2 = HashMap::new();
    m2.insert("tonbo.key".to_string(), "2".to_string());
    let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
    let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
    let f_v = Field::new("v", DataType::Int32, false);
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
    let db: DB<InMemoryFs, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("composite field metadata");

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
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert batch");

    let pred = Expr::and(vec![
        Expr::eq("id", ScalarValue::from("a")),
        Expr::and(vec![
            Expr::gt_eq("ts", ScalarValue::from(5i64)),
            Expr::lt_eq("ts", ScalarValue::from(10i64)),
        ]),
    ]);
    let batches = db.scan().filter(pred).collect().await.expect("collect");
    let got: Vec<(String, i64)> = batches
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id col");
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ts col");
            ids.iter()
                .zip(ts.iter())
                .filter_map(|(id, t)| Some((id?.to_string(), t?)))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_composite_from_schema_list_and_scan() {
    let f_id = Field::new("id", DataType::Utf8, false);
    let f_ts = Field::new("ts", DataType::Int64, false);
    let f_v = Field::new("v", DataType::Int32, false);
    let mut sm = HashMap::new();
    sm.insert("tonbo.keys".to_string(), "[\"id\", \"ts\"]".to_string());
    let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]).with_metadata(sm));
    let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
    let db: DB<InMemoryFs, NoopExecutor> = DB::new(config, Arc::new(NoopExecutor))
        .await
        .expect("composite schema metadata");

    let rows = vec![
        DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I64(5)),
            Some(DynCell::I32(1)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I64(10)),
            Some(DynCell::I32(2)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("b".into())),
            Some(DynCell::I64(1)),
            Some(DynCell::I32(3)),
        ]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("insert batch");

    let pred = Expr::and(vec![
        Expr::eq("id", ScalarValue::from("a")),
        Expr::and(vec![
            Expr::gt_eq("ts", ScalarValue::from(1i64)),
            Expr::lt_eq("ts", ScalarValue::from(10i64)),
        ]),
    ]);
    let batches = db.scan().filter(pred).collect().await.expect("collect");
    let got: Vec<(String, i64)> = batches
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id col");
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ts col");
            ids.iter()
                .zip(ts.iter())
                .filter_map(|(id, t)| Some((id?.to_string(), t?)))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
}
