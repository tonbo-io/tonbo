use std::sync::Arc;

use arrow::{
    array::{AsArray, RecordBatch},
    datatypes::{DataType, Field, Schema as ArrowSchema, UInt32Type},
};
use parquet::{format::SortingColumn, schema::types::ColumnPath};
use tonbo::{
    record::typed::{
        compose_arrow_schema_with_sentinels, compute_pk_paths_and_sorting, TonboTypedBatch,
        TonboTypedBuilders,
    },
    typed as t,
};

#[t::record]
#[derive(t::Record, Debug, Clone, Default)]
pub struct Person {
    #[record(primary_key)]
    id: i64,
    name: String,
    age: Option<i16>,
}

#[test]
fn typed_adapter_builds_batch_with_sentinels() {
    // Build using typed builders wrapper
    let mut b = TonboTypedBuilders::<Person>::new(3);
    b.append_option_row(
        7,
        Some(Person {
            id: 1,
            name: "a".into(),
            age: Some(10),
        }),
    );
    // tombstone row
    b.append_option_row(8, None);
    b.append_option_row(
        9,
        Some(Person {
            id: 2,
            name: "b".into(),
            age: None,
        }),
    );
    let arrays: TonboTypedBatch = b.finish();
    let rb: &RecordBatch = arrays.as_record_batch();

    // Validate schema shape: _null, _ts, then user fields
    let schema: &Arc<ArrowSchema> = &rb.schema();
    assert_eq!(schema.fields().len(), 5);
    assert_eq!(schema.field(0).name(), "_null");
    assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
    assert_eq!(schema.field(1).name(), "_ts");
    assert_eq!(schema.field(1).data_type(), &DataType::UInt32);
    assert_eq!(schema.field(2).name(), "id");
    assert_eq!(schema.field(3).name(), "name");
    assert_eq!(schema.field(4).name(), "age");

    // Validate data rows
    assert_eq!(rb.num_rows(), 3);
    let nulls = rb.column(0).as_boolean();
    let ts = rb.column(1).as_primitive::<UInt32Type>();
    // row 0
    assert_eq!(nulls.value(0), false);
    assert_eq!(ts.value(0), 7);
    // row 1 tombstone
    assert_eq!(nulls.value(1), true);
    assert_eq!(ts.value(1), 8);
    // row 2
    assert_eq!(nulls.value(2), false);
    assert_eq!(ts.value(2), 9);
}

#[test]
fn typed_adapter_pk_paths_and_sorting() {
    // Build user fields and compute PK metadata
    let user_fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int16, true),
    ];
    let full_schema = compose_arrow_schema_with_sentinels(&user_fields);
    assert_eq!(full_schema.fields().len(), 5);

    // primary key is `id` (user index 0)
    let (paths, sorting): (Vec<ColumnPath>, Vec<SortingColumn>) =
        compute_pk_paths_and_sorting(&user_fields, &[0]);

    assert_eq!(paths.len(), 1);
    // Path is ["_ts", "id"]
    assert_eq!(paths[0].parts(), &["_ts", "id"]);

    // Sorting: _ts then pk(id). Indices are in full schema coordinates.
    // 1 is _ts, 2 is id
    assert_eq!(sorting.len(), 2);
    assert_eq!(sorting[0].column_idx, 1);
    assert_eq!(sorting[0].descending, true); // TS is descending for latest-wins
    assert_eq!(sorting[0].nulls_first, true);
    assert_eq!(sorting[1].column_idx, 2);
    assert_eq!(sorting[1].descending, false);
    assert_eq!(sorting[1].nulls_first, true);
}

#[test]
fn typed_adapter_embeds_metadata() {
    let mut b = TonboTypedBuilders::<Person>::new(1);
    b.append_option_row(
        1,
        Some(Person {
            id: 42,
            name: "x".into(),
            age: None,
        }),
    );
    let arrays = b.finish();
    let rb = arrays.as_record_batch();
    let schema = rb.schema();
    let meta = schema.metadata();
    // Verify PK user indices embedded
    assert_eq!(
        meta.get("tonbo.primary_key_user_indices")
            .map(|s| s.as_str()),
        Some("0")
    );
    // Sorting columns: "1:desc:1;2:asc:1" (ts then id)
    assert_eq!(
        meta.get("tonbo.sorting_columns").map(|s| s.as_str()),
        Some("1:desc:1;2:asc:1")
    );
}
