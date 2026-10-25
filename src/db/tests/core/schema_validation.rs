use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
use typed_arrow::{Record, Union, schema::SchemaMeta};

use crate::{
    db::{DbBuildError, DbBuilder},
    extractor::KeyExtractError,
};

#[derive(Union)]
#[union(mode = "dense")]
enum BeamValue {
    Int(i64),
    Text(String),
}

#[derive(Record)]
struct BeamRecord {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    value: BeamValue,
}

fn union_type() -> DataType {
    let fields = UnionFields::try_new(
        vec![0, 1],
        vec![
            Field::new("int", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ],
    )
    .expect("union fields");
    DataType::Union(fields, UnionMode::Sparse)
}

fn rejected_path(field: Field) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        field,
    ]));
    match DbBuilder::from_schema_key_name(schema, "id") {
        Err(DbBuildError::Mode(KeyExtractError::UnsupportedDurableType { path, .. })) => path,
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("schema initialization should reject the field"),
    }
}

#[test]
fn typed_arrow_union_field_rejected_at_schema_init() {
    let err = DbBuilder::from_schema(BeamRecord::schema())
        .err()
        .expect("union field must be rejected before the table opens");

    let message = err.to_string();
    assert!(
        message.starts_with("unsupported durable field type at `value`: Arrow Union("),
        "unexpected message: {message}"
    );
    assert!(
        message.ends_with(" columns cannot currently be encoded in Parquet SSTs"),
        "unexpected message: {message}"
    );
}

#[test]
fn nested_union_field_rejected_with_field_path() {
    let meta = Field::new(
        "meta",
        DataType::Struct(Fields::from(vec![
            Field::new("source", DataType::Utf8, true),
            Field::new_list("tags", Field::new_list_field(union_type(), true), true),
        ])),
        true,
    );

    assert_eq!(rejected_path(meta), "meta.tags.item");
}

#[test]
fn list_view_field_rejected_at_schema_init() {
    let field = Field::new(
        "samples",
        DataType::ListView(Arc::new(Field::new_list_field(DataType::Int64, true))),
        true,
    );

    assert_eq!(rejected_path(field), "samples");
}

#[test]
fn union_primary_key_rejected_at_schema_init() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", union_type(), false)]));

    let result = DbBuilder::from_schema_key_name(schema, "id");

    assert!(matches!(
        result,
        Err(DbBuildError::Mode(KeyExtractError::UnsupportedType {
            col: 0,
            data_type: DataType::Union(_, _),
        }))
    ));
}
