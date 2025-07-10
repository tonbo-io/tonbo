use std::{collections::HashMap, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema as ArrowSchema},
    error::ArrowError,
};
use common::PrimaryKey;
use parquet::{format::SortingColumn, schema::types::ColumnPath};
use thiserror::Error;

use super::{array::DynRecordImmutableArrays, DynRecord, ValueDesc};
use crate::{magic, record::Schema};

#[derive(Debug)]
pub struct DynSchema {
    schema: Vec<ValueDesc>,
    primary_index: usize,
    arrow_schema: Arc<ArrowSchema>,
}

#[derive(Debug, Error)]
#[error("exceeds max level, max level is {}", MAX_LEVEL)]
pub enum SchemaError {
    #[error("write io error: {0}")]
    Arrow(#[from] ArrowError),
}

impl DynSchema {
    pub fn new(schema: Vec<ValueDesc>, primary_index: usize) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("primary_key_index".to_string(), primary_index.to_string());
        let arrow_schema = Arc::new(ArrowSchema::new_with_metadata(
            [
                Field::new("_null", DataType::Boolean, false),
                Field::new(magic::TS, DataType::UInt32, false),
            ]
            .into_iter()
            .chain(schema.iter().map(|desc| desc.arrow_field()))
            .collect::<Vec<_>>(),
            metadata,
        ));
        Self {
            schema,
            primary_index,
            arrow_schema,
        }
    }

    /// create [`DynSchema`] from [`arrow::datatypes::Schema`]
    pub fn from_arrow_schema(
        arrow_schema: ArrowSchema,
        primary_index: usize,
    ) -> Result<Self, SchemaError> {
        let mut metadata = HashMap::new();
        metadata.insert("primary_key_index".to_string(), primary_index.to_string());

        let arrow_schema = ArrowSchema::try_merge(vec![
            ArrowSchema::new_with_metadata(
                vec![
                    Field::new("_null", DataType::Boolean, false),
                    Field::new(magic::TS, DataType::UInt32, false),
                ],
                metadata,
            ),
            arrow_schema,
        ])?;
        let mut schema = Vec::with_capacity(arrow_schema.fields.len());
        for field in arrow_schema.fields.iter() {
            let col = ValueDesc::from(field.as_ref());
            schema.push(col);
        }

        Ok(Self {
            schema,
            primary_index,
            arrow_schema: Arc::new(arrow_schema),
        })
    }
}

impl Schema for DynSchema {
    type Record = DynRecord;

    type Columns = DynRecordImmutableArrays;

    type Key = PrimaryKey;

    fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.arrow_schema
    }

    fn primary_key_index(&self) -> usize {
        self.primary_index + 2
    }

    fn primary_key_path(&self) -> (ColumnPath, Vec<SortingColumn>) {
        (
            ColumnPath::new(vec![
                magic::TS.to_string(),
                self.schema[self.primary_index].name.clone(),
            ]),
            vec![
                SortingColumn::new(1_i32, true, true),
                SortingColumn::new(self.primary_key_index() as i32, false, true),
            ],
        )
    }
}

/// Creates a [`DynSchema`] from literal slice of values and primary key index, suitable for rapid
/// testing and development.
///
/// ## Example:
///
/// ```no_run
/// // dyn_schema!(
/// //      (name, type, nullable),
/// //         ......
/// //      (name, type, nullable),
/// //      primary_key_index
/// // );
/// use tonbo::dyn_schema;
///
/// let schema = dyn_schema!(
///     ("foo", String, false),
///     ("bar", Int32, true),
///     ("baz", UInt64, true),
///     0
/// );
/// ```
#[macro_export]
macro_rules! dyn_schema {
    ($(($name: expr, $type: ident, $nullable: expr )),*, $primary: literal) => {
        {
            $crate::record::DynSchema::new(
                vec![
                    $(
                        $crate::record::ValueDesc::new($name.into(), $crate::datatype::DataType::$type, $nullable),
                    )*
                ],
                $primary,
            )
        }
    }
}

#[macro_export]
macro_rules! make_dyn_schema {
    ($(($name: expr, $type: expr, $nullable: expr )),*, $primary: literal) => {
        {
            $crate::record::DynSchema::new(
                vec![
                    $(
                        $crate::record::ValueDesc::new($name.into(), $type, $nullable),
                    )*
                ],
                $primary,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::DynSchema;

    #[test]
    fn test_from_arrow_schema() {
        let fields = vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("grade", DataType::Float32, true),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ];
        let arrow_schema = Schema::new(fields.clone());

        let dyn_schema = DynSchema::from_arrow_schema(arrow_schema.clone(), 0).unwrap();
        for (expected, actual) in dyn_schema
            .arrow_schema
            .fields()
            .iter()
            .skip(2)
            .zip(arrow_schema.fields())
        {
            assert_eq!(expected, actual)
        }
        for (expected, actual) in dyn_schema.schema.iter().skip(2).zip(arrow_schema.fields()) {
            assert_eq!(&expected.name, actual.name());
            assert_eq!(expected.is_nullable, actual.is_nullable());
            assert_eq!(expected.datatype, actual.data_type().into());
        }

        let metadata = dyn_schema.arrow_schema.metadata();
        let primary_key_index = metadata.get("primary_key_index");
        assert_eq!(primary_key_index, Some(&"0".into()));
    }
}
