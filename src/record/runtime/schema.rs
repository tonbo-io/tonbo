use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use parquet::{format::SortingColumn, schema::types::ColumnPath};

use super::{array::DynRecordImmutableArrays, DynRecord, Value, ValueDesc};
use crate::{magic, record::Schema};

#[derive(Debug)]
pub struct DynSchema {
    schema: Vec<ValueDesc>,
    primary_index: usize,
    arrow_schema: Arc<ArrowSchema>,
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
}

impl Schema for DynSchema {
    type Record = DynRecord;

    type Columns = DynRecordImmutableArrays;

    type Key = Value;

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
                        $crate::record::ValueDesc::new($name.into(), $crate::record::DataType::$type, $nullable),
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
