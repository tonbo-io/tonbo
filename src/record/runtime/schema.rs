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
        self.primary_index
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
