use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use fusio::SeqRead;
use parquet::{format::SortingColumn, schema::types::ColumnPath};

use super::{array::DynRecordImmutableArrays, Column, ColumnDesc, Datatype, DynRecordRef};
use crate::{
    record::{Record, RecordDecodeError},
    serdes::{Decode, Encode},
};

#[derive(Debug)]
pub struct DynRecord {
    columns: Vec<Column>,
    primary_index: usize,
}

#[allow(unused)]
impl DynRecord {
    pub fn new(columns: Vec<Column>, primary_index: usize) -> Self {
        Self {
            columns,
            primary_index,
        }
    }

    pub(crate) fn primary_key_index(&self) -> usize {
        self.primary_index + 2
    }

    pub(crate) fn arrow_schema(&self) -> Arc<Schema> {
        let mut fields = vec![
            Field::new("_null", DataType::Boolean, false),
            Field::new("_ts", DataType::UInt32, false),
        ];

        for (idx, col) in self.columns.iter().enumerate() {
            if idx == self.primary_index && col.is_nullable {
                panic!("Primary key must not be nullable")
            }
            let mut field = Field::from(col);
            fields.push(field);
        }
        let mut metadata = HashMap::new();
        metadata.insert(
            "primary_key_index".to_string(),
            self.primary_index.to_string(),
        );
        Arc::new(Schema::new_with_metadata(fields, metadata))
    }
}

impl DynRecord {
    pub(crate) fn empty_record(column_descs: Vec<ColumnDesc>, primary_index: usize) -> DynRecord {
        let mut columns = vec![];
        for desc in column_descs.iter() {
            let value: Arc<dyn Any + Send + Sync> = match desc.datatype {
                Datatype::UInt8 => match desc.is_nullable {
                    true => Arc::<Option<u8>>::new(None),
                    false => Arc::new(u8::default()),
                },
                Datatype::UInt16 => match desc.is_nullable {
                    true => Arc::<Option<u16>>::new(None),
                    false => Arc::new(u16::default()),
                },
                Datatype::UInt32 => match desc.is_nullable {
                    true => Arc::<Option<u32>>::new(None),
                    false => Arc::new(u32::default()),
                },
                Datatype::UInt64 => match desc.is_nullable {
                    true => Arc::<Option<u64>>::new(None),
                    false => Arc::new(u64::default()),
                },
                Datatype::Int8 => match desc.is_nullable {
                    true => Arc::<Option<i8>>::new(None),
                    false => Arc::new(i8::default()),
                },
                Datatype::Int16 => match desc.is_nullable {
                    true => Arc::<Option<i16>>::new(None),
                    false => Arc::new(i16::default()),
                },
                Datatype::Int32 => match desc.is_nullable {
                    true => Arc::<Option<i32>>::new(None),
                    false => Arc::new(i32::default()),
                },
                Datatype::Int64 => match desc.is_nullable {
                    true => Arc::<Option<i64>>::new(None),
                    false => Arc::new(i64::default()),
                },
                Datatype::String => match desc.is_nullable {
                    true => Arc::<Option<String>>::new(None),
                    false => Arc::new(String::default()),
                },
                Datatype::Boolean => match desc.is_nullable {
                    true => Arc::<Option<bool>>::new(None),
                    false => Arc::new(bool::default()),
                },
                Datatype::Bytes => match desc.is_nullable {
                    true => Arc::<Option<Vec<u8>>>::new(None),
                    false => Arc::new(Vec::<u8>::default()),
                },
            };
            columns.push(Column::new(
                desc.datatype,
                desc.name.to_owned(),
                value,
                desc.is_nullable,
            ));
        }

        DynRecord::new(columns, primary_index)
    }
}

impl Decode for DynRecord {
    type Error = RecordDecodeError;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        let len = u32::decode(reader).await? as usize;
        let primary_index = u32::decode(reader).await? as usize;
        let mut columns = vec![];
        // keep invariant for record: nullable --> Some(v); non-nullable --> v
        for i in 0..len {
            let mut col = Column::decode(reader).await?;
            if i != primary_index && !col.is_nullable {
                match col.datatype {
                    Datatype::UInt8 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u8>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::UInt16 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u16>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::UInt32 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u32>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::UInt64 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u64>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::Int8 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i8>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::Int16 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i16>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::Int32 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i32>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::Int64 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i64>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::String => {
                        let value = col.value.as_ref().downcast_ref::<Option<String>>().unwrap();
                        col.value = Arc::new(value.clone().unwrap());
                    }
                    Datatype::Boolean => {
                        let value = col.value.as_ref().downcast_ref::<Option<bool>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    Datatype::Bytes => {
                        let value = col
                            .value
                            .as_ref()
                            .downcast_ref::<Option<Vec<u8>>>()
                            .unwrap();
                        col.value = Arc::new(value.clone().unwrap());
                    }
                }
            }
            columns.push(col);
        }

        Ok(DynRecord {
            columns,
            primary_index,
        })
    }
}

impl Record for DynRecord {
    type Columns = DynRecordImmutableArrays;

    type Key = Column;

    type Ref<'r> = DynRecordRef<'r>;

    fn primary_key_index() -> usize {
        unreachable!("This method is not used.")
    }

    fn primary_key_path() -> (ColumnPath, Vec<SortingColumn>) {
        unreachable!("This method is not used.")
    }

    fn as_record_ref(&self) -> Self::Ref<'_> {
        let mut columns = vec![];
        for (idx, col) in self.columns.iter().enumerate() {
            let datatype = col.datatype;
            let is_nullable = col.is_nullable;
            let mut value = col.value.clone();
            if idx != self.primary_index && !is_nullable {
                value = match datatype {
                    Datatype::UInt8 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u8>().unwrap()))
                    }
                    Datatype::UInt16 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u16>().unwrap()))
                    }
                    Datatype::UInt32 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u32>().unwrap()))
                    }
                    Datatype::UInt64 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u64>().unwrap()))
                    }
                    Datatype::Int8 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i8>().unwrap()))
                    }
                    Datatype::Int16 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i16>().unwrap()))
                    }
                    Datatype::Int32 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i32>().unwrap()))
                    }
                    Datatype::Int64 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i64>().unwrap()))
                    }
                    Datatype::String => Arc::new(Some(
                        col.value
                            .as_ref()
                            .downcast_ref::<String>()
                            .unwrap()
                            .to_owned(),
                    )),
                    Datatype::Boolean => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<bool>().unwrap()))
                    }
                    Datatype::Bytes => Arc::new(Some(
                        col.value
                            .as_ref()
                            .downcast_ref::<Vec<u8>>()
                            .unwrap()
                            .to_owned(),
                    )),
                };
            }

            columns.push(Column::new(
                datatype,
                col.name.to_owned(),
                value,
                is_nullable,
            ));
        }
        DynRecordRef::new(columns, self.primary_index)
    }

    fn arrow_schema() -> &'static std::sync::Arc<Schema> {
        unreachable!("This method is not used.")
    }

    fn size(&self) -> usize {
        self.columns.iter().fold(0, |acc, col| acc + col.size())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::sync::Arc;

    use super::DynRecord;
    use crate::record::{Column, ColumnDesc, Datatype};

    #[allow(unused)]
    pub(crate) fn test_dyn_item_schema() -> (Vec<ColumnDesc>, usize) {
        let descs = vec![
            ColumnDesc::new("id".to_string(), Datatype::Int64, false),
            ColumnDesc::new("age".to_string(), Datatype::Int8, true),
            ColumnDesc::new("height".to_string(), Datatype::Int16, true),
            ColumnDesc::new("weight".to_string(), Datatype::Int32, false),
            ColumnDesc::new("name".to_string(), Datatype::String, false),
            ColumnDesc::new("email".to_string(), Datatype::String, true),
            ColumnDesc::new("enabled".to_string(), Datatype::Boolean, false),
            ColumnDesc::new("bytes".to_string(), Datatype::Bytes, true),
        ];
        (descs, 0)
    }

    #[allow(unused)]
    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let mut columns = vec![
                Column::new(Datatype::Int64, "id".to_string(), Arc::new(i as i64), false),
                Column::new(
                    Datatype::Int8,
                    "age".to_string(),
                    Arc::new(Some(i as i8)),
                    true,
                ),
                Column::new(
                    Datatype::Int16,
                    "height".to_string(),
                    Arc::new(Some(i as i16 * 20)),
                    true,
                ),
                Column::new(
                    Datatype::Int32,
                    "weight".to_string(),
                    Arc::new(i * 200_i32),
                    false,
                ),
                Column::new(
                    Datatype::String,
                    "name".to_string(),
                    Arc::new(i.to_string()),
                    false,
                ),
                Column::new(
                    Datatype::String,
                    "email".to_string(),
                    Arc::new(Some(format!("{}@tonbo.io", i))),
                    true,
                ),
                Column::new(
                    Datatype::Boolean,
                    "enabled".to_string(),
                    Arc::new(i % 2 == 0),
                    false,
                ),
                Column::new(
                    Datatype::Bytes,
                    "bytes".to_string(),
                    Arc::new(Some(i.to_le_bytes().to_vec())),
                    true,
                ),
            ];
            if i >= 45 {
                columns[2].value = Arc::<Option<i16>>::new(None);
            }

            let record = DynRecord::new(columns, 0);
            items.push(record);
        }
        items
    }
}
