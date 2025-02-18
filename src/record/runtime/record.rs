use std::sync::Arc;

use fusio::SeqRead;
use fusio_log::{Decode, Encode};

use super::{schema::DynSchema, DataType, DynRecordRef, Value};
use crate::record::{Record, RecordDecodeError};

#[derive(Debug)]
pub struct DynRecord {
    values: Vec<Value>,
    primary_index: usize,
}

#[allow(unused)]
impl DynRecord {
    pub fn new(values: Vec<Value>, primary_index: usize) -> Self {
        Self {
            values,
            primary_index,
        }
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
        let mut values = vec![];
        // keep invariant for record: nullable --> Some(v); non-nullable --> v
        for i in 0..len {
            let mut col = Value::decode(reader).await?;
            if i != primary_index && !col.is_nullable() {
                match col.datatype() {
                    DataType::UInt8 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u8>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::UInt16 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u16>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::UInt32 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u32>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::UInt64 => {
                        let value = col.value.as_ref().downcast_ref::<Option<u64>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::Int8 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i8>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::Int16 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i16>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::Int32 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i32>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::Int64 => {
                        let value = col.value.as_ref().downcast_ref::<Option<i64>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::String => {
                        let value = col.value.as_ref().downcast_ref::<Option<String>>().unwrap();
                        col.value = Arc::new(value.clone().unwrap());
                    }
                    DataType::Boolean => {
                        let value = col.value.as_ref().downcast_ref::<Option<bool>>().unwrap();
                        col.value = Arc::new(value.unwrap());
                    }
                    DataType::Bytes => {
                        let value = col
                            .value
                            .as_ref()
                            .downcast_ref::<Option<Vec<u8>>>()
                            .unwrap();
                        col.value = Arc::new(value.clone().unwrap());
                    }
                }
            }
            values.push(col);
        }

        Ok(DynRecord {
            values,
            primary_index,
        })
    }
}

impl Record for DynRecord {
    type Schema = DynSchema;

    type Ref<'r> = DynRecordRef<'r>;

    fn as_record_ref(&self) -> Self::Ref<'_> {
        let mut columns = vec![];
        for (idx, col) in self.values.iter().enumerate() {
            let datatype = col.datatype();
            let is_nullable = col.is_nullable();
            let mut value = col.value.clone();
            if idx != self.primary_index && !is_nullable {
                value = match datatype {
                    DataType::UInt8 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u8>().unwrap()))
                    }
                    DataType::UInt16 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u16>().unwrap()))
                    }
                    DataType::UInt32 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u32>().unwrap()))
                    }
                    DataType::UInt64 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<u64>().unwrap()))
                    }
                    DataType::Int8 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i8>().unwrap()))
                    }
                    DataType::Int16 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i16>().unwrap()))
                    }
                    DataType::Int32 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i32>().unwrap()))
                    }
                    DataType::Int64 => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<i64>().unwrap()))
                    }
                    DataType::String => Arc::new(Some(
                        col.value
                            .as_ref()
                            .downcast_ref::<String>()
                            .unwrap()
                            .to_owned(),
                    )),
                    DataType::Boolean => {
                        Arc::new(Some(*col.value.as_ref().downcast_ref::<bool>().unwrap()))
                    }
                    DataType::Bytes => Arc::new(Some(
                        col.value
                            .as_ref()
                            .downcast_ref::<Vec<u8>>()
                            .unwrap()
                            .to_owned(),
                    )),
                };
            }

            columns.push(Value::new(
                datatype,
                col.desc.name.to_owned(),
                value,
                is_nullable,
            ));
        }
        DynRecordRef::new(columns, self.primary_index)
    }

    fn size(&self) -> usize {
        self.values.iter().fold(0, |acc, col| acc + col.size())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::sync::Arc;

    use super::{DynRecord, DynSchema};
    use crate::record::{DataType, Value, ValueDesc};

    #[allow(unused)]
    pub(crate) fn test_dyn_item_schema() -> DynSchema {
        let descs = vec![
            ValueDesc::new("id".to_string(), DataType::Int64, false),
            ValueDesc::new("age".to_string(), DataType::Int8, true),
            ValueDesc::new("height".to_string(), DataType::Int16, true),
            ValueDesc::new("weight".to_string(), DataType::Int32, false),
            ValueDesc::new("name".to_string(), DataType::String, false),
            ValueDesc::new("email".to_string(), DataType::String, true),
            ValueDesc::new("enabled".to_string(), DataType::Boolean, false),
            ValueDesc::new("bytes".to_string(), DataType::Bytes, true),
        ];
        DynSchema::new(descs, 0)
    }

    #[allow(unused)]
    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let mut columns = vec![
                Value::new(DataType::Int64, "id".to_string(), Arc::new(i as i64), false),
                Value::new(
                    DataType::Int8,
                    "age".to_string(),
                    Arc::new(Some(i as i8)),
                    true,
                ),
                Value::new(
                    DataType::Int16,
                    "height".to_string(),
                    Arc::new(Some(i as i16 * 20)),
                    true,
                ),
                Value::new(
                    DataType::Int32,
                    "weight".to_string(),
                    Arc::new(i * 200_i32),
                    false,
                ),
                Value::new(
                    DataType::String,
                    "name".to_string(),
                    Arc::new(i.to_string()),
                    false,
                ),
                Value::new(
                    DataType::String,
                    "email".to_string(),
                    Arc::new(Some(format!("{}@tonbo.io", i))),
                    true,
                ),
                Value::new(
                    DataType::Boolean,
                    "enabled".to_string(),
                    Arc::new(i % 2 == 0),
                    false,
                ),
                Value::new(
                    DataType::Bytes,
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
