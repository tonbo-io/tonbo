use std::{any::Any, sync::Arc};

use fusio::SeqRead;

use super::{Datatype, DynRecordRef, Value, ValueDesc};
use crate::{
    record::{DynSchema, Record, RecordDecodeError},
    serdes::{Decode, Encode},
};

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

impl DynRecord {
    pub(crate) fn empty_record(column_descs: Vec<ValueDesc>, primary_index: usize) -> DynRecord {
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
            columns.push(Value::new(
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
        let mut values = vec![];
        // keep invariant for record: nullable --> Some(v); non-nullable --> v
        for i in 0..len {
            let mut col = Value::decode(reader).await?;
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

            columns.push(Value::new(
                datatype,
                col.name.to_owned(),
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

    use super::DynRecord;
    use crate::record::{Datatype, Value, ValueDesc};

    #[allow(unused)]
    pub(crate) fn test_dyn_item_schema() -> (Vec<ValueDesc>, usize) {
        let descs = vec![
            ValueDesc::new("id".to_string(), Datatype::Int64, false),
            ValueDesc::new("age".to_string(), Datatype::Int8, true),
            ValueDesc::new("height".to_string(), Datatype::Int16, true),
            ValueDesc::new("weight".to_string(), Datatype::Int32, false),
            ValueDesc::new("name".to_string(), Datatype::String, false),
            ValueDesc::new("email".to_string(), Datatype::String, true),
            ValueDesc::new("enabled".to_string(), Datatype::Boolean, false),
            ValueDesc::new("bytes".to_string(), Datatype::Bytes, true),
        ];
        (descs, 0)
    }

    #[allow(unused)]
    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let mut columns = vec![
                Value::new(Datatype::Int64, "id".to_string(), Arc::new(i as i64), false),
                Value::new(
                    Datatype::Int8,
                    "age".to_string(),
                    Arc::new(Some(i as i8)),
                    true,
                ),
                Value::new(
                    Datatype::Int16,
                    "height".to_string(),
                    Arc::new(Some(i as i16 * 20)),
                    true,
                ),
                Value::new(
                    Datatype::Int32,
                    "weight".to_string(),
                    Arc::new(i * 200_i32),
                    false,
                ),
                Value::new(
                    Datatype::String,
                    "name".to_string(),
                    Arc::new(i.to_string()),
                    false,
                ),
                Value::new(
                    Datatype::String,
                    "email".to_string(),
                    Arc::new(Some(format!("{}@tonbo.io", i))),
                    true,
                ),
                Value::new(
                    Datatype::Boolean,
                    "enabled".to_string(),
                    Arc::new(i % 2 == 0),
                    false,
                ),
                Value::new(
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
