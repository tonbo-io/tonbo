use std::{any::Any, marker::PhantomData, mem, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, GenericBinaryArray,
        PrimitiveArray, StringArray,
    },
    datatypes::{
        Int16Type, Int32Type, Int64Type, Int8Type, Schema as ArrowSchema, UInt16Type, UInt32Type,
        UInt64Type, UInt8Type,
    },
};
use fusio::Write;
use fusio_log::Encode;

use super::{DataType, DynRecord, Value};
use crate::{
    magic::USER_COLUMN_OFFSET,
    record::{option::OptionRecordRef, Key, Record, RecordEncodeError, RecordRef, Schema},
};

#[derive(Clone)]
pub struct DynRecordRef<'r> {
    pub columns: Vec<Value>,
    // XXX: log encode should keep the same behavior
    pub primary_index: usize,
    _marker: PhantomData<&'r ()>,
}

impl<'r> DynRecordRef<'r> {
    pub(crate) fn new(columns: Vec<Value>, primary_index: usize) -> Self {
        Self {
            columns,
            primary_index,
            _marker: PhantomData,
        }
    }
}

impl<'r> Encode for DynRecordRef<'r> {
    type Error = RecordEncodeError;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        (self.columns.len() as u32).encode(writer).await?;
        (self.primary_index as u32).encode(writer).await?;
        for col in self.columns.iter() {
            col.encode(writer).await.map_err(RecordEncodeError::Fusio)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = 2 * mem::size_of::<u32>();
        for col in self.columns.iter() {
            size += col.size();
        }
        size
    }
}

impl<'r> RecordRef<'r> for DynRecordRef<'r> {
    type Record = DynRecord;

    fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r> {
        self.columns
            .get(self.primary_index)
            .cloned()
            .expect("The primary key must exist")
    }

    fn from_record_batch(
        record_batch: &'r arrow::array::RecordBatch,
        offset: usize,
        projection_mask: &'r parquet::arrow::ProjectionMask,
        full_schema: &'r Arc<ArrowSchema>,
    ) -> OptionRecordRef<'r, Self> {
        let null = record_batch.column(0).as_boolean().value(offset);
        let metadata = full_schema.metadata();

        let primary_index = metadata
            .get("primary_key_index")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let ts = record_batch
            .column(1)
            .as_primitive::<arrow::datatypes::UInt32Type>()
            .value(offset)
            .into();

        let mut columns = vec![];

        for (idx, field) in full_schema.fields().iter().enumerate().skip(2) {
            let datatype = DataType::from(field.data_type());
            let schema = record_batch.schema();
            let flattened_fields = schema.fields();
            let batch_field = flattened_fields
                .iter()
                .enumerate()
                .find(|(_idx, f)| field.contains(f));
            if batch_field.is_none() {
                columns.push(Value::with_none_value(
                    datatype,
                    field.name().to_owned(),
                    field.is_nullable(),
                ));
                continue;
            }
            let col = record_batch.column(batch_field.unwrap().0);
            let is_nullable = field.is_nullable();
            let value = match &datatype {
                DataType::UInt8 => Self::primitive_value::<UInt8Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::UInt16 => Self::primitive_value::<UInt16Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::UInt32 => Self::primitive_value::<UInt32Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::UInt64 => Self::primitive_value::<UInt64Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::Int8 => Self::primitive_value::<Int8Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::Int16 => Self::primitive_value::<Int16Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::Int32 => Self::primitive_value::<Int32Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::Int64 => Self::primitive_value::<Int64Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                DataType::String => {
                    let v = col.as_string::<i32>();

                    if primary_index == idx - 2 {
                        Arc::new(v.value(offset).to_owned()) as Arc<dyn Any + Send + Sync>
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset).to_owned());
                        Arc::new(value) as Arc<dyn Any + Send + Sync>
                    }
                }
                DataType::Boolean => {
                    let v = col.as_boolean();

                    if primary_index == idx - 2 {
                        Arc::new(v.value(offset).to_owned()) as Arc<dyn Any + Send + Sync>
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset).to_owned());
                        Arc::new(value) as Arc<dyn Any + Send + Sync>
                    }
                }
                DataType::Bytes => {
                    let v = col.as_binary::<i32>();
                    if primary_index == idx - 2 {
                        Arc::new(v.value(offset).to_owned()) as Arc<dyn Any + Send + Sync>
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset).to_owned());
                        Arc::new(value) as Arc<dyn Any + Send + Sync>
                    }
                }
                DataType::List(desc) => {
                    let array = col.as_list::<i32>().value(offset);
                    match &desc.datatype {
                        DataType::UInt8 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<UInt8Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<u8>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::UInt16 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<UInt16Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<u16>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::UInt32 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<UInt32Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<u32>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::UInt64 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<UInt64Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<u64>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::Int8 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int8Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<i8>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::Int16 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int16Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<i16>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::Int32 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int32Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<i32>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::Int64 => {
                            let data = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int64Type>>()
                                .unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<i64>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::String => {
                            let data = array.as_any().downcast_ref::<StringArray>().unwrap();
                            let v = data
                                .iter()
                                .map(|v| v.unwrap().to_string())
                                .collect::<Vec<String>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::Boolean => {
                            let data = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                            let v = data.iter().map(|v| v.unwrap()).collect::<Vec<bool>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::Bytes => {
                            let data = array
                                .as_any()
                                .downcast_ref::<GenericBinaryArray<i32>>()
                                .unwrap();
                            let v = data
                                .iter()
                                .map(|v| v.unwrap().to_vec())
                                .collect::<Vec<Vec<u8>>>();
                            if primary_index == idx - 2 {
                                Arc::new(v) as Arc<dyn Any + Send + Sync>
                            } else {
                                Arc::new((!array.is_null(offset)).then_some(v))
                            }
                        }
                        DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supporte yet"),
                    }
                }
            };

            columns.push(Value::new(
                datatype,
                field.name().to_owned(),
                value,
                is_nullable,
            ));
        }

        let record = DynRecordRef {
            columns,
            primary_index,
            _marker: PhantomData,
        };
        OptionRecordRef::new(ts, record, null)
    }

    fn projection(&mut self, projection_mask: &parquet::arrow::ProjectionMask) {
        for (idx, col) in self.columns.iter_mut().enumerate() {
            if idx != self.primary_index && !projection_mask.leaf_included(idx + USER_COLUMN_OFFSET)
            {
                match col.datatype() {
                    DataType::UInt8 => col.value = Arc::<Option<u8>>::new(None),
                    DataType::UInt16 => col.value = Arc::<Option<u16>>::new(None),
                    DataType::UInt32 => col.value = Arc::<Option<u32>>::new(None),
                    DataType::UInt64 => col.value = Arc::<Option<u64>>::new(None),
                    DataType::Int8 => col.value = Arc::<Option<i8>>::new(None),
                    DataType::Int16 => col.value = Arc::<Option<i16>>::new(None),
                    DataType::Int32 => col.value = Arc::<Option<i32>>::new(None),
                    DataType::Int64 => col.value = Arc::<Option<i64>>::new(None),
                    DataType::String => col.value = Arc::<Option<String>>::new(None),
                    DataType::Boolean => col.value = Arc::<Option<bool>>::new(None),
                    DataType::Bytes => col.value = Arc::<Option<Vec<u8>>>::new(None),
                    DataType::List(field) => {
                        col.value = match &field.datatype {
                            DataType::UInt8 => Arc::<Option<Vec<u8>>>::new(None),
                            DataType::UInt16 => Arc::<Option<Vec<u16>>>::new(None),
                            DataType::UInt32 => Arc::<Option<Vec<u32>>>::new(None),
                            DataType::UInt64 => Arc::<Option<Vec<u64>>>::new(None),
                            DataType::Int8 => Arc::<Option<Vec<i8>>>::new(None),
                            DataType::Int16 => Arc::<Option<Vec<i16>>>::new(None),
                            DataType::Int32 => Arc::<Option<Vec<i32>>>::new(None),
                            DataType::Int64 => Arc::<Option<Vec<i64>>>::new(None),
                            DataType::String => Arc::<Option<Vec<String>>>::new(None),
                            DataType::Boolean => Arc::<Option<Vec<bool>>>::new(None),
                            DataType::Bytes => Arc::<Option<Vec<Vec<u8>>>>::new(None),
                            DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supporte yet"),
                        };
                    }
                };
            }
        }
    }
}

impl<'r> DynRecordRef<'r> {
    fn primitive_value<T>(
        col: &ArrayRef,
        offset: usize,
        idx: usize,
        projection_mask: &'r parquet::arrow::ProjectionMask,
        primary: bool,
    ) -> Arc<dyn Any + Send + Sync>
    where
        T: ArrowPrimitiveType,
    {
        let v = col.as_primitive::<T>();

        if primary {
            Arc::new(v.value(offset)) as Arc<dyn Any + Send + Sync>
        } else {
            let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                .then_some(v.value(offset));
            Arc::new(value) as Arc<dyn Any + Send + Sync>
        }
    }
}
