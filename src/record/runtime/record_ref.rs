use std::{any::Any, marker::PhantomData, mem, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ArrowPrimitiveType, AsArray},
    datatypes::{
        Int16Type, Int32Type, Int64Type, Int8Type, Schema as ArrowSchema, UInt16Type, UInt32Type,
        UInt64Type, UInt8Type,
    },
};
use fusio::Write;

use super::{Datatype, DynRecord, Value};
use crate::{
    record::{internal::InternalRecordRef, Key, Record, RecordEncodeError, RecordRef, Schema},
    serdes::Encode,
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
    ) -> InternalRecordRef<'r, Self> {
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

        for (idx, field) in full_schema.flattened_fields().iter().enumerate().skip(2) {
            let datatype = Datatype::from(field.data_type());
            let schema = record_batch.schema();
            let flattened_fields = schema.flattened_fields();
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
            let value = match datatype {
                Datatype::UInt8 => Self::primitive_value::<UInt8Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::UInt16 => Self::primitive_value::<UInt16Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::UInt32 => Self::primitive_value::<UInt32Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::UInt64 => Self::primitive_value::<UInt64Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::Int8 => Self::primitive_value::<Int8Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::Int16 => Self::primitive_value::<Int16Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::Int32 => Self::primitive_value::<Int32Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::Int64 => Self::primitive_value::<Int64Type>(
                    col,
                    offset,
                    idx,
                    projection_mask,
                    primary_index == idx - 2,
                ),
                Datatype::String => {
                    let v = col.as_string::<i32>();

                    if primary_index == idx - 2 {
                        Arc::new(v.value(offset).to_owned()) as Arc<dyn Any + Send + Sync>
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset).to_owned());
                        Arc::new(value) as Arc<dyn Any + Send + Sync>
                    }
                }
                Datatype::Boolean => {
                    let v = col.as_boolean();

                    if primary_index == idx - 2 {
                        Arc::new(v.value(offset).to_owned()) as Arc<dyn Any + Send + Sync>
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset).to_owned());
                        Arc::new(value) as Arc<dyn Any + Send + Sync>
                    }
                }
                Datatype::Bytes => {
                    let v = col.as_binary::<i32>();
                    if primary_index == idx - 2 {
                        Arc::new(v.value(offset).to_owned()) as Arc<dyn Any + Send + Sync>
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset).to_owned());
                        Arc::new(value) as Arc<dyn Any + Send + Sync>
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
        InternalRecordRef::new(ts, record, null)
    }

    fn projection(&mut self, projection_mask: &parquet::arrow::ProjectionMask) {
        for (idx, col) in self.columns.iter_mut().enumerate() {
            if idx != self.primary_index && !projection_mask.leaf_included(idx + 2) {
                match col.datatype {
                    Datatype::UInt8 => col.value = Arc::<Option<u8>>::new(None),
                    Datatype::UInt16 => col.value = Arc::<Option<u16>>::new(None),
                    Datatype::UInt32 => col.value = Arc::<Option<u32>>::new(None),
                    Datatype::UInt64 => col.value = Arc::<Option<u64>>::new(None),
                    Datatype::Int8 => col.value = Arc::<Option<i8>>::new(None),
                    Datatype::Int16 => col.value = Arc::<Option<i16>>::new(None),
                    Datatype::Int32 => col.value = Arc::<Option<i32>>::new(None),
                    Datatype::Int64 => col.value = Arc::<Option<i64>>::new(None),
                    Datatype::String => col.value = Arc::<Option<String>>::new(None),
                    Datatype::Boolean => col.value = Arc::<Option<bool>>::new(None),
                    Datatype::Bytes => col.value = Arc::<Option<Vec<u8>>>::new(None),
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
