use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{Array, AsArray},
    datatypes::{DataType, Schema},
};

use super::{Column, Datatype, DynRecord};
use crate::{
    record::{internal::InternalRecordRef, Key, Record, RecordEncodeError, RecordRef},
    serdes::Encode,
};

#[derive(Clone)]
pub struct DynRecordRef<'r> {
    pub columns: Vec<Column>,
    // XXX: log encode should keep the same behavior
    pub primary_index: usize,
    _marker: PhantomData<&'r ()>,
}

impl<'r> DynRecordRef<'r> {
    pub(crate) fn new(columns: Vec<Column>, primary_index: usize) -> Self {
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
        W: tokio::io::AsyncWrite + Unpin + Send,
    {
        (self.primary_index as u32).encode(writer).await?;
        for col in self.columns.iter() {
            col.encode(writer)
                .await
                .map_err(|err| RecordEncodeError::Encode {
                    field_name: "col".to_string(),
                    error: Box::new(err),
                })?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = 0;
        for col in self.columns.iter() {
            size += col.size();
        }
        size
    }
}

impl<'r> RecordRef<'r> for DynRecordRef<'r> {
    type Record = DynRecord;

    fn key(self) -> <<Self::Record as Record>::Key as Key>::Ref<'r> {
        self.columns
            .get(self.primary_index)
            .cloned()
            .expect("The primary key must exist")
    }

    fn from_record_batch(
        record_batch: &'r arrow::array::RecordBatch,
        offset: usize,
        projection_mask: &'r parquet::arrow::ProjectionMask,
        full_schema: &'r Arc<Schema>,
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
            let datatype = field.data_type();
            let schema = record_batch.schema();
            let flattened_fields = schema.flattened_fields();
            let batch_field = flattened_fields
                .iter()
                .enumerate()
                .find(|(_idx, f)| field.contains(f));
            if batch_field.is_none() {
                columns.push(Column::with_none_value(
                    Datatype::from(datatype),
                    field.name().to_owned(),
                    field.is_nullable(),
                ));
                continue;
            }
            let col = record_batch.column(batch_field.unwrap().0);
            let is_nullable = field.is_nullable();
            match datatype {
                DataType::Int8 => {
                    let v = col.as_primitive::<arrow::datatypes::Int8Type>();
                    if primary_index == idx - 2 {
                        columns.push(Column {
                            datatype: Datatype::INT8,
                            name: field.name().to_owned(),
                            value: Arc::new(v.value(offset)),
                            is_nullable,
                        });
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset));
                        columns.push(Column {
                            datatype: Datatype::INT8,
                            name: field.name().to_owned(),
                            value: Arc::new(value),
                            is_nullable,
                        });
                    }
                }
                arrow::datatypes::DataType::Int16 => {
                    let v = col.as_primitive::<arrow::datatypes::Int16Type>();

                    if primary_index == idx - 2 {
                        columns.push(Column {
                            datatype: Datatype::INT16,
                            name: field.name().to_owned(),
                            value: Arc::new(v.value(offset)),
                            is_nullable,
                        });
                    } else {
                        let value = (!v.is_null(offset) && projection_mask.leaf_included(idx))
                            .then_some(v.value(offset));
                        columns.push(Column::new(
                            Datatype::INT16,
                            field.name().to_owned(),
                            Arc::new(value),
                            is_nullable,
                        ));
                    }
                }
                _ => todo!(),
            }
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
                    Datatype::INT8 => col.value = Arc::<Option<i8>>::new(None),
                    Datatype::INT16 => col.value = Arc::<Option<i16>>::new(None),
                };
            }
        }
    }
}

unsafe impl<'r> Send for DynRecordRef<'r> {}
unsafe impl<'r> Sync for DynRecordRef<'r> {}
