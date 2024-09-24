use std::{marker::PhantomData, sync::Arc};

use arrow::array::AsArray;

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
    ) -> InternalRecordRef<'r, Self> {
        let null = record_batch.column(0).as_boolean().value(offset);
        let ts = record_batch
            .column(1)
            .as_primitive::<arrow::datatypes::UInt32Type>()
            .value(offset)
            .into();

        let mut columns = vec![];
        for (idx, col) in record_batch.columns().iter().enumerate().skip(2) {
            let datatype = col.data_type();
            let is_null = col.is_null(idx);
            match datatype {
                arrow::datatypes::DataType::Int8 => {
                    let v = col
                        .as_primitive::<arrow::datatypes::Int8Type>()
                        .value(offset);

                    let value = (!is_null && projection_mask.leaf_included(idx)).then_some(v);
                    columns.push(Column {
                        datatype: Datatype::INT8,
                        value: Arc::new(value),
                        is_nullable: true,
                    });
                }
                arrow::datatypes::DataType::Int16 => {
                    let v = col
                        .as_primitive::<arrow::datatypes::Int16Type>()
                        .value(offset);

                    let value = (!is_null && projection_mask.leaf_included(idx)).then_some(v);
                    columns.push(Column {
                        datatype: Datatype::INT8,
                        value: Arc::new(value),
                        is_nullable: true,
                    });
                }
                _ => todo!(),
            }
        }

        let record = DynRecordRef {
            columns,
            primary_index: 1,
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
