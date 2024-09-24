use std::{any::Any, io, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use once_cell::sync::OnceCell;
use parquet::{format::SortingColumn, schema::types::ColumnPath};
use tokio::io::AsyncRead;

use super::{array::DynRecordImmutableArrays, Column, DynRecordRef};
use crate::{
    record::{Record, RecordDecodeError},
    serdes::Decode,
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

    // fn arrow_schema(&self) -> &'static std::sync::Arc<Schema> {
    pub(crate) fn arrow_schema(&self) -> &'static std::sync::Arc<Schema> {
        static DYN_SCHEMA: OnceCell<Arc<Schema>> = OnceCell::new();
        DYN_SCHEMA.get_or_init(|| {
            let mut fields = vec![
                Field::new("_null", DataType::Boolean, false),
                Field::new("_ts", DataType::UInt32, false),
            ];

            for (idx, col) in self.columns.iter().enumerate() {
                if idx == self.primary_index && col.is_nullable {
                    panic!("Primary key must not be nullable")
                }
                match col.datatype {
                    super::Datatype::INT8 => {
                        fields.push(Field::new("&col.name", DataType::Int8, col.is_nullable));
                    }
                    super::Datatype::INT16 => {
                        fields.push(Field::new("&col.name", DataType::Int16, col.is_nullable));
                    }
                };
            }
            Arc::new(Schema::new(fields))
        })
    }
}

impl Decode for DynRecord {
    type Error = RecordDecodeError;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: AsyncRead + Unpin,
    {
        let primary_index = u32::decode(reader).await? as usize;
        let mut columns = vec![];
        loop {
            match Column::decode(reader).await {
                Ok(col) => columns.push(col),
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => break,
                    _ => {
                        return Err(RecordDecodeError::Decode {
                            field_name: "col".to_string(),
                            error: Box::new(err),
                        })
                    }
                },
            }
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
        todo!("This method is not used.")
        // dyn_schema_primary_index()
    }

    fn primary_key_path() -> (ColumnPath, Vec<SortingColumn>) {
        todo!("This method is not used.")
    }

    fn as_record_ref(&self) -> Self::Ref<'_> {
        let mut columns = vec![];
        for (idx, col) in self.columns.iter().enumerate() {
            let datatype = col.datatype;
            let is_nullable = col.is_nullable;
            let mut value = col.value.clone();
            if idx != self.primary_index {
                value = match datatype {
                    super::Datatype::INT8 if !is_nullable => {
                        let v = *col.value.as_ref().downcast_ref::<i8>().unwrap();
                        Arc::new(Some(v)) as Arc<dyn Any>
                    }
                    super::Datatype::INT16 if !is_nullable => {
                        let v = *col.value.as_ref().downcast_ref::<i16>().unwrap();
                        Arc::new(Some(v)) as Arc<dyn Any>
                    }
                    _ => col.value.clone() as Arc<dyn Any>,
                };
            }

            columns.push(Column::new(datatype, value, is_nullable));
        }
        DynRecordRef::new(columns, self.primary_index)
    }

    fn arrow_schema() -> &'static std::sync::Arc<Schema> {
        todo!("This method is not used.")
    }

    fn size(&self) -> usize {
        self.columns
            .iter()
            .fold(0, |acc, col| acc + col.datatype.size())
    }
}

unsafe impl Send for DynRecord {}
unsafe impl Sync for DynRecord {}
