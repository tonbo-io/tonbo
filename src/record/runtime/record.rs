use std::{any::Any, collections::HashMap, io, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use fusio::Read;
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
            match desc.datatype {
                Datatype::Int8 => match desc.is_nullable {
                    true => columns.push(Column::new(
                        desc.datatype,
                        desc.name.to_owned(),
                        Arc::<Option<i8>>::new(None),
                        desc.is_nullable,
                    )),
                    false => columns.push(Column::new(
                        desc.datatype,
                        desc.name.to_owned(),
                        Arc::new(0_i8),
                        desc.is_nullable,
                    )),
                },
                Datatype::Int16 => match desc.is_nullable {
                    true => columns.push(Column::new(
                        desc.datatype,
                        desc.name.to_owned(),
                        Arc::<Option<i16>>::new(None),
                        desc.is_nullable,
                    )),
                    false => columns.push(Column::new(
                        desc.datatype,
                        desc.name.to_owned(),
                        Arc::new(0_i16),
                        desc.is_nullable,
                    )),
                },
                Datatype::Int32 => match desc.is_nullable {
                    true => columns.push(Column::new(
                        desc.datatype,
                        desc.name.to_owned(),
                        Arc::<Option<i32>>::new(None),
                        desc.is_nullable,
                    )),
                    false => columns.push(Column::new(
                        desc.datatype,
                        desc.name.to_owned(),
                        Arc::new(0_i32),
                        desc.is_nullable,
                    )),
                },
            }
        }

        DynRecord::new(columns, primary_index)
    }
}

impl Decode for DynRecord {
    type Error = RecordDecodeError;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: Read + Unpin,
    {
        let primary_index = u32::decode(reader).await? as usize;
        let mut columns = vec![];
        loop {
            match Column::decode(reader).await {
                Ok(col) => columns.push(col),
                Err(err) => match err {
                    fusio::Error::Io(io_error) => match io_error.kind() {
                        io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(RecordDecodeError::Io(io_error)),
                    },
                    _ => return Err(RecordDecodeError::Fusio(err)),
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
            if idx != self.primary_index {
                value = match datatype {
                    super::Datatype::Int8 if !is_nullable => {
                        let v = *col.value.as_ref().downcast_ref::<i8>().unwrap();
                        Arc::new(Some(v)) as Arc<dyn Any>
                    }
                    super::Datatype::Int16 if !is_nullable => {
                        let v = *col.value.as_ref().downcast_ref::<i16>().unwrap();
                        Arc::new(Some(v)) as Arc<dyn Any>
                    }
                    super::Datatype::Int32 if !is_nullable => {
                        let v = *col.value.as_ref().downcast_ref::<i32>().unwrap();
                        Arc::new(Some(v)) as Arc<dyn Any>
                    }
                    _ => col.value.clone() as Arc<dyn Any>,
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

unsafe impl Send for DynRecord {}
unsafe impl Sync for DynRecord {}

#[cfg(test)]
pub(crate) mod test {
    use std::sync::Arc;

    use super::DynRecord;
    use crate::record::{Column, ColumnDesc, Datatype};

    pub(crate) fn test_dyn_item_schema() -> (Vec<ColumnDesc>, usize) {
        let descs = vec![
            ColumnDesc::new("age".to_string(), Datatype::Int8, false),
            ColumnDesc::new("height".to_string(), Datatype::Int16, true),
            ColumnDesc::new("weight".to_string(), Datatype::Int32, false),
        ];
        (descs, 0)
    }

    pub(crate) fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let mut columns = vec![
                Column::new(Datatype::Int8, "age".to_string(), Arc::new(i as i8), false),
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
            ];
            if i >= 45 {
                columns[1].value = Arc::<Option<i16>>::new(None);
            }

            let record = DynRecord::new(columns, 0);
            items.push(record);
        }
        items
    }
}
