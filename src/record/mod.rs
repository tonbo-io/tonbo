pub mod internal;
mod key;
pub mod runtime;
#[cfg(test)]
pub(crate) mod test;

use std::{collections::HashMap, error::Error, fmt::Debug, io, sync::Arc};

use array::DynRecordImmutableArrays;
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema as ArrowSchema},
};
use internal::InternalRecordRef;
pub use key::{Key, KeyRef};
use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};
pub use runtime::*;
use thiserror::Error;

use crate::{
    inmem::immutable::ArrowArrays,
    serdes::{Decode, Encode},
};

// #[allow(unused)]
// pub(crate) enum RecordInstance {
//     Normal,
//     Runtime(DynRecord),
// }

// #[allow(unused)]
// impl RecordInstance {
//     pub(crate) fn primary_key_index<R>(&self) -> usize
//     where
//         R: Record,
//     {
//         match self {
//             RecordInstance::Normal => R::primary_key_index(),
//             RecordInstance::Runtime(record) => record.primary_key_index(),
//         }
//     }

//     pub(crate) fn arrow_schema<R>(&self) -> Arc<ArrowSchema>
//     where
//         R: Record,
//     {
//         match self {
//             RecordInstance::Normal => R::arrow_schema().clone(),
//             RecordInstance::Runtime(record) => record.arrow_schema(),
//         }
//     }
// }

pub trait Schema: Debug + Send + Sync {
    type Record: Record<Schema = Self>;

    type Columns: ArrowArrays<Record = Self::Record>;

    type Key: Key;

    fn arrow_schema(&self) -> &Arc<ArrowSchema>;

    fn primary_key_index(&self) -> usize;

    fn primary_key_path(&self) -> (ColumnPath, Vec<SortingColumn>);
}

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
                Field::new("_ts", DataType::UInt32, false),
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
                "_ts".to_string(),
                self.schema[self.primary_index].name.clone(),
            ]),
            vec![
                SortingColumn::new(1_i32, true, true),
                SortingColumn::new(self.primary_key_index() as i32, false, true),
            ],
        )
    }
}

pub trait Record: 'static + Sized + Decode + Debug + Send + Sync {
    type Schema: Schema<Record = Self>;

    type Ref<'r>: RecordRef<'r, Record = Self>
    where
        Self: 'r;

    fn key(&self) -> <<<Self as Record>::Schema as Schema>::Key as Key>::Ref<'_> {
        self.as_record_ref().key()
    }

    fn as_record_ref(&self) -> Self::Ref<'_>;

    fn size(&self) -> usize;
}

pub trait RecordRef<'r>: Clone + Sized + Encode + Send + Sync {
    type Record: Record;

    fn key(self) -> <<<Self::Record as Record>::Schema as Schema>::Key as Key>::Ref<'r>;

    fn projection(&mut self, projection_mask: &ProjectionMask);

    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        projection_mask: &'r ProjectionMask,
        full_schema: &'r Arc<ArrowSchema>,
    ) -> InternalRecordRef<'r, Self>;
}

#[derive(Debug, Error)]
pub enum RecordEncodeError {
    #[error("record's field: {field_name} encode error: {error}")]
    Encode {
        field_name: String,
        error: Box<dyn Error + Send + Sync + 'static>,
    },
    #[error("record io error: {0}")]
    Io(#[from] io::Error),
    #[error("record fusio error: {0}")]
    Fusio(#[from] fusio::Error),
}

#[derive(Debug, Error)]
pub enum RecordDecodeError {
    #[error("record's field: {field_name} decode error: {error}")]
    Decode {
        field_name: String,
        error: Box<dyn Error + Send + Sync + 'static>,
    },
    #[error("record io error: {0}")]
    Io(#[from] io::Error),
    #[error("record fusio error: {0}")]
    Fusio(#[from] fusio::Error),
}
