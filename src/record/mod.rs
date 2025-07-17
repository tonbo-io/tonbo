pub mod option;
pub mod runtime;
#[cfg(test)]
pub(crate) mod test;

use std::{collections::HashMap, error::Error, fmt::Debug, io, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    error::ArrowError,
};
use common::{Key, PrimaryKey, PrimaryKeyRef};
use fusio_log::{Decode, Encode};
use option::OptionRecordRef;
use parquet::arrow::ProjectionMask;
pub use runtime::*;
use thiserror::Error;

use crate::{inmem::immutable::ArrowArrays, magic};

pub trait Record: 'static + Sized + Decode + Debug + Send + Sync {
    // type Key: Key;

    type Columns: ArrowArrays<Record = Self>;

    type Ref<'r>: RecordRef<'r, Record = Self>
    where
        Self: 'r;

    /// Returns the primary key of the record. This should be the type defined in the
    /// [`Schema`].
    fn key(&self) -> PrimaryKeyRef<'_>;

    /// Returns a reference to the record.
    fn as_record_ref(&self) -> Self::Ref<'_>;

    /// Returns the size of the record in bytes.
    fn size(&self) -> usize;
}

pub trait RecordRef<'r>: Clone + Sized + Encode + Send + Sync {
    type Record: Record;

    /// Returns the primary key of the record. This should be the type that defined in the
    /// [`Schema`].
    fn key(self) -> PrimaryKey;

    /// Do projection on the record. Only keep the columns specified in the projection mask.
    ///
    /// **Note**: Primary key column are always kept.
    fn projection(&mut self, projection_mask: &ProjectionMask);

    /// Get the [`RecordRef`] from the [`RecordBatch`] at the given offset.
    ///
    /// `full_schema` is the combination of `_null`, `_ts` and all fields defined in the [`Schema`].
    fn from_record_batch(
        record_batch: &'r RecordBatch,
        offset: usize,
        projection_mask: &'r ProjectionMask,
        full_schema: &'r Arc<ArrowSchema>,
    ) -> OptionRecordRef<'r, Self>;
}

#[derive(Debug)]
pub struct Schema {
    schema: Arc<ArrowSchema>,
    primary_key: usize,
}

impl Schema {
    /// create [`DynSchema`] from [`arrow::datatypes::Schema`]
    pub fn new(fields: Vec<Field>, primary_key: usize) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("primary_key_index".to_string(), primary_key.to_string());

        let fields = [
            Field::new("_null", ArrowDataType::Boolean, false),
            Field::new(magic::TS, ArrowDataType::UInt32, false),
        ]
        .into_iter()
        .chain(fields)
        .collect::<Vec<Field>>();
        let schema = Arc::new(ArrowSchema::new_with_metadata(fields, metadata));

        Self {
            schema,
            primary_key,
        }
    }

    /// create [`DynSchema`] from [`arrow::datatypes::Schema`]
    pub fn from_arrow_schema(
        arrow_schema: ArrowSchema,
        primary_key: usize,
    ) -> Result<Self, ArrowError> {
        let mut metadata = HashMap::new();
        metadata.insert("primary_key_index".to_string(), primary_key.to_string());

        let schema = Arc::new(ArrowSchema::try_merge(vec![
            ArrowSchema::new_with_metadata(
                vec![
                    Field::new("_null", ArrowDataType::Boolean, false),
                    Field::new(magic::TS, ArrowDataType::UInt32, false),
                ],
                metadata,
            ),
            arrow_schema,
        ])?);

        Ok(Self {
            schema,
            primary_key,
        })
    }

    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.schema
    }

    /// Returns the index of the primary key column.
    pub fn primary_key_index(&self) -> usize {
        self.primary_key + 2
    }

    /// Returns the name of the primary key column.
    pub fn primary_key_name(&self) -> String {
        let fields = self.schema.fields();
        fields[self.primary_key].name().to_string()
    }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};

    use super::Schema;

    #[test]
    fn test_from_arrow_schema() {
        let fields = vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
        ];
        let mut metadata = HashMap::new();
        metadata.insert("k".to_string(), "v".to_string());
        let arrow_schema = ArrowSchema::new_with_metadata(fields.clone(), metadata);

        let schema = Schema::from_arrow_schema(arrow_schema, 0).unwrap();
        let expected = [
            Field::new("_null", DataType::Boolean, false),
            Field::new("_ts", DataType::UInt32, false),
        ]
        .into_iter()
        .chain(fields)
        .collect::<Fields>();
        let metadata = schema.arrow_schema().metadata();

        assert_eq!(schema.arrow_schema().fields(), &expected);
        assert_eq!(metadata.get("k"), Some(&"v".into()));
        assert_eq!(metadata.get("primary_key_index"), Some(&"0".into()));
    }
}
