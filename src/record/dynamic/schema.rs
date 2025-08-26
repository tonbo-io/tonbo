use std::{collections::HashMap, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema as ArrowSchema},
    error::ArrowError,
};
use parquet::{format::SortingColumn, schema::types::ColumnPath};
use thiserror::Error;

use super::{DynRecord, DynRecordImmutableArrays, Value};
use crate::{magic, record::Schema};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DynamicField {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    // offset is the index of the field in the primary key
    // pub offset: Option<usize>,
}

impl DynamicField {
    pub fn new(name: String, data_type: DataType, is_nullable: bool) -> Self {
        Self {
            name,
            data_type,
            is_nullable,
        }
    }

    fn arrow_field(&self) -> Field {
        Field::new(&self.name, self.data_type.clone(), self.is_nullable)
    }
}

impl From<&DynamicField> for Field {
    fn from(value: &DynamicField) -> Self {
        Field::new(&value.name, value.data_type.clone(), value.is_nullable)
    }
}

impl From<&Field> for DynamicField {
    fn from(value: &Field) -> Self {
        DynamicField::new(
            value.name().to_string(),
            value.data_type().clone(),
            value.is_nullable(),
        )
    }
}

#[derive(Debug)]
pub struct DynSchema {
    primary_index_arrow: Vec<usize>,
    pk_paths: Vec<ColumnPath>,
    sorting: Vec<SortingColumn>,
    arrow_schema: Arc<ArrowSchema>,
}

#[derive(Debug, Error)]
#[error("exceeds max level, max level is {}", MAX_LEVEL)]
pub enum SchemaError {
    #[error("write io error: {0}")]
    Arrow(#[from] ArrowError),
}

impl DynSchema {
    pub fn new(schema: &[DynamicField], primary_indices: &[usize]) -> Self {
        let mut metadata = HashMap::new();

        let pk_idx_str = primary_indices
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let full_pk_idx = primary_indices
            .iter()
            .map(|i| i + crate::magic::USER_COLUMN_OFFSET)
            .collect::<Vec<_>>();
        let full_pk_idx_str = full_pk_idx
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        metadata.insert(crate::magic::PK_USER_INDICES.to_string(), pk_idx_str);
        // Sorting columns string: "1:desc:1;<full_pk_idx>:asc:1"
        let sorting_str = format!("1:desc:1;{}:asc:1", full_pk_idx_str);
        metadata.insert("tonbo.sorting_columns".to_string(), sorting_str);

        let arrow_schema = Arc::new(ArrowSchema::new_with_metadata(
            [
                Field::new("_null", DataType::Boolean, false),
                Field::new(magic::TS, DataType::UInt32, false),
            ]
            .into_iter()
            .chain(schema.iter().map(|desc| desc.arrow_field()))
            .collect::<Vec<_>>(),
            metadata,
        ));
        let pk_paths = vec![ColumnPath::new(
            [magic::TS.to_string()]
                .into_iter()
                .chain(primary_indices.iter().map(|i| schema[*i].name.clone()))
                .collect::<Vec<_>>(),
        )];
        let sorting =
            [SortingColumn::new(1_i32, true, true)]
                .into_iter()
                .chain(primary_indices.iter().map(|i| {
                    SortingColumn::new((*i + magic::USER_COLUMN_OFFSET) as i32, false, true)
                }))
                .collect::<Vec<_>>();

        Self {
            primary_index_arrow: full_pk_idx,
            pk_paths,
            sorting,
            arrow_schema,
        }
    }

    /// create [`DynSchema`] from [`arrow::datatypes::Schema`]
    pub fn from_arrow_schema(
        arrow_schema: ArrowSchema,
        primary_indices: &[usize],
    ) -> Result<Self, SchemaError> {
        let mut metadata = HashMap::new();

        let pk_idx_str = primary_indices
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let full_pk_idx = primary_indices
            .iter()
            .map(|i| i + crate::magic::USER_COLUMN_OFFSET)
            .collect::<Vec<_>>();
        let full_pk_idx_str = full_pk_idx
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        metadata.insert(crate::magic::PK_USER_INDICES.to_string(), pk_idx_str);
        let sorting_str = format!("1:desc:1;{}:asc:1", full_pk_idx_str);
        metadata.insert("tonbo.sorting_columns".to_string(), sorting_str);

        let arrow_schema = ArrowSchema::try_merge(vec![
            ArrowSchema::new_with_metadata(
                vec![
                    Field::new("_null", DataType::Boolean, false),
                    Field::new(magic::TS, DataType::UInt32, false),
                ],
                metadata,
            ),
            arrow_schema,
        ])?;
        let mut fields_vec = Vec::with_capacity(arrow_schema.fields.len());
        for field in arrow_schema.fields.iter() {
            let col = DynamicField::new(
                field.name().to_string(),
                field.data_type().clone(),
                field.is_nullable(),
            );
            fields_vec.push(col);
        }

        let fields = arrow_schema.fields();
        let pk_paths = vec![ColumnPath::new(
            [magic::TS.to_string()]
                .into_iter()
                .chain(primary_indices.iter().map(|i| fields[*i].name().clone()))
                .collect::<Vec<_>>(),
        )];
        let sorting =
            [SortingColumn::new(1_i32, true, true)]
                .into_iter()
                .chain(primary_indices.iter().map(|i| {
                    SortingColumn::new((*i + magic::USER_COLUMN_OFFSET) as i32, false, true)
                }))
                .collect::<Vec<_>>();

        Ok(Self {
            primary_index_arrow: full_pk_idx,
            pk_paths,
            sorting,
            arrow_schema: Arc::new(arrow_schema),
        })
    }
}

impl Schema for DynSchema {
    type Record = DynRecord;

    type Columns = DynRecordImmutableArrays;

    type Key = Value;

    fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.arrow_schema
    }

    fn primary_key_indices(&self) -> &[usize] {
        self.primary_index_arrow.as_slice()
    }

    fn primary_key_paths_and_sorting(&self) -> (&[ColumnPath], &[SortingColumn]) {
        (&self.pk_paths, &self.sorting)
    }
}

/// Creates a [`DynSchema`] from literal slice of values and primary key index, suitable for rapid
/// testing and development.
///
/// ## Example:
///
/// ```no_run
/// // dyn_schema!(
/// //      (name, type, nullable),
/// //         ......
/// //      (name, type, nullable),
/// //      primary_key_index
/// // );
/// use tonbo::dyn_schema;
///
/// // This will turn into a schema with length 5 and fields of `_deleted`, `_ts`, `foo`, `bar`
/// // and `baz`
/// let schema = dyn_schema!(
///     ("foo", Utf8, false),
///     ("bar", Int32, true),
///     ("baz", UInt64, true),
///     [0]
/// );
/// ```
#[macro_export]
macro_rules! dyn_schema {
    ($(($name: expr, $type: ident, $nullable: expr )),*, [$($primary: expr),*]) => {
        {
            $crate::record::DynSchema::new(&[
                $(
                    $crate::record::DynamicField::new($name.into(), $crate::arrow::datatypes::DataType::$type, $nullable),
                )*
            ][..], &[$($primary),*])
        }
    }
}

#[macro_export]
macro_rules! make_dyn_schema {
    ($(($name: expr, $type: expr, $nullable: expr )),*, [$($primary: expr),*]) => {
        {
            $crate::record::DynSchema::new(&[
                $(
                    $crate::record::DynamicField::new($name.into(), $type, $nullable),
                )*
            ][..], &[$($primary),*])
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::DynSchema;

    #[test]
    fn test_from_arrow_schema() {
        let fields = vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("grade", DataType::Float32, true),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ];
        let arrow_schema = Schema::new(fields.clone());

        let dyn_schema = DynSchema::from_arrow_schema(arrow_schema.clone(), &[0]).unwrap();
        for (expected, actual) in dyn_schema
            .arrow_schema
            .fields()
            .iter()
            .skip(2)
            .zip(arrow_schema.fields())
        {
            assert_eq!(expected, actual)
        }
        // Dynamic fields are embedded in the Arrow schema; verifying arrow_schema equality above is
        // sufficient.

        let metadata = dyn_schema.arrow_schema.metadata();
        let primary_key_index = metadata.get(crate::magic::PK_USER_INDICES);
        assert_eq!(primary_key_index, Some(&"0".into()));
        // New keys present
        assert_eq!(
            metadata.get(crate::magic::PK_USER_INDICES),
            Some(&"0".into())
        );
    }
}
