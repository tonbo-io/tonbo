use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Fields, SchemaRef};
use typed_arrow_dyn::{DynProjection, DynSchema, DynViewError};

use super::{KeyProjection, errors::KeyExtractError};
use crate::{
    inmem::immutable::memtable::{MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL},
    key::{KeyRow, KeyRowError},
};

/// Build a boxed key projection for a single column.
pub fn projection_for_field(
    schema: SchemaRef,
    col: usize,
) -> Result<Box<dyn KeyProjection>, KeyExtractError> {
    DynKeyProjection::new(schema, vec![col]).map(|proj| Box::new(proj) as Box<dyn KeyProjection>)
}

/// Build a boxed key projection covering the provided column indices.
pub fn projection_for_columns(
    schema: SchemaRef,
    columns: Vec<usize>,
) -> Result<Box<dyn KeyProjection>, KeyExtractError> {
    DynKeyProjection::new(schema, columns).map(|proj| Box::new(proj) as Box<dyn KeyProjection>)
}

/// Runtime projection that delegates to typed-arrow-dyn and converts into Tonbo key views.
struct DynKeyProjection {
    schema: SchemaRef,
    dyn_schema: DynSchema,
    projection: DynProjection,
    columns: Vec<usize>,
}

impl DynKeyProjection {
    fn new(schema: SchemaRef, columns: Vec<usize>) -> Result<Self, KeyExtractError> {
        if columns.is_empty() {
            return Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                "key projection requires at least one column".to_string(),
            )));
        }
        for &col in &columns {
            ensure_supported_type(schema.field(col).data_type(), col)?;
        }

        let projection =
            DynProjection::from_indices(schema.as_ref(), columns.clone()).map_err(map_view_err)?;
        Ok(Self {
            dyn_schema: DynSchema::from_ref(schema.clone()),
            schema,
            projection,
            columns,
        })
    }

    fn ensure_batch_schema(&self, batch_schema: &SchemaRef) -> Result<(), KeyExtractError> {
        if batch_schema.as_ref() != self.schema.as_ref() {
            return Err(KeyExtractError::SchemaMismatch {
                expected: self.schema.clone(),
                actual: batch_schema.clone(),
            });
        }
        Ok(())
    }
}

impl KeyProjection for DynKeyProjection {
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        self.ensure_batch_schema(schema)
    }

    fn project_view(
        &self,
        batch: &RecordBatch,
        rows: &[usize],
    ) -> Result<Vec<KeyRow>, KeyExtractError> {
        self.ensure_batch_schema(&batch.schema())?;
        let mut out = Vec::with_capacity(rows.len());
        for &row in rows {
            let raw = self
                .projection
                .project_row_raw(&self.dyn_schema, batch, row)
                .map_err(map_view_err)?;
            let fields = raw.fields().clone();
            let key = KeyRow::from_dyn(raw).map_err(|err| map_key_row_err(err, row, &fields))?;
            out.push(key);
        }
        Ok(out)
    }

    fn key_indices(&self) -> Vec<usize> {
        self.columns.clone()
    }
}

fn ensure_supported_type(data_type: &DataType, col: usize) -> Result<(), KeyExtractError> {
    match data_type {
        DataType::Boolean
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => Ok(()),
        other => Err(KeyExtractError::UnsupportedType {
            col,
            data_type: other.clone(),
        }),
    }
}

fn map_key_row_err(err: KeyRowError, row: usize, fields: &Fields) -> KeyExtractError {
    match err {
        KeyRowError::NullComponent { index } => {
            let name = fields
                .get(index)
                .map(|f| f.name().as_str())
                .unwrap_or("unknown");
            KeyExtractError::Arrow(ArrowError::ComputeError(format!(
                "key column '{name}' contained null at row {row}"
            )))
        }
        KeyRowError::Owned(err) => {
            KeyExtractError::Arrow(ArrowError::ComputeError(err.to_string()))
        }
        KeyRowError::DynView(err) => map_view_err(err),
    }
}

fn map_view_err(err: DynViewError) -> KeyExtractError {
    match err {
        DynViewError::RowOutOfBounds { row, len } => KeyExtractError::RowOutOfBounds(row, len),
        DynViewError::ColumnOutOfBounds { column, width } => {
            KeyExtractError::ColumnOutOfBounds(column, width)
        }
        DynViewError::SchemaMismatch {
            column,
            expected,
            actual,
            ..
        }
        | DynViewError::TypeMismatch {
            column,
            expected,
            actual,
            ..
        } => KeyExtractError::WrongType {
            col: column,
            expected,
            actual,
        },
        DynViewError::UnexpectedNull { path, .. } => KeyExtractError::Arrow(
            ArrowError::ComputeError(format!("{path}: unexpected null value")),
        ),
        DynViewError::Invalid { path, message, .. } => {
            KeyExtractError::Arrow(ArrowError::ComputeError(format!("{path}: {message}")))
        }
    }
}

/// Build a row of dynamic cells by reading a single row from a `RecordBatch`.
pub(crate) fn row_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<Option<typed_arrow_dyn::DynCell>>, KeyExtractError> {
    use typed_arrow_dyn::DynCell as C;
    if row >= batch.num_rows() {
        return Err(KeyExtractError::RowOutOfBounds(row, batch.num_rows()));
    }
    let schema = batch.schema();
    let mut cells = Vec::with_capacity(batch.num_columns());
    for (col_idx, arr) in batch.columns().iter().enumerate() {
        let field = schema.field(col_idx);
        if field.name() == MVCC_COMMIT_COL || field.name() == MVCC_TOMBSTONE_COL {
            continue;
        }
        if arr.is_null(row) {
            cells.push(None);
            continue;
        }
        let dt = arr.data_type();
        let cell = match dt {
            DataType::Boolean => Some(C::Bool(
                arr.as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int32 => Some(C::I32(
                arr.as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64 => Some(C::I64(
                arr.as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::UInt32 => Some(C::U32(
                arr.as_any()
                    .downcast_ref::<arrow_array::UInt32Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::UInt64 => Some(C::U64(
                arr.as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Float32 => Some(C::F32(
                arr.as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Float64 => Some(C::F64(
                arr.as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Utf8 => Some(C::Str(
                arr.as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap()
                    .value(row)
                    .to_owned(),
            )),
            DataType::Binary => Some(C::Bin(
                arr.as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .unwrap()
                    .value(row)
                    .to_vec(),
            )),
            other => {
                return Err(KeyExtractError::UnsupportedType {
                    col: col_idx,
                    data_type: other.clone(),
                });
            }
        };
        cells.push(cell);
    }
    Ok(cells)
}

#[cfg(test)]
mod tests {
    use typed_arrow::schema::BuildRows;
    use typed_arrow_dyn::DynCell;

    use super::*;

    #[derive(typed_arrow::Record, Clone)]
    struct User {
        id: String,
        score: i32,
    }

    #[test]
    fn extract_single_and_composite_keys() {
        let mut builders = User::new_builders(3);
        <User as BuildRows>::Builders::append_row(
            &mut builders,
            User {
                id: "a".into(),
                score: 1,
            },
        );
        <User as BuildRows>::Builders::append_row(
            &mut builders,
            User {
                id: "b".into(),
                score: 2,
            },
        );
        let batch = <User as BuildRows>::Builders::finish(builders).into_record_batch();
        let schema = batch.schema();

        let utf8 = projection_for_field(schema.clone(), 0).unwrap();
        let i32k = projection_for_field(schema.clone(), 1).unwrap();

        KeyProjection::validate_schema(&*utf8, &schema).unwrap();
        KeyProjection::validate_schema(&*i32k, &schema).unwrap();

        let first = utf8
            .project_view(&batch, &[0])
            .unwrap()
            .remove(0)
            .to_owned();
        assert_eq!(first.as_utf8(), Some("a"));

        let second = i32k
            .project_view(&batch, &[1])
            .unwrap()
            .remove(0)
            .to_owned();
        let second_cell = second
            .as_row()
            .cells()
            .first()
            .and_then(|cell| cell.as_ref())
            .expect("i32 key");
        assert!(matches!(second_cell, DynCell::I32(2)));

        let composite = projection_for_columns(schema.clone(), vec![0, 1]).unwrap();
        let tuple = composite
            .project_view(&batch, &[1])
            .unwrap()
            .remove(0)
            .to_owned();
        let parts = tuple.as_row().cells();
        assert_eq!(parts.len(), 2);
        match parts[0].as_ref().expect("utf8 component") {
            DynCell::Str(value) => assert_eq!(value, "b"),
            other => panic!("unexpected component: {other:?}"),
        }
        match parts[1].as_ref().expect("i32 component") {
            DynCell::I32(value) => assert_eq!(*value, 2),
            other => panic!("unexpected component: {other:?}"),
        }
    }
}
