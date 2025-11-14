use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Fields, Schema, SchemaRef};
use typed_arrow_dyn::{DynProjection, DynSchema, DynViewError};

use super::{KeyProjection, errors::KeyExtractError};
use crate::key::{KeyRow, KeyRowError};

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
    key_schema: SchemaRef,
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
        let key_fields = columns
            .iter()
            .map(|&idx| schema.field(idx).clone())
            .collect::<Vec<_>>();
        let key_schema = std::sync::Arc::new(Schema::new(key_fields));

        Ok(Self {
            dyn_schema: DynSchema::from_ref(schema.clone()),
            schema,
            columns,
            key_schema,
            projection,
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

    fn key_schema(&self) -> SchemaRef {
        self.key_schema.clone()
    }

    fn key_indices(&self) -> &[usize] {
        &self.columns
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
        | DataType::Union(_, _)
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

pub(crate) fn map_view_err(err: DynViewError) -> KeyExtractError {
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
