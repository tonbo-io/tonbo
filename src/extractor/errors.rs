use arrow_schema::{ArrowError, DataType, SchemaRef};
use typed_arrow_dyn::DynViewError;

use crate::wal::WalError;

/// Error returned when key extraction fails due to type/schema mismatches or out-of-bounds.
#[derive(Debug, thiserror::Error)]
pub enum KeyExtractError {
    /// Column index is outside the schema's field range.
    #[error("column index {0} out of bounds (num_columns={1})")]
    ColumnOutOfBounds(usize, usize),
    /// The field's Arrow data type does not match the extractor's expectation.
    #[error("unexpected data type for column {col}: expected {expected:?}, got {actual:?}")]
    WrongType {
        /// Column index with the mismatch.
        col: usize,
        /// Expected Arrow data type.
        expected: DataType,
        /// Actual Arrow data type.
        actual: DataType,
    },
    /// Row index is outside the batch's row range.
    #[error("invalid row index {0} (num_rows={1})")]
    RowOutOfBounds(usize, usize),
    /// Encountered an unsupported Arrow type when extracting from a batch.
    #[error("unsupported data type for column {col}: {data_type:?}")]
    UnsupportedType {
        /// Column index of the unsupported field.
        col: usize,
        /// The Arrow data type that is not supported.
        data_type: DataType,
    },
    /// Referenced field by name was not found in the schema.
    #[error("no such field in schema: {name}")]
    NoSuchField {
        /// The missing field name.
        name: String,
    },
    /// Batch schema does not match the DB's configured schema (dynamic mode).
    #[error("schema mismatch: expected {expected:?}, got {actual:?}")]
    SchemaMismatch {
        /// The DB's configured schema.
        expected: SchemaRef,
        /// The incoming batch schema.
        actual: SchemaRef,
    },
    /// Tombstone bitmap length does not match the batch row count.
    #[error("tombstone bitmap length mismatch: expected {expected}, got {actual}")]
    TombstoneLengthMismatch {
        /// Expected number of rows.
        expected: usize,
        /// Provided tombstone entries.
        actual: usize,
    },
    /// WAL submission or durability hook failed while ingesting.
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    /// Generic Arrow failure while materializing dynamic rows.
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// Error when viewing the data.
    #[error("dyn view error: {0}")]
    DynView(#[from] DynViewError),
}
