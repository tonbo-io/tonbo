use arrow::datatypes::DataType;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResolveError {
    #[error("Unknown column: {0}")]
    UnknownColumn(String),
    #[error("Invalid arity for {op}: expected {expected}, got {got}")]
    InvalidArity {
        op: &'static str,
        expected: &'static str,
        got: usize,
    },
    #[error("Type mismatch for column '{column}': expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        column: String,
        expected: DataType,
        actual: DataType,
    },
    #[error("Unsupported operator {op} for column '{column}' of type {data_type:?}")]
    UnsupportedOperator {
        column: String,
        data_type: DataType,
        op: &'static str,
    },
    #[error("Equality with NULL is not allowed; use IsNull/IsNotNull")]
    EqNull,
    #[error("IS NULL/IS NOT NULL only valid on nullable columns: {0}")]
    NullabilityViolation(String),
}
