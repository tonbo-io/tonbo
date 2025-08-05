use thiserror::Error;

use crate::record::ValueError;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("value error: {0}")]
    ValueError(#[source] ValueError),
    #[error("Null value not allowed: {0}")]
    NullNotAllowed(String),
    #[error("Invalid argument : {0}")]
    InvalidArgumentError(String),
}
