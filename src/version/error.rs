use flume::SendError;
use fusio_log::error::LogError;
use thiserror::Error;

use crate::version::cleaner::CleanTag;

/// Errors for `Version`
#[derive(Debug, Error)]
pub enum VersionError {
    #[error("version encode error: {0}")]
    Encode(#[source] fusio::Error),
    #[error("version io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("version parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("version fusio error: {0}")]
    Fusio(#[from] fusio::Error),
    #[error("version ulid decode error: {0}")]
    UlidDecode(#[from] ulid::DecodeError),
    #[error("version send error: {0}")]
    Send(#[from] SendError<CleanTag>),
    #[error("log error: {0}")]
    Logger(#[from] LogError),
}
