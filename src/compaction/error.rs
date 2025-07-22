use thiserror::Error;

use crate::{manifest::ManifestStorageError, record::Record, CommitError};

#[derive(Debug, Error)]
pub enum CompactionError<R>
where
    R: Record,
{
    #[error("compaction io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("compaction parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("compaction fusio error: {0}")]
    Fusio(#[from] fusio::Error),
    #[error("compaction manifest storage error: {0}")]
    Manifest(#[from] ManifestStorageError<R>),
    #[error("compaction logger error: {0}")]
    Logger(#[from] fusio_log::error::LogError),
    #[error("compaction channel is closed")]
    ChannelClose,
    #[error("database error: {0}")]
    Commit(#[from] CommitError<R>),
    #[error("the level being compacted does not have a table")]
    EmptyLevel,
}
