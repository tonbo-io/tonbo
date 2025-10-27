use thiserror::Error;

/// Errors for `Metadata service`
#[derive(Debug, Error)]
pub enum MetadataServiceError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("ulid decode error: {0}")]
    Decode(#[from] ulid::DecodeError),
    #[error("fusio error: {0}")]
    Fusio(#[from] fusio::Error),
    #[error("unexpected error: {0}")]
    Unexpected(String),
}
