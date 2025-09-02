use thiserror::Error;

/// Errors for `Metadata manifest storage`
#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("database connection error: {0}")]
    DbConnection(#[from] &'static dyn std::error::Error), // TODO @liguoso: Use sqlx Error
}
