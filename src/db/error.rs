use crate::{db::KeyExtractError, manifest::ManifestError, query::stream::StreamError};

/// Error returned for DB
#[derive(Debug, thiserror::Error)]
pub enum DBError {
    /// Key extract error
    #[error("key extract error: {0}")]
    Key(#[from] KeyExtractError),
    /// Manifest error
    #[error("manifest error: {0}")]
    Manifest(#[from] ManifestError),
    /// Read stream composition failed.
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),
}
