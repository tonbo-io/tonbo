use typed_arrow_dyn::DynViewError;

use crate::{
    db::KeyExtractError, manifest::ManifestError, ondisk::sstable::SsTableError,
    query::stream::StreamError, transaction::SnapshotError,
};

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
    /// SSTable read/write error.
    #[error("sstable error: {0}")]
    SsTable(#[from] SsTableError),
    /// Snapshot creation error.
    #[error("snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),
    /// Dynamic view error.
    #[error("dynamic view error: {0}")]
    DynView(#[from] DynViewError),
    /// Predicate uses an unsupported expression variant.
    #[error("unsupported predicate: {reason}")]
    UnsupportedPredicate {
        /// Details about the unsupported predicate.
        reason: String,
    },
}
