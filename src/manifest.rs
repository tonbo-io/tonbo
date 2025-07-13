//! Manifest storage
//!
//! This module provides abstractions for interacting with manifest components
//! of the database. Manifest components include version sets, SSTable and MVCC
//! information.

use async_trait::async_trait;
use thiserror::Error;

use crate::{
    fs::FileId,
    record::{Record, Schema},
    version::{edit::VersionEdit, TransactionTs, VersionError, VersionRef},
};

#[derive(Debug, Error)]
pub enum ManifestStorageError<R>
where
    R: Record,
{
    #[error("manifest version error")]
    Version(#[from] VersionError<R>),
}

/// Trait for storing and managing LSM-tree manifest
///
/// The `ManifestStorage` trait provides an interface for managing LSM-tree manifest
/// including version sets, SSTable structures, and MVCC information. Different
/// implementations may choose to store and access manifest using different
/// backends (local, remote database, distributed consensus, etc.)
#[async_trait]
#[allow(unused)]
pub(crate) trait ManifestStorage<R: Record>: Send + Sync + TransactionTs {
    /// Return reference to the current version of the LSM-tree.
    async fn current(&self) -> VersionRef<R>;

    /// Recover manifest state while applying version edits. Use to rebuild
    /// the in-memory manifest state from persisted log.
    async fn recover(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<(FileId, usize)>>,
    ) -> Result<(), ManifestStorageError<R>>;

    /// Apply version edits and update the manifest.
    async fn update(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<(FileId, usize)>>,
    ) -> Result<(), ManifestStorageError<R>>;

    /// Perform a rewrite of manifest log. Can be used to perform log
    /// compaction.
    async fn rewrite(&self) -> Result<(), ManifestStorageError<R>>;

    /// Completely destroy all manifest data.
    async fn destroy(&mut self) -> Result<(), ManifestStorageError<R>>;
}
