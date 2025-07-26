//! Manifest storage
//!
//! This module provides abstractions for interacting with manifest components
//! of the database. Manifest components include version sets, SSTable and MVCC
//! information.

use async_trait::async_trait;
use fusio::{MaybeSend, MaybeSync};
use thiserror::Error;

use crate::{
    ondisk::sstable::SsTableID,
    record::{Record, Schema},
    version::{edit::VersionEdit, TransactionTs, VersionError, VersionRef},
};

#[derive(Debug, Error)]
pub enum ManifestStorageError {
    #[error("manifest version error")]
    Version(#[from] VersionError),
}

/// Trait for storing and managing LSM-tree manifest
///
/// The `ManifestStorage` trait provides an interface for managing LSM-tree manifest
/// including version sets, SSTable structures, and MVCC information. Different
/// implementations may choose to store and access manifest using different
/// backends (local, remote database, distributed consensus, etc.)
#[allow(unused)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub(crate) trait ManifestStorage<R: Record>: MaybeSend + MaybeSync + TransactionTs {
    /// Return reference to the current version of the LSM-tree.
    async fn current(&self) -> VersionRef<R>;

    /// Recover manifest state while applying version edits. Use to rebuild
    /// the in-memory manifest state from persisted log.
    async fn recover(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError>;

    /// Apply version edits and update the manifest.
    async fn update(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError>;

    /// Perform a rewrite of manifest log. Can be used to perform log
    /// compaction.
    async fn rewrite(&self) -> Result<(), ManifestStorageError>;

    /// Completely destroy all manifest data.
    async fn destroy(&mut self) -> Result<(), ManifestStorageError>;
}
