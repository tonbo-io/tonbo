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

#[async_trait]
#[allow(unused)]
pub(crate) trait ManifestStorage<R: Record>: Send + Sync + TransactionTs {
    async fn current(&self) -> VersionRef<R>;

    async fn recover(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<(FileId, usize)>>,
    ) -> Result<(), ManifestStorageError<R>>;

    async fn update(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<(FileId, usize)>>,
    ) -> Result<(), ManifestStorageError<R>>;

    async fn rewrite(&self) -> Result<(), ManifestStorageError<R>>;

    async fn destroy(&mut self) -> Result<(), ManifestStorageError<R>>;
}
