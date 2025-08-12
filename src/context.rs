use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{
    fs::manager::StoreManager,
    manifest::{ManifestStorage, ManifestStorageError},
    ondisk::sstable::SsTableID,
    record::Record,
    version::{edit::VersionEdit, timestamp::Timestamp, VersionRef},
    ParquetLru,
};

pub struct Context<R: Record> {
    pub(crate) manager: Arc<StoreManager>,
    pub(crate) parquet_lru: ParquetLru,
    // TODO: Use concrete implementation of ManifestStorage.
    // ManifestStorage implementation choice should be statically
    // defined during start-up and should not change during runtime.
    pub(crate) manifest: Box<dyn ManifestStorage<R>>,
    pub(crate) arrow_schema: Arc<Schema>,
}

impl<R> Context<R>
where
    R: Record,
{
    pub(crate) fn new(
        manager: Arc<StoreManager>,
        parquet_lru: ParquetLru,
        manifest: Box<dyn ManifestStorage<R>>,
        arrow_schema: Arc<Schema>,
    ) -> Self {
        Self {
            manager,
            parquet_lru,
            manifest,
            arrow_schema,
        }
    }

    pub(crate) fn manifest(&self) -> &dyn ManifestStorage<R> {
        self.manifest.as_ref()
    }

    pub(crate) fn storage_manager(&self) -> &StoreManager {
        &self.manager
    }

    pub(crate) fn cache(&self) -> &ParquetLru {
        &self.parquet_lru
    }

    pub(crate) fn arrow_schema(&self) -> &Arc<Schema> {
        &self.arrow_schema
    }

    pub(crate) fn load_ts(&self) -> Timestamp {
        self.manifest.load_ts()
    }

    pub(crate) fn increase_ts(&self) -> Timestamp {
        self.manifest.increase_ts()
    }

    pub fn manager(&self) -> &Arc<StoreManager> {
        &self.manager
    }

    pub async fn current_manifest(&self) -> VersionRef<R> {
        self.manifest.current().await
    }

    pub async fn update_manifest(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as crate::record::Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError> {
        self.manifest.update(version_edits, delete_gens).await
    }
}
