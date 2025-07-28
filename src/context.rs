use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{
    fs::manager::StoreManager, manifest::ManifestStorage, record::Record,
    version::timestamp::Timestamp, ParquetLru,
};

pub(crate) struct Context<R: Record> {
    pub(crate) manager: Arc<StoreManager>,
    pub(crate) parquet_lru: ParquetLru,
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

    pub(crate) fn manifest(&self) -> &Box<dyn ManifestStorage<R>> {
        &self.manifest
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
}
