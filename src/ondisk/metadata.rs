//! Parquet metadata caching utilities for SST planning.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use parquet::file::metadata::ParquetMetaData;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    path: fusio::path::Path,
}

#[derive(Debug)]
pub(crate) struct ParquetMetadataCache {
    max_entries: usize,
    order: VecDeque<CacheKey>,
    entries: HashMap<CacheKey, Arc<ParquetMetaData>>,
}

impl ParquetMetadataCache {
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            order: VecDeque::new(),
            entries: HashMap::new(),
        }
    }

    pub(crate) fn default() -> Self {
        Self::new(256)
    }

    pub(crate) fn get(&self, path: &fusio::path::Path) -> Option<Arc<ParquetMetaData>> {
        let key = CacheKey { path: path.clone() };
        self.entries.get(&key).cloned()
    }

    pub(crate) fn insert(&mut self, path: &fusio::path::Path, metadata: Arc<ParquetMetaData>) {
        let key = CacheKey { path: path.clone() };
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.entries.entry(key.clone())
        {
            entry.insert(metadata);
            return;
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, metadata);
        while self.entries.len() > self.max_entries {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
    }
}

pub(crate) fn default_parquet_metadata_cache<E>() -> Arc<E::Mutex<ParquetMetadataCache>>
where
    E: fusio::executor::Executor + Clone + 'static,
{
    Arc::new(E::mutex(ParquetMetadataCache::default()))
}
