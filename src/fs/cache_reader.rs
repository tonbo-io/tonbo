use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use foyer::{Cache, DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};
use fusio::path::{path_to_local, Path};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};

use crate::fs::{CacheError, CacheOption};

pub(crate) type MetaCache = Arc<Cache<Path, Arc<ParquetMetaData>>>;

pub(crate) struct CacheReader<R> {
    inner: R,
    meta_path: Path,
    cache: HybridCache<Range<usize>, Bytes>,
    meta_cache: MetaCache,
}

impl<R> CacheReader<R> {
    pub(crate) async fn new(
        option: &CacheOption,
        meta_cache: MetaCache,
        inner: R,
        meta_path: Path,
    ) -> Result<CacheReader<R>, CacheError> {
        // SAFETY: `meta_path` must be the path of a parquet file
        let path = path_to_local(&option.path.child(meta_path.filename().unwrap()))?;
        let cache: HybridCache<Range<usize>, Bytes> = HybridCacheBuilder::new()
            .memory(option.memory)
            .storage(Engine::Large) // use large object disk cache engine only
            .with_device_options(DirectFsDeviceOptions::new(path).with_capacity(option.local))
            .build()
            .await?;

        Ok(Self {
            inner,
            meta_path,
            cache,
            meta_cache,
        })
    }
}

impl<R: AsyncFileReader> AsyncFileReader for CacheReader<R> {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            if let Some(entry) = self
                .cache
                .get(&range)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?
            {
                return Ok(entry.value().clone());
            }

            let bytes = self.inner.get_bytes(range.clone()).await?;
            let entry = self.cache.insert(range, bytes);
            Ok(entry.value().clone())
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            if let Some(entry) = self.meta_cache.get(&self.meta_path) {
                return Ok(entry.value().clone());
            }

            let meta = self
                .inner
                .get_metadata()
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?;
            let entry = self.meta_cache.insert(self.meta_path.clone(), meta);

            Ok(entry.value().clone())
        }
        .boxed()
    }
}
