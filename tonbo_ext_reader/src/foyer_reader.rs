use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use foyer::{
    Cache, CacheBuilder, DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder, LruConfig,
};
use fusio_parquet::reader::AsyncReader;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};
use ulid::Ulid;

use crate::{CacheError, CacheReader, TonboCache};

#[derive(Debug, Clone)]
pub struct FoyerMetaCache(Cache<Ulid, Arc<ParquetMetaData>>);
#[derive(Debug, Clone)]
pub struct FoyerRangeCache(HybridCache<(Ulid, Range<usize>), Bytes>);

pub struct FoyerReader {
    gen: Ulid,
    inner: AsyncReader,
    range_cache: FoyerRangeCache,
    meta_cache: FoyerMetaCache,
}

impl TonboCache<Ulid, Arc<ParquetMetaData>> for FoyerMetaCache {
    async fn get(&self, gen: &Ulid) -> Result<Option<Arc<ParquetMetaData>>, CacheError> {
        Ok(self.0.get(gen).map(|entry| entry.value().clone()))
    }

    fn insert(&self, gen: Ulid, data: Arc<ParquetMetaData>) -> Arc<ParquetMetaData> {
        self.0.insert(gen, data).value().clone()
    }
}

impl TonboCache<(Ulid, Range<usize>), Bytes> for FoyerRangeCache {
    async fn get(&self, key: &(Ulid, Range<usize>)) -> Result<Option<Bytes>, CacheError> {
        Ok(self.0.get(key).await?.map(|entry| entry.value().clone()))
    }

    fn insert(&self, key: (Ulid, Range<usize>), bytes: Bytes) -> Bytes {
        self.0.insert(key, bytes).value().clone()
    }
}

impl CacheReader for FoyerReader {
    type MetaCache = FoyerMetaCache;
    type RangeCache = FoyerRangeCache;

    fn new(
        meta_cache: Self::MetaCache,
        range_cache: Self::RangeCache,
        gen: Ulid,
        inner: AsyncReader,
    ) -> Self {
        Self {
            gen,
            inner,
            range_cache,
            meta_cache,
        }
    }

    async fn build_caches(
        cache_path: impl AsRef<std::path::Path> + Send,
        cache_meta_capacity: usize,
        cache_meta_shards: usize,
        cache_meta_ratio: f64,
        cache_range_memory: usize,
        cache_range_disk: usize,
        _: usize,
        _: usize,
    ) -> Result<(Self::MetaCache, Self::RangeCache), CacheError> {
        let meta_cache = CacheBuilder::new(cache_meta_capacity)
            .with_shards(cache_meta_shards)
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: cache_meta_ratio,
            })
            .build();
        let range_cache = HybridCacheBuilder::new()
            .memory(cache_range_memory)
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(cache_path).with_capacity(cache_range_disk),
            )
            .build()
            .await
            .map_err(CacheError::from)?;
        Ok((FoyerMetaCache(meta_cache), FoyerRangeCache(range_cache)))
    }
}

impl AsyncFileReader for FoyerReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let key = (self.gen, range);
            if let Some(bytes) = self
                .range_cache
                .get(&key)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?
            {
                return Ok(bytes);
            }

            let bytes = self.inner.get_bytes(key.1.clone()).await?;
            Ok(self.range_cache.insert(key, bytes))
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            if let Some(meta) = self
                .meta_cache
                .get(&self.gen)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?
            {
                return Ok(meta);
            }

            let meta = self
                .inner
                .get_metadata()
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?;

            Ok(self.meta_cache.insert(self.gen, meta))
        }
        .boxed()
    }
}
