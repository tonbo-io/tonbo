use std::{
    hash::{BuildHasher, Hash, RandomState},
    num::NonZeroUsize,
    ops::Range,
    path::Path,
    sync::Arc,
};

use bytes::Bytes;
use fusio_parquet::reader::AsyncReader;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use lru::LruCache;
use parking_lot::Mutex;
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};
use ulid::Ulid;

use crate::{CacheError, CacheReader, MetaCache, RangeCache};

pub(crate) trait SharedKey<S: BuildHasher>: Hash + PartialEq + Eq {
    fn shared(&self, hash_builder: &S, shared: usize) -> usize;
}

#[derive(Debug)]
pub(crate) struct SharedLruCache<K, V, S = RandomState>
where
    K: SharedKey<S> + Clone,
    V: Clone,
    S: BuildHasher,
{
    caches: Vec<Mutex<LruCache<K, V>>>,
    hasher: S,
}

impl<K, V, S> SharedLruCache<K, V, S>
where
    K: SharedKey<S> + Clone,
    V: Clone,
    S: BuildHasher,
{
    pub fn new(cap: usize, shared: usize, hasher: S) -> Self {
        assert_eq!(cap % shared, 0);

        let mut caches = Vec::with_capacity(shared);

        for _ in 0..shared {
            caches.push(Mutex::new(LruCache::new(NonZeroUsize::new(cap).unwrap())));
        }

        SharedLruCache { caches, hasher }
    }

    #[inline]
    pub fn get<T, F: FnOnce(Option<&V>) -> Option<T>>(&self, key: &K, fn_value: F) -> Option<T> {
        let mut guard = self.shard(key).lock();
        fn_value(guard.get(key))
    }

    #[inline]
    pub fn put(&self, key: K, value: V) -> Option<V> {
        self.shard(&key).lock().put(key, value)
    }

    fn sharding_size(&self) -> usize {
        self.caches.len()
    }

    fn shard(&self, key: &K) -> &Mutex<LruCache<K, V>> {
        let pos = key.shared(&self.hasher, self.sharding_size());
        &self.caches[pos]
    }
}

impl<S: BuildHasher> SharedKey<S> for Ulid {
    fn shared(&self, hash_builder: &S, shared: usize) -> usize {
        hash_builder.hash_one(self) as usize % shared
    }
}

// let the Range of the same Gen be sharded to the same LRU
impl<S: BuildHasher> SharedKey<S> for (Ulid, Range<usize>) {
    fn shared(&self, hash_builder: &S, shared: usize) -> usize {
        self.0.shared(hash_builder, shared)
    }
}

#[derive(Debug, Clone)]
pub struct LruMetaCache(Arc<SharedLruCache<Ulid, Arc<ParquetMetaData>>>);
#[derive(Debug, Clone)]
pub struct LruRangeCache(Arc<SharedLruCache<(Ulid, Range<usize>), Bytes>>);

pub struct LruReader {
    gen: Ulid,
    inner: AsyncReader,
    range_cache: LruRangeCache,
    meta_cache: LruMetaCache,
}

impl MetaCache for LruMetaCache {
    fn get(&self, gen: &Ulid) -> Option<Arc<ParquetMetaData>> {
        self.0.get(gen, |v| v.map(Arc::clone))
    }

    fn insert(&self, gen: Ulid, data: Arc<ParquetMetaData>) -> Arc<ParquetMetaData> {
        let _ = self.0.put(gen, data.clone());
        data
    }
}

impl RangeCache for LruRangeCache {
    async fn get(&self, key: &(Ulid, Range<usize>)) -> Result<Option<Bytes>, CacheError> {
        Ok(self.0.get(key, |v| v.cloned()))
    }

    fn insert(&self, key: (Ulid, Range<usize>), bytes: Bytes) -> Bytes {
        let _ = self.0.put(key, bytes.clone());
        bytes
    }
}

impl AsyncFileReader for LruReader {
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
            if let Some(meta) = self.meta_cache.get(&self.gen) {
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

impl CacheReader for LruReader {
    type MetaCache = LruMetaCache;
    type RangeCache = LruRangeCache;

    fn new(
        meta_cache: Self::MetaCache,
        range_cache: Self::RangeCache,
        gen: Ulid,
        inner: AsyncReader,
    ) -> Self {
        LruReader {
            gen,
            inner,
            range_cache,
            meta_cache,
        }
    }

    async fn build_caches(
        _: impl AsRef<Path> + Send,
        cache_meta_capacity: usize,
        cache_meta_shards: usize,
        _: f64,
        _: usize,
        _: usize,
        cache_range_capacity: usize,
        cache_range_shards: usize,
    ) -> Result<(Self::MetaCache, Self::RangeCache), CacheError> {
        let meta_cache = LruMetaCache(Arc::new(SharedLruCache::new(
            cache_meta_capacity,
            cache_meta_shards,
            RandomState::default(),
        )));
        let range_cache = LruRangeCache(Arc::new(SharedLruCache::new(
            cache_range_capacity,
            cache_range_shards,
            RandomState::default(),
        )));

        Ok((meta_cache, range_cache))
    }
}
