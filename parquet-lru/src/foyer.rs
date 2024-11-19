use std::{hash::Hash, ops::Range, sync::Arc};

use bytes::Bytes;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use parquet::{
    arrow::async_reader::AsyncFileReader,
    errors::{ParquetError, Result},
    file::metadata::ParquetMetaData,
};
use serde::{Deserialize, Serialize};

use crate::LruCache;

#[derive(Clone)]
pub struct FoyerCache<K>
where
    for<'a> K: Send + Sync + Hash + Eq + Serialize + Deserialize<'a> + 'static,
{
    inner: Arc<FoyerCacheInner<K>>,
}

pub struct FoyerCacheInner<K>
where
    for<'a> K: Send + Sync + Hash + Eq + Serialize + Deserialize<'a> + 'static,
{
    meta: foyer::Cache<K, Arc<ParquetMetaData>>,
    data: foyer::HybridCache<(K, Range<usize>), Bytes>,
}

impl<K> LruCache<K> for FoyerCache<K>
where
    for<'a> K: Send + Sync + Hash + Eq + Serialize + Deserialize<'a> + Clone + 'static,
{
    type LruReader<R>
        = FoyerReader<K, R>
    where
        R: AsyncFileReader + 'static;

    async fn get_reader<R>(&self, key: K, reader: R) -> FoyerReader<K, R>
    where
        R: AsyncFileReader,
    {
        FoyerReader::new(self.clone(), key, reader)
    }
}

pub struct FoyerReader<K, R>
where
    for<'a> K: Send + Sync + Hash + Eq + Serialize + Deserialize<'a> + 'static,
{
    cache: FoyerCache<K>,
    key: K,
    reader: R,
}

impl<K, R> FoyerReader<K, R>
where
    for<'a> K: Send + Sync + Hash + Eq + Serialize + Deserialize<'a> + 'static,
    R: AsyncFileReader,
{
    fn new(cache: FoyerCache<K>, key: K, reader: R) -> Self {
        Self { cache, key, reader }
    }
}

impl<K, R> AsyncFileReader for FoyerReader<K, R>
where
    for<'a> K: Send + Sync + Hash + Eq + Serialize + Deserialize<'a> + Clone + 'static,
    R: AsyncFileReader,
{
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            if let Some(data) = self
                .cache
                .inner
                .data
                .get(&(self.key.clone(), range.clone()))
                .await
                .map_err(|e| ParquetError::External(e.into()))?
            {
                Ok(data.value().clone())
            } else {
                let data = self.reader.get_bytes(range.clone()).await?;
                self.cache
                    .inner
                    .data
                    .insert((self.key.clone(), range), data.clone());
                Ok(data)
            }
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
        async move {
            if let Some(meta) = self.cache.inner.meta.get(&self.key) {
                Ok(meta.value().clone())
            } else {
                let meta = self.reader.get_metadata().await?;
                self.cache.inner.meta.insert(self.key.clone(), meta.clone());
                Ok(meta)
            }
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<usize>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            let mut missed = Vec::with_capacity(ranges.len());
            let mut results = Vec::with_capacity(ranges.len());
            for (id, range) in ranges.iter().enumerate() {
                if let Some(data) = self
                    .cache
                    .inner
                    .data
                    .get(&(self.key.clone(), range.clone()))
                    .await
                    .map_err(|e| ParquetError::External(e.into()))?
                {
                    results.push((id, data.value().clone()));
                } else {
                    missed.push((id, range));
                }
            }
            if !missed.is_empty() {
                let data = self
                    .reader
                    .get_byte_ranges(missed.iter().map(|&(_, r)| r.clone()).collect())
                    .await?;
                for (id, range) in missed {
                    let data = data[id].clone();
                    self.cache
                        .inner
                        .data
                        .insert((self.key.clone(), range.clone()), data.clone());
                    results.push((id, data));
                }
            }
            results.sort_by_key(|(id, _)| *id);
            Ok(results.into_iter().map(|(_, data)| data).collect())
        }
        .boxed()
    }
}
