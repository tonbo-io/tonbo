//! Bloom filter loading utilities for Parquet-backed SSTs.

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    sync::Arc,
};

use bytes::Bytes;
use fusio::{
    dynamic::DynFile,
    executor::{Executor, Mutex},
};
use fusio_parquet::reader::AsyncReader;
use futures::FutureExt;
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    bloom_filter::Sbbf,
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};

use crate::ondisk::scan::UnpinExec;

const DEFAULT_BLOOM_CACHE_ENTRIES: usize = 256;
const DEFAULT_RANGE_COALESCE_GAP_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct BloomCacheKey {
    path: fusio::path::Path,
    row_group: usize,
    column: usize,
}

#[derive(Debug)]
pub(crate) struct BloomFilterCache {
    max_entries: usize,
    order: VecDeque<BloomCacheKey>,
    entries: HashMap<BloomCacheKey, Sbbf>,
}

impl BloomFilterCache {
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            order: VecDeque::new(),
            entries: HashMap::new(),
        }
    }

    pub(crate) fn default() -> Self {
        Self::new(DEFAULT_BLOOM_CACHE_ENTRIES)
    }

    fn get(&self, key: &BloomCacheKey) -> Option<Sbbf> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: BloomCacheKey, value: Sbbf) {
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.entries.entry(key.clone())
        {
            entry.insert(value);
            return;
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, value);
        while self.entries.len() > self.max_entries {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
    }
}

pub(crate) struct BatchedAsyncReader<E>
where
    E: Executor + Clone + 'static,
{
    inner: AsyncReader<UnpinExec<E>>,
    coalesce_gap_bytes: u64,
}

impl<E> BatchedAsyncReader<E>
where
    E: Executor + Clone + 'static,
{
    pub(crate) async fn new(
        file: Box<dyn DynFile>,
        content_length: u64,
        executor: E,
    ) -> Result<Self, fusio::error::Error> {
        Ok(Self {
            inner: AsyncReader::new(file, content_length, UnpinExec(executor)).await?,
            coalesce_gap_bytes: DEFAULT_RANGE_COALESCE_GAP_BYTES,
        })
    }
}

impl<E> AsyncFileReader for BatchedAsyncReader<E>
where
    E: Executor + Clone + 'static,
{
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> futures::future::BoxFuture<'_, Result<Bytes, ParquetError>> {
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> futures::future::BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        async move {
            if ranges.is_empty() {
                return Ok(Vec::new());
            }
            let mut indexed: Vec<(usize, Range<u64>)> = ranges.into_iter().enumerate().collect();
            indexed.sort_by_key(|(_, range)| range.start);
            let total_ranges = indexed.len();

            struct Batch {
                start: u64,
                end: u64,
                items: Vec<(usize, Range<u64>)>,
            }

            let mut batches: Vec<Batch> = Vec::new();
            for (idx, range) in indexed {
                if let Some(last) = batches.last_mut()
                    && range.start <= last.end.saturating_add(self.coalesce_gap_bytes)
                {
                    last.end = last.end.max(range.end);
                    last.items.push((idx, range));
                    continue;
                }
                batches.push(Batch {
                    start: range.start,
                    end: range.end,
                    items: vec![(idx, range)],
                });
            }

            let mut results: Vec<Option<Bytes>> = vec![None; total_ranges];
            for batch in batches {
                let buffer = self.inner.get_bytes(batch.start..batch.end).await?;
                for (idx, range) in batch.items {
                    let offset = (range.start - batch.start) as usize;
                    let len = (range.end - range.start) as usize;
                    results[idx] = Some(buffer.slice(offset..offset + len));
                }
            }

            let mut output = Vec::with_capacity(results.len());
            for item in results {
                let Some(bytes) = item else {
                    return Err(ParquetError::General(
                        "missing range result for bloom filter batch".to_string(),
                    ));
                };
                output.push(bytes);
            }
            Ok(output)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> futures::future::BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        self.inner.get_metadata(options)
    }
}

pub(crate) struct SstBloomFilterProvider<E>
where
    E: Executor + Clone + 'static,
{
    path: fusio::path::Path,
    metadata: Arc<ParquetMetaData>,
    reader: BatchedAsyncReader<E>,
    cache: Arc<E::Mutex<BloomFilterCache>>,
}

impl<E> SstBloomFilterProvider<E>
where
    E: Executor + Clone + 'static,
{
    pub(crate) fn new(
        path: fusio::path::Path,
        metadata: Arc<ParquetMetaData>,
        reader: BatchedAsyncReader<E>,
        cache: Arc<E::Mutex<BloomFilterCache>>,
    ) -> Self {
        Self {
            path,
            metadata,
            reader,
            cache,
        }
    }

    fn cache_key(&self, row_group_idx: usize, column_idx: usize) -> BloomCacheKey {
        BloomCacheKey {
            path: self.path.clone(),
            row_group: row_group_idx,
            column: column_idx,
        }
    }

    async fn lookup_cached(&self, key: &BloomCacheKey) -> Option<Sbbf> {
        let guard = self.cache.lock().await;
        guard.get(key)
    }

    async fn insert_cache(&self, key: BloomCacheKey, filter: Sbbf) {
        let mut guard = self.cache.lock().await;
        guard.insert(key, filter);
    }

    fn bloom_range(&self, row_group_idx: usize, column_idx: usize) -> Option<Range<u64>> {
        let row_group = self.metadata.row_group(row_group_idx);
        let column = row_group.column(column_idx);
        let offset: u64 = column.bloom_filter_offset()?.try_into().ok()?;
        let length: u64 = column.bloom_filter_length()?.try_into().ok()?;
        Some(offset..offset + length)
    }
}

impl<E> aisle::AsyncBloomFilterProvider for SstBloomFilterProvider<E>
where
    E: Executor + Clone + 'static,
{
    async fn bloom_filter(&mut self, row_group_idx: usize, column_idx: usize) -> Option<Sbbf> {
        let key = self.cache_key(row_group_idx, column_idx);
        if let Some(filter) = self.lookup_cached(&key).await {
            return Some(filter);
        }

        let range = self.bloom_range(row_group_idx, column_idx)?;
        let bytes = self.reader.get_bytes(range).await.ok()?;
        let filter = Sbbf::from_bytes(bytes.as_ref()).ok()?;
        self.insert_cache(key, filter.clone()).await;
        Some(filter)
    }

    async fn bloom_filters_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> HashMap<(usize, usize), Sbbf> {
        let mut result = HashMap::new();
        let mut misses: Vec<(usize, usize, Range<u64>)> = Vec::new();

        for &(row_group_idx, column_idx) in requests {
            let key = self.cache_key(row_group_idx, column_idx);
            if let Some(filter) = self.lookup_cached(&key).await {
                result.insert((row_group_idx, column_idx), filter);
                continue;
            }
            if let Some(range) = self.bloom_range(row_group_idx, column_idx) {
                misses.push((row_group_idx, column_idx, range));
            }
        }

        if misses.is_empty() {
            return result;
        }

        let ranges: Vec<Range<u64>> = misses.iter().map(|(_, _, range)| range.clone()).collect();
        let bytes = match self.reader.get_byte_ranges(ranges).await {
            Ok(bytes) => bytes,
            Err(_) => return result,
        };

        for ((row_group_idx, column_idx, _range), blob) in misses.into_iter().zip(bytes) {
            if let Ok(filter) = Sbbf::from_bytes(blob.as_ref()) {
                let key = self.cache_key(row_group_idx, column_idx);
                self.insert_cache(key, filter.clone()).await;
                result.insert((row_group_idx, column_idx), filter);
            }
        }

        result
    }
}

pub(crate) fn default_bloom_cache<E>() -> Arc<E::Mutex<BloomFilterCache>>
where
    E: Executor + Clone + 'static,
{
    Arc::new(E::mutex(BloomFilterCache::default()))
}
