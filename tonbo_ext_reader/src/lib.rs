use std::{fmt::Debug, io, ops::Range, sync::Arc};

use bytes::Bytes;
use fusio_parquet::reader::AsyncReader;
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};
use thiserror::Error;
use ulid::Ulid;

pub mod foyer_reader;

pub trait MetaCache: Sync + Send + Clone + Debug {
    fn get(&self, gen: &Ulid) -> Option<Arc<ParquetMetaData>>;

    fn insert(&self, gen: Ulid, data: Arc<ParquetMetaData>) -> Arc<ParquetMetaData>;
}

pub trait RangeCache: Sync + Send + Clone + Debug {
    fn get(
        &self,
        key: &(Ulid, Range<usize>),
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, CacheError>> + Send;

    fn insert(&self, key: (Ulid, Range<usize>), bytes: Bytes) -> Bytes;
}

pub trait CacheReader: AsyncFileReader + Unpin {
    type MetaCache: MetaCache;
    type RangeCache: RangeCache;

    fn new(
        meta_cache: Self::MetaCache,
        range_cache: Self::RangeCache,
        gen: Ulid,
        inner: AsyncReader,
    ) -> Self;

    fn build_caches(
        cache_path: impl AsRef<std::path::Path> + Send,
        cache_meta_capacity: usize,
        cache_meta_shards: usize,
        cache_meta_ratio: f64,
        cache_range_memory: usize,
        cache_range_disk: usize,
    ) -> impl std::future::Future<Output = Result<(Self::MetaCache, Self::RangeCache), CacheError>> + Send;
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache io error: {0}")]
    Io(#[from] io::Error),
    #[error("foyer error: {0}")]
    Foyer(#[from] anyhow::Error),
}
