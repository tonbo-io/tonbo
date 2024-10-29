use std::{io, ops::Range, sync::Arc};

use bytes::Bytes;
use foyer::{Cache, HybridCache};
use parquet::file::metadata::ParquetMetaData;
use thiserror::Error;
use ulid::Ulid;

pub mod foyer_reader;

// Tips: Conditional compilation adapts to different implementations
pub type MetaCache = Arc<Cache<Ulid, Arc<ParquetMetaData>>>;
pub type RangeCache = HybridCache<(Ulid, Range<usize>), Bytes>;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache io error: {0}")]
    Io(#[from] io::Error),
    #[error("foyer error: {0}")]
    Foyer(#[from] anyhow::Error),
}
