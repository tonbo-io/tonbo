use std::{future::Future, ops::Bound, pin::Pin};

use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use tonbo::{
    executor::Executor,
    parquet::errors::ParquetError,
    record::{Key, Record, Schema},
    Entry,
};

mod aws;
mod compaction;
mod error;
mod metadata;

/// Trait for implmenting a cloud instance over different object storages
#[async_trait]
pub trait TonboCloud<R, E>
where
    R: Record,
    E: Executor,
{
    /// Creates a new Tonbo cloud instnace
    async fn new(&self, name: String);

    fn write(&self, records: impl ExactSizeIterator<Item = R>);

    fn read<'a>(
        &'a self,
        scan: ScanRequest<<R::Schema as Schema>::Key>,
    ) -> impl Stream<Item = Result<Entry<'a, R>, ParquetError>>;

    /// Listens to new read requests from connections
    fn listen<'a>(&'a self) -> Pin<Box<(dyn Future<Output = std::io::Result<()>> + Send + 'a)>>;

    // Updates metadata
    fn update_metadata();

    /// Creates SST and writes to object storage and local Tonbo instance
    fn flush();
}

// Readers will send a scan request to Tonbo Cloud
#[derive(Serialize, Deserialize)]
pub struct ScanRequest<K>
where
    K: Key,
{
    bounds: (Bound<K>, Bound<K>),
    projection: Vec<String>,
}
