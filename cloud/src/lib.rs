use std::{future::Future, ops::Bound, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::io;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use tonbo::{
    executor::Executor,
    parquet::errors::ParquetError,
    record::{Key, Record, Schema},
    transaction::Transaction,
    Entry,
};

use crate::error::CloudError;

mod aws;
mod compaction;
mod error;
mod metadata;

/// Trait for implmenting a cloud instance over different object storages
#[async_trait]
pub trait TonboCloud<R>
where
    R: Record,
{
    /// Creates a new Tonbo cloud instnace
     async fn new(&self, name: String, schema: R::Schema) -> Self;

    fn write(&self, records: impl ExactSizeIterator<Item = R>);

    async fn read<'a>(
        &'a self,
        transaction: &'a Transaction<'_, R>,
        scan: &'a ScanRequest<<R::Schema as Schema>::Key>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Entry<'a, R>, ParquetError>> + Send + 'a>>, CloudError>;

    /// Listens to new read requests from connections
    fn listen(&'static self) -> impl Future<Output = std::io::Result<()>> + 'static;

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
