use std::{future::Future, net::SocketAddr, ops::Bound, pin::Pin};

use async_trait::async_trait;
use futures_core::Stream;
use tonbo::{
    parquet::errors::ParquetError,
    record::{dynamic::Value, DynRecord, Key, Record, Schema, TimeUnit},
    transaction::Transaction,
    Entry,
};

use crate::error::CloudError;

mod aws;
mod compaction;
mod error;
pub mod gen;
mod metadata;

/// Trait for implmenting a cloud instance over different object storages
#[async_trait]
pub trait TonboCloud {
    /// Creates a new Tonbo cloud instnace
    async fn new(name: String, schema: <DynRecord as Record>::Schema) -> Self;

    fn write(&self, records: impl ExactSizeIterator<Item = DynRecord>);

    async fn read<'a>(
        &'a self,
        transaction: &'a Transaction<'_, DynRecord>,
        scan: &'a ScanRequest,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Entry<'a, DynRecord>, ParquetError>> + Send + 'a>>,
        CloudError,
    >;

    /// Listens to new read requests from connections
    async fn listen(self, addr: SocketAddr) -> impl Future<Output = std::io::Result<()>> + 'static;

    // Updates metadata
    fn update_metadata();

    /// Creates SST and writes to object storage and local Tonbo instance
    fn flush();
}

// Readers will send a scan request to Tonbo Cloud
pub struct ScanRequest {
    lower: Bound<Value>,
    upper: Bound<Value>,
    projection: Vec<String>,
}
