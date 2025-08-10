use std::{net::SocketAddr, ops::Bound, pin::Pin};

use async_trait::async_trait;
use futures_core::Stream;
use tonbo::{
    executor::tokio::TokioExecutor, parquet::errors::ParquetError, record::{dynamic::Value, DynRecord, Record}, transaction::Transaction, Entry
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
        transaction: &'a Transaction<'_, DynRecord, TokioExecutor>,
        scan: &'a ScanRequest,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Entry<'a, DynRecord>, ParquetError>> + Send + 'a>>,
        CloudError,
    >;

    /// Listens to new read requests from connections
    async fn listen(self, addr: SocketAddr) -> std::io::Result<()>;

    // Updates metadata
    fn update_metadata();

    /// Creates SST and writes to object storage and local Tonbo instance
    fn flush();
}

/// Readers will send a scan request to Tonbo Cloud
/// This can be sent for a metadata or data request
pub struct ScanRequest {
    lower: Bound<Value>,
    upper: Bound<Value>,
    projection: Vec<String>,
}
