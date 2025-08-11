pub mod aws_tonbo_svc;
pub mod flight_svc;

use std::{
    env,
    marker::Send,
    net::{Ipv4Addr, SocketAddr},
    ops::Bound,
    pin::Pin,
    sync::Arc,
};

use arrow_flight::flight_service_server::FlightServiceServer;
use async_stream::try_stream;
use async_trait::async_trait;
use fusio::{
    path::Path as TonboPath,
    remotes::aws::{fs::AmazonS3Builder, AwsCredential},
    DynFs,
};
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::fs::create_dir_all;
use tonbo::{
    arrow::array::RecordBatch,
    executor::tokio::TokioExecutor,
    parquet::errors::ParquetError,
    record::{dynamic::Value, util::records_to_record_batch, DynRecord, Record},
    transaction::Transaction,
    DbOption, Entry, DB,
};
use tonic::transport::Server;

use crate::{
    aws::{aws_tonbo_svc::AWSTonboSvc, flight_svc::TonboFlightSvc},
    error::CloudError,
    gen::grpc::{
        self,
        aws_tonbo_server::{AwsTonbo as AwsTonboRPC, AwsTonboServer},
    },
    ScanRequest, TonboCloud,
};

pub const DEFAULT_PORT: u32 = 8080;

/// Every table has its own tonbo cloud instance.
#[allow(dead_code)]
pub struct AWSTonbo {
    // TODO: Add Tonbo DB instance
    // Name of the Tonbo cloud instance
    name: String,
    // Local Tonbo instance
    tonbo: DB<DynRecord, TokioExecutor>,
    // Remote file system
    s3_fs: Arc<dyn DynFs>,
    // Endpoint for read requests (scans)
    endpoint: String,
    buffered_data: Option<RecordBatch>,
}

impl AWSTonbo {
    // todo: Separate the data
    // Returns number of rows and row size
    async fn parquet_metadata<'a>(
        &self,
        transaction: &'a Transaction<'_, DynRecord, TokioExecutor>,
        scan: &'a ScanRequest,
    ) -> Result<(i64, i32), CloudError> {
        let mut row_count = 0;
        let mut row_size = 0;

        let mut inner = self
            .read(transaction, scan)
            .await
            .map_err(|e| CloudError::Cloud(e.to_string()))
            .unwrap();
        let mut calculate_size = true;
        let mut batch_builder: Vec<(u32, DynRecord)> = vec![];

        while let Some(res) = inner.next().await {
            match res {
                Ok(Entry::RecordBatch(batch_entry)) => {
                    let batch = batch_entry.record_batch();
                    row_count += batch.num_rows() as i64;
                    if calculate_size {
                        row_size =
                            (batch.get_array_memory_size() as i64 / batch.num_rows() as i64) as i32;
                        calculate_size = false;
                    }
                }
                Ok(Entry::Mutable(entry)) => {
                    if let Some(record) = entry.value() {
                        batch_builder.push((0, (*record).clone()));
                    }
                }
                Ok(Entry::Transaction((_, record))) => {
                    if let Some(record) = record {
                        batch_builder.push((0, (*record).clone()));
                    }
                }
                // TODO: deal with projection
                Ok(Entry::Projection((_record, _projection))) => {
                    todo!()
                }
                Err(_e) => todo!(),
            }
        }

        if !batch_builder.is_empty() {
            let batch = records_to_record_batch(&batch_builder[0].1.schema(0), batch_builder);
            row_count += batch.num_rows() as i64;
            if calculate_size {
                row_size = (batch.get_array_memory_size() as i64 / batch.num_rows() as i64) as i32;
                calculate_size = false;
            }
        }

        Ok((row_count, row_size))
    }
}

#[async_trait]
impl TonboCloud for AWSTonbo {
    /// Creates new Tonbo cloud instance on S3
    async fn new(name: String, schema: <DynRecord as Record>::Schema) -> Self {
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        let s3_fs: Arc<dyn DynFs> = Arc::new(
            AmazonS3Builder::new("fusio-test".into())
                .credential(AwsCredential {
                    key_id,
                    secret_key,
                    token: None,
                })
                .region("ap-southeast-1".into())
                .sign_payload(true)
                .build(),
        );

        let _ = create_dir_all("./db_path/users").await;

        let options = DbOption::new(
            TonboPath::from_filesystem_path("./db_path/users").unwrap(),
            &schema,
        );

        let tonbo: DB<DynRecord, TokioExecutor> =
            DB::new(options, TokioExecutor::default(), schema)
                .await
                .unwrap();

        let local_host = Ipv4Addr::new(127, 0, 0, 1);
        let endpoint = format!("http://{}:{}/tables/{}/", local_host, DEFAULT_PORT, name);

        Self {
            name,
            tonbo,
            s3_fs,
            endpoint,
            buffered_data: None,
        }
    }

    fn write(&self, _records: impl ExactSizeIterator<Item = DynRecord>) {}

    async fn read<'a>(
        &'a self,
        transaction: &'a Transaction<'_, DynRecord, TokioExecutor>,
        scan: &'a ScanRequest,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Entry<'a, DynRecord>, ParquetError>> + Send + 'a>>,
        CloudError,
    > {
        Ok(Box::pin(try_stream! {
                let projection: Vec<&str> = scan.projection.iter().map(String::as_str).collect();

            let mut inner = transaction
                .scan((
                    scan.lower.as_ref(),
                    scan.upper.as_ref(),
                ))
                .projection(&projection)
                .take()
                .await
                .map_err(|_db_err| {
                    ParquetError::General("Error occured while creating
        transaction.".to_string())         })?;

            while let Some(entry) = inner.next().await {
                yield entry?;
            }
        }))
    }

    async fn listen(self, addr: SocketAddr) -> std::io::Result<()> {
        let shared = Arc::new(self);

        let app = AwsTonboServer::new(AWSTonboSvc::new(shared.clone()));
        let flight = FlightServiceServer::new(TonboFlightSvc::new(shared.clone()));

        Server::builder()
            .add_service(app)
            .add_service(flight)
            .serve(addr)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    // This will create the new metadata update that will be sent to S3
    fn update_metadata() {
        todo!()
    }

    // Writes SSTable to S3
    fn flush() {
        todo!()
    }
}

impl From<grpc::ScanRequest> for ScanRequest {
    fn from(g: grpc::ScanRequest) -> Self {
        // helper to map one side
        fn map_bound(opt: Option<grpc::BoundValue>) -> Bound<Value> {
            match opt {
                Some(bv) => match bv.kind {
                    Some(grpc::bound_value::Kind::Inclusive(v)) => Bound::Included(Value::from(v)),
                    Some(grpc::bound_value::Kind::Exclusive(v)) => Bound::Excluded(Value::from(v)),
                    Some(grpc::bound_value::Kind::Unbounded(())) => Bound::Unbounded,
                    None => Bound::Unbounded,
                },
                None => Bound::Unbounded,
            }
        }

        let lower = map_bound(g.lower);
        let upper = map_bound(g.upper);

        ScanRequest {
            lower,
            upper,
            projection: g.projection,
        }
    }
}

impl From<grpc::Value> for Value {
    fn from(g: grpc::Value) -> Self {
        match g.kind {
            None => Value::Null,

            Some(grpc::value::Kind::Null(_)) => Value::Null,

            Some(grpc::value::Kind::Boolean(b)) => Value::Boolean(b),

            Some(grpc::value::Kind::Int8(i)) => Value::Int8(i as i8),
            Some(grpc::value::Kind::Int16(i)) => Value::Int16(i as i16),
            Some(grpc::value::Kind::Int32(i)) => Value::Int32(i),
            Some(grpc::value::Kind::Int64(i)) => Value::Int64(i),

            Some(grpc::value::Kind::Uint8(u)) => Value::UInt8(u as u8),
            Some(grpc::value::Kind::Uint16(u)) => Value::UInt16(u as u16),
            Some(grpc::value::Kind::Uint32(u)) => Value::UInt32(u),
            Some(grpc::value::Kind::Uint64(u)) => Value::UInt64(u),

            Some(grpc::value::Kind::Float32(f)) => Value::Float32(f),
            Some(grpc::value::Kind::Float64(f)) => Value::Float64(f),

            Some(grpc::value::Kind::StringValue(s)) => Value::String(s),
            Some(grpc::value::Kind::Binary(b)) => Value::Binary(b),

            Some(grpc::value::Kind::FixedSizeBinary(fsb)) => {
                // see if this is right
                Value::FixedSizeBinary(fsb.value, fsb.length)
            }

            Some(grpc::value::Kind::Date32(d)) => Value::Date32(d),
            Some(grpc::value::Kind::Date64(d)) => Value::Date64(d),

            Some(grpc::value::Kind::Time32(_t)) => {
                todo!()
            }

            Some(grpc::value::Kind::Time64(_t)) => {
                todo!()
            }

            Some(grpc::value::Kind::Timestamp(_ts)) => {
                todo!()
            }
        }
    }
}

#[cfg(test)]
mod tests {}
