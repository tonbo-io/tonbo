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
    record::{
        dynamic::Value, util::records_to_record_batch, ArrowArrays, ArrowArraysBuilder, DynRecord,
        DynRecordImmutableArrays, Record, RecordRef,
    },
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
    #[cfg(test)]
    pub fn tonbo(&self) -> &DB<DynRecord, TokioExecutor> {
        &self.tonbo
    }

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
                Ok(Entry::Projection((record, projection))) => {
                    match *record {
                        // TODO: Make more efficient by batching build batch tranformation
                        Entry::RecordBatch(entry) => {
                            let schema = entry.batch_as_ref().schema();
                            let value = entry.get();
                            if let Some(mut value) = value {
                                let mut dyn_record_builder =
                                    DynRecordImmutableArrays::builder(schema, 1);

                                // Apply projection
                                value.projection(&projection);
                                dyn_record_builder.push(entry.internal_key(), Some(value));
                                let dyn_record_array = dyn_record_builder.finish(None);
                                let batch = dyn_record_array.as_record_batch();

                                row_count += batch.num_rows() as i64;
                                if calculate_size {
                                    row_size = (batch.get_array_memory_size() as i64
                                        / batch.num_rows() as i64)
                                        as i32;
                                    calculate_size = false;
                                }
                            }
                        }
                        _ => {
                            let dyn_record = record.owned_value();

                            if let Some(mut dyn_record) = dyn_record {
                                dyn_record.projection(&projection);
                                batch_builder.push((0, dyn_record));
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(CloudError::Cloud(format!(
                        "Error occured while converting `Entry`s to `RecordBatches`: {}",
                        e
                    )))
                }
            }
        }

        if !batch_builder.is_empty() {
            let batch = records_to_record_batch(&batch_builder[0].1.schema(0), batch_builder);
            row_count += batch.num_rows() as i64;
            if calculate_size {
                row_size = (batch.get_array_memory_size() as i64 / batch.num_rows() as i64) as i32;
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

        let db_root = std::env::var("TONBO_DB_DIR").unwrap_or_else(|_| "./db_path/users".into());

        let _ = create_dir_all(&db_root).await;

        let abs = std::fs::canonicalize(&db_root).expect("canonicalize TONBO_DB_DIR");

        let options = DbOption::new(
            TonboPath::from_filesystem_path(abs.as_path())
                .expect("valid filesystem path for Tonbo"),
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
mod tests {
    use arrow::datatypes::DataType;
    use arrow_array::{Array, Int64Array, Int8Array, StringArray};
    use arrow_flight::{
        flight_service_server::FlightService, utils::flight_data_to_batches, Ticket,
    };
    use prost::Message;
    use tempfile::TempDir;
    use tokio::fs;
    use tonbo::record::{DynSchema, DynamicField};
    use tonic::Request;

    use super::*;

    // Full end to end test
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ete_aws_tonbo() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        let _ = fs::remove_dir_all("./db_path/users").await;

        let tmp = TempDir::new().unwrap();
        std::env::set_var("TONBO_DB_DIR", tmp.path());

        let schema = DynSchema::new(
            &[
                DynamicField::new("id".to_string(), DataType::Int64, false),
                DynamicField::new("age".to_string(), DataType::Int8, true),
                DynamicField::new("name".to_string(), DataType::Utf8, false),
            ],
            0,
        );

        let tonbo_cloud = Arc::new(AWSTonbo::new("Tonbo".to_string(), schema).await);
        let flight_svc = TonboFlightSvc::new(tonbo_cloud.clone());

        let r1 = DynRecord::try_new(
            vec![
                Value::Int64(1),
                Value::Int8(23),
                Value::String("Ava".into()),
            ],
            0,
        )
        .unwrap();
        tonbo_cloud.tonbo().insert(r1).await.unwrap();

        // This is an example of what the scan request looks like
        let _scan_request = ScanRequest::new(
            Bound::Included(Value::Int64(0)),
            Bound::Included(Value::Int64(1)),
            vec!["id".to_string(), "age".to_string(), "name".to_string()],
        );

        // Create mock grpc::ScanRequest for sending to the `AWSTonbo`` instance to work with
        let scan_pb = grpc::ScanRequest {
            lower: Some(grpc::BoundValue {
                kind: Some(grpc::bound_value::Kind::Inclusive(grpc::Value {
                    kind: Some(grpc::value::Kind::Int64(0)),
                })),
            }),
            upper: Some(grpc::BoundValue {
                kind: Some(grpc::bound_value::Kind::Inclusive(grpc::Value {
                    kind: Some(grpc::value::Kind::Int64(1)),
                })),
            }),
            projection: vec!["id".into(), "age".into(), "name".into()],
        };

        let mut buf = Vec::with_capacity(scan_pb.encoded_len());
        scan_pb.encode(&mut buf).unwrap();
        let ticket = Ticket { ticket: buf.into() };

        let resp = flight_svc.do_get(Request::new(ticket)).await.unwrap();
        let mut stream = resp.into_inner();

        let mut all_msgs = Vec::new();
        while let Some(msg) = stream.next().await {
            all_msgs.push(msg.unwrap());
        }

        let batches = flight_data_to_batches(&all_msgs).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_ete_aws_tonbo_empty_scan_errors() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        let tmp = TempDir::new().unwrap();
        std::env::set_var("TONBO_DB_DIR", tmp.path());

        let schema = DynSchema::new(
            &[
                DynamicField::new("id".to_string(), DataType::Int64, false),
                DynamicField::new("age".to_string(), DataType::Int8, true),
                DynamicField::new("name".to_string(), DataType::Utf8, false),
            ],
            0,
        );

        let tonbo_cloud = Arc::new(AWSTonbo::new("Tonbo".to_string(), schema).await);
        let flight_svc = TonboFlightSvc::new(tonbo_cloud.clone());

        let scan_pb = grpc::ScanRequest {
            lower: Some(grpc::BoundValue {
                kind: Some(grpc::bound_value::Kind::Inclusive(grpc::Value {
                    kind: Some(grpc::value::Kind::Int64(10)),
                })),
            }),
            upper: Some(grpc::BoundValue {
                kind: Some(grpc::bound_value::Kind::Inclusive(grpc::Value {
                    kind: Some(grpc::value::Kind::Int64(20)),
                })),
            }),
            projection: vec!["id".into()],
        };

        let mut buf = Vec::with_capacity(scan_pb.encoded_len());
        scan_pb.encode(&mut buf).unwrap();
        let ticket = Ticket { ticket: buf.into() };

        // Expect an error due to missing schema
        let resp = flight_svc.do_get(Request::new(ticket)).await;
        assert!(resp.is_err());
    }
}
