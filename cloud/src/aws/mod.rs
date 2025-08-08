use std::{env, fmt::Debug, future::Future, marker::Send, ops::Bound, net::SocketAddr, pin::Pin, sync::Arc};

use arrow::ipc::RecordBatchBuilder;
use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient};
use async_stream::try_stream;
use async_trait::async_trait;
use fusio::{
    path::Path as TonboPath,
    remotes::aws::{fs::AmazonS3Builder, AwsCredential},
    DynFs,
};
use futures::TryStreamExt;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::fs::create_dir_all;
use tonbo::{
    arrow::array::RecordBatch,
    executor::tokio::TokioExecutor,
    parquet::errors::ParquetError,
    record::{DynRecord, Record, Schema},
    transaction::Transaction,
    DbOption, Entry, Record, DB,
};

use tonic::transport::{Channel, Server};

use crate::{error::CloudError, gen::tonbo_cloud::aws_tonbo_server::AwsTonboServer, ScanRequest, TonboCloud};

pub const DEFAULT_PORT: u32 = 8080;

// Temporarily using a static schema
#[derive(Record, Default, Debug)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
}

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
    read_endpoint: String,
    // Endpoint for writes
    write_endpoint: String,
    buffered_data: Option<RecordBatch>,
}

impl AWSTonbo
{
    fn read_endpoint(self) -> String {
        self.read_endpoint
    }

    // todo: Separate the data
    // Returns number of rows and row size
    async fn parquet_metadata<'a>(
        &self,
        transaction: &'a Transaction<'_, DynRecord>,
        scan: &'a ScanRequest,
    ) -> (i64, i32) {
        let mut row_count = 0;
        let mut row_size = 0;

        let scan_req = scan.clone();
        let mut inner = self
            .read(&transaction, &scan_req)
            .await
            .map_err(ErrorInternalServerError)
            .unwrap();

        while let Some(res) = inner.next().await {
            match res {
                Ok(Entry::RecordBatch(batch_entry)) => {
                    let batch = batch_entry.record_batch();
                    row_count += batch.num_rows() as i64;
                    row_size =
                        (batch.get_array_memory_size() as i64 / batch.num_rows() as i64) as i32;
                }
                Ok(Entry::Mutable(entry)) => {
                    // if let Some(batch) = entry.value() {
                    //     let batch_builder = RecordBatchBuilder
                    // }
                }
                Ok(_) => todo!(),
                Err(e) => todo!(),
            }
        }

        (row_count, row_size)
    }
}

#[async_trait]
impl TonboCloud for AWSTonbo
{
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

        let tonbo: DB<DynRecord, TokioExecutor> = DB::new(options, TokioExecutor::current(), schema)
            .await
            .unwrap();

        let host = std::env::var("DEFAULT_HOST").unwrap();

        let read_endpoint = format!("http://{}:{}/tables/{}/scan", host, DEFAULT_PORT, name);
        let write_endpoint = format!("http://{}:{}/tables/{}/write", host, DEFAULT_PORT, name);

        Self {
            name,
            tonbo,
            s3_fs,
            read_endpoint,
            write_endpoint,
            buffered_data: None,
        }
    }

    // TODO: Use `DynRecord`
    fn write(&self, _records: impl ExactSizeIterator<Item = DynRecord>) {}

    async fn read<'a>(
        &'a self,
        transaction: &'a Transaction<'_, DynRecord>,
        scan: &'a ScanRequest,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Entry<'a, DynRecord>, ParquetError>> + Send + 'a>>,
        CloudError
    > {
        // Ok(Box::pin(try_stream! {
        //         let projection: Vec<&str> = scan.projection.iter().map(String::as_str).collect();

        //     let mut inner = transaction
        //         .scan((
        //             scan.bounds.0.as_ref(),
        //             scan.bounds.1.as_ref(),
        //         ))
        //         .projection(&projection)
        //         .take()
        //         .await
        //         .map_err(|_db_err| {
        //             ParquetError::General("Error occured while creating transaction.".to_string())
        //         })?;

        //     while let Some(entry) = inner.next().await {
        //         yield entry?;
        //     }
        // }))
    }

    async fn listen(self, addr: SocketAddr) -> impl Future<Output = std::io::Result<()>> + 'static {
        Server::builder()
            .add_service(AwsTonboServer::new(self))
            .serve(addr)
            .await
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

// async fn handle_scan(
//     path: web::Path<String>,
//     req: web::Json<ScanRequest>,
//     data: web::Data<Arc<AWSTonbo>>,
// ) -> impl Responder
// {
//     let table = path.into_inner();
//     if table != data.name {
        
//     }

//     let channel = Channel::from_static("http://127.0.0.1:5005")
//         .connect()
//         .await
//         .unwrap();
//     let mut client = FlightClient::new(channel);

//     let batch_stream = async_stream::stream! {
//     let db = data.clone();

//     let transaction = db.tonbo.transaction().await;
//     let mut inner = db
//         .read(&transaction, &req)
//         .await
//         .map_err(ErrorInternalServerError)
//         .unwrap();
//     while let Some(res) = inner.next().await {
//             match res {
//                 Ok(Entry::RecordBatch(batch_entry)) => {
//                     let arrow_batch = batch_entry.record_batch();
//                     yield Ok(arrow_batch);
//                 }
//                 Ok(_) => todo!(),
//                 Err(e) => todo!(),
//             }
//         }
//     };

//     let flight_data_stream = FlightDataEncoderBuilder::new().build(batch_stream);

//     let response_stream = client.do_put(flight_data_stream).await.unwrap();
//     let results: Vec<_> = response_stream.try_collect().await.unwrap();

//     HttpResponse::Ok().into()
// }

#[cfg(test)]
mod tests {}
