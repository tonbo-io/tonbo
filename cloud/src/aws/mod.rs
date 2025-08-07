use std::{env, fmt::Debug, future::Future, marker::Send, pin::Pin, sync::Arc};

use actix_web::{
    error::ErrorInternalServerError,
    web::{self, Bytes},
    App, HttpResponse, HttpServer, Responder,
};
use arrow_flight::{encode::FlightDataEncoderBuilder, error::FlightError, flight_service_client::FlightServiceClient, FlightClient};
use async_stream::try_stream;
use async_trait::async_trait;
use fusio::{
    path::Path as TonboPath,
    remotes::aws::{fs::AmazonS3Builder, AwsCredential},
    DynFs,
};
use futures::{channel::mpsc, stream::BoxStream, TryStreamExt};
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::fs::create_dir_all;
use tonbo::{
    arrow::{array::RecordBatch, ipc::writer::StreamWriter},
    executor::tokio::TokioExecutor,
    parquet::errors::ParquetError,
    record::{Record, Schema},
    transaction::Transaction,
    DbOption, Entry, Record, DB,
};
use tonic::transport::Channel;

use crate::{error::CloudError, ScanRequest, TonboCloud};

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
pub struct AWSTonbo<R>
where
    R: Record,
{
    // TODO: Add Tonbo DB instance
    // Name of the Tonbo cloud instance
    name: String,
    // Local Tonbo instance
    tonbo: DB<R, TokioExecutor>,
    // Remote file system
    s3_fs: Arc<dyn DynFs>,
    // Endpoint for read requests (scans)
    read_endpoint: String,
    // Endpoint for writes
    write_endpoint: String,
    buffered_data: Option<RecordBatch>,
}

#[async_trait]
impl<R> TonboCloud<R> for AWSTonbo<R>
where
    R: Record + Send + Sync,
    <R::Schema as Schema>::Columns: Send + Sync,
{
    /// Creates new Tonbo cloud instance on S3
    async fn new(name: String, schema: R::Schema) -> Self {
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

        let tonbo: DB<R, TokioExecutor> = DB::new(options, TokioExecutor::current(), schema)
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
    fn write(&self, _records: impl ExactSizeIterator<Item = R>) {}

    // Returns the number of rows and row size (in bytes)
    // async fn prune_metadata<'a>(
    //     &'a self,
    //     transaction: &'a Transaction<'_, R>,
    //     scan: &'a ScanRequest<<R::Schema as Schema>::Key>,
    // ) -> Result<(i64, i32), CloudError> {
    //     Ok()
    // }

    async fn read<'a>(
        &'a self,
        transaction: &'a Transaction<'_, R>,
        scan: &'a ScanRequest<<R::Schema as Schema>::Key>,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Entry<'a, R>, ParquetError>> + Send + 'a>>,
        CloudError,
    > {
        Ok(Box::pin(try_stream! {
                let projection: Vec<&str> = scan.projection.iter().map(String::as_str).collect();

            let mut inner = transaction
                .scan((
                    scan.bounds.0.as_ref(),
                    scan.bounds.1.as_ref(),
                ))
                .projection(&projection)
                .take()
                .await
                .map_err(|_db_err| {
                    ParquetError::General("Error occured while creating transaction.".to_string())
                })?;

            while let Some(entry) = inner.next().await {
                yield entry?;
            }
        }))
    }

    fn listen(&'static self) -> impl Future<Output = std::io::Result<()>> + 'static {
        let tonbo = Arc::new(self);
        let endpoint = tonbo.read_endpoint.clone();

        async move {
            HttpServer::new(move || {
                App::new().app_data(web::Data::from(tonbo.clone())).route(
                    "/tables/{table_name}/scan",
                    web::post().to(handle_scan::<User>),
                )
            })
            .bind(endpoint)?
            .run()
            .await
        }
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

async fn handle_scan<R>(
    path: web::Path<String>,
    req: web::Json<ScanRequest<<R::Schema as Schema>::Key>>,
    data: web::Data<Arc<AWSTonbo<R>>>,
) -> impl Responder
where
    R: Record + Send + Sync + 'static,
    <R::Schema as Schema>::Columns: Send + Sync,
{
    let table = path.into_inner();
    if table != data.name {
        return HttpResponse::NotFound().body(format!("No Tonbo instance named “{}”", table));
    }

    let channel = Channel::from_static("http://127.0.0.1:5005")  
        .connect()  
        .await  
        .unwrap(); 
    let mut client = FlightClient::new(channel); 
    

    let batch_stream  = async_stream::stream! {  
        let tonbo = data.clone();
    let scan_req = req.clone();

    let transaction = tonbo.tonbo.transaction().await;
    let mut inner = tonbo
        .read(&transaction, &scan_req)
        .await
        .map_err(ErrorInternalServerError)
        .unwrap();
    while let Some(res) = inner.next().await {  
            match res {  
                Ok(Entry::RecordBatch(batch_entry)) => {  
                    let arrow_batch = batch_entry.record_batch();  
                    yield Ok(arrow_batch);  
                }  
                Ok(_) => todo!(),  
                Err(e) => todo!(),  
            }  
        }  
    };  

    let flight_data_stream = FlightDataEncoderBuilder::new()  
      .build(batch_stream);  

    let response_stream = client.do_put(flight_data_stream).await.unwrap();  
    let results: Vec<_> = response_stream.try_collect().await.unwrap();  

    HttpResponse::Ok().into()
}
