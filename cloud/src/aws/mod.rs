use std::{env, fmt::Debug, future::Future, marker::Send, pin::Pin, sync::Arc};

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use async_stream::try_stream;
use async_trait::async_trait;
use fusio::{
    path::Path as TonboPath,
    remotes::aws::{fs::AmazonS3Builder, AwsCredential},
    DynFs,
};
use tonbo::record::Schema;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::fs::create_dir_all;
use tonbo::{
     executor::tokio::TokioExecutor, parquet::errors::ParquetError, record::Record, transaction::Transaction, DbOption, Entry, Record, DB
};

use crate::{error::CloudError, ScanRequest, TonboCloud};

pub const DEFAULT_PORT: u32 = 8080;

// Temporarily using a static schema
#[derive(Record, Default,Debug)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
}

/// Every table has its own tonbo cloud instnace.
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
}

#[async_trait]
impl<R> TonboCloud<R> for AWSTonbo<R>
where
    R: Record + Send + Sync,
    <R::Schema as Schema>::Columns: Send + Sync,
{
    /// Creates new Tonbo cloud instance on S3
    async fn new(&self, name: String, schema: R::Schema) -> Self {
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
        }
    }

    // TODO: Use `DynRecord`
    fn write(&self, _records: impl ExactSizeIterator<Item = R>) {}

    async fn read<'a>(
        &'a self,
        transaction: &'a Transaction<'_, R>,
        scan: &'a ScanRequest<<R::Schema as Schema>::Key>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Entry<'a, R>, ParquetError>> + Send + 'a>>, CloudError> {
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
                    ParquetError::General("Tonbo Error".to_string())
                })?;

            while let Some(entry) = inner.next().await {
                yield entry?;
            }
        }))
    }

    fn listen(&'static self) -> impl Future<Output = std::io::Result<()>> + 'static {
        let tonbo = Arc::new(self.clone());
        let endpoint = tonbo.read_endpoint.clone();

        async move {
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::from(tonbo.clone()))
                    .route(
                    "/tables/{table_name}/scan",
                    web::post().to(scan_handler::<User>),
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

async fn scan_handler<R>(
    path: web::Path<String>,
    req: web::Json<ScanRequest<<R::Schema as Schema>::Key>>,
 data: web::Data<Arc<AWSTonbo<R>>>,
) -> impl Responder
where
    R: Record + Send + Sync + 'static ,
    <R::Schema as Schema>::Columns: Send + Sync,
{
    let table = path.into_inner();
    if table != data.name {
        return HttpResponse::NotFound().body(format!("No Tonbo instance named “{}”", table));
    }

    let tx = data.tonbo.transaction().await;
    match data.read(&tx, &req).await {
        Ok(bytes) => HttpResponse::Ok()
            .content_type("application/octet-stream")
            .body(bytes),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
