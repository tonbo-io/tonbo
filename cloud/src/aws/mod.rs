use std::{env, future::Future, marker::Send, pin::Pin, sync::Arc};

use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use async_stream::try_stream;
use async_trait::async_trait;
use fusio::{
    path::Path as TonboPath,
    remotes::aws::{fs::AmazonS3Builder, AwsCredential},
    DynFs,
};
use futures_core::Stream;
use tokio::fs::create_dir_all;
use tonbo::{
    executor::{tokio::TokioExecutor, Executor},
    parquet::errors::ParquetError,
    record::{Record, Schema},
    DbOption, Entry, Record, DB,
};

use crate::{ScanRequest, TonboCloud};

pub const DEFAULT_PORT: u32 = 8080;

// Temporarily using a static schema
#[derive(Record, Debug)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
}

/// Every table has its own tonbo cloud instnace.
pub struct AWSTonbo<R, E>
where
    R: Record,
    E: Executor,
{
    // TODO: Add Tonbo DB instance
    // Name of the Tonbo cloud instance
    name: String,
    // Local Tonbo instance
    tonbo: DB<R, E>,
    // Remote file system
    s3_fs: Arc<dyn DynFs>,
    // Endpoint for read requests (scans)
    read_endpoint: String,
    // Endpoint for writes
    write_endpoint: String,
}

#[async_trait]
impl<R, E> TonboCloud<R, E> for AWSTonbo<R, E>
where
    R: Record + Send + Sync,
    <R::Schema as Schema>::Columns: Send + Sync,
    E: Executor + Send + Sync + 'static,
{
    /// Creates new Tonbo cloud instance on S3
    async fn new(&self, name: String) {
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        let s3: Arc<dyn DynFs> = Arc::new(
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
            &UserSchema,
        );

        let db = DB::new(options, TokioExecutor::current(), UserSchema)
            .await
            .unwrap();

        let host = std::env::var("DEFAULT_HOST").unwrap();

        self.read_endpoint = format!("http://{}:{}/tables/{}/scan", host, DEFAULT_PORT, name);
        self.read_endpoint = format!("http://{}:{}/tables/{}/write", host, DEFAULT_PORT, name);

        let _ = tokio::spawn(self.listen());
    }

    // TODO: Use `DynRecord`
    fn write(&self, records: impl ExactSizeIterator<Item = R>) {}

    fn read<'a>(
        &'a self,
        scan: ScanRequest<<R::Schema as Schema>::Key>,
    ) -> Pin<Box<dyn Stream<Item = Result<Entry<'a, R>, ParquetError>> + Send + 'a>> {
        Box::pin(try_stream! {
            // 1) await the async take() builder
            let mut inner: _ = self
                .tonbo
                .transaction()
                .await
                .scan((scan.bounds.0.as_ref(), scan.bounds.1.as_ref()))
                .projection(&["email"])
                .take()
                .await?;

            while let Some(item) = inner.next().await {
                // `item` is Result<Entry<'a,R>, ParquetError>
                yield item?;
            }
        })
    }

    // TODO: Set up API for listening to write requests to Tonbo
    fn listen<'a>(&'a self) -> Pin<Box<(dyn Future<Output = std::io::Result<()>> + Send + 'a)>> {
        Box::pin(async move {
            let shared = Arc::new(self.clone());
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::from(shared.clone()))
                    .service(scan_handler)
            })
            .bind(self.read_endpoint)?
            .run()
            .await
        })
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

#[post("/tables/{table_name}/scan")]
async fn scan_handler<R, E>(
    path: web::Path<String>,
    req: web::Json<ScanRequest<<R::Schema as Schema>::Key>, data: web::Data<Arc<AWSTonbo<R, E>>>>,
) -> impl Responder
where
    R: Record + Send + Sync + 'static,
    E: Executor + Send + Sync + 'static,
{
    let table = path.into_inner();
    if table != data.name {
        return HttpResponse::NotFound().body(format!("No Tonbo instance named “{}”", table));
    }

    match data.read(req).await {
        Ok(bytes) => HttpResponse::Ok()
            .content_type("application/octet-stream")
            .body(bytes),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
