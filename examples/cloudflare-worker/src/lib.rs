//! Tonbo on Cloudflare Workers example.
//!
//! Endpoints:
//! - GET  /       - Info page
//! - POST /write  - Write test data to Tonbo DB
//! - GET  /read   - Read all data from Tonbo DB (proves persistence)

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::web::WebExecutor, impls::remotes::aws::fs::AmazonS3};
use futures::StreamExt;
use tonbo::db::{AwsCreds, ObjectSpec, S3Spec, DB};
use worker::*;

#[event(start)]
fn start() {
    console_error_panic_hook::set_once();
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get("/", |_, _| Response::ok("Tonbo on Cloudflare Workers!\n\nEndpoints:\n  POST /write - Write test data\n  GET  /read  - Read all data\n  GET  /debug - Debug S3 access"))
        .post_async("/write", handle_write)
        .get_async("/read", handle_read)
        .get_async("/debug", handle_debug)
        .run(req, env)
        .await
}

async fn handle_debug(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    console_log!("GET /debug - Testing S3 access...");

    let endpoint = ctx.var("TONBO_S3_ENDPOINT")?.to_string();
    let bucket = ctx.var("TONBO_S3_BUCKET")?.to_string();
    let region = ctx.var("TONBO_S3_REGION")?.to_string();
    let access_key = ctx.secret("TONBO_S3_ACCESS_KEY")?.to_string();
    let secret_key = ctx.secret("TONBO_S3_SECRET_KEY")?.to_string();

    console_log!("Debug: endpoint={}, bucket={}", endpoint, bucket);

    // Try to list objects using fusio
    use fusio::DynFs;
    use fusio::path::Path;
    use fusio::impls::remotes::aws::fs::AmazonS3Builder;
    use fusio::impls::remotes::aws::AwsCredential;

    let credential = AwsCredential {
        key_id: access_key,
        secret_key,
        token: None,
    };

    let fs = AmazonS3Builder::new(bucket)
        .endpoint(endpoint)
        .region(region)
        .credential(credential)
        .sign_payload(true)
        .build();

    let root = Path::parse("tonbo-worker-demo").unwrap();

    let mut output = String::from("Files in S3:\n");

    match fs.list(&root).await {
        Ok(mut stream) => {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(meta) => {
                        output.push_str(&format!("  {} ({} bytes)\n", meta.path, meta.size));
                    }
                    Err(e) => {
                        output.push_str(&format!("  Error: {}\n", e));
                    }
                }
            }
        }
        Err(e) => {
            return Response::error(format!("List failed: {}", e), 500);
        }
    }

    Response::ok(output)
}

fn get_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

async fn open_db(ctx: &RouteContext<()>) -> Result<DB<AmazonS3, WebExecutor>> {
    console_log!("open_db: getting env vars...");
    let endpoint = ctx.var("TONBO_S3_ENDPOINT")?.to_string();
    let bucket = ctx.var("TONBO_S3_BUCKET")?.to_string();
    let region = ctx.var("TONBO_S3_REGION")?.to_string();
    let access_key = ctx.secret("TONBO_S3_ACCESS_KEY")?.to_string();
    let secret_key = ctx.secret("TONBO_S3_SECRET_KEY")?.to_string();

    console_log!("open_db: S3 endpoint={}", endpoint);

    console_log!("open_db: creating schema...");
    let schema = get_schema();
    let schema_cfg = tonbo::schema::SchemaBuilder::from_schema(schema)
        .primary_key("id")
        .build()
        .map_err(|e| Error::RustError(format!("schema error: {e}")))?;

    console_log!("open_db: configuring S3...");
    let credentials = AwsCreds::new(access_key, secret_key);
    let mut s3_spec = S3Spec::new(bucket, "tonbo-worker-demo", credentials);
    s3_spec.endpoint = Some(endpoint);
    s3_spec.region = Some(region);
    s3_spec.sign_payload = Some(true);

    console_log!("open_db: building DB...");
    let executor = Arc::new(WebExecutor::new());
    let db = DB::<AmazonS3, WebExecutor>::builder(schema_cfg)
        .object_store(ObjectSpec::s3(s3_spec))
        .map_err(|e| Error::RustError(format!("object_store error: {e}")))?
        .open_with_executor(executor)
        .await
        .map_err(|e| Error::RustError(format!("db open error: {e}")))?;

    console_log!("open_db: DB opened successfully!");
    Ok(db)
}

async fn handle_write(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    use tonbo::db::{ColumnRef, Predicate, ScalarValue};

    console_log!("POST /write - Opening DB...");
    let db = open_db(&ctx).await?;

    let schema = get_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])) as _,
            Arc::new(Int32Array::from(vec![100, 200])) as _,
        ],
    )
    .map_err(|e| Error::RustError(format!("batch error: {e}")))?;

    console_log!("Ingesting 2 rows...");
    db.ingest(batch)
        .await
        .map_err(|e| Error::RustError(format!("ingest error: {e}")))?;

    console_log!("Write complete! Now reading back in same request...");

    // Read back one row to verify (keeping under subrequest limit)
    let pred = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("alice"));
    let read_result = match db.scan().filter(pred).limit(1).collect().await {
        Ok(batches) => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            if total_rows > 0 {
                let batch = &batches[0];
                let values = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
                format!("alice = {}", values.value(0))
            } else {
                "alice = (not found)".to_string()
            }
        }
        Err(e) => format!("read error: {}", e),
    };

    Response::ok(format!(
        "Wrote 2 rows (alice=100, bob=200) to Tonbo DB.\n\nRead back: {}\n\n\
         Note: Cloudflare Workers have subrequest limits that restrict\n\
         how many operations can be done per request.",
        read_result
    ))
}

async fn handle_read(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    use tonbo::db::{ColumnRef, Predicate, ScalarValue};

    console_log!("GET /read - Opening DB...");
    let db = match open_db(&ctx).await {
        Ok(db) => {
            console_log!("DB opened successfully");
            db
        }
        Err(e) => {
            console_log!("DB open failed: {:?}", e);
            return Response::error(format!("DB open failed: {e}"), 500);
        }
    };

    let mut output = String::from("Read from Tonbo DB:\n\n");

    // Query specific keys using filtered scan
    for key_str in ["alice", "bob"] {
        console_log!("Querying key: {}", key_str);
        let pred = Predicate::eq(ColumnRef::new("id"), ScalarValue::from(key_str));
        match db.scan().filter(pred).limit(1).collect().await {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if total_rows > 0 {
                    let batch = &batches[0];
                    let ids = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    let values = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
                    output.push_str(&format!("  {} = {}\n", ids.value(0), values.value(0)));
                } else {
                    output.push_str(&format!("  {} = (not found)\n", key_str));
                }
            }
            Err(e) => {
                console_log!("Query failed for {}: {:?}", key_str, e);
                output.push_str(&format!("  {} = (error: {})\n", key_str, e));
            }
        }
    }

    console_log!("Read complete!");
    Response::ok(output)
}
