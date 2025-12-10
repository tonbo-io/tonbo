//! S3 Object Storage: serverless database on S3-compatible storage
//!
//! This example shows how to use Tonbo with S3 (or MinIO, R2, etc.)
//! The database is just a manifest and Parquet files - no server process needed.
//!
//! Required environment variables:
//!   TONBO_S3_BUCKET       - S3 bucket name
//!   TONBO_S3_ENDPOINT     - S3 endpoint URL (for MinIO/R2/LocalStack)
//!   TONBO_S3_REGION       - AWS region (e.g., "us-east-1")
//!   AWS_ACCESS_KEY_ID     - Access key
//!   AWS_SECRET_ACCESS_KEY - Secret key
//!
//! For local testing with MinIO:
//!   docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
//!   # Create bucket "tonbo-test" in MinIO console at http://localhost:9001
//!
//! Then run:
//!   TONBO_S3_BUCKET=tonbo-test \
//!   TONBO_S3_ENDPOINT=http://localhost:9000 \
//!   TONBO_S3_REGION=us-east-1 \
//!   AWS_ACCESS_KEY_ID=minioadmin \
//!   AWS_SECRET_ACCESS_KEY=minioadmin \
//!   cargo run --example 04_s3
//!
//! Run: cargo run --example 04_s3

use std::env;

use tonbo::{
    ColumnRef, Predicate, ScalarValue,
    db::{AwsCreds, DbBuilder, ObjectSpec, S3Spec},
    typed_arrow::{Record, prelude::*, schema::SchemaMeta},
};

#[derive(Record)]
struct Event {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    event_type: String,
    payload: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read S3 configuration from environment
    let bucket = env::var("TONBO_S3_BUCKET").map_err(|_| "TONBO_S3_BUCKET not set")?;
    let endpoint = env::var("TONBO_S3_ENDPOINT").ok();
    let region = env::var("TONBO_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
    let credentials = AwsCreds::from_env()?;

    // Create S3 specification
    // The "prefix" is a folder path within the bucket for this table
    let prefix = format!("tonbo-example-{}", std::process::id());
    let mut s3_spec = S3Spec::new(&bucket, &prefix, credentials);
    s3_spec.endpoint = endpoint.clone();
    s3_spec.region = Some(region.clone());
    s3_spec.sign_payload = Some(true); // Required for MinIO

    println!("Connecting to S3...");
    println!("  Bucket: {}", bucket);
    println!("  Prefix: {}", prefix);
    if let Some(ep) = &endpoint {
        println!("  Endpoint: {}", ep);
    }

    // Open database on S3
    let db = DbBuilder::from_schema(Event::schema())?
        .object_store(ObjectSpec::s3(s3_spec))?
        .open()
        .await?;

    println!("\nDatabase opened on S3!");

    // Insert data - writes go to S3
    let events = vec![
        Event {
            id: "evt-001".into(),
            event_type: "user.created".into(),
            payload: Some(r#"{"user_id": 42}"#.into()),
        },
        Event {
            id: "evt-002".into(),
            event_type: "order.placed".into(),
            payload: Some(r#"{"order_id": 123}"#.into()),
        },
        Event {
            id: "evt-003".into(),
            event_type: "user.created".into(),
            payload: None,
        },
    ];

    let mut builders = Event::new_builders(events.len());
    builders.append_rows(events);
    db.ingest(builders.finish().into_record_batch()).await?;

    println!("Inserted 3 events to S3");

    // Query from S3
    let filter = Predicate::eq(
        ColumnRef::new("event_type"),
        ScalarValue::from("user.created"),
    );
    let batches = db.scan().filter(filter).collect().await?;

    println!("\nEvents where event_type = 'user.created':");
    for batch in &batches {
        for event in batch.iter_views::<Event>()?.try_flatten()? {
            let payload = event.payload.unwrap_or("(none)");
            println!("  {} - {} | {}", event.id, event.event_type, payload);
        }
    }

    println!("\nData is stored as Parquet files on S3:");
    println!("  s3://{}/{}/", bucket, prefix);

    Ok(())
}
