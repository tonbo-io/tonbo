#![cfg(feature = "s3-smoke")]
//! Integration smoke test that exercises the S3 object-store plumbing against a
//! live endpoint. Enable via `cargo test --features s3-smoke --test s3_smoke`
//! (requires the TONBO_S3_* environment variables).

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{executor::tokio::TokioExecutor, impls::remotes::aws::fs::AmazonS3};
use tonbo::db::{AwsCreds, DB, ObjectSpec, S3Spec, WalSyncPolicy};

#[path = "common/mod.rs"]
mod common;

use common::config_with_pk;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = match std::env::var("TONBO_S3_ENDPOINT") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping s3_smoke – TONBO_S3_ENDPOINT missing");
            return Ok(());
        }
    };
    let bucket = match std::env::var("TONBO_S3_BUCKET") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping s3_smoke – TONBO_S3_BUCKET missing");
            return Ok(());
        }
    };
    let region = match std::env::var("TONBO_S3_REGION") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping s3_smoke – TONBO_S3_REGION missing");
            return Ok(());
        }
    };
    let access = match std::env::var("TONBO_S3_ACCESS_KEY") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping s3_smoke – TONBO_S3_ACCESS_KEY missing");
            return Ok(());
        }
    };
    let secret = match std::env::var("TONBO_S3_SECRET_KEY") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping s3_smoke – TONBO_S3_SECRET_KEY missing");
            return Ok(());
        }
    };
    let session_token = std::env::var("TONBO_S3_SESSION_TOKEN").ok();

    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();

    let label_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock before unix epoch: {err}"))?
        .as_millis();
    let label = format!("smoke-{label_millis}");

    let credentials = match session_token {
        Some(token) => AwsCreds::with_session_token(access, secret, token),
        None => AwsCreds::new(access, secret),
    };

    let mut s3 = S3Spec::new(bucket.clone(), label.clone(), credentials);
    s3.endpoint = Some(endpoint);
    s3.region = Some(region);
    s3.sign_payload = Some(true);

    let db: DB<AmazonS3, TokioExecutor> = DB::<AmazonS3, TokioExecutor>::builder(config)
        .object_store(ObjectSpec::s3(s3))
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_sync_policy(WalSyncPolicy::Always)
        .wal_retention_bytes(Some(1 << 20))
        .build()
        .await
        .map_err(|err| format!("failed to build S3-backed DB: {err}"))?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])) as _,
            Arc::new(Int32Array::from(vec![10, 20])) as _,
        ],
    )?;

    db.ingest(batch).await?;

    // Verify we can read the data back via scan
    let results: Vec<RecordBatch> = db.scan().collect().await?;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    if total_rows < 2 {
        return Err(format!("expected at least 2 rows, got {total_rows}").into());
    }

    // Verify the data content
    let mut found_alice = false;
    let mut found_bob = false;

    for batch in &results {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column should be StringArray");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value column should be Int32Array");

        for i in 0..batch.num_rows() {
            match ids.value(i) {
                "alice" => {
                    assert_eq!(values.value(i), 10, "alice should have value 10");
                    found_alice = true;
                }
                "bob" => {
                    assert_eq!(values.value(i), 20, "bob should have value 20");
                    found_bob = true;
                }
                other => {
                    return Err(format!("unexpected id: {other}").into());
                }
            }
        }
    }

    if !found_alice {
        return Err("expected to find alice in results".into());
    }
    if !found_bob {
        return Err("expected to find bob in results".into());
    }

    Ok(())
}
