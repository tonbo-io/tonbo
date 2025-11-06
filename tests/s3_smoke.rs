#![cfg(feature = "s3-smoke")]
//! Integration smoke test that exercises the S3 object-store plumbing against a
//! live endpoint. Enable via `cargo test --features s3-smoke --test s3_smoke`
//! (requires the TONBO_S3_* environment variables).

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use fusio::executor::tokio::TokioExecutor;
use tonbo::{
    db::{DB, DynMode},
    mode::DynModeConfig,
    wal::{WalConfig, WalExt, WalSyncPolicy, frame::WalEvent, replay::Replayer},
};

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

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id")?;

    let label = format!(
        "smoke-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(config)
        .on_object_store(|os| {
            os.provider("s3")
                .endpoint(endpoint)
                .bucket(bucket)
                .root(label)
                .region(region)
                .access_key(access)
                .secret_key(secret)
                .sign_payload(true);
            if let Some(token) = session_token.as_deref() {
                os.session_token(token.to_string());
            }
        })
        .configure_wal(|cfg| {
            cfg.sync = WalSyncPolicy::Always;
            cfg.retention_bytes = Some(1 << 20);
        })
        .build()
        .map_err(|err| format!("failed to build S3-backed DB: {err}"))?;

    let wal_cfg: WalConfig = db
        .wal_config()
        .cloned()
        .ok_or("builder should seed wal config")?;
    db.enable_wal(wal_cfg.clone())?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])) as _,
            Arc::new(Int32Array::from(vec![10, 20])) as _,
        ],
    )?;

    db.ingest(batch).await?;
    db.disable_wal()?;

    // Verify we can stream the newly written WAL frames back from the backend.
    let replayer = Replayer::new(wal_cfg.clone());
    let events = replayer
        .scan()
        .await
        .map_err(|err| format!("failed to replay wal from s3: {err}"))?;

    let mut append_verified = false;
    let mut commit_seen = false;

    for event in events {
        match event {
            WalEvent::DynAppend { payload, .. } => {
                let expected_rows = payload.batch.num_rows();

                if let Some(hint) = payload.commit_ts_hint {
                    assert!(hint.get() > 0, "commit ts hint should be positive");
                }

                let commit_column = payload
                    .commit_ts_column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("commit column missing");
                assert_eq!(commit_column.len(), expected_rows);

                let tombstone_column = payload
                    .tombstones
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("tombstone column missing");
                assert_eq!(tombstone_column.len(), expected_rows);
                assert!(
                    tombstone_column
                        .iter()
                        .all(|val| matches!(val, Some(false)))
                );

                let ids = payload
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id column missing");
                assert_eq!(ids.len(), 2);
                assert_eq!(ids.value(0), "alice");
                assert_eq!(ids.value(1), "bob");

                let values = payload
                    .batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("value column missing");
                assert_eq!(values.len(), 2);
                assert_eq!(values.value(0), 10);
                assert_eq!(values.value(1), 20);

                append_verified = true;
            }
            WalEvent::TxnCommit { .. } => {
                commit_seen = true;
            }
            _ => {}
        }
    }

    if !append_verified {
        return Err("expected wal append event".into());
    }

    if !commit_seen {
        return Err("expected wal commit event".into());
    }

    Ok(())
}
