#!/usr/bin/env bash
set -euo pipefail

command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 1; }
command -v cargo >/dev/null 2>&1 || { echo "cargo is required" >&2; exit 1; }

LOCALSTACK_CONTAINER=${LOCALSTACK_CONTAINER:-tonbo-localstack-smoke}
LOCALSTACK_PORT=${LOCALSTACK_PORT:-4566}
LOCALSTACK_IMAGE=${LOCALSTACK_IMAGE:-localstack/localstack:latest}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}
AWS_REGION=${AWS_REGION:-us-east-1}
BUCKET_NAME=${BUCKET_NAME:-tonbo-smoke}
EXAMPLE_NAME="__s3_smoke_tmp"
EXAMPLE_PATH="examples/${EXAMPLE_NAME}.rs"

cleanup() {
  if docker ps --format '{{.Names}}' | grep -q "^${LOCALSTACK_CONTAINER}$"; then
    docker rm -f "${LOCALSTACK_CONTAINER}" >/dev/null 2>&1 || true
  fi
  rm -f "${EXAMPLE_PATH}"
}
trap cleanup EXIT

cleanup

echo "Starting LocalStack (${LOCALSTACK_CONTAINER})..."
docker run -d --name "${LOCALSTACK_CONTAINER}" \
  -e SERVICES="s3" \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e AWS_DEFAULT_REGION="${AWS_REGION}" \
  -p "${LOCALSTACK_PORT}:4566" \
  "${LOCALSTACK_IMAGE}" >/dev/null

echo -n "Waiting for LocalStack to become ready"
until docker exec "${LOCALSTACK_CONTAINER}" awslocal s3api list-buckets >/dev/null 2>&1; do
  sleep 1
  printf '.'
done
echo

echo "Provisioning S3 bucket ${BUCKET_NAME}..."
docker exec "${LOCALSTACK_CONTAINER}" awslocal s3api create-bucket --bucket "${BUCKET_NAME}" >/dev/null 2>&1 || true

env_vars=(
  "TONBO_S3_ENDPOINT=http://localhost:${LOCALSTACK_PORT}"
  "TONBO_S3_BUCKET=${BUCKET_NAME}"
  "TONBO_S3_REGION=${AWS_REGION}"
  "TONBO_S3_ACCESS_KEY=${AWS_ACCESS_KEY_ID}"
  "TONBO_S3_SECRET_KEY=${AWS_SECRET_ACCESS_KEY}"
)

export "${env_vars[@]}"

cat > "${EXAMPLE_PATH}" <<'RUST'
use std::{error::Error, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::executor::tokio::TokioExecutor;
use tonbo::{
    db::{DB, DynMode},
    mode::DynModeConfig,
    wal::{WalConfig, WalSyncPolicy, WalExt},
};

#[derive(Debug, thiserror::Error)]
enum SmokeError {
    #[error(transparent)]
    Wal(#[from] tonbo::wal::WalError),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("{0}")]
    Message(&'static str),
}

fn dump_error(mut err: &dyn Error) {
    while let Some(source) = err.source() {
        eprintln!("  caused by: {source:?}");
        err = source;
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), SmokeError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id")
        .map_err(|_| SmokeError::Message("key extraction failed"))?;

    let endpoint = std::env::var("TONBO_S3_ENDPOINT").map_err(|_| SmokeError::Message("missing endpoint"))?;
    let bucket = std::env::var("TONBO_S3_BUCKET").map_err(|_| SmokeError::Message("missing bucket"))?;
    let region = std::env::var("TONBO_S3_REGION").map_err(|_| SmokeError::Message("missing region"))?;
    let access = std::env::var("TONBO_S3_ACCESS_KEY").map_err(|_| SmokeError::Message("missing access key"))?;
    let secret = std::env::var("TONBO_S3_SECRET_KEY").map_err(|_| SmokeError::Message("missing secret key"))?;

    let label = format!(
        "smoke-{}",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
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
                .sign_payload(true)
                .checksum(false);
        })
        .configure_wal(|cfg| {
            cfg.sync = WalSyncPolicy::Always;
            cfg.retention_bytes = Some(1 << 20);
        })
        .build()
        .map_err(|e| {
            eprintln!("builder error: {e:?}");
            SmokeError::Message("failed to build DB")
        })?;

    let wal_cfg: WalConfig = db
        .wal_config()
        .cloned()
        .ok_or(SmokeError::Message("builder should seed wal config"))?;
    eprintln!("wal dir: {:?}", wal_cfg.dir);
    db.enable_wal(wal_cfg.clone())?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])) as _,
            Arc::new(Int32Array::from(vec![10, 20])) as _,
        ],
    )?;

    eprintln!("starting ingest");
    match db.ingest(batch).await {
        Ok(()) => {
            println!("SMOKE_OK");
            db.disable_wal().ok();
            Ok(())
        }
        Err(err) => {
            eprintln!("INGEST ERROR: {err:#?}");
            dump_error(&err);
            match db.disable_wal() {
                Ok(()) => eprintln!("disable wal succeeded"),
                Err(dis_err) => {
                    eprintln!("disable wal error: {dis_err:?}");
                    dump_error(&dis_err);
                }
            }
            Err(SmokeError::Message("ingest failed"))
        }
    }
}
RUST

cargo run --example "${EXAMPLE_NAME}"
status=$?
if [ $status -ne 0 ]; then
  echo "-- LocalStack logs (tail) --"
  docker logs --tail 40 "${LOCALSTACK_CONTAINER}" || true
else
  echo "-- LocalStack logs (tail after success) --"
  docker logs --tail 20 "${LOCALSTACK_CONTAINER}" || true
fi

echo "Smoke test complete."
exit $status
