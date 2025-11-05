#!/usr/bin/env bash
# S3 integration smoke harness. Run from the repo root via `./tests/s3_smoke.sh`.
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

cleanup() {
  if docker ps --format '{{.Names}}' | grep -q "^${LOCALSTACK_CONTAINER}$"; then
    docker rm -f "${LOCALSTACK_CONTAINER}" >/dev/null 2>&1 || true
  fi
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
echo "Running cargo test --features s3-smoke --test s3_smoke"
if cargo test --features s3-smoke --test s3_smoke; then
  echo "Smoke test complete."
  echo "-- LocalStack logs (tail after success) --"
  docker logs --tail 20 "${LOCALSTACK_CONTAINER}" || true
  exit 0
else
  status=$?
  echo "Smoke test failed (status $status)."
  echo "-- LocalStack logs (tail) --"
  docker logs --tail 40 "${LOCALSTACK_CONTAINER}" || true
  exit $status
fi
