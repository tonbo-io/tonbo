#!/usr/bin/env bash
# Bootstrap LocalStack for S3-backed tests and export TONBO_S3_* env vars.
# Source this script to set env in the current shell:
#   source tests/s3_localstack_env.sh
#
# Variables (override as needed):
#   LOCALSTACK_CONTAINER (default: tonbo-localstack-e2e)
#   LOCALSTACK_PORT      (default: 4566)
#   AWS_ACCESS_KEY_ID    (default: test)
#   AWS_SECRET_ACCESS_KEY(default: test)
#   AWS_REGION           (default: us-east-1)
#   BUCKET_NAME          (default: tonbo-e2e)

set -euo pipefail

LOCALSTACK_CONTAINER=${LOCALSTACK_CONTAINER:-tonbo-localstack-e2e}
LOCALSTACK_PORT=${LOCALSTACK_PORT:-4566}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}
AWS_REGION=${AWS_REGION:-us-east-1}
BUCKET_NAME=${BUCKET_NAME:-tonbo-e2e}

tonbo_localstack_started_by_script=0
tonbo_localstack_available=0

ensure_localstack() {
  if command -v docker >/dev/null 2>&1; then
    if docker ps --format '{{.Names}}' | grep -q "^${LOCALSTACK_CONTAINER}\$"; then
      tonbo_localstack_available=1
    else
      if docker ps -a --format '{{.Names}}' | grep -q "^${LOCALSTACK_CONTAINER}\$"; then
        docker rm -f "${LOCALSTACK_CONTAINER}" >/dev/null 2>&1 || true
      fi

      echo "Starting LocalStack (${LOCALSTACK_CONTAINER}) on port ${LOCALSTACK_PORT}..."
      docker run -d --name "${LOCALSTACK_CONTAINER}" \
        -e SERVICES="s3" \
        -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
        -e AWS_DEFAULT_REGION="${AWS_REGION}" \
        -p "${LOCALSTACK_PORT}:4566" \
        localstack/localstack:latest >/dev/null
      tonbo_localstack_started_by_script=1
      tonbo_localstack_available=1

      echo -n "Waiting for LocalStack to become ready"
      until docker exec "${LOCALSTACK_CONTAINER}" awslocal s3api list-buckets >/dev/null 2>&1; do
        sleep 1
        printf '.'
      done
      echo
    fi

    # Always ensure bucket exists (idempotent)
    echo "Ensuring S3 bucket ${BUCKET_NAME} exists..."
    docker exec "${LOCALSTACK_CONTAINER}" awslocal s3api create-bucket --bucket "${BUCKET_NAME}" >/dev/null 2>&1 || true
  else
    echo "docker is required to start LocalStack; skipping LocalStack startup" >&2
  fi
}

export_s3_env() {
  if [ "${tonbo_localstack_available}" -ne 1 ]; then
    return 1
  fi
  export TONBO_S3_ENDPOINT="http://localhost:${LOCALSTACK_PORT}"
  export TONBO_S3_BUCKET="${BUCKET_NAME}"
  export TONBO_S3_REGION="${AWS_REGION}"
  export TONBO_S3_ACCESS_KEY="${AWS_ACCESS_KEY_ID}"
  export TONBO_S3_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}"
  export TONBO_LOCALSTACK_CONTAINER="${LOCALSTACK_CONTAINER}"
  export TONBO_LOCALSTACK_STARTED_BY_SCRIPT="${tonbo_localstack_started_by_script}"
}

# If executed directly, start LocalStack and print exports for convenience.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  ensure_localstack
  if export_s3_env; then
    cat <<EOF
export TONBO_S3_ENDPOINT=${TONBO_S3_ENDPOINT}
export TONBO_S3_BUCKET=${TONBO_S3_BUCKET}
export TONBO_S3_REGION=${TONBO_S3_REGION}
export TONBO_S3_ACCESS_KEY=${TONBO_S3_ACCESS_KEY}
export TONBO_S3_SECRET_KEY=${TONBO_S3_SECRET_KEY}
export TONBO_LOCALSTACK_CONTAINER=${TONBO_LOCALSTACK_CONTAINER}
export TONBO_LOCALSTACK_STARTED_BY_SCRIPT=${TONBO_LOCALSTACK_STARTED_BY_SCRIPT}
EOF
  else
    echo "LocalStack not available; TONBO_S3_* not exported." >&2
    exit 1
  fi
else
  ensure_localstack
  export_s3_env || true
fi
