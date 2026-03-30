#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

BENCH_NAME="compaction_local"
SCENARIO="${TONBO_MATRIX_SCENARIO:-read_compaction_quiesced}"
SCALES_CSV="${TONBO_MATRIX_SCALES:-1,4,8}"
CELLS_CSV="${TONBO_MATRIX_CELLS:-dev_local,dev_s3,dev_s3express,ec2_s3,ec2_s3express}"
REPORT_BASENAME="${TONBO_MATRIX_REPORT_BASENAME:-matrix-$(date +%Y%m%d-%H%M%S)}"
REPORT_DIR="${TONBO_MATRIX_REPORT_DIR:-target/tonbo-bench/reports}"
REPORT_PATH="${REPORT_DIR}/${REPORT_BASENAME}.md"
ARTIFACT_DIR="target/tonbo-bench"

DEFAULT_ROWS_PER_BATCH="${TONBO_MATRIX_ROWS_PER_BATCH:-64}"
DEFAULT_INGEST_BATCHES="${TONBO_MATRIX_INGEST_BATCHES:-160}"
DEFAULT_ARTIFACT_ITERATIONS="${TONBO_MATRIX_ARTIFACT_ITERATIONS:-12}"
DEFAULT_CRITERION_SAMPLE_SIZE="${TONBO_MATRIX_CRITERION_SAMPLE_SIZE:-10}"
RUN_MODE="${TONBO_MATRIX_RUN_MODE:-execute}"

mkdir -p "${REPORT_DIR}"
mkdir -p "${ARTIFACT_DIR}"

IFS=',' read -r -a SCALES <<< "${SCALES_CSV}"
IFS=',' read -r -a CELLS <<< "${CELLS_CSV}"

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "${value}"
}

require_var() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        echo "missing required environment variable: ${name}" >&2
        exit 1
    fi
}

require_any_var() {
    local found=0
    local name
    for name in "$@"; do
        if [[ -n "${!name:-}" ]]; then
            found=1
            break
        fi
    done
    if [[ "${found}" -eq 0 ]]; then
        echo "missing required environment variable; expected one of: $*" >&2
        exit 1
    fi
}

append_report() {
    printf '%s\n' "$1" >> "${REPORT_PATH}"
}

join_csv_to_bullets() {
    local input="$1"
    local item
    IFS=',' read -r -a items <<< "${input}"
    for item in "${items[@]}"; do
        item="$(trim "${item}")"
        [[ -z "${item}" ]] && continue
        append_report "- \`${item}\`"
    done
}

resolve_runner_env() {
    local cell="$1"
    case "${cell}" in
        dev_*)
            printf '%s' "${TONBO_MATRIX_DEV_RUNNER_ENV:-dev}"
            ;;
        ec2_*)
            printf '%s' "${TONBO_MATRIX_EC2_RUNNER_ENV:-ec2}"
            ;;
        *)
            printf '%s' "${TONBO_MATRIX_RUNNER_ENV:-unknown}"
            ;;
    esac
}

resolve_runner_region() {
    local cell="$1"
    case "${cell}" in
        dev_*)
            printf '%s' "${TONBO_MATRIX_DEV_RUNNER_REGION:-${TONBO_BENCH_RUNNER_REGION:-}}"
            ;;
        ec2_*)
            printf '%s' "${TONBO_MATRIX_EC2_RUNNER_REGION:-${TONBO_BENCH_RUNNER_REGION:-}}"
            ;;
        *)
            printf '%s' "${TONBO_BENCH_RUNNER_REGION:-}"
            ;;
    esac
}

resolve_runner_az() {
    local cell="$1"
    case "${cell}" in
        dev_*)
            printf '%s' "${TONBO_MATRIX_DEV_RUNNER_AZ:-${TONBO_BENCH_RUNNER_AZ:-}}"
            ;;
        ec2_*)
            printf '%s' "${TONBO_MATRIX_EC2_RUNNER_AZ:-${TONBO_BENCH_RUNNER_AZ:-}}"
            ;;
        *)
            printf '%s' "${TONBO_BENCH_RUNNER_AZ:-}"
            ;;
    esac
}

resolve_instance_type() {
    local cell="$1"
    case "${cell}" in
        ec2_*)
            printf '%s' "${TONBO_MATRIX_EC2_INSTANCE_TYPE:-${TONBO_BENCH_RUNNER_INSTANCE_TYPE:-}}"
            ;;
        *)
            printf '%s' "${TONBO_MATRIX_DEV_INSTANCE_TYPE:-${TONBO_BENCH_RUNNER_INSTANCE_TYPE:-}}"
            ;;
    esac
}

resolve_network_path() {
    local cell="$1"
    case "${cell}" in
        dev_s3|dev_s3express)
            printf '%s' "${TONBO_MATRIX_DEV_NETWORK_PATH:-public_internet}"
            ;;
        ec2_s3|ec2_s3express)
            printf '%s' "${TONBO_MATRIX_EC2_NETWORK_PATH:-vpc_gateway_endpoint}"
            ;;
        *)
            printf '%s' "${TONBO_BENCH_NETWORK_PATH:-}"
            ;;
    esac
}

resolve_rtt_ms() {
    local cell="$1"
    case "${cell}" in
        dev_*)
            printf '%s' "${TONBO_MATRIX_DEV_MEDIAN_RTT_MS:-${TONBO_BENCH_MEDIAN_RTT_MS:-}}"
            ;;
        ec2_*)
            printf '%s' "${TONBO_MATRIX_EC2_MEDIAN_RTT_MS:-${TONBO_BENCH_MEDIAN_RTT_MS:-}}"
            ;;
        *)
            printf '%s' "${TONBO_BENCH_MEDIAN_RTT_MS:-}"
            ;;
    esac
}

run_cell() {
    local cell="$1"
    local scale="$2"
    local started_marker
    local artifact_path=""
    local prefix=""
    local backend=""
    local object_store_flavor=""
    local endpoint_kind=""
    local bucket_region=""
    local bucket_az=""
    local s3_bucket=""
    local s3_region=""
    local s3_endpoint=""

    started_marker="$(mktemp)"
    touch "${started_marker}"

    case "${cell}" in
        dev_local)
            backend="local"
            object_store_flavor="local_fs"
            endpoint_kind="none"
            ;;
        dev_s3|ec2_s3)
            backend="object_store"
            object_store_flavor="standard_s3"
            endpoint_kind="regional"
            require_var TONBO_MATRIX_STD_S3_BUCKET
            require_var TONBO_MATRIX_STD_S3_REGION
            s3_bucket="${TONBO_MATRIX_STD_S3_BUCKET}"
            s3_region="${TONBO_MATRIX_STD_S3_REGION}"
            s3_endpoint="${TONBO_MATRIX_STD_S3_ENDPOINT:-}"
            bucket_region="${TONBO_MATRIX_STD_S3_BUCKET_REGION:-${TONBO_MATRIX_STD_S3_REGION}}"
            prefix="tonbo-compare-${cell}-s${scale}-$(date +%s)"
            ;;
        dev_s3express|ec2_s3express)
            backend="object_store"
            object_store_flavor="s3_express"
            endpoint_kind="zonal"
            require_var TONBO_MATRIX_EXPRESS_S3_BUCKET
            require_var TONBO_MATRIX_EXPRESS_S3_REGION
            require_var TONBO_MATRIX_EXPRESS_S3_ENDPOINT
            require_var TONBO_MATRIX_EXPRESS_S3_BUCKET_AZ
            s3_bucket="${TONBO_MATRIX_EXPRESS_S3_BUCKET}"
            s3_region="${TONBO_MATRIX_EXPRESS_S3_REGION}"
            s3_endpoint="${TONBO_MATRIX_EXPRESS_S3_ENDPOINT}"
            bucket_region="${TONBO_MATRIX_EXPRESS_S3_BUCKET_REGION:-${TONBO_MATRIX_EXPRESS_S3_REGION}}"
            bucket_az="${TONBO_MATRIX_EXPRESS_S3_BUCKET_AZ}"
            prefix="tonbo-compare-${cell}-s${scale}-$(date +%s)"
            ;;
        *)
            echo "unknown cell: ${cell}" >&2
            rm -f "${started_marker}"
            exit 1
            ;;
    esac

    if [[ "${backend}" == "object_store" ]]; then
        require_any_var TONBO_S3_ACCESS_KEY AWS_ACCESS_KEY_ID
        require_any_var TONBO_S3_SECRET_KEY AWS_SECRET_ACCESS_KEY
    fi

    local runner_env runner_region runner_az runner_instance_type network_path median_rtt_ms
    runner_env="$(resolve_runner_env "${cell}")"
    runner_region="$(resolve_runner_region "${cell}")"
    runner_az="$(resolve_runner_az "${cell}")"
    runner_instance_type="$(resolve_instance_type "${cell}")"
    network_path="$(resolve_network_path "${cell}")"
    median_rtt_ms="$(resolve_rtt_ms "${cell}")"

    local -a cmd
    cmd=(
        env
        "TONBO_BENCH_BACKEND=${backend}"
        "TONBO_BENCH_DATASET_SCALE=${scale}"
        "TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=${DEFAULT_ROWS_PER_BATCH}"
        "TONBO_COMPACTION_BENCH_INGEST_BATCHES=${DEFAULT_INGEST_BATCHES}"
        "TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=${DEFAULT_ARTIFACT_ITERATIONS}"
        "TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=${DEFAULT_CRITERION_SAMPLE_SIZE}"
        "TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0"
        "TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0"
        "TONBO_BENCH_RUNNER_ENV=${runner_env}"
        "TONBO_BENCH_OBJECT_STORE_FLAVOR=${object_store_flavor}"
    )

    if [[ -n "${runner_region}" ]]; then
        cmd+=("TONBO_BENCH_RUNNER_REGION=${runner_region}")
    fi
    if [[ -n "${runner_az}" ]]; then
        cmd+=("TONBO_BENCH_RUNNER_AZ=${runner_az}")
    fi
    if [[ -n "${runner_instance_type}" ]]; then
        cmd+=("TONBO_BENCH_RUNNER_INSTANCE_TYPE=${runner_instance_type}")
    fi
    if [[ -n "${network_path}" ]]; then
        cmd+=("TONBO_BENCH_NETWORK_PATH=${network_path}")
    fi
    if [[ -n "${median_rtt_ms}" ]]; then
        cmd+=("TONBO_BENCH_MEDIAN_RTT_MS=${median_rtt_ms}")
    fi

    if [[ "${backend}" == "object_store" ]]; then
        cmd+=(
            "TONBO_S3_BUCKET=${s3_bucket}"
            "TONBO_S3_REGION=${s3_region}"
            "TONBO_BENCH_BUCKET_REGION=${bucket_region}"
            "TONBO_BENCH_ENDPOINT_KIND=${endpoint_kind}"
            "TONBO_BENCH_OBJECT_PREFIX=${prefix}"
            "TONBO_S3_ACCESS_KEY=${TONBO_S3_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:-}}"
            "TONBO_S3_SECRET_KEY=${TONBO_S3_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-}}"
        )
        if [[ -n "${bucket_az}" ]]; then
            cmd+=("TONBO_BENCH_BUCKET_AZ=${bucket_az}")
        fi
        if [[ -n "${s3_endpoint}" ]]; then
            cmd+=("TONBO_S3_ENDPOINT=${s3_endpoint}")
        fi
        if [[ -n "${TONBO_S3_SESSION_TOKEN:-${AWS_SESSION_TOKEN:-}}" ]]; then
            cmd+=("TONBO_S3_SESSION_TOKEN=${TONBO_S3_SESSION_TOKEN:-${AWS_SESSION_TOKEN:-}}")
        fi
    fi

    cmd+=(
        cargo
        bench
        -p
        tonbo
        --bench
        "${BENCH_NAME}"
        --
        "${SCENARIO}"
        --nocapture
    )

    append_report ""
    append_report "## ${cell} scale=${scale}"
    append_report ""
    append_report "- status: pending"
    append_report "- backend: \`${backend}\`"
    append_report "- object_store_flavor: \`${object_store_flavor}\`"
    append_report "- scenario: \`${SCENARIO}\`"

    if [[ "${RUN_MODE}" == "print" ]]; then
        printf '%q ' "${cmd[@]}"
        printf '\n'
        append_report "- mode: \`print\`"
        append_report "- command: \`$(printf '%q ' "${cmd[@]}")\`"
        rm -f "${started_marker}"
        return 0
    fi

    "${cmd[@]}"
    artifact_path="$(find "${ARTIFACT_DIR}" -maxdepth 1 -type f -name "${BENCH_NAME}-*.json" -newer "${started_marker}" | sort | tail -n 1)"
    rm -f "${started_marker}"

    if [[ -z "${artifact_path}" ]]; then
        echo "failed to discover artifact for ${cell} scale=${scale}" >&2
        exit 1
    fi

    append_report "- status: completed"
    append_report "- artifact: \`${artifact_path}\`"
    if [[ -n "${prefix}" ]]; then
        append_report "- object_prefix: \`${prefix}\`"
    fi
    append_report "- notes:"
    append_report "  replace this line with interpretation after reviewing the JSON artifact"
}

cat > "${REPORT_PATH}" <<EOF
# Benchmark Matrix Report

- generated_at_utc: \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`
- host: \`$(hostname)\`
- scenario: \`${SCENARIO}\`
- run_mode: \`${RUN_MODE}\`
- rows_per_batch: \`${DEFAULT_ROWS_PER_BATCH}\`
- ingest_batches: \`${DEFAULT_INGEST_BATCHES}\`
- artifact_iterations: \`${DEFAULT_ARTIFACT_ITERATIONS}\`
- criterion_sample_size: \`${DEFAULT_CRITERION_SAMPLE_SIZE}\`

## Scales
EOF

join_csv_to_bullets "${SCALES_CSV}"

append_report ""
append_report "## Cells"
join_csv_to_bullets "${CELLS_CSV}"

for raw_cell in "${CELLS[@]}"; do
    cell="$(trim "${raw_cell}")"
    [[ -z "${cell}" ]] && continue
    for raw_scale in "${SCALES[@]}"; do
        scale="$(trim "${raw_scale}")"
        [[ -z "${scale}" ]] && continue
        run_cell "${cell}" "${scale}"
    done
done

append_report ""
append_report "## Follow-up"
append_report ""
append_report "- Compare the \`topology\` section across artifacts before comparing latency."
append_report "- For S3 Express, prefer same-AZ EC2 runs over cross-AZ or laptop runs."
append_report "- If a cell fails, rerun it with a fresh object prefix instead of reusing the same prefix."

echo "matrix report: ${REPORT_PATH}"
