# Compaction Directional Matrix (`compaction_directional_local_s3_2026-02-27`)

## Scope

This report captures the directional CPU-vs-I/O benchmark matrix run on `2026-02-27`:

- Question: `CPU vs I/O bound under dataset scaling and backend change?`
- Scenario pair:
  - `read_baseline`
  - `read_compaction_quiesced`
- Matrix dimensions:
  - Backend: `local`, `object_store`
  - Dataset scale: `1`, `10`
- Included cells:
  - `local` scale `1`
  - `local` scale `10`
  - `object_store` scale `1`
- Incomplete cell:
  - `object_store` scale `10` (failed during scenario preparation)

## Run Profile

- Date (UTC): `2026-02-27`
- Harness: `benches/compaction_local.rs`
- Shared benchmark knobs:
  - `TONBO_COMPACTION_BENCH_INGEST_BATCHES=640`
  - `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8`
  - `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10`
  - `TONBO_COMPACTION_BENCH_COMPACTION_WAIT_TIMEOUT_MS=600000`
  - `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=false`
  - `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=false`

## Directional Results (Completed Cells)

| Backend | Scale | Artifact | Read ops delta | Bytes delta | Mean latency delta | p99 latency delta | Throughput delta |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| `local` | `1` | `target/tonbo-bench/compaction_local-1772210385445-1074695.json` | `-50.00%` | `-54.82%` | `-24.14%` | `-22.57%` | `+31.83%` |
| `local` | `10` | `target/tonbo-bench/compaction_local-1772210396458-1074996.json` | `-89.00%` | `-92.38%` | `-58.52%` | `-57.84%` | `+141.06%` |
| `object_store` | `1` | `target/tonbo-bench/compaction_local-1772210468296-1076637.json` | `n/a` | `n/a` | `-80.18%` | `-79.83%` | `+404.44%` |

## Failed Cell

| Backend | Scale | Status | Failure signature |
| --- | ---: | --- | --- |
| `object_store` | `10` | failed in scenario preparation | `failed to persist wal state: error sending request for url (...)` |

Reference log path from run: `/tmp/object_store_s10_bounded_1772211597/run.log`

## Interpretation

- Local scale `1` already shows lower read work and lower mean/p99 latency after compaction.
- Local scale `10` strengthens the same trend: larger read-work reduction tracks larger latency reduction.
- Object-store scale `1` shows a larger latency drop than local scale `1`, consistent with higher remote I/O sensitivity.
- Mean and p99 deltas move closely in completed cells, indicating broad latency improvement rather than tail-only effects.

## Caveats

- This historical run predates object-store probe wiring in the harness, so object-store `read_ops` and `bytes_read` are unavailable for this artifact.
- The `object_store` scale `10` cell must be rerun after stabilizing WAL-state persistence on that path.

## Reproduction Commands

```bash
# local scale 1
TONBO_BENCH_BACKEND=local TONBO_BENCH_DATASET_SCALE=1 TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture

# local scale 10
TONBO_BENCH_BACKEND=local TONBO_BENCH_DATASET_SCALE=10 TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture

# object-store scale 1
eval "$(aws configure export-credentials --profile tonbo --format env)"
export TONBO_S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
export TONBO_S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export TONBO_S3_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
export TONBO_S3_BUCKET=tonbo-smoke-euc1-232814779190-1772115011
export TONBO_S3_REGION=eu-central-1
TONBO_BENCH_BACKEND=object_store TONBO_BENCH_DATASET_SCALE=1 TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-dir-object_store-s1-$(date +%s) \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
```
