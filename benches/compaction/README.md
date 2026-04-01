# Compaction Benchmark Harness

This benchmark suite lives in `benches/compaction_local.rs` and now supports:

- Dataset scaling to reduce page-cache/memory effects.
- Backend selection (`local` vs `object_store`).
- Tail latency reporting (p50/p95/p99 in addition to mean).
- A directional report block for `read_baseline` vs `read_compaction_quiesced`.

## Program Context

This suite is one current engine-layer benchmark inside Tonbo's broader
benchmark program. The wider roadmap and instrumentation plan live in
`docs/benchmark_program.md`.

In that broader program:

- `micro` benchmarks isolate narrow internal costs,
- `engine` benchmarks cover mixed read/write, durability, compaction, GC, and
  object-store effects,
- `surface` benchmarks cover user-facing query/open/freshness behavior.

This README stays focused on the current compaction harness, but its artifact
and scenario design should evolve toward that larger program.

## New Environment Variables

- `TONBO_BENCH_DATASET_SCALE` (default: `1`)
  - Multiplies ingest batches deterministically (fixed seed).
  - Effective rows scale with this factor while scenario shape remains unchanged.

- `TONBO_BENCH_BACKEND=local|object_store` (default: `local`)
  - `local`: uses `LocalFs` benchmark workspace under `target/tonbo-bench/workspaces/...`.
  - `object_store`: uses Tonbo object-store builder path.

- `TONBO_BENCH_OBJECT_PREFIX` (optional; default: `tonbo-bench`)
  - Prefix used for object-store benchmark runs.

- Topology metadata env vars (optional, but recommended for cross-environment comparison)
  - `TONBO_BENCH_RUNNER_ENV`
    - Example: `dev`, `ec2`
  - `TONBO_BENCH_RUNNER_REGION`
    - Example: `eu-central-1`
  - `TONBO_BENCH_RUNNER_AZ`
    - Example: `euc1-az1` or `eu-central-1a` if AZ ID is not available
  - `TONBO_BENCH_RUNNER_INSTANCE_TYPE`
    - Example: `c7i.large`
  - `TONBO_BENCH_BUCKET_REGION`
    - Region of the target bucket
  - `TONBO_BENCH_BUCKET_AZ`
    - Directory-bucket zone for S3 Express cells
  - `TONBO_BENCH_OBJECT_STORE_FLAVOR`
    - Example: `standard_s3`, `s3_express`
  - `TONBO_BENCH_ENDPOINT_KIND`
    - Example: `regional`, `zonal`, `custom`
  - `TONBO_BENCH_NETWORK_PATH`
    - Example: `public_internet`, `vpc_gateway_endpoint`
  - `TONBO_BENCH_MEDIAN_RTT_MS`
    - Optional measured median RTT for the runner -> endpoint path

## Object-Store Configuration

When `TONBO_BENCH_BACKEND=object_store`, these env vars must be set:

- `TONBO_S3_BUCKET`
- `TONBO_S3_REGION`
- `TONBO_S3_ACCESS_KEY`
- `TONBO_S3_SECRET_KEY`

Optional:

- `TONBO_S3_ENDPOINT`
- `TONBO_S3_SESSION_TOKEN`

If required object-store variables are missing, the benchmark prints a clear skip reason and exits cleanly instead of failing the entire run.

## Example Commands

LocalFS, baseline scale:

```bash
TONBO_BENCH_BACKEND=local TONBO_BENCH_DATASET_SCALE=1 cargo bench -p tonbo -- <bench-filter-if-needed>
```

LocalFS, larger dataset:

```bash
TONBO_BENCH_DATASET_SCALE=10 cargo bench -p tonbo -- <...>
```

Object store (if configured):

```bash
TONBO_BENCH_BACKEND=object_store TONBO_BENCH_DATASET_SCALE=10 cargo bench -p tonbo -- <...>
```

## S3 Execution Guide

Use short-lived credentials from your AWS profile:

```bash
eval "$(aws configure export-credentials --profile tonbo --format env)"
export TONBO_S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
export TONBO_S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export TONBO_S3_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
```

Set S3 target:

```bash
export TONBO_S3_BUCKET=tonbo-smoke-euc1-232814779190-1772115011
export TONBO_S3_REGION=eu-central-1
```

Set topology fields for a standard regional S3 run from a developer machine:

```bash
export TONBO_BENCH_RUNNER_ENV=dev
export TONBO_BENCH_RUNNER_REGION=eu-central-1
export TONBO_BENCH_BUCKET_REGION=eu-central-1
export TONBO_BENCH_OBJECT_STORE_FLAVOR=standard_s3
export TONBO_BENCH_ENDPOINT_KIND=regional
export TONBO_BENCH_NETWORK_PATH=public_internet
```

Matrix helper script:

```bash
chmod +x benches/compaction/run_matrix.sh
```

Example: run only the developer-machine cells and generate a report under
`target/tonbo-bench/reports/`:

```bash
export TONBO_MATRIX_CELLS=dev_local,dev_s3
export TONBO_MATRIX_STD_S3_BUCKET=tonbo-smoke-euc1-232814779190-1772115011
export TONBO_MATRIX_STD_S3_REGION=eu-central-1
benches/compaction/run_matrix.sh
```

Example: print the EC2 commands without executing them:

```bash
export TONBO_MATRIX_RUN_MODE=print
export TONBO_MATRIX_CELLS=ec2_s3,ec2_s3express
benches/compaction/run_matrix.sh
```

Run a directional cell:

```bash
TONBO_BENCH_BACKEND=object_store \
TONBO_BENCH_DATASET_SCALE=1 \
TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-dir-object_store-s1-$(date +%s) \
TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0 \
TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0 \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
```

Recommended matrix (same ingest base across cells):

```bash
# local
TONBO_BENCH_BACKEND=local TONBO_BENCH_DATASET_SCALE=1  TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
TONBO_BENCH_BACKEND=local TONBO_BENCH_DATASET_SCALE=10 TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture

# object store
TONBO_BENCH_BACKEND=object_store TONBO_BENCH_DATASET_SCALE=1  TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 TONBO_BENCH_OBJECT_PREFIX=tonbo-dir-object_store-s1-$(date +%s)  cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
TONBO_BENCH_BACKEND=object_store TONBO_BENCH_DATASET_SCALE=10 TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 TONBO_BENCH_OBJECT_PREFIX=tonbo-dir-object_store-s10-$(date +%s) cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
```

Cross-environment comparison matrix for this branch, using smaller first-pass scales:

```bash
# Common first-pass scale knobs. These are intended to keep runs cheap while still
# exposing remote latency floors and compaction/read directionality.
export TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=64
export TONBO_COMPACTION_BENCH_INGEST_BATCHES=160
export TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=12
export TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10
export TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0
export TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0

# Cell 1: dev machine, local
TONBO_BENCH_BACKEND=local \
TONBO_BENCH_DATASET_SCALE=1 \
TONBO_BENCH_RUNNER_ENV=dev \
TONBO_BENCH_OBJECT_STORE_FLAVOR=local_fs \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture

# Cell 2: dev machine, standard S3
TONBO_BENCH_BACKEND=object_store \
TONBO_BENCH_DATASET_SCALE=1 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-compare-dev-s3-s1-$(date +%s) \
TONBO_BENCH_RUNNER_ENV=dev \
TONBO_BENCH_RUNNER_REGION=eu-central-1 \
TONBO_BENCH_BUCKET_REGION=eu-central-1 \
TONBO_BENCH_OBJECT_STORE_FLAVOR=standard_s3 \
TONBO_BENCH_ENDPOINT_KIND=regional \
TONBO_BENCH_NETWORK_PATH=public_internet \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture

# Cell 3: dev machine, S3 Express smoke-test only. This requires a real directory
# bucket endpoint and may need a CreateSession-capable client path before it works.
TONBO_BENCH_BACKEND=object_store \
TONBO_BENCH_DATASET_SCALE=1 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-compare-dev-s3express-s1-$(date +%s) \
TONBO_BENCH_RUNNER_ENV=dev \
TONBO_BENCH_RUNNER_REGION=eu-central-1 \
TONBO_BENCH_BUCKET_REGION=eu-central-1 \
TONBO_BENCH_BUCKET_AZ=euc1-az1 \
TONBO_BENCH_OBJECT_STORE_FLAVOR=s3_express \
TONBO_BENCH_ENDPOINT_KIND=zonal \
TONBO_BENCH_NETWORK_PATH=public_internet \
TONBO_S3_ENDPOINT=<directory-bucket-zonal-endpoint> \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture

# Cell 4: EC2 same-region, standard S3
TONBO_BENCH_BACKEND=object_store \
TONBO_BENCH_DATASET_SCALE=1 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-compare-ec2-s3-s1-$(date +%s) \
TONBO_BENCH_RUNNER_ENV=ec2 \
TONBO_BENCH_RUNNER_REGION=eu-central-1 \
TONBO_BENCH_RUNNER_AZ=euc1-az1 \
TONBO_BENCH_RUNNER_INSTANCE_TYPE=c7i.large \
TONBO_BENCH_BUCKET_REGION=eu-central-1 \
TONBO_BENCH_OBJECT_STORE_FLAVOR=standard_s3 \
TONBO_BENCH_ENDPOINT_KIND=regional \
TONBO_BENCH_NETWORK_PATH=vpc_gateway_endpoint \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture

# Cell 5: EC2 same-AZ, S3 Express
TONBO_BENCH_BACKEND=object_store \
TONBO_BENCH_DATASET_SCALE=1 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-compare-ec2-s3express-s1-$(date +%s) \
TONBO_BENCH_RUNNER_ENV=ec2 \
TONBO_BENCH_RUNNER_REGION=eu-central-1 \
TONBO_BENCH_RUNNER_AZ=euc1-az1 \
TONBO_BENCH_RUNNER_INSTANCE_TYPE=c7i.large \
TONBO_BENCH_BUCKET_REGION=eu-central-1 \
TONBO_BENCH_BUCKET_AZ=euc1-az1 \
TONBO_BENCH_OBJECT_STORE_FLAVOR=s3_express \
TONBO_BENCH_ENDPOINT_KIND=zonal \
TONBO_BENCH_NETWORK_PATH=vpc_gateway_endpoint \
TONBO_S3_ENDPOINT=<directory-bucket-zonal-endpoint> \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
```

The helper script uses the same cell names:

- `dev_local`
- `dev_s3`
- `dev_s3express`
- `ec2_s3`
- `ec2_s3express`

Cell-specific environment expected by `run_matrix.sh`:

- Standard S3 cells:
  - `TONBO_MATRIX_STD_S3_BUCKET`
  - `TONBO_MATRIX_STD_S3_REGION`
  - optional `TONBO_MATRIX_STD_S3_ENDPOINT`
- S3 Express cells:
  - `TONBO_MATRIX_EXPRESS_S3_BUCKET`
  - `TONBO_MATRIX_EXPRESS_S3_REGION`
  - `TONBO_MATRIX_EXPRESS_S3_ENDPOINT`
  - `TONBO_MATRIX_EXPRESS_S3_BUCKET_AZ`
  - `run_matrix.sh` now preflights both the regional control endpoint
    (`s3express-control.<region>.amazonaws.com`) and the configured zonal endpoint.
    If either hostname does not resolve from the current host, the Express cell is
    marked `skipped` in the report instead of failing mid-run.
- Credentials:
  - either `TONBO_S3_ACCESS_KEY` / `TONBO_S3_SECRET_KEY`
  - or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- Optional host metadata overrides:
  - `TONBO_MATRIX_DEV_RUNNER_REGION`
  - `TONBO_MATRIX_DEV_RUNNER_AZ`
  - `TONBO_MATRIX_EC2_RUNNER_REGION`
  - `TONBO_MATRIX_EC2_RUNNER_AZ`
  - `TONBO_MATRIX_EC2_INSTANCE_TYPE`
  - `TONBO_MATRIX_DEV_MEDIAN_RTT_MS`
  - `TONBO_MATRIX_EC2_MEDIAN_RTT_MS`

Suggested first-pass scales:

- `TONBO_BENCH_DATASET_SCALE=1`
- `TONBO_BENCH_DATASET_SCALE=4`
- `TONBO_BENCH_DATASET_SCALE=8`

These scales are usually enough to expose:

- fixed remote setup cost,
- regional vs zonal latency floor differences,
- whether compaction meaningfully changes read cost,
- whether the EC2-in-region / EC2-in-AZ path changes the result materially.

Move to `~1 GB` only if:

- the direction changes at higher scales,
- the smaller scales are too noisy,
- or you need stronger throughput claims rather than a first topology comparison.

High-scale object-store tuned profile (reduces small-operation amplification while preserving
benchmark intent):

```bash
TONBO_BENCH_BACKEND=object_store \
TONBO_BENCH_DATASET_SCALE=7 \
TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=192 \
TONBO_COMPACTION_BENCH_INGEST_BATCHES=214 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0 \
TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-object-s7-$(date +%s) \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
```

## Compaction Eligibility and Skips

`read_after_first_compaction_observed` and `read_compaction_quiesced` wait for compaction-state transitions. If compaction never triggers, those scenarios can time out; increase `TONBO_COMPACTION_BENCH_INGEST_BATCHES` or `TONBO_BENCH_DATASET_SCALE` in that case.

## Troubleshooting Notes

- If object-store run fails before artifact with:
  - `failed to persist wal state: error sending request for url (...)`
  - treat the cell as failed infra/transport setup and rerun with a fresh prefix.
- If object-store `read_ops` / `bytes_read` are `0` in artifact:
  - this indicates probe wiring/regression; current harness instruments both local and object-store filesystem paths.
- If object-store post-compaction read scenarios report `rows_per_scan=0`:
  - treat directional CPU-vs-I/O conclusions as invalid for that run; first fix/read-verify data visibility for those scenarios.
- To avoid cross-run contamination:
  - always use a unique `TONBO_BENCH_OBJECT_PREFIX` per run.

## Output Notes

Each scenario summary includes latency fields in nanoseconds:

- `mean`
- `p50`
- `p95`
- `p99`

Read workloads also include a scan-phase breakdown:

- `read_path_latency_ns.mean_prepare_ns` (`setup phase`):
  - measured around `db.scan().stream().await`
  - includes snapshot/read-ts resolution, scan planning/pruning, and stream construction/open
- `read_path_latency_ns.mean_consume_ns` (`execute phase`):
  - measured around `while let Some(batch_result) = stream.next().await`
  - includes stream polling/materialization/merge work and async wait while consuming rows
- `read_path_latency_ns.prepare_share_pct` / `consume_share_pct`:
  - approximate setup-vs-execute split (harness-level phases, not hardware-level CPU/IO buckets)

Artifact design principle:

- always record end-to-end latency, and add phase-level timers so the top-line
  number is explainable

For schema `6+` artifacts, read workloads also include internal setup-stage means:

- `read_path_internal_ns.mean_snapshot_ns`
- `read_path_internal_ns.mean_plan_scan_ns`
- `read_path_internal_ns.mean_build_scan_streams_ns`
- `read_path_internal_ns.mean_merge_init_ns`
- `read_path_internal_ns.mean_package_init_ns`

These internal setup-stage timers should approximately sum to `mean_prepare_ns` (small residual
measurement overhead is expected).

For schema `8+` artifacts, setup payloads include both logical and physical volume sections:

- Primary logical live-set view:
  - `setup.logical_before_compaction`
  - `setup.logical_ready`
- Debug physical prefix snapshots:
  - `setup.volume_before_compaction`
  - `setup.volume_ready`

Logical section fields:

- `sst_count`
- `sst_bytes`
- `wal_bytes` (nullable best-effort; `null` when live WAL byte accounting is unavailable)
- `manifest_bytes` (nullable best-effort)
- `total_bytes` (sum of included logical components only; excludes obsolete/unreferenced files)

Physical section fields:

- `setup.volume_before_compaction`
- `setup.volume_ready`

Each volume snapshot includes:

- `object_count`
- `total_bytes`
- `sst_bytes`
- `wal_bytes`
- `manifest_bytes`
- `other_bytes`

Capture points:

- `logical_before_compaction` / `volume_before_compaction`: after ingest, before compaction wait.
- `logical_ready` / `volume_ready`: after scenario reaches ready state.

Semantics:

- Physical can increase from before -> ready because compaction writes new SSTs before old files are GC'd.
- Logical should be used as the primary compaction-effectiveness metric because it tracks manifest-visible live state.
- Physical remains useful for debugging storage amplification and cleanup lag.

Planned artifact growth for the broader benchmark program:

- topology fields such as runner region, bucket region, path placement, and
  cold/warm run state
- request-economics fields such as GET/HEAD/range-GET/PUT counts and
  bytes/request
- engine-pressure fields such as compaction backlog, GC backlog, WAL queue
  depth, CPU, RSS, and network throughput
- stable configuration snapshots so each run can be traced back to WAL mode,
  compaction settings, and workload shape

Current artifact note:

- schema `11+` artifacts now include a top-level `topology` object populated
  from the optional `TONBO_BENCH_*` topology env vars above
- the directional report also prints a one-line topology summary when present

Interpretation guideline:

- If `consume_share_pct` remains larger after compaction, prioritize execute-path optimizations (decode/merge/filter/materialization) before setup-path tweaks.
- If setup optimization is targeted, use `read_path_internal_ns` first; in current local validation
  runs, `snapshot` remains the dominant setup component after compaction.

After `read_baseline` and `read_compaction_quiesced` complete, the harness prints:

- Directional Question
- Scenario Set
- Dataset Scale
- Backend
- Observed deltas (read ops, bytes, mean, p99)
- Conservative interpretation
- Suggested next tests

## Report Files

- Benchmark-program roadmap and instrumentation guide:
  - `docs/benchmark_program.md`
- Consolidated benchmark summary for the PR:
  - `docs/benchmark_results.md`
- Large read baseline (current branch format):
  - `benches/compaction/results/compaction_local_baseline.md`
- Directional matrix (ported from `feat/bench-cpu-vs-io-directional-loop`, reformatted):
  - `benches/compaction/results/compaction_directional_matrix_2026-02-27.md`
- Directional + latency phase split validation (new probe wiring + dominance view):
  - `benches/compaction/results/compaction_directional_phase_split_2026-03-01.md`
