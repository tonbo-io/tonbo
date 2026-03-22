# Compaction Benchmark Harness

This benchmark suite lives in `benches/compaction_local.rs` and now supports:

- Dataset scaling to reduce page-cache/memory effects.
- Backend selection (`local` vs `object_store`).
- Tail latency reporting (p50/p95/p99 in addition to mean).
- A directional report block for `read_baseline` vs `read_compaction_quiesced`.

For the SST-GC pre-enablement baseline workflow, use
`benches/compaction/sst_gc_baseline.md`.

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

For schema `11+` artifacts, setup payloads may also include `setup.gc_observation` for
GC-enabled scenarios. This captures:

- `volume_before_gc` and `volume_after_gc`
- `physical_stale_estimate_before_explicit_sweep` and `physical_stale_estimate_after_explicit_sweep`
- `persisted_plan_before_explicit_sweep` and `persisted_plan_after_explicit_sweep`
- `reclaimed_sst_objects` and `reclaimed_sst_bytes`
- `explicit_sweep_result.deleted_objects`
- `explicit_sweep_result.deleted_bytes`
- `explicit_sweep_result.delete_failures`
- `explicit_sweep_result.duration_ms`
- cumulative counters before/after the explicit sweep, including:
  `sweep_runs`, `deleted_objects`, `deleted_bytes`, `delete_failures`,
  `sweep_duration_ms_total`, `gc_plan_write_runs`, `gc_plan_overwrite_non_empty`,
  `gc_plan_take_runs`, `gc_plan_taken_sst_candidates`,
  `gc_plan_authorized_sst_candidates`, `gc_plan_blocked_sst_candidates`,
  `gc_plan_requeued_sst_candidates`

Interpretation:

- `physical_stale_estimate_*` is a physical-vs-logical SST storage delta estimate. It is not the persisted GC plan.
- `persisted_plan_*` is the manifest GC plan at the explicit sweep observation point, including the currently authorized subset.
- `explicit_sweep_result.*` is what that explicit benchmark sweep invocation actually deleted.
- A non-zero `physical_stale_estimate_before_explicit_sweep` with `persisted_plan_before_explicit_sweep = null` means stale-looking SST objects still exist physically, but no persisted plan remained staged for the explicit sweep to consume.

Use these fields to compare a GC-on scenario against its GC-off companion:

- `read_compaction_quiesced` vs `read_compaction_quiesced_after_gc`
- `write_heavy_no_sst_sweep` vs `write_heavy_with_sst_sweep`

Planned artifact growth for the broader benchmark program:

- topology fields such as runner region, bucket region, path placement, and
  cold/warm run state
- request-economics fields such as GET/HEAD/range-GET/PUT counts and
  bytes/request
- engine-pressure fields such as compaction backlog, GC backlog, WAL queue
  depth, CPU, RSS, and network throughput
- stable configuration snapshots so each run can be traced back to WAL mode,
  compaction settings, and workload shape

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
- SST GC timing rerun with snapshot-pin semantics:
  - `benches/compaction/results/sst_gc_rerun_2026-03-19.md`
