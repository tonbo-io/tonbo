# Benchmark Results

## Scope

This is the rolling benchmark summary for Tonbo compaction/read directional runs.

- Detailed run logs and deep notes live under `benches/compaction/results/`.
- This page keeps one consolidated view for current decision-making.

## What Scale Means

For this benchmark, effective ingested rows are:

`effective_rows = TONBO_COMPACTION_BENCH_INGEST_BATCHES * TONBO_BENCH_DATASET_SCALE * TONBO_COMPACTION_BENCH_ROWS_PER_BATCH`

Example:

- Tuned profile (`ingest_batches=214`, `rows_per_batch=192`):
  - scale `10` => `214 * 10 * 192 = 410,880` rows
- Historical profile (`ingest_batches=640`, `rows_per_batch=64`):
  - scale `10` => `640 * 10 * 64 = 409,600` rows

So the tuned profile keeps nearly the same data volume intent while reducing small-operation pressure.

## Current Comprehensive View (Logical Live-Set Primary, 2026-03-03)

Profile used for rows below:

- `TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=192`
- `TONBO_COMPACTION_BENCH_INGEST_BATCHES=214`
- `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8`
- `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10`
- `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0`
- `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0`
- scenario: `read_compaction_quiesced`

Backend legend:

- `local`: Tonbo on local filesystem (`LocalFs`) on the benchmark runner.
- `object_store`: Tonbo object-store path from the same local runner to S3-compatible remote storage in the closest configured S3 region.

Column shorthand:

- `b->r`: before -> ready.
- `Live b->r (MiB)`: manifest-visible logical live-set (`setup.logical_before_compaction.total_bytes -> setup.logical_ready.total_bytes`) in MiB with percent change.
- `Phys b->r (MiB)`: physical prefix volume (`setup.volume_before_compaction.total_bytes -> setup.volume_ready.total_bytes`) in MiB with percent change.

### Local

| Scale | Status | Mean (ms) | p99 (ms) | Ops/s | SST b->r | Live b->r (MiB) | Phys b->r (MiB) |
| ---: | --- | ---: | ---: | ---: | --- | --- | --- |
| `3` | `ok` | `58.454` | `63.993` | `17.108` | `10 -> 5` | `11.25 -> 5.52 (-50.9%)` | `15.21 -> 16.24 (+6.8%)` |
| `4` | `ok` | `86.503` | `93.041` | `11.560` | `13 -> 8` | `14.63 -> 8.90 (-39.2%)` | `19.91 -> 20.94 (+5.2%)` |
| `7` | `ok` | `83.925` | `88.189` | `11.915` | `23 -> 8` | `25.88 -> 8.70 (-66.4%)` | `35.18 -> 38.27 (+8.8%)` |
| `10` | `ok` | `81.851` | `85.603` | `12.217` | `33 -> 8` | `37.13 -> 8.49 (-77.1%)` | `50.47 -> 55.64 (+10.2%)` |

### Object Store

| Scale | Status | Mean (ms) | p99 (ms) | Ops/s | SST b->r | Live b->r (MiB) | Phys b->r (MiB) |
| ---: | --- | ---: | ---: | ---: | --- | --- | --- |
| `3` | `ok` | `405.980` | `429.655` | `2.463` | `10 -> 5` | `11.25 -> 5.52 (-50.9%)` | `11.28 -> 16.24 (+44.0%)` |
| `4` | `ok` | `390.380` | `410.206` | `2.562` | `13 -> 8` | `14.63 -> 8.90 (-39.2%)` | `14.66 -> 20.94 (+42.8%)` |
| `7` | `ok` | `393.249` | `408.938` | `2.543` | `23 -> 18` | `25.88 -> 20.15 (-22.1%)` | `25.98 -> 37.22 (+43.3%)` |
| `10` | `ok` | `381.587` | `399.655` | `2.621` | `33 -> 23` | `37.13 -> 25.67 (-30.9%)` | `37.34 -> 52.51 (+40.7%)` |

## Interpretation

1. Physical bytes can still increase after compaction readiness because compaction writes replacement SSTs before obsolete files are reclaimed.
2. Logical live-set bytes (`logical_*`) decrease strongly across all local scales, matching read-visible state and intended compaction effect.
3. Logical live-set should be the primary decision metric for compaction effectiveness; physical bytes remain useful for debugging backend cleanup/GC timing.
4. Object-store latency remains substantially higher than local at the same scales, even when logical live-set reductions are present.

## What Changed To Make This Work

1. Scenario-filter-aware setup:
   - filtered runs no longer prepare all baseline scenarios first.
2. Tuned ingest shape:
   - larger per-ingest batch (`192` rows) and fewer ingest calls (`214` base batches).
3. Setup diagnostics:
   - explicit phase timing for open/ingest/wait/read.
4. Volume checkpoints:
   - logical live-set (schema `8`): `setup.logical_before_compaction`, `setup.logical_ready`
   - physical prefix snapshot (debug): `setup.volume_before_compaction`, `setup.volume_ready`

Logical vs physical semantics:

- Logical (`setup.logical_*`):
  - Derived from manifest-visible version state only.
  - Includes only currently live/referenced components.
  - `wal_bytes` and `manifest_bytes` are nullable best-effort fields (currently `null` in these runs).
- Physical (`setup.volume_*`):
  - Derived from recursive prefix listing of stored objects at capture time.
  - Includes transient and obsolete files that may still exist before GC.

Why previous `ready > before` could happen:

- Compaction is publish-new-before-GC-old, so prefix totals can rise at `ready` even when the live read-visible set shrinks.
- This is expected and not a compaction regression by itself.

Why logical live-set is primary:

- Read-path and retention decisions operate on manifest-visible references.
- Logical live-set directly tracks that state; physical totals conflate correctness with cleanup timing.

5. Physical volume checkpoints (debug):
   - `setup.volume_before_compaction` and `setup.volume_ready` in artifacts.

## Source Reports

- `benches/compaction/results/compaction_local_baseline.md`
- `benches/compaction/results/compaction_directional_matrix_2026-02-27.md`
- `benches/compaction/results/compaction_directional_phase_split_2026-03-01.md`
