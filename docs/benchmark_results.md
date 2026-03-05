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

Column meaning:

- `Logical live-set before -> ready`: `setup.logical_before_compaction.total_bytes -> setup.logical_ready.total_bytes` (manifest-visible, read-live bytes only).
- `Physical bytes before -> ready`: `setup.volume_before_compaction.total_bytes -> setup.volume_ready.total_bytes` (all objects currently present under the benchmark prefix, including transient/obsolete files awaiting GC).

| Backend | Scale | Status | Mean latency | p99 | Ops/sec | SST before -> ready | Logical live-set before -> ready (bytes) | Physical bytes before -> ready (bytes) |
| --- | ---: | --- | ---: | ---: | ---: | --- | --- | --- |
| `local` | `3` | `ok` | `58.454 ms` | `63.993 ms` | `17.108` | `10 -> 5` | `11,797,089 -> 5,792,031` | `15,951,663 -> 17,028,823` |
| `local` | `4` | `ok` | `86.503 ms` | `93.041 ms` | `11.560` | `13 -> 8` | `15,336,272 -> 9,331,214` | `20,882,160 -> 21,960,868` |
| `local` | `7` | `ok` | `83.925 ms` | `88.189 ms` | `11.915` | `23 -> 8` | `27,133,554 -> 9,117,998` | `36,883,843 -> 40,125,799` |
| `local` | `10` | `ok` | `81.851 ms` | `85.603 ms` | `12.217` | `33 -> 8` | `38,930,832 -> 8,904,778` | `52,924,919 -> 58,338,067` |
| `object_store` | `3` | `ok` | `405.980 ms` | `429.655 ms` | `2.463` | `10 -> 5` | `11,797,089 -> 5,792,031` | `11,823,466 -> 17,024,599` |
| `object_store` | `4` | `ok` | `390.380 ms` | `410.206 ms` | `2.562` | `13 -> 8` | `15,336,272 -> 9,331,214` | `15,377,079 -> 21,954,007` |
| `object_store` | `7` | `ok` | `393.249 ms` | `408.938 ms` | `2.543` | `23 -> 18` | `27,133,554 -> 21,128,496` | `27,245,479 -> 39,029,728` |
| `object_store` | `10` | `ok` | `381.587 ms` | `399.655 ms` | `2.621` | `33 -> 23` | `38,930,832 -> 26,920,523` | `39,149,933 -> 55,065,425` |

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
