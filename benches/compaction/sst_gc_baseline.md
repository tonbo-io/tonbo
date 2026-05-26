# SST GC Baseline Harness

This baseline uses the existing `compaction_local` Criterion bench to measure the
pre-SST-GC state: compaction publishes replacement SSTs, but obsolete SST objects
remain physically present because no SST sweep/delete path is enabled yet.

## Scenarios

- `read_compaction_quiesced`
  - Reads from the current HEAD after compaction settles.
  - This is the main read-latency baseline while obsolete SSTs accumulate.
- `write_heavy_no_sst_sweep`
  - Measures sustained write latency/throughput with compaction enabled and no SST sweep.
- `estimate_sweep_candidates`
  - Benchmarks the reclaim-estimation pass itself.
  - The operation compares physical SST bytes/object counts against the current live manifest view.

Optional comparison:

- `read_baseline`
  - Captures read cost before obsolete SSTs exist.
  - Useful for directional comparison against `read_compaction_quiesced`.

## What The Harness Records

Each scenario artifact now includes:

- End-to-end latency and throughput.
- Read-path breakdowns for read scenarios.
- Physical storage footprint before/after the setup workload.
- Logical live SST footprint from `list_versions()`.
- `physical_stale_estimate_before_compaction` and `physical_stale_estimate_ready`:
  - `candidate_sst_objects`
  - `candidate_sst_bytes`
  - `stale_sst_byte_amplification_pct`
- Request counters visible to the benchmark:
  - read/write/list/remove/copy/link
  - CAS load / CAS put
  - head metadata
  - request totals split by SST / WAL / manifest / other paths

The physical stale estimate metrics are intentionally estimates. They do not delete or mutate SSTs.
They quantify the physical-minus-live SST gap, not the persisted GC plan and not what an explicit sweep has necessarily deleted.

## Environment Assumptions

- Offline and deterministic by default on `TONBO_BENCH_BACKEND=local`.
- Object-store runs are supported, but use a unique `TONBO_BENCH_OBJECT_PREFIX` for each run.
- Recommended for saved baselines:
  - `TONBO_COMPACTION_BENCH_WAL_SYNC=always`
  - fixed `TONBO_BENCH_DATASET_SCALE`
  - fixed `TONBO_COMPACTION_BENCH_INGEST_BATCHES`
  - fixed `TONBO_COMPACTION_BENCH_ROWS_PER_BATCH`

## Recommended Baseline Command

```bash
TONBO_BENCH_BACKEND=local \
TONBO_BENCH_DATASET_SCALE=4 \
TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 \
TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=64 \
TONBO_COMPACTION_BENCH_WAL_SYNC=always \
TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0 \
TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0 \
cargo bench -p tonbo --bench compaction_local -- \
  --save-baseline sst_gc_pre_enablement_v1
```

To shorten a focused local validation run while iterating on the harness:

```bash
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=4 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
cargo bench -p tonbo --bench compaction_local -- estimate_sweep_candidates --nocapture
```

## Baseline Outputs To Save

Save all of the following together:

- Criterion baseline name:
  - `sst_gc_pre_enablement_v1` or a dated successor
- JSON artifact path printed by the bench:
  - `target/tonbo-bench/compaction_local-<run_id>.json`
- Git commit SHA
- Local environment knobs used for the run
- A short markdown summary capturing the retained artifact path, environment,
  and the key latency/storage/reclaim fields listed below

Minimum fields to copy from the artifact:

- `summary.latency_ns.mean`, `p95`, `p99`
- `summary.throughput`
- `summary.io`
- `setup.volume_before_compaction`, `setup.volume_ready`
- `setup.logical_before_compaction`, `setup.logical_ready`
- `setup.physical_stale_estimate_before_compaction`, `setup.physical_stale_estimate_ready`
- `summary.physical_stale_estimate` for `estimate_sweep_candidates`

## Interpreting The Baseline

- `physical_stale_estimate_ready.candidate_sst_bytes > 0`
  - confirms compaction has created reclaimable SST debt with no sweep enabled.
- `manifest_request_ops`
  - shows how much manifest coordination the workload already pays before SST GC exists.
- `list_ops` in `estimate_sweep_candidates`
  - approximates the object-store/list cost of a no-delete planning pass.
- `read_compaction_quiesced` vs `read_baseline`
  - separates read-visible benefits of compaction from the still-unreclaimed physical footprint.
