# Compaction Directional + Phase Split (`compaction_local_phase_split_2026-03-01`)

## Scope

This report validates two questions with the updated benchmark harness:

- Does object-store now emit `read_ops` / `bytes_read` counters? (probe wiring check)
- In local scale `1`, what dominates scan latency: scan setup/planning or stream consumption?

Scenario pair:

- `read_baseline`
- `read_compaction_quiesced`

## Run Profile

- Date (UTC): `2026-03-01`
- Harness: `benches/compaction_local.rs`
- Artifact: `target/tonbo-bench/compaction_local-1772394468583-50617.json`
- Configuration:
  - `TONBO_BENCH_BACKEND=local`
  - `TONBO_BENCH_DATASET_SCALE=1`
  - `TONBO_COMPACTION_BENCH_INGEST_BATCHES=640`
  - `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8`
  - `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10`
  - `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0`
  - `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0`

## Measured Results (Local, Scale 1)

| Scenario | Mean latency | p99 | Ops/sec | Read ops | Bytes read |
| --- | ---: | ---: | ---: | ---: | ---: |
| `read_baseline` | `36.456 ms` | `40.809 ms` | `27.430` | `240` | `3,715,192` |
| `read_compaction_quiesced` | `35.347 ms` | `36.968 ms` | `28.291` | `120` | `1,678,648` |

Relative change vs `read_baseline`:

- Mean latency: `-3.04%`
- p99 latency: `-9.41%`
- Throughput: `+3.14%`
- Read ops: `-50.00%`
- Bytes read: `-54.82%`

## Read-Path Phase Split (Dominance)

Each read iteration is divided into:

- `prepare` (`setup phase`): snapshot/scan setup + planning/pruning + stream construction
- `consume` (`execute phase`): stream polling/consumption/materialization

Measurement boundaries (directly timed in harness):

- `prepare` timer starts before `db.scan().stream().await` and stops right after stream creation:
  - `benches/compaction/common.rs` (`read_all_rows_local` / `read_all_rows_object_store`)
- `consume` timer wraps `while let Some(batch_result) = stream.next().await`:
  - `benches/compaction/common.rs` (`read_all_rows_local` / `read_all_rows_object_store`)

What this means operationally:

- `prepare` includes snapshot/plan/stream setup done in `ScanBuilder::stream()`:
  - `src/db/scan.rs` (`ScanBuilder::stream`, `plan_scan`, `execute_with_txn_scan`)
- `consume` includes per-batch stream execution work and wait time:
  - stream polling and any async wait,
  - SST/mutable/immutable stream work and merge/package execution
  - see `src/db/scan.rs` (`build_scan_streams`, `MergeStream`, `PackageStream` wiring)

| Scenario | mean_prepare_ns | mean_consume_ns | prepare share | consume share | mean batches/scan |
| --- | ---: | ---: | ---: | ---: | ---: |
| `read_baseline` | `12,483,031.875` | `23,962,171.500` | `34.25%` | `65.75%` | `2.0` |
| `read_compaction_quiesced` | `16,313,055.125` | `19,024,886.500` | `46.16%` | `53.84%` | `2.0` |

## Actionable Conclusions

1. Post-compaction reads are still **consume-path dominated**:
   - `read_compaction_quiesced`: `consume=53.84%`, `prepare=46.16%`.
2. Compaction clearly reduces consume work (`read_ops` and `bytes_read` drop), but total mean latency improves only modestly because prepare share grows relatively.
3. Optimization priority should target consume-side execution costs first:
   - scan stream execution/materialization,
   - decode/merge/filter path efficiency,
   - batch-shape and per-batch overhead reduction.
4. `prepare`/`consume` are harness phases, not hardware buckets; they guide where to instrument next (fine-grained timers in scan internals) before deciding CPU vs memory vs I/O micro-optimizations.

## Setup-Phase Internal Decomposition (Schema 6 Follow-up, 2026-03-02)

A follow-up run with the new internal setup-stage timings (`schema_version=6`) was executed:

- Artifact: `target/tonbo-bench/compaction_local-1772459539994-86663.json`
- Backend/scale: `local`, `1`
- Scenario pair: `read_baseline`, `read_compaction_quiesced`

Internal setup timings (means, milliseconds):

| Scenario | prepare_ms | snapshot_ms | plan_ms | build_streams_ms | merge_init_ms | package_init_ms | setup accounted |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `read_baseline` | `18.123` | `9.146` | `3.573` | `2.910` | `2.479` | `0.003` | `99.94%` |
| `read_compaction_quiesced` | `14.798` | `9.988` | `1.592` | `1.527` | `1.682` | `0.003` | `99.96%` |

Interpretation:

- The internal setup decomposition is consistent (`~100%` of `prepare` accounted).
- Compaction reduces setup work mainly in `plan_scan`, `build_scan_streams`, and `merge_init`.
- After compaction, `snapshot` remains the largest setup component.
- Next high-value optimization path for setup latency is snapshot/read-view overhead reduction.

## Object-Store Probe Wiring Check (Executed)

Object-store runs now execute and generate artifacts with probe counters.

Executed runs:

- `target/tonbo-bench/compaction_local-1772395193561-83275.json` (`wal_sync_policy=disabled`)
- `target/tonbo-bench/compaction_local-1772395493982-111701.json` (`wal_sync_policy=always`)

Observed issue in both runs:

- `read_baseline` has `rows_per_scan=2048` and non-zero read IO in summary.
- `read_after_first_compaction_observed` and `read_compaction_quiesced` have:
  - `rows_per_scan=0`
  - `rows_processed=0`
  - `summary.io.read_ops=0`, `summary.io.bytes_read=0`
- Result: post-compaction S3 directional deltas and phase-split percentages are not comparable to baseline output shape yet.

Conclusion for S3 from these runs:

- Probe wiring is active (object-store counters are populated where data is read).
- Dominance conclusion is currently blocked by a post-compaction read correctness anomaly (`0` rows), not by missing instrumentation.

Re-run command used:

```bash
TONBO_BENCH_BACKEND=object_store \
TONBO_BENCH_DATASET_SCALE=1 \
TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0 \
TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0 \
TONBO_BENCH_OBJECT_PREFIX=tonbo-dir-object_store-s1-$(date +%s) \
cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture
```
