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

## Object-Store Scale Sweep (2026-03-02, fresh SSO session)

Goal: find the largest dataset scale that completes under a fixed wall-clock cap using object-store backend.

Fixed configuration:

- `TONBO_BENCH_BACKEND=object_store`
- `TONBO_COMPACTION_BENCH_INGEST_BATCHES=640`
- `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0`
- `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0`
- command: `timeout 900s cargo bench -p tonbo --bench compaction_local -- read_compaction_quiesced --nocapture`

Observed results:

| Scale | Status | Exit code | Duration | Objects created | First object (UTC) | Last object (UTC) |
| --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `ok` | `0` | `383s` | `89` | `2026-03-02T16:32:43Z` | `2026-03-02T16:39:05Z` |
| `2` | `ok` | `0` | `688s` | `162` | `2026-03-02T16:39:06Z` | `2026-03-02T16:50:33Z` |
| `4` | `timeout` | `124` | `900s` | `276` | `2026-03-02T16:50:34Z` | `2026-03-02T17:05:33Z` |

Notes:

- `scale=4` failed due to wall-clock timeout (`124` from `timeout`), not panic.
- S3 object count/timestamps continued advancing during `scale=4`, so the run was making slow progress.
- No new benchmark artifact JSON was emitted for the timed-out run, so only external progress data (duration + object-store progression) is available for that cell.
- Current practical completion boundary (with this configuration/cap) is `scale=2`; first non-completing point is `scale=4`.

## Local Directional Rerun (2026-03-02, Documented Matrix Commands)

To refresh the disk (`local`) directional baseline used for interpretation, we reran the same documented matrix shape:

- `TONBO_BENCH_BACKEND=local`
- `TONBO_COMPACTION_BENCH_INGEST_BATCHES=640`
- `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8`
- `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10`
- `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0`
- `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0`

Runs:

- scale `1`:
  - Artifact: `target/tonbo-bench/compaction_local-1772481615158-731017.json`
  - Log: `/tmp/tonbo-local-s1-directional-20260302-210014.log`
- scale `10`:
  - Artifact: `target/tonbo-bench/compaction_local-1772481663386-732893.json`
  - Log: `/tmp/tonbo-local-s10-directional-20260302-210102.log`

### Measured Results (Baseline vs Quiesced)

| Scale | Baseline mean | Quiesced mean | Mean delta | Baseline p99 | Quiesced p99 | p99 delta | Throughput delta | Read ops delta | Bytes delta |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1` | `39.881 ms` | `32.891 ms` | `-17.53%` | `43.399 ms` | `36.546 ms` | `-15.79%` | `+21.25%` | `-50.00%` | `-54.82%` |
| `10` | `361.782 ms` | `152.021 ms` | `-57.98%` | `377.609 ms` | `157.616 ms` | `-58.26%` | `+138.05%` | `-89.00%` | `-92.38%` |

Interpretation update:

- Local directional behavior remains stable and strong:
  - compaction reduces read work (`read_ops`, `bytes_read`) and improves both mean and p99 latency.
- Scale amplification effect is reinforced:
  - larger scale (`10`) yields much larger read-work reduction and much larger latency gains than scale (`1`).
- These refreshed local cells are suitable as the current disk reference when interpreting object-store experiments.

### Setup Timing Notes (Local Rerun)

Setup remains ingest-dominated on local as scale increases.

- scale `1`, `read_compaction_quiesced` prep: `2610.409 ms`
  - ingest: `2008.016 ms` (`76.92%`)
  - wait-for-compaction-state: `367.669 ms` (`14.08%`)
- scale `10`, `read_compaction_quiesced` prep: `23523.353 ms`
  - ingest: `19566.319 ms` (`83.18%`)
  - wait-for-compaction-state: `2037.578 ms` (`8.66%`)

## Volume Snapshot Validation (2026-03-03, Local vs Object Store)

We added setup-phase physical volume snapshots in `schema_version=7` artifacts:

- `setup.volume_before_compaction`
- `setup.volume_ready`

Captured runs (scale `1`, documented directional knobs):

- object-store artifact: `target/tonbo-bench/compaction_local-1772532194050-236728.json`
- local artifact: `target/tonbo-bench/compaction_local-1772533336456-279622.json`

### `read_compaction_quiesced` volume comparison

| Backend | Before objects | Before total bytes | Before SST | Before WAL | Before manifest | Ready objects | Ready total bytes | Ready SST | Ready WAL | Ready manifest |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `object_store` | `26` | `10,986,081` | `10,959,869` | `189` | `26,023` | `31` | `13,798,954` | `12,033,411` | `1,735,869` | `29,674` |
| `local` | `27` | `12,725,915` | `10,959,869` | `1,735,869` | `30,177` | `31` | `13,803,505` | `12,033,411` | `1,735,869` | `34,225` |

Observations:

- SST bytes are effectively aligned across backends at both checkpoints.
- Ready-state total bytes are also very close (`13,798,954` vs `13,803,505`).
- Biggest before-state delta is WAL representation (`189` bytes on object-store vs
  `1,735,869` bytes on local), reflecting backend-specific WAL publication/layout timing.
- Manifest bytes differ slightly but stay in a narrow band; this is expected from run timing and
  segment churn.

Draft conclusion:

- Physical volume instrumentation works and is useful for interpreting latency/IO results.
- Backend comparisons should use the per-component breakdown (`sst`, `wal`, `manifest`), not just
  `total_bytes`, because `wal` behavior differs by backend even when SST bytes match.

## Midpoint Scale Extension (2026-03-03, Scale 4 and 7)

Goal: add intermediate scales between `1` and `10` for directional tracking with the tuned profile
that avoids small-operation amplification stalls.

Profile used:

- `TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=192`
- `TONBO_COMPACTION_BENCH_INGEST_BATCHES=214`
- `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8`
- `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10`
- `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0`
- `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0`
- scenario: `read_compaction_quiesced`

### Results captured

| Backend | Scale | Status | Artifact | Mean latency | p99 | Ops/sec | SST before -> ready | Volume before -> ready |
| --- | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `local` | `4` | `ok` | `target/tonbo-bench/compaction_local-1772535077913-26118.json` | `85.058 ms` | `91.843 ms` | `11.757` | `13 -> 8` | `20,882,160 -> 21,960,868` |
| `local` | `7` | `ok` | `target/tonbo-bench/compaction_local-1772535098543-27330.json` | `85.427 ms` | `86.232 ms` | `11.706` | `23 -> 8` | `36,883,843 -> 40,125,799` |
| `object_store` | `4` | `ok` | `target/tonbo-bench/compaction_local-1772534023868-309320.json` | `376.231 ms` | `392.005 ms` | `2.658` | `13 -> 8` | `15,376,715 -> 21,953,587` |
| `object_store` | `7` | `ok` | `target/tonbo-bench/compaction_local-1772537127942-121689.json` | `389.854 ms` | `396.812 ms` | `2.565` | `23 -> 18` | `27,243,271 -> 39,027,048` |

Transient retry note:

- During 2026-03-03 reruns, several object-store attempts failed before scenario prep with:
  - `failed to read wal metadata ... error sending request for url (... list-type=2 ...)`
- Direct `aws s3api list-objects-v2` checks during those windows also failed intermittently.
- A later retry completed successfully for `object_store` scale `7` (artifact above), indicating an
  environment/network flap rather than a deterministic scale-specific engine failure.

### Interpretation from midpoint extension

- The tuned profile provides stable local scaling at `4` and `7` and keeps compaction transitions
  (`sst_before -> sst_ready`) consistent.
- Object-store remains significantly slower than local at the same scales (`4` and `7`), while
  producing comparable structural transitions (same order of SST evolution and volume growth).
- This supports the broader Tonbo performance concern: small operation count and remote round-trips
  dominate latency in object-store mode more than raw data volume alone.

### What changed to make the benchmark workable

1. Scenario-filter-aware preparation:
   - benchmark now prepares only selected scenarios for filtered runs (instead of always preparing
     all baseline scenarios first).
2. Tuned ingest shape to reduce small operation amplification:
   - switched from many tiny ingest calls (`64 x 640`) to larger batches (`192 x 214`) while
     preserving compaction intent and scale behavior.
3. Added setup-phase diagnostics:
   - explicit timings for `open`, `ingest`, `wait_for_compaction_state`, `read_all_rows`.
4. Added before/after physical volume snapshots:
   - `setup.volume_before_compaction` and `setup.volume_ready` with
     `sst/wal/manifest/total` byte breakdown.

## Logical Live-Set Refresh (2026-03-03, Schema 8)

Goal: switch compaction volume accounting to manifest-visible logical live data so "after
compaction" reflects read-visible state rather than transient prefix bytes.

Profile used:

- `TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=192`
- `TONBO_COMPACTION_BENCH_INGEST_BATCHES=214`
- `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8`
- `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10`
- `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0`
- `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0`
- scenario: `read_compaction_quiesced`

### Artifacts (this refresh)

- `target/tonbo-bench/compaction_local-1772546828639-26095.json` (`local`, scale `3`)
- `target/tonbo-bench/compaction_local-1772546842564-26583.json` (`local`, scale `4`)
- `target/tonbo-bench/compaction_local-1772546862962-27348.json` (`local`, scale `7`)
- `target/tonbo-bench/compaction_local-1772546886395-29076.json` (`local`, scale `10`)
- `target/tonbo-bench/compaction_local-1772547614126-63019.json` (`object_store`, scale `3`)
- `target/tonbo-bench/compaction_local-1772547714367-66562.json` (`object_store`, scale `4`)
- `target/tonbo-bench/compaction_local-1772547842506-73264.json` (`object_store`, scale `7`)
- `target/tonbo-bench/compaction_local-1772548041614-80748.json` (`object_store`, scale `10`)

Object-store execution status:

- Attempted scales: `3`, `4`, `7`, `10`
- Completed with profile-backed credentials and unique prefixes.

### Results

| Backend | Scale | Status | Mean latency | p99 | Ops/sec | SST before -> ready | Logical before -> ready | Physical before -> ready |
| --- | ---: | --- | ---: | ---: | ---: | --- | --- | --- |
| `local` | `3` | `ok` | `58.454 ms` | `63.993 ms` | `17.108` | `10 -> 5` | `11,797,089 -> 5,792,031` | `15,951,663 -> 17,028,823` |
| `local` | `4` | `ok` | `86.503 ms` | `93.041 ms` | `11.560` | `13 -> 8` | `15,336,272 -> 9,331,214` | `20,882,160 -> 21,960,868` |
| `local` | `7` | `ok` | `83.925 ms` | `88.189 ms` | `11.915` | `23 -> 8` | `27,133,554 -> 9,117,998` | `36,883,843 -> 40,125,799` |
| `local` | `10` | `ok` | `81.851 ms` | `85.603 ms` | `12.217` | `33 -> 8` | `38,930,832 -> 8,904,778` | `52,924,919 -> 58,338,067` |
| `object_store` | `3` | `ok` | `405.980 ms` | `429.655 ms` | `2.463` | `10 -> 5` | `11,797,089 -> 5,792,031` | `11,823,466 -> 17,024,599` |
| `object_store` | `4` | `ok` | `390.380 ms` | `410.206 ms` | `2.562` | `13 -> 8` | `15,336,272 -> 9,331,214` | `15,377,079 -> 21,954,007` |
| `object_store` | `7` | `ok` | `393.249 ms` | `408.938 ms` | `2.543` | `23 -> 18` | `27,133,554 -> 21,128,496` | `27,245,479 -> 39,029,728` |
| `object_store` | `10` | `ok` | `381.587 ms` | `399.655 ms` | `2.621` | `33 -> 23` | `38,930,832 -> 26,920,523` | `39,149,933 -> 55,065,425` |

### Metric Semantics (explicit)

- Physical (`setup.volume_*`): recursive prefix snapshot; includes transient and obsolete objects.
- Logical (`setup.logical_*`): manifest-referenced live set only; excludes unreferenced files.
- `ready > before` on physical bytes is possible and expected under write-new-before-GC behavior.
- Logical live-set is the primary compaction decision metric because it matches read-visible state.

### Notes on Logical Components

- `setup.logical_*.sst_bytes` is computed from manifest-visible SST entries (version metadata).
- `setup.logical_*.wal_bytes` and `setup.logical_*.manifest_bytes` are currently emitted as `null`
  best-effort placeholders in this harness version.
- `setup.logical_*.total_bytes` sums only included logical components.
