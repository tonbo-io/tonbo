# Benchmark Results

## Context

This branch benchmarks one narrow but important behavior: `read_compaction_quiesced`, meaning
read performance after compaction has completed and the replacement SST output is read-visible.

This matters because Tonbo is not trying to be only a local analytical engine. Tonbo is intended
to operate as a live analytical engine on top of object storage, where users expect:

- low-latency reads on fresh data,
- interleaved reads and writes,
- parallel access from stateless compute,
- manageable cost even when ingestion creates many small objects or pages.

For those users, compaction is only valuable if it improves real read behavior, not just file
layout. This report answers the current question first and separates what is already validated from
what is still blocked.

## Benchmark Program Context

This report documents one current engine-layer scenario inside a broader
benchmark program. The companion roadmap lives in `docs/benchmark_program.md`.

That broader program makes three framing changes:

- benchmark layers are split into `micro`, `engine`, and `surface`, so Tonbo is
  evaluated as both a storage engine and a live analytical system,
- artifact design should evolve beyond one latency table to include topology,
  logical and physical work, request economics, and engine-pressure metrics,
- scenario priority should follow the user journey first: freshness,
  durability, object-store path cost, scale effects, maintenance impact, and
  cleanup economics.

This current document remains intentionally narrower: it validates
`read_after_compaction` first, then points to the next scenarios and
instrumentation work needed to grow into that larger program.

### Local vs Standard S3 Topology Follow-up

As of March 30, 2026, this branch now has one valid first-pass comparison for
`read_compaction_quiesced` between local storage and standard S3 from a
non-EC2 host, plus one partially blocked higher-scale follow-up.

Reference run note:

- `benches/compaction/results/compaction_topology_2026-03-30.md`

Reference artifacts:

- valid local `scale=1`:
  `target/tonbo-bench/compaction_local-1774884026526-2253191.json`
- valid standard S3 `scale=1`:
  `target/tonbo-bench/compaction_local-1774884042845-2253870.json`
- invalid standard S3 `scale=4` after extended compaction wait:
  `target/tonbo-bench/compaction_local-1774884882909-2283436.json`

What changed before these runs:

- the earlier object-store benchmark path used a probed S3 wrapper in the DB
  read path,
- that path produced invalid object-store artifacts during benchmark setup:
  - `rows_per_scan = 0`
  - `rows_processed = 0`
  - `read_ops = 0`
- a focused public-API S3 reopen-with-compaction test passed on the native
  object-store path,
- the benchmark was therefore switched back to the native object-store builder
  path for correctness.

Tradeoff of that benchmark fix:

- local benchmark I/O counters remain valid,
- object-store row visibility and latency are now valid again for the passing
  cells,
- object-store request counters (`read_ops`, `bytes_read`) should currently be
  treated as unsupported until the probe wrapper is fixed.

Valid `scale=1` comparison from this host:

| Cell | Mean | p95 | Rows Processed | Notes |
| --- | ---: | ---: | ---: | --- |
| `local, scale=1` | `41.19 ms` | `47.31 ms` | `16,384` | valid |
| `standard S3, scale=1` | `1.678 s` | `1.726 s` | `16,384` | valid |

Interpretation:

- On this non-EC2 host, standard S3 is about `40x` slower than local on mean
  latency for this small `read_compaction_quiesced` cell.
- The standard-S3 cost is dominated by setup:
  - mean prepare: about `1.652 s`
  - mean consume: about `25.7 ms`
  - mean snapshot stage alone: about `537 ms`
- This means the branch is currently paying a large fixed object-store cost on
  this topology before it starts streaming rows.

Current block at `scale=4`:

- local `scale=4` completed with valid rows,
- standard S3 `scale=4` remained invalid even after increasing the compaction
  wait timeout from `20 s` to `120 s`,
- the run finished, but returned:
  - `setup.rows_per_scan = 0`
  - `summary.rows_processed = 0`

Current conclusion:

- We now have a real local-versus-standard-S3 comparison at `scale=1`.
- We do not yet have a trustworthy standard-S3 `scale=4` comparison from this
  host.
- We still do not have EC2 or S3 Express numbers.

Practical next step:

- move to an EC2 instance in-region and run `ec2_s3` first,
- then retry `scale=4`,
- then attempt S3 Express from an instance in the same AZ as the directory
  bucket.

### SWMR Pinned-Snapshot Follow-up

As of March 27, 2026, the pinned-reader validity gap in the earlier local
harness path had been explained and fixed.

Root cause:

- the first-pass harness did not hold a true pinned snapshot object,
- instead it reconstructed pinned readers via
  `snapshot_at(latest_manifest_version.timestamp)`,
- after preload, the held read timestamp was `914` while the latest manifest
  publish timestamp was only `14`,
- that meant the reconstructed pinned path only saw the earliest manifest-backed
  prefix of preload data,
- the benchmark's heavy reader range starts at `warm-00000000`, and those warm
  keys were not present at manifest timestamp `14`, so `pinned_heavy` returned
  `0` rows for a harness reason, not a performance reason.

The updated harness now:

- holds a real snapshot object for pinned readers,
- records both the held snapshot timestamp and the manifest-version timestamp in
  the artifact,
- records expected rows per reader class from the held pinned snapshot,
- records the comparative row counts from manifest-version reconstruction so the
  old mismatch is visible in machine-readable output,
- fails the scenario if a reader expected to return rows instead returns `0`.

Reference rerun note:

- `benches/compaction/results/swmr_gb_scale_2026-03-27.md`

Updated local `~1 GB` cell from artifact
`target/tonbo-bench/compaction_local-1774592990634-1898420.json`:

| Cell | Mean Step (ms) | p95 Step (ms) | Throughput | Writer Mean (ms) | Head Light (ms) | Head Heavy (ms) | Pinned Light (ms) | Pinned Heavy (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `local, ~1 GB logical` | `134.47` | `145.96` | `59.02 Krows/s` | `104.77` | `4.59` | `13.44` | `3.90` | `7.78` |

Pinned-reader validity evidence now visible in the artifact:

- held pinned snapshot mode: `held_snapshot`
- held pinned snapshot timestamp: `914`
- manifest-version timestamp previously used for reconstruction: `14`
- held snapshot expected rows per scan:
  - `pinned_light = 256`
  - `pinned_heavy = 2048`
- manifest-version reconstruction rows per scan:
  - `pinned_light = 256`
  - `pinned_heavy = 0`

Current conclusion:

- `pinned_heavy = 0` was a benchmark harness bug, not an engine bug proven by
  this investigation.
- The local pinned-reader path is now trustworthy enough for further local SWMR
  work because it uses a true held snapshot and enforces non-empty invariants.
- S3 / `10 GB` expansion should still wait until the same pinned-reader
  semantics are exercised in those cells and we confirm the path remains stable
  under their storage/topology costs.

### SWMR Correctness-Gated Follow-up

As of March 28, 2026, the SWMR harness adds a second layer of correctness
gating so future local and object-store numbers are validity-checked against
the intended scan shape instead of only checking for non-empty results.

Reference rerun note:

- `benches/compaction/results/swmr_gb_scale_2026-03-28.md`

Reference artifact:

- `target/tonbo-bench/compaction_local-1774729594256-2047627.json`

The harness now records, per reader class:

- key band (`hot` or `warm`)
- validation model
- expected rows per scan
- expected first key
- expected last key
- expected key fingerprint

Validation model used:

- `pinned_light`, `pinned_heavy`, and `head_heavy` use
  `exact_shape_stable`
- `head_light` uses `count_and_key_band`

Why `head_light` is treated differently:

- the workload intentionally deletes from the hot range while the benchmark is
  running,
- that can legitimately move the exact first `256` visible hot keys without
  meaning the harness scanned the wrong slice,
- the useful invariant there is therefore:
  - rows stay at the expected limit
  - every returned key remains in the intended `hot-*` family

Updated local `~1 GB` cell:

| Cell | Mean Step (ms) | p95 Step (ms) | Throughput | Writer Mean (ms) | Head Light (ms) | Head Heavy (ms) | Pinned Light (ms) | Pinned Heavy (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `local, ~1 GB logical` | `168.61` | `237.65` | `47.07 Krows/s` | `139.77` | `4.88` | `13.96` | `1.76` | `8.21` |

What the new validity fields prove in that rerun:

- `head_heavy` matched `2048` rows every iteration, stayed in the `warm-*`
  band, and kept one fingerprint for the whole measured window.
- `pinned_light` matched its held-snapshot row count, first/last keys, and key
  fingerprint for the full run.
- `pinned_heavy` matched its held-snapshot row count, first/last keys, and key
  fingerprint for the full run.
- `head_light` remained valid under the declared model:
  - `256` rows every iteration
  - every key stayed in the `hot-*` family
  - the fingerprint changed across iterations, which is expected because hot
    deletes can reshape the leading visible slice

What the manifest-reconstruction comparison still proves:

- the old `snapshot_at(manifest_version.timestamp)` substitute is invalid for
  the heavy readers in this workload shape:
  - `head_heavy = 0 rows`
  - `pinned_heavy = 0 rows`
- the artifact now records those failures as full shape mismatches, not just as
  a non-zero/zero difference.

Object-store parity and cleanup:

- The same SWMR correctness checks are now applied on the object-store backend
  in code for the `1 GB` cell.
- No object-store `1 GB` run was executed in this session because no
  `TONBO_S3_*` or `AWS_*` credentials were present in the benchmark shell.
- The benchmark now performs best-effort workspace cleanup after the run:
  - local/object-store scenario data under the benchmark workspace or prefix is
    removed
  - the JSON artifact under `target/tonbo-bench/` remains

Current conclusion after the March 28 follow-up:

- Local `1 GB` SWMR numbers are now guarded by a more defensible correctness
  contract than simple non-empty scans.
- These checks prove stable pinned-snapshot keysets and stable warm-band HEAD
  shape for the measured run.
- These checks still do not prove reopen-time pinned historical durability for
  the benchmark path, and they do not yet prove object-store behavior until the
  `1 GB` object-store cell is executed.

### SWMR Object-Store 1 GB

As of March 29, 2026, this branch now includes both:

- one longer object-store `1 GB` benchmark session that also ran additional
  benchmark scenarios, and
- one clean isolated object-store `1 GB` `swmr_gb_scale_mixed` cell

Reference run note:

- `benches/compaction/results/swmr_gb_scale_2026-03-29.md`

Reference artifacts:

- mixed long session:
  `target/tonbo-bench/compaction_local-1774733239775-2053617.json`
- clean isolated object-store SWMR cell:
  `target/tonbo-bench/compaction_local-1774810066470-2135436.json`

Why there are two March 29 object-store artifacts:

- the first successful object-store session also ran
  `read_while_compaction` and
  `write_throughput_vs_compaction_frequency`, so it is useful as a longer
  `1 GB` object-store benchmark session but not as a pure SWMR-only artifact,
- the follow-up rerun explicitly disabled those scenario families so only
  `swmr_gb_scale_mixed` executed

Clean isolated object-store `1 GB` SWMR numbers:

| Cell | Mean Step (s) | p95 Step (s) | Throughput | Writer Mean (s) | Head Light (s) | Head Heavy (s) | Pinned Light (s) | Pinned Heavy (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `object_store, ~1 GB logical` | `22.99` | `27.06` | `345.13 rows/s` | `14.75` | `1.94` | `3.50` | `0.55` | `2.26` |

Correctness outcome in the clean object-store cell:

- all four reader classes were `valid = true`
- `head_heavy`, `pinned_light`, and `pinned_heavy` each held one key
  fingerprint across the measured window
- `head_light` remained valid under `count_and_key_band`
- the manifest-version reconstruction remained invalid for the heavy readers:
  - `head_heavy = 0 rows`
  - `pinned_heavy = 0 rows`

This matters because it means the held-snapshot correctness work was not only a
local-filesystem fix; it also changes whether object-store SWMR numbers are
trustworthy.

Comparison with the longer mixed object-store session:

- mixed long session SWMR mean step: `24.03 s`
- clean isolated SWMR mean step: `22.99 s`
- mixed long session writer mean: `15.88 s`
- clean isolated SWMR writer mean: `14.75 s`

Interpretation:

- the clean isolated object-store cell and the longer mixed object-store
  session tell the same SWMR story:
  - object-store `1 GB` SWMR is correctness-valid in this environment
  - writer latency dominates the mixed-step budget
  - object-store is dramatically slower than local for this workload shape
- the longer mixed session did not materially change the SWMR conclusion; its
  value was mostly operational:
  - it exposed how easily a broad benchmark session can accidentally include
    unrelated scenario families,
  - it exercised remote cleanup and remote-failure behavior over a longer wall
    time,
  - it acted more like a light soak run than a better SWMR decision artifact

What this implies for future benchmarking:

- for routine iteration and PR-facing evidence, the isolated SWMR cell is the
  more valuable benchmark product:
  - it is easier to interpret,
  - it finishes sooner,
  - it reaches the same SWMR conclusion as the longer mixed session
- longer `1 GB` object-store sessions still have occasional value as soak or
  operational checks, but they should be treated as a separate benchmark class
  rather than the default path for answering the SWMR question

Current SWMR conclusion across local and object-store `1 GB` cells:

- The branch now has correctness-gated `1 GB` evidence on both local and
  object-store backends.
- The benchmark now provides trustworthy `1 GB` SWMR numbers within its current
  scope.
- The local and object-store cells support the same qualitative result:
  HEAD and pinned readers remain shape-valid, and writer latency dominates the
  mixed-step budget on both backends.
- The main performance lesson from the clean object-store run is not a reader
  correctness problem; it is that Tonbo is currently hitting a very large fixed
  write-path cost wall in this environment.

What this PR teaches about Tonbo:

- Tonbo's current SWMR read semantics are stable enough to benchmark at `1 GB`
  on both local and object-store backends.
- Pinned snapshots are not the performance bottleneck exposed by these runs.
- The main bottleneck exposed by object-store SWMR at `1 GB` is fixed remote
  write-path cost, with read cost materially lower but still shaped by large
  remote setup overheads.
- Longer `1 GB` object-store sessions did not materially change the SWMR
  conclusion relative to the isolated clean SWMR cell, so the smaller scoped
  run is the better default PR artifact.

Suggested follow-up actions:

- First: instrument and investigate the object-store write path so we can say
  exactly where the fixed cost is going.
  Candidate buckets include WAL upload/publication, manifest publication, SST
  upload/commit, and any benchmark-side setup overhead folded into write steps.
- Add an optional artifact-only benchmark mode that skips Criterion when the
  goal is to get one remote `1 GB` answer quickly rather than a statistical
  benchmark report.
- Keep isolated SWMR object-store runs as an explicit optional mode so broader
  benchmark sessions do not accidentally contaminate SWMR artifacts.
- Add topology metadata to object-store artifacts so remote numbers can be read
  in the context of runner region, bucket region, and network placement.
- Keep longer object-store runs as an optional soak-style benchmark class
  rather than the default way to answer the SWMR question.

## Scenario Under Test

Current PR-facing scenario:

- `read_baseline`: read after ingest, before waiting for compaction readiness.
- `read_compaction_quiesced`: read after the scenario reaches compaction-ready state and new
  compaction output is visible.

Question being answered:

- does compaction reduce read work and improve latency?
- how does that trend change as the live dataset grows?
- do small workloads stay dominated by fixed overhead?

What this scenario does **not** answer yet:

- interleaved reads and writes,
- parallel reader pressure during compaction,
- GC lag under sustained traffic,
- deployment-topology effects such as in-VPC vs public object-store access.

## How To Read The Current Numbers

The harness still uses `TONBO_BENCH_DATASET_SCALE`, but `scale` is only a workload control knob.
For interpretation, the primary x-axis should be **bytes**, specifically the logical live-set before
compaction:

- `setup.logical_before_compaction.total_bytes`
- `setup.logical_ready.total_bytes`

Why bytes are primary:

- they map better to user-visible dataset size than the synthetic `scale` knob,
- they make it easier to identify constant costs,
- they let us compare trend lines across future scenario shapes.

Physical prefix bytes are still reported, but only as a debugging view:

- physical bytes can increase at `ready` because compaction publishes replacement SSTs before GC
  removes obsolete files,
- logical live-set is the correct compaction-effectiveness metric because it reflects the
  manifest-visible read set.

## Environment And Limits

Profile used for the main summary below:

- `TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=192`
- `TONBO_COMPACTION_BENCH_INGEST_BATCHES=214`
- `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8`
- `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10`
- `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0`
- `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0`
- scenario: `read_compaction_quiesced`

Backend legend:

- `local`: Tonbo on local filesystem (`LocalFs`) on the benchmark runner.
- `object_store`: Tonbo object-store path from the same local runner to S3-compatible remote
  storage in the closest configured S3 region.

Known environment caveats:

- The object-store runs in this report were executed from a local machine in Berlin against the
  closest configured AWS region, Frankfurt (`eu-central-1`).
- This is therefore a developer-machine-to-nearest-region measurement, not an in-VPC or
  production-colocated deployment measurement.
- The PR summary still does not capture deployment metadata as first-class artifact fields, so
  future reports should record topology explicitly when comparing environments.

## Current Summary

### Local

| Scale | Live Before (MiB) | Live Ready (MiB) | Mean (ms) | p99 (ms) | Ops/s | SST Before -> Ready |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `3` | `11.25` | `5.52` | `58.454` | `63.993` | `17.108` | `10 -> 5` |
| `4` | `14.63` | `8.90` | `86.503` | `93.041` | `11.560` | `13 -> 8` |
| `7` | `25.88` | `8.70` | `83.925` | `88.189` | `11.915` | `23 -> 8` |
| `10` | `37.13` | `8.49` | `81.851` | `85.603` | `12.217` | `33 -> 8` |

### Object Store

| Scale | Live Before (MiB) | Live Ready (MiB) | Mean (ms) | p99 (ms) | Ops/s | SST Before -> Ready |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `3` | `11.25` | `5.52` | `405.980` | `429.655` | `2.463` | `10 -> 5` |
| `4` | `14.63` | `8.90` | `390.380` | `410.206` | `2.562` | `13 -> 8` |
| `7` | `25.88` | `20.15` | `393.249` | `408.938` | `2.543` | `23 -> 18` |
| `10` | `37.13` | `25.67` | `381.587` | `399.655` | `2.621` | `33 -> 23` |

### Trend View

The current table should be read as an early trend line:

- local latency improves or stays flat while logical live-set shrinks strongly,
- object-store latency remains in a narrow `~382-406 ms` band despite logical live-set reduction,
- that narrow object-store band is the strongest current signal of a large constant-cost floor.

## Validated Conclusions

### Validated: local backend

1. Compaction reduces read work and improves read latency on local storage.
2. The gain gets larger as live data volume grows.
3. Compaction effectiveness is visible both structurally and operationally:
   - fewer SSTs,
   - smaller logical live-set,
   - lower read IO,
   - lower mean and p99 latency.

Supporting local directional evidence from the refreshed matrix:

- scale `1`: mean latency `39.881 ms -> 32.891 ms` (`-17.53%`)
- scale `10`: mean latency `361.782 ms -> 152.021 ms` (`-57.98%`)
- scale `10`: read ops `-89.00%`, bytes read `-92.38%`

### Validated: small workloads expose fixed cost

The local phase-split runs explain why small operations show weaker end-to-end gains than larger
ones.

- Compaction reduces consume-path work after read amplification drops.
- But at small scale, setup cost remains a large fraction of total latency.
- This means compaction can be working correctly while total latency barely moves.

In the validated local phase split:

- `read_baseline`: `prepare=34.25%`, `consume=65.75%`
- `read_compaction_quiesced`: `prepare=46.16%`, `consume=53.84%`

Interpretation:

- the engine saves work after compaction,
- but the fixed setup floor becomes more visible,
- so small operations remain expensive unless setup overhead also falls.

### Validated: physical bytes are not the primary success metric

Ready-state physical bytes can increase even when compaction is beneficial, because Tonbo currently
publishes new SSTs before reclaiming old physical objects.

Therefore:

- use logical live-set for compaction effectiveness,
- use physical bytes for GC/debug interpretation.

## Inconclusive Or Blocked Conclusions

### Inconclusive: object-store root cause

The current object-store runs show a strong latency floor, but they do **not** yet prove where that
time is spent.

What we can say:

- object-store `read_compaction_quiesced` stays around `~382-406 ms`,
- this remains much slower than local at comparable logical live-set sizes,
- the flat band suggests constant overhead dominates at these sizes.

What we cannot yet say:

- whether the main blocker is network path, object-store request overhead, snapshot/setup cost,
  small-object amplification, or deployment topology.

### Blocked: broad product scenario claims

This branch validates `read after compaction` only. It does not yet validate the live-engine
scenarios that matter most for Tonbo positioning:

- interleaved reads and writes,
- parallel readers during background compaction,
- small-page ingestion under object-store request pressure,
- GC cost window after compaction publish,
- durable streaming-style ingestion patterns.

## Implications For Tonbo Positioning

The current scenario supports one specific claim:

- Tonbo compaction already improves read behavior on local storage and should continue to matter for
  larger live analytical workloads.

The current scenario does **not** yet support a broad external claim that Tonbo is already tuned for
serverless object-store analytics under live mixed traffic.

The gap between those statements is exactly what the next scenarios must close.

For positioning, the target comparison set is not only embedded analytical engines. It is also
systems that care about object-store-native or serverless execution economics, including:

- SlateDB,
- RocksDB / LevelDB for compaction and test-design inspiration,
- Turso / libSQL and adjacent serverless database products for operational scenario selection.

The goal is to show where Tonbo fits as a **live analytical engine**:

- Arrow-native,
- object-storage-first,
- read/write parallelism friendly,
- useful for fresh, continuously changing data rather than only batch-style offline analysis.

## Broader Roadmap

The broader benchmark roadmap, scenario priority, and instrumentation plan now
live in `docs/benchmark_program.md`.

This report should stay narrower:

- it validates the current `read_after_compaction` scenario,
- it explains what was learned from that scenario,
- it records what is still inconclusive,
- and it points to the broader program for the next scenario definitions and
  ordering.

## Local Follow-up Issues

- `bench: plot read-after-compaction latency against logical live-set bytes`
- `bench: record deployment metadata in compaction benchmark artifacts and reports`
- `bench: add object-store request accounting to compaction benchmark artifacts`
- `bench: add read-after-compaction byte-sweep scenario`
- `bench: measure GC lag and physical amplification after compaction publish`

## Supporting Reports

Treat the documents below as appendices and raw evidence, not as the primary PR narrative:

- `benches/compaction/results/compaction_local_baseline.md`
- `benches/compaction/results/compaction_directional_matrix_2026-02-27.md`
- `benches/compaction/results/compaction_directional_phase_split_2026-03-01.md`
