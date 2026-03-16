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
