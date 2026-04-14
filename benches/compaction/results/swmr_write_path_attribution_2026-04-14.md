# SWMR Write-Path Attribution (`swmr_write_path_attribution_2026-04-14`)

## Context

This note captures a narrow follow-up on top of `feat/swmr-benchmark-first-pass`.

The branch already established that the next useful step was instrumentation of
the write/setup path rather than more topology hunting. This note records the
first local attribution pass for the `swmr_gb_scale_mixed` workload at `~1 GB`
logical state.

Scope:

- backend: `local`
- workload: `swmr_gb_scale_mixed`
- logical target: `1 GB`
- goal: explain foreground writer latency inside Tonbo
- non-goal: claim final object-store percentages or expand the benchmark matrix

## Command

```bash
TONBO_BENCH_BACKEND=local \
TONBO_SWMR_BENCH_LOGICAL_GB=1 \
TONBO_SWMR_BENCH_ROWS_PER_BATCH=256 \
TONBO_SWMR_BENCH_PAYLOAD_BYTES=4096 \
TONBO_SWMR_BENCH_LIGHT_SCAN_LIMIT=256 \
TONBO_SWMR_BENCH_HEAVY_SCAN_LIMIT=2048 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=1 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
cargo bench --bench compaction_local -- swmr_gb_scale_mixed --nocapture
```

Artifact:

- `target/tonbo-bench/compaction_local-1776169451877-1118580.json`

## Result

Writer mean latency observed by the benchmark:

- `writer_latency_ns.mean = 674.26 ms`

Profiled foreground writer path inside Tonbo:

- `writer_path_ns.mean_total_ns = 624.13 ms`

Profiled writer-path breakdown:

- `minor_compaction`: `229.18 ms` (`36.7%` of profiled writer path)
- `wal_append`: `176.93 ms` (`28.3%`)
- `mutable_insert`: `109.17 ms` (`17.5%`)
- `seal`: `60.04 ms` (`9.6%`)
- `wal_commit`: `41.38 ms` (`6.6%`)
- `partition`: `4.89 ms` (`0.8%`)

Grouped view:

- durability (`wal_append + wal_commit`): `218.31 ms` (`35.0%`)
- post-insert maintenance (`seal + minor_compaction`): `289.22 ms` (`46.3%`)
- mutable insert: `109.17 ms` (`17.5%`)

## Interpretation

This result does **not** mean minor compaction runs before the WAL.

It means the current autocommit ingest path returns only after all of these
foreground phases complete:

1. WAL durable ack
2. mutable memtable insert
3. seal decision / seal
4. opportunistic minor compaction

So the current user-visible write latency is not a WAL-only ack path. It is a
durability-plus-maintenance path.

The local attribution supports a narrower next-step conclusion:

- the current SWMR write latency is materially shaped by both WAL durability and
  inline minor-compaction work
- further topology hunting alone is unlikely to explain that structure
- the next meaningful design question is whether the default ingest contract
  should continue to await minor compaction, or whether that work should move
  behind a background publish/flush boundary

## Limits

- This note uses a local `~1 GB` run only.
- It does not prove that object-store cells have the exact same percentages.
- It does not replace object-store request accounting or manifest-path
  attribution work.
- It should be read as local attribution, not as the final performance
  explanation for all backends.
