# Compaction Local Baseline (`compaction_local_v1`)

## TL;DR

- This benchmark is new in Tonbo; there is no older `compaction_local` baseline for like-for-like improvement claims.
- Baseline ID established for future comparisons: `compaction_local_v1`.
- Stable benchmark IDs:
  - `compaction_local/read_baseline`
  - `compaction_local/read_post_compaction`

## Run Context

- Date (UTC): `2026-02-23T19:02:42Z`
- Git commit: `3305555`
- Branch: `feat/bench-compaction-local-criterion-json`
- OS: `Linux 6.17.0-14-generic x86_64 GNU/Linux`
- CPU: `Intel(R) Core(TM) Ultra 9 185H` (`22` logical CPUs, `2` threads/core)
- Rust: `rustc 1.90.0 (1159e78c4 2025-09-14)`
- Cargo: `cargo 1.90.0 (840b83a10 2025-07-30)`
- Backend: local filesystem (`LocalFs`)
- WAL policy: `TONBO_COMPACTION_BENCH_WAL_SYNC=always`

## Command

```bash
TONBO_COMPACTION_BENCH_INGEST_BATCHES=768 \
TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=96 \
TONBO_COMPACTION_BENCH_KEY_SPACE=4096 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=64 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=30 \
TONBO_COMPACTION_BENCH_WAL_SYNC=always \
cargo bench --bench compaction_local -- --save-baseline compaction_local_v1
```

## Resolved Config

- `ingest_batches`: `768`
- `rows_per_batch`: `96`
- `key_space`: `4096`
- `artifact_iterations`: `64`
- `criterion_sample_size`: `30`
- `compaction_wait_timeout_ms`: `20000`
- `compaction_poll_interval_ms`: `50`
- `compaction_periodic_tick_ms`: `200`
- `seed`: `584`
- `wal_sync_policy`: `always`

## Scenario Setup Snapshot

From `target/tonbo-bench/compaction_local-1771873339494-3585814.json`.

| Scenario | Rows/scan | SSTs before compaction | Levels before | SSTs ready | Levels ready |
| --- | ---: | ---: | ---: | ---: | ---: |
| `read_baseline` | 4,096 | 12 | 1 | 12 | 1 |
| `read_post_compaction` | 4,096 | 12 | 1 | 7 | 2 |

## Baseline Latency (Criterion)

From `target/criterion/compaction_local/*/compaction_local_v1/estimates.json`.

| Benchmark ID | Mean latency | 95% CI |
| --- | ---: | ---: |
| `compaction_local/read_baseline` | `115.934 ms` | `[113.054 ms, 118.984 ms]` |
| `compaction_local/read_post_compaction` | `109.804 ms` | `[108.029 ms, 111.638 ms]` |

## Artifact Summary (64 Iterations)

From `target/tonbo-bench/compaction_local-1771873339494-3585814.json`.

| Scenario | Ops/sec | Rows/sec | p50 latency | p95 latency | Mean latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| `read_baseline` | `9.021` | `36,951.80` | `107.595 ms` | `127.270 ms` | `110.846 ms` |
| `read_post_compaction` | `9.078` | `37,182.21` | `106.839 ms` | `127.945 ms` | `110.160 ms` |

## Artifacts

- JSON artifact:
  - `target/tonbo-bench/compaction_local-1771873339494-3585814.json`
- Criterion baseline files:
  - `target/criterion/compaction_local/read_baseline/compaction_local_v1/estimates.json`
  - `target/criterion/compaction_local/read_post_compaction/compaction_local_v1/estimates.json`
- HTML reports:
  - `target/criterion/compaction_local/report/index.html`

## Interpretation

- This document sets the first reproducible baseline for `compaction_local`.
- Extended matrix and I/O probe scenarios from `feat/bench-compaction-io-probes` are intentionally out of scope for this baseline.
- Do not claim improvement/regression in this PR; use this baseline for future comparisons:

```bash
cargo bench --bench compaction_local -- --baseline compaction_local_v1
```
