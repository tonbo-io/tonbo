# Compaction Local IO-Probes Baseline (`compaction_local_io_probes_v1`)

## TL;DR

- Baseline ID established for this scenario matrix: `compaction_local_io_probes_v1`.
- Artifact schema version for this branch: `3`.
- This run documents all default scenarios in `feat/bench-compaction-io-probes`:
  - `read_baseline`
  - `read_after_first_compaction_observed`
  - `read_compaction_quiesced`
  - `read_while_compaction` (sweep variant)
  - `write_throughput_vs_compaction_frequency` (tick `50ms` + `200ms`)

## Run Context

- Date (UTC): `2026-02-23T19:28:36Z`
- Git commit: `46f25dd`
- Branch: `feat/bench-compaction-io-probes`
- OS: `Linux 6.17.0-14-generic x86_64 GNU/Linux`
- CPU: `Intel(R) Core(TM) Ultra 9 185H` (`22` logical CPUs, `2` threads/core)
- Rust: `rustc 1.90.0 (1159e78c4 2025-09-14)`
- Cargo: `cargo 1.90.0 (840b83a10 2025-07-30)`
- Backend: local filesystem (`LocalFs`) wrapped by benchmark-only probe wrappers
- WAL policy: `TONBO_COMPACTION_BENCH_WAL_SYNC=always`

## Command

```bash
TONBO_COMPACTION_BENCH_INGEST_BATCHES=768 \
TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=96 \
TONBO_COMPACTION_BENCH_KEY_SPACE=4096 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=64 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=30 \
TONBO_COMPACTION_BENCH_WAL_SYNC=always \
cargo bench --bench compaction_local -- --save-baseline compaction_local_io_probes_v1
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
- `sweep_l0_triggers`: `[8]`
- `sweep_max_inputs`: `[6]`
- `sweep_max_task_bytes`: `[null]`
- `write_frequency_periodic_ticks_ms`: `[50, 200]`
- `enable_read_while_compaction`: `true`
- `enable_write_throughput_vs_compaction_frequency`: `true`

## Stable Benchmark IDs

- `compaction_local/read_baseline`
- `compaction_local/read_after_first_compaction_observed`
- `compaction_local/read_compaction_quiesced`
- `compaction_local/read_while_compaction__l08_max6_tasknone_tick200ms`
- `compaction_local/write_throughput_vs_compaction_frequency__l08_max6_tasknone_tick50ms`
- `compaction_local/write_throughput_vs_compaction_frequency__l08_max6_tasknone_tick200ms`

## Scenario Setup Snapshot

From `target/tonbo-bench/compaction_local-1771874851642-3632019.json`.

| Scenario | Variant | Workload | Rows/op | Sweep (`l0,max,task,tick`) | SSTs before -> ready | Levels before -> ready |
| --- | --- | --- | ---: | --- | ---: | ---: |
| `read_baseline` | `default` | `read_only` | 4,096 | `-` | `12 -> 12` | `1 -> 1` |
| `read_after_first_compaction_observed` | `default` | `read_only` | 4,096 | `-` | `12 -> 7` | `1 -> 2` |
| `read_compaction_quiesced` | `default` | `read_only` | 4,096 | `-` | `12 -> 7` | `1 -> 2` |
| `read_while_compaction` | `l08_max6_tasknone_tick200ms` | `read_while_compaction` | 4,096 | `8,6,none,200` | `12 -> 12` | `1 -> 1` |
| `write_throughput_vs_compaction_frequency` | `l08_max6_tasknone_tick50ms` | `write_throughput` | 96 | `8,6,none,50` | `12 -> 12` | `1 -> 1` |
| `write_throughput_vs_compaction_frequency` | `l08_max6_tasknone_tick200ms` | `write_throughput` | 96 | `8,6,none,200` | `12 -> 12` | `1 -> 1` |

## Baseline Latency (Criterion)

From `target/criterion/compaction_local/*/compaction_local_io_probes_v1/estimates.json`.

| Benchmark ID | Mean latency | 95% CI |
| --- | ---: | ---: |
| `compaction_local/read_baseline` | `115.598 ms` | `[112.976 ms, 118.245 ms]` |
| `compaction_local/read_after_first_compaction_observed` | `112.134 ms` | `[109.868 ms, 114.610 ms]` |
| `compaction_local/read_compaction_quiesced` | `112.066 ms` | `[109.604 ms, 114.686 ms]` |
| `compaction_local/read_while_compaction__l08_max6_tasknone_tick200ms` | `149.984 ms` | `[146.404 ms, 153.581 ms]` |
| `compaction_local/write_throughput_vs_compaction_frequency__l08_max6_tasknone_tick50ms` | `5.607 ms` | `[4.825 ms, 6.889 ms]` |
| `compaction_local/write_throughput_vs_compaction_frequency__l08_max6_tasknone_tick200ms` | `5.593 ms` | `[4.765 ms, 6.948 ms]` |

## Artifact Summary (64 Iterations, with IO Counters)

From `target/tonbo-bench/compaction_local-1771874851642-3632019.json`.

| Scenario | Ops/sec | Rows/sec | p50 | p95 | Mean | Summary IO (`r_ops/w_ops/r_bytes/w_bytes/ssts`) |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `read_baseline` | `8.743` | `35,811.49` | `109.819 ms` | `133.713 ms` | `114.376 ms` | `2304 / 0 / 55344576 / 0 / 12` |
| `read_after_first_compaction_observed` | `8.784` | `35,979.62` | `111.371 ms` | `124.207 ms` | `113.842 ms` | `1344 / 0 / 30771328 / 0 / 7` |
| `read_compaction_quiesced` | `8.691` | `35,598.60` | `111.792 ms` | `131.187 ms` | `115.060 ms` | `1344 / 0 / 30771328 / 0 / 7` |
| `read_while_compaction` | `5.586` | `271,177.72` | `174.369 ms` | `204.962 ms` | `179.006 ms` | `1541 / 129 / 88509173 / 2111674 / 8` |
| `write_throughput_vs_compaction_frequency` (`50ms`) | `182.080` | `17,479.67` | `4.271 ms` | `5.407 ms` | `5.492 ms` | `2 / 129 / 5647536 / 2111674 / 1` |
| `write_throughput_vs_compaction_frequency` (`200ms`) | `159.674` | `15,328.68` | `4.783 ms` | `5.745 ms` | `6.263 ms` | `2 / 129 / 5647536 / 2111674 / 1` |

## Artifacts

- JSON artifact:
  - `target/tonbo-bench/compaction_local-1771874851642-3632019.json`
- Criterion baseline files:
  - `target/criterion/compaction_local/read_baseline/compaction_local_io_probes_v1/estimates.json`
  - `target/criterion/compaction_local/read_after_first_compaction_observed/compaction_local_io_probes_v1/estimates.json`
  - `target/criterion/compaction_local/read_compaction_quiesced/compaction_local_io_probes_v1/estimates.json`
  - `target/criterion/compaction_local/read_while_compaction__l08_max6_tasknone_tick200ms/compaction_local_io_probes_v1/estimates.json`
  - `target/criterion/compaction_local/write_throughput_vs_compaction_frequency__l08_max6_tasknone_tick/compaction_local_io_probes_v1/estimates.json`
  - `target/criterion/compaction_local/write_throughput_vs_compaction_frequency__l08_max6_tasknone_tick_2/compaction_local_io_probes_v1/estimates.json`
- HTML reports:
  - `target/criterion/compaction_local/report/index.html`

## Interpretation

- This document sets the branch baseline for expanded scenario matrix + IO-probe counters.
- Because this matrix and schema (`v3`) differ from the earlier two-scenario baseline, compare future changes against `compaction_local_io_probes_v1` for this branch line.
- Use:

```bash
cargo bench --bench compaction_local -- --baseline compaction_local_io_probes_v1
```
