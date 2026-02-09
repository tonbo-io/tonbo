# Compaction Local Benchmark (`compaction_local`)

This benchmark provides a Criterion harness for local-filesystem compaction read scenarios:

- `read_baseline`
- `read_post_compaction`

It also emits a stable versioned JSON artifact for machine diffing under `target/tonbo-bench/`.

## Run

```bash
cargo bench --bench compaction_local
```

## Environment Configuration

All knobs are env-only and use the `TONBO_COMPACTION_BENCH_*` prefix.

| Variable | Default | Description |
| --- | --- | --- |
| `TONBO_COMPACTION_BENCH_INGEST_BATCHES` | `640` | Number of ingest batches used to prepare each scenario dataset. |
| `TONBO_COMPACTION_BENCH_ROWS_PER_BATCH` | `64` | Rows per ingest batch. |
| `TONBO_COMPACTION_BENCH_KEY_SPACE` | `2048` | Distinct key-space size for deterministic overwrite density. |
| `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS` | `48` | Fixed read iterations used to build JSON throughput/latency summaries. |
| `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE` | `20` | Criterion sample size (`>= 10`). |
| `TONBO_COMPACTION_BENCH_COMPACTION_WAIT_TIMEOUT_MS` | `20000` | Timeout while waiting for post-compaction scenario readiness. |
| `TONBO_COMPACTION_BENCH_COMPACTION_POLL_INTERVAL_MS` | `50` | Poll interval for compaction readiness checks. |
| `TONBO_COMPACTION_BENCH_COMPACTION_PERIODIC_TICK_MS` | `200` | Periodic major-compaction worker tick interval. |
| `TONBO_COMPACTION_BENCH_SEED` | `584` | Deterministic dataset seed. |
| `TONBO_COMPACTION_BENCH_WAL_SYNC` | `disabled` | WAL sync policy for benchmark setup (`disabled` or `always`). |

Example:

```bash
TONBO_COMPACTION_BENCH_INGEST_BATCHES=768 \
TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=96 \
TONBO_COMPACTION_BENCH_KEY_SPACE=4096 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=64 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=30 \
TONBO_COMPACTION_BENCH_WAL_SYNC=always \
cargo bench --bench compaction_local
```

## JSON Artifact

Per run, one artifact is written to:

- `target/tonbo-bench/compaction_local-<run_id>.json`

Top-level fields:

- `schema_version`
- `benchmark_id`
- `run_id`
- `generated_at_unix_ms`
- `config` (resolved env config)
- `scenarios`

Per-scenario fields include:

- `scenario_id` / `scenario_name`
- `setup` (rows per scan + manifest SST/level counts before and after compaction readiness)
- `summary.iterations`
- `summary.total_elapsed_ns`
- `summary.throughput.ops_per_sec`
- `summary.throughput.rows_per_sec`
- `summary.latency_ns.{min,p50,p95,max,mean}`
