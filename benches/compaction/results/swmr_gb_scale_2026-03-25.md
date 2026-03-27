# SWMR GB-Scale First Pass (2026-03-25)

Artifact:

- `target/tonbo-bench/compaction_local-1774444358822-1769545.json`

Commands run:

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo +nightly fmt --all
TONBO_BENCH_BACKEND=local \
TONBO_SWMR_BENCH_LOGICAL_GB=1 \
TONBO_SWMR_BENCH_ROWS_PER_BATCH=256 \
TONBO_SWMR_BENCH_PAYLOAD_BYTES=4096 \
TONBO_SWMR_BENCH_LIGHT_SCAN_LIMIT=256 \
TONBO_SWMR_BENCH_HEAVY_SCAN_LIMIT=2048 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
cargo bench --bench compaction_local -- swmr_gb_scale_mixed --nocapture
```

What was implemented:

- Added `swmr_gb_scale_mixed` to the existing engine benchmark target.
- Added deterministic preload + steady-state SWMR shaping with one writer and four reader classes:
  - `head_light`
  - `head_heavy`
  - `pinned_light`
  - `pinned_heavy`
- Added artifact sections for SWMR logical-size targets, writer latency, and per-reader-class latency.
- Added deterministic correctness test `swmr_snapshot_stability_survives_writes_and_compaction`.

1 GB local cell:

- Logical target: `1,073,741,824` bytes
- Estimated preload logical bytes: `966,942,720`
- Estimated steady logical bytes: `107,790,336`
- Preload batches: `915`
- Steady batches: `102`
- Writer batches per measured step: `13`

Top-line numbers from the artifact:

- Whole mixed step latency mean: `111.6 ms`
- Whole mixed step p95: `153.0 ms`
- Whole mixed throughput: `8.96 ops/s`
- Whole mixed throughput: `52.75 Krows/s`
- Writer latency mean: `83.6 ms`
- Writer latency p95: `124.5 ms`
- `head_light` latency mean: `4.72 ms`
- `head_heavy` latency mean: `14.28 ms`
- `pinned_light` latency mean: `5.32 ms`
- `pinned_heavy` latency mean: `3.66 ms`

What the numbers say:

- The current local SWMR envelope is practical to benchmark at roughly `1 GB logical` inside the existing harness shape.
- Writer cost dominates the mixed-step budget; read classes stayed materially below write latency in this local cell.
- `head_heavy` remained non-trivial (`14.28 ms` mean) while `head_light` stayed near the setup floor (`4.72 ms` mean).
- `pinned_light` worked, but `pinned_heavy` reported `0` rows across the run. Treat its latency as a harness/correctness warning, not a trustworthy reader result.

Current limitations exposed by this pass:

- `10 GB logical` was not executed in this environment.
- Object-store SWMR cells were not executed in this environment.
- The pinned-reader path currently relies on `snapshot_at(version.timestamp)` because the benchmark cannot hold the internal snapshot type directly; the `pinned_heavy = 0 rows` result means this path needs more validation before using pinned-snapshot read numbers for decision-making.
- An attempted reopen-time pinned-snapshot assertion exposed that in-process snapshot stability works, but `snapshot_at(pinned_ts)` after reopen did not preserve the older view in the attempted form. The new gated test therefore validates the in-process SWMR snapshot invariant and leaves reopen-time pinned-snapshot durability as follow-up work.
