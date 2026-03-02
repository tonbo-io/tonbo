# Compaction Local Large-Read Baseline (`compaction_local_io_probes_large_v1`)

## Scope

This report includes only the large read profile run:

- Baseline ID: `compaction_local_io_probes_large_v1`
- Artifact origin (historical run, not checked into this branch workspace):
  `target/tonbo-bench/compaction_local-1771877579581-3694094.json`
- Schema version: `3`
- Scenarios:
  - `read_baseline`
  - `read_after_first_compaction_observed`
  - `read_compaction_quiesced`

## Run Profile

- Date (UTC): `2026-02-23T20:14:16Z`
- Branch: `feat/bench-compaction-io-probes`
- Commit: `1352c43`
- Backend: local filesystem (`LocalFs`) with benchmark IO probes
- Workload shape:
  - `TONBO_COMPACTION_BENCH_INGEST_BATCHES=2048`
  - `TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=128`
  - `TONBO_COMPACTION_BENCH_KEY_SPACE=65536`
  - `TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=32`
  - `TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=20`
  - `TONBO_COMPACTION_BENCH_WAL_SYNC=always`
  - `TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=false`
  - `TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=false`

Command:

```bash
TONBO_COMPACTION_BENCH_INGEST_BATCHES=2048 \
TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=128 \
TONBO_COMPACTION_BENCH_KEY_SPACE=65536 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=32 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=20 \
TONBO_COMPACTION_BENCH_WAL_SYNC=always \
TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=false \
TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=false \
cargo bench --bench compaction_local -- --save-baseline compaction_local_io_probes_large_v1
```

## Scenario Setup

| Scenario | Rows/op | SSTs before -> ready | Levels before -> ready |
| --- | ---: | ---: | ---: |
| `read_baseline` | `65,536` | `32 -> 32` | `1 -> 1` |
| `read_after_first_compaction_observed` | `65,536` | `32 -> 27` | `1 -> 2` |
| `read_compaction_quiesced` | `65,536` | `32 -> 22` | `1 -> 2` |

## Measured Results (Artifact)

Derived metrics:

- `read_ops_per_scan = summary.io.read_ops / summary.iterations`
- `bytes_per_scan = summary.io.bytes_read / summary.iterations`

| Scenario | Ops/sec | Mean latency | p50 | p95 | Read ops/scan | Bytes/scan | SSTs touched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `read_baseline` | `2.302` | `434.486 ms` | `431.871 ms` | `455.777 ms` | `96.00` | `3,426,443` | `32` |
| `read_after_first_compaction_observed` | `2.413` | `414.494 ms` | `411.675 ms` | `425.770 ms` | `21.38` | `1,275,615` | `7` |
| `read_compaction_quiesced` | `2.390` | `418.417 ms` | `411.299 ms` | `439.562 ms` | `21.28` | `1,275,585` | `7` |

## Relative Change vs `read_baseline`

| Scenario | Ops/sec | Mean latency | Read ops/scan | Bytes/scan |
| --- | ---: | ---: | ---: | ---: |
| `read_after_first_compaction_observed` | `+4.82%` | `-4.60%` | `-77.73%` | `-62.77%` |
| `read_compaction_quiesced` | `+3.84%` | `-3.70%` | `-77.83%` | `-62.77%` |

## Criterion Latency (Mean, 95% CI)

| Scenario | Mean latency | 95% CI |
| --- | ---: | ---: |
| `read_baseline` | `437.313 ms` | `[432.007 ms, 443.004 ms]` |
| `read_after_first_compaction_observed` | `432.126 ms` | `[422.386 ms, 443.124 ms]` |
| `read_compaction_quiesced` | `410.241 ms` | `[406.626 ms, 414.611 ms]` |

Interpretation from CI overlap:

- `read_after_first_compaction_observed` overlaps baseline CI (directional improvement, weaker confidence).
- `read_compaction_quiesced` does not overlap baseline CI (stronger latency improvement signal in this run).

## Conclusion

- For the same logical scan output (`65,536` rows/op), compaction reduces physical read work substantially.
- In this large-read profile, reduced amplification is accompanied by lower scan latency and higher scan throughput.
- This baseline is the reference point for future comparisons of compaction read behavior under larger scan pressure.

## Files

- Availability in this checkout:
  - Historical artifact/criterion files above are not present by default in this branch.
  - Re-run the command in this report to regenerate equivalent local outputs under `target/`.
