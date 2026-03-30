# SWMR Object-Store 1 GB Follow-up (2026-03-29)

Artifacts:

- mixed long session:
  `target/tonbo-bench/compaction_local-1774733239775-2053617.json`
- clean isolated SWMR object-store run:
  `target/tonbo-bench/compaction_local-1774810066470-2135436.json`

Commands run:

Mixed long session that unintentionally included additional scenarios:

```bash
eval "$(aws configure export-credentials --profile tonbo --format env)"
export TONBO_S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
export TONBO_S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export TONBO_S3_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
export TONBO_S3_BUCKET=tonbo-smoke-euc1-232814779190-1772115011
export TONBO_S3_REGION=eu-central-1
export TONBO_BENCH_OBJECT_PREFIX=tonbo-swmr-object-1gb-retry-1774733239
RUST_BACKTRACE=1 \
TONBO_BENCH_BACKEND=object_store \
TONBO_SWMR_BENCH_LOGICAL_GB=1 \
TONBO_SWMR_BENCH_ROWS_PER_BATCH=256 \
TONBO_SWMR_BENCH_PAYLOAD_BYTES=4096 \
TONBO_SWMR_BENCH_LIGHT_SCAN_LIMIT=256 \
TONBO_SWMR_BENCH_HEAVY_SCAN_LIMIT=2048 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
cargo bench --bench compaction_local -- swmr_gb_scale_mixed --nocapture
```

Clean isolated object-store SWMR run:

```bash
eval "$(aws configure export-credentials --profile tonbo --format env)"
export TONBO_S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
export TONBO_S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export TONBO_S3_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
export TONBO_S3_BUCKET=tonbo-smoke-euc1-232814779190-1772115011
export TONBO_S3_REGION=eu-central-1
export TONBO_BENCH_OBJECT_PREFIX=tonbo-swmr-clean-1gb-1774810065
TONBO_BENCH_BACKEND=object_store \
TONBO_SWMR_BENCH_LOGICAL_GB=1 \
TONBO_SWMR_BENCH_ROWS_PER_BATCH=256 \
TONBO_SWMR_BENCH_PAYLOAD_BYTES=4096 \
TONBO_SWMR_BENCH_LIGHT_SCAN_LIMIT=256 \
TONBO_SWMR_BENCH_HEAVY_SCAN_LIMIT=2048 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=8 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0 \
TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0 \
cargo bench --bench compaction_local -- swmr_gb_scale_mixed --nocapture
```

What happened:

- The first object-store attempt failed during scenario preparation while
  persisting a manifest segment to S3. The remote prefix was cleaned up.
- The next run completed, but because the broader scenario families were still
  enabled it also ran:
  - `read_while_compaction`
  - `write_throughput_vs_compaction_frequency` (two cells)
- A final clean rerun disabled those extra scenario families so the artifact
  contains only `swmr_gb_scale_mixed`.

Clean object-store `1 GB` SWMR result:

- whole mixed step latency mean: `22.99 s`
- whole mixed step p95: `27.06 s`
- whole mixed throughput: `345.13 rows/s`
- writer latency mean: `14.75 s`
- `head_light` latency mean: `1.94 s`
- `head_heavy` latency mean: `3.50 s`
- `pinned_light` latency mean: `0.55 s`
- `pinned_heavy` latency mean: `2.26 s`

Correctness outcome in the clean object-store run:

- all reader classes were `valid = true`
- `head_heavy`, `pinned_light`, and `pinned_heavy` each held one fingerprint
  across the measured run
- `head_light` stayed valid under `count_and_key_band`
- manifest-version reconstruction was still invalid for `head_heavy` and
  `pinned_heavy`, so the held-snapshot gating matters on object store too

Comparison with the mixed long session:

- mixed long session SWMR mean step: `24.03 s`
- clean isolated SWMR mean step: `22.99 s`
- mixed long session writer mean: `15.88 s`
- clean isolated SWMR writer mean: `14.75 s`

Takeaways:

- The clean isolated result and the longer mixed session tell the same story:
  object-store `1 GB` SWMR is valid but slow in this environment, and writer
  latency dominates the mixed-step budget.
- The long mixed session did not materially change the SWMR interpretation. Its
  main value was operational:
  - it exposed that broad benchmark sessions can quietly include extra
    scenarios if they are not disabled explicitly
  - it exercised remote cleanup and remote failure handling over a longer wall
    time
- For routine iteration, the smaller scoped run is the higher-value tool:
  - it is easier to interpret
  - it reaches the same SWMR conclusion
  - it avoids conflating SWMR results with unrelated benchmark families
- Longer `1 GB` object-store sessions still have value occasionally as soak
  checks, but they should be treated as a different benchmark product from the
  isolated SWMR cell rather than the default loop for everyday benchmarking
