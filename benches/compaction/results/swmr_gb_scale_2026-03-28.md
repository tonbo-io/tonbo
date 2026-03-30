# SWMR Correctness-Gated Follow-up (2026-03-28)

Artifact:

- `target/tonbo-bench/compaction_local-1774729594256-2047627.json`

Commands run:

```bash
cargo test
cargo test --bench compaction_local --no-run
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

What changed:

- The SWMR harness now captures a deterministic result shape per reader class:
  - expected rows
  - first key
  - last key
  - stable key fingerprint
  - declared validation model
- Validation is now explicit per reader class instead of only checking
  `rows > 0`:
  - `pinned_light`, `pinned_heavy`, and `head_heavy` use
    `exact_shape_stable`
  - `head_light` uses `count_and_key_band` because hot-key deletes can move the
    exact frontier while the scan is still valid
- The artifact now records manifest-reconstruction observations against those
  same expectations, so the old pinned reconstruction bug is visible as a full
  shape mismatch rather than only a row-count mismatch.
- The same correctness logic is wired through both local and object-store SWMR
  reader paths.
- The benchmark now performs best-effort workspace cleanup after the run. This
  deletes local/object-store benchmark data under the scenario workspace/prefix
  while preserving the JSON artifact under `target/tonbo-bench/`.

Local `~1 GB` rerun summary:

- whole mixed step latency mean: `168.61 ms`
- whole mixed step p95: `237.65 ms`
- whole mixed throughput: `47.07 Krows/s`
- writer latency mean: `139.77 ms`
- `head_light` latency mean: `4.88 ms`
- `head_heavy` latency mean: `13.96 ms`
- `pinned_light` latency mean: `1.76 ms`
- `pinned_heavy` latency mean: `8.21 ms`

Validity signals from the artifact:

- held pinned snapshot mode: `held_snapshot`
- held pinned snapshot timestamp: `914`
- manifest-version timestamp: `14`
- held snapshot expectations now include:
  - key band
  - validation model
  - expected rows
  - expected first/last key
  - expected key fingerprint
- per-iteration reader summaries now include:
  - min/max/mean rows per scan
  - unique key fingerprint count
  - per-check validity flags

Observed `~1 GB` local result shape:

- `head_heavy`, `pinned_light`, and `pinned_heavy` all stayed at one key
  fingerprint for the full measured window and matched their expected first/last
  keys exactly.
- `head_light` stayed valid under the declared `count_and_key_band` model:
  `256` rows every iteration, all keys remained in the `hot-*` band, while the
  fingerprint changed across iterations as hot-key deletes reshaped the first
  `256` visible rows.
- manifest-version reconstruction still proves the old pinned bug:
  - `head_heavy`: `0` rows, invalid
  - `pinned_heavy`: `0` rows, invalid

Object-store status:

- The same correctness checks are implemented in the object-store SWMR path.
- This session did not execute an object-store `1 GB` run because no
  `TONBO_S3_*`, `AWS_*`, or `TONBO_BENCH_OBJECT_PREFIX` environment variables
  were present in the shell.

Cleanup policy:

- A benchmark run should clean up its scenario workspace/prefix automatically
  after artifact capture and Criterion execution.
- What should remain:
  - the JSON artifact under `target/tonbo-bench/`
  - any checked-in markdown write-up
- What should not remain by default:
  - local workspace files under `target/tonbo-bench/workspaces/...`
  - remote objects under the benchmark object-store prefix for that run
