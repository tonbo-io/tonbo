# SWMR Pinned-Snapshot Follow-up (2026-03-27)

Artifact:

- `target/tonbo-bench/compaction_local-1774592990634-1898420.json`

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

Root cause:

- The earlier harness pinned readers with
  `snapshot_at(latest_manifest_version.timestamp)`.
- In the practical local preload, the true held snapshot read timestamp was
  `914` while the latest manifest publish timestamp was `14`.
- The heavy pinned reader scans `warm-*`.
- Warm rows existed in the held pinned snapshot, but not in the manifest state
  at timestamp `14`.
- Result: `pinned_heavy = 0` was a harness bug caused by reconstructing the
  wrong historical view.

What changed:

- The SWMR benchmark now holds a true snapshot object for pinned readers.
- The setup artifact records:
  - pinned snapshot mode,
  - held snapshot timestamp,
  - manifest-version timestamp,
  - expected rows per reader class from the held snapshot,
  - comparative rows from manifest-version reconstruction.
- The harness now fails if a reader expected to return rows instead returns
  `0`, so invalid pinned scans cannot silently look fast.
- Added deterministic regression coverage proving that
  `snapshot_at(manifest_version.timestamp)` can under-read compared with a held
  snapshot in the benchmark-like preload shape.

Local `~1 GB` rerun summary:

- whole mixed step latency mean: `134.47 ms`
- whole mixed step p95: `145.96 ms`
- whole mixed throughput: `59.02 Krows/s`
- writer latency mean: `104.77 ms`
- `head_light` latency mean: `4.59 ms`
- `head_heavy` latency mean: `13.44 ms`
- `pinned_light` latency mean: `3.90 ms`
- `pinned_heavy` latency mean: `7.78 ms`

Pinned-reader validity from the artifact:

- held snapshot mode: `held_snapshot`
- held pinned snapshot timestamp: `914`
- manifest-version timestamp: `14`
- held snapshot expected rows per scan:
  - `head_light = 256`
  - `head_heavy = 2048`
  - `pinned_light = 256`
  - `pinned_heavy = 2048`
- manifest-version reconstruction rows per scan:
  - `head_light = 256`
  - `head_heavy = 0`
  - `pinned_light = 256`
  - `pinned_heavy = 0`

Conclusion:

- The pinned-heavy gap was a benchmark harness bug, not an engine bug proven by
  this run.
- The local pinned-reader path is now semantically sound enough to use for
  further local SWMR benchmarking.
- S3 and `10 GB` expansion should still wait for the same validity checks to be
  exercised there.
