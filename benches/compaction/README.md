# Compaction Benchmark Harness

This benchmark suite lives in `benches/compaction_local.rs` and now supports:

- Dataset scaling to reduce page-cache/memory effects.
- Backend selection (`local` vs `object_store`).
- Tail latency reporting (p50/p95/p99 in addition to mean).
- A directional report block for `read_baseline` vs `read_compaction_quiesced`.

## New Environment Variables

- `TONBO_BENCH_DATASET_SCALE` (default: `1`)
  - Multiplies ingest batches deterministically (fixed seed).
  - Effective rows scale with this factor while scenario shape remains unchanged.

- `TONBO_BENCH_BACKEND=local|object_store` (default: `local`)
  - `local`: uses `LocalFs` benchmark workspace under `target/tonbo-bench/workspaces/...`.
  - `object_store`: uses Tonbo object-store builder path.

- `TONBO_BENCH_OBJECT_PREFIX` (optional; default: `tonbo-bench`)
  - Prefix used for object-store benchmark runs.

## Object-Store Configuration

When `TONBO_BENCH_BACKEND=object_store`, these env vars must be set:

- `TONBO_S3_BUCKET`
- `TONBO_S3_REGION`
- `TONBO_S3_ACCESS_KEY`
- `TONBO_S3_SECRET_KEY`

Optional:

- `TONBO_S3_ENDPOINT`
- `TONBO_S3_SESSION_TOKEN`

If required object-store variables are missing, the benchmark prints a clear skip reason and exits cleanly instead of failing the entire run.

## Example Commands

LocalFS, baseline scale:

```bash
TONBO_BENCH_BACKEND=local TONBO_BENCH_DATASET_SCALE=1 cargo bench -p tonbo -- <bench-filter-if-needed>
```

LocalFS, larger dataset:

```bash
TONBO_BENCH_DATASET_SCALE=10 cargo bench -p tonbo -- <...>
```

Object store (if configured):

```bash
TONBO_BENCH_BACKEND=object_store TONBO_BENCH_DATASET_SCALE=10 cargo bench -p tonbo -- <...>
```

## Output Notes

Each scenario summary includes latency fields in nanoseconds:

- `mean`
- `p50`
- `p95`
- `p99`

After `read_baseline` and `read_compaction_quiesced` complete, the harness prints:

- Directional Question
- Scenario Set
- Dataset Scale
- Backend
- Observed deltas (read ops, bytes, mean, p99)
- Conservative interpretation
- Suggested next tests
