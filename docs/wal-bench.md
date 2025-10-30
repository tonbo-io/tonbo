# WAL Bench Harness

A lightweight benchmark harness lives in `examples/wal_bench.rs`. It measures WAL append throughput for a configurable batch size, tombstone density, and sync policy. Run it with:

```bash
cargo run --example wal_bench -- \
  --rows=4096 \
  --batches=64 \
  --density=0.2 \
  --sync=disabled \
  --columns=2
```

The default parameters (shown above) produce approximately **70 MB/s** and **~900 ops/s** on the current branch when targeting the in-memory WAL backend (sync disabled). Use `SYNC=always` to include fsync cost, and adjust `ROWS`, `BATCHES`, or `DENSITY` to match your workload.

The binary prints metrics in a single line, e.g.

```
mode=wal rows=4096 batches=64 density=0.20 sync=disabled duration_ms=70.11 bytes=5279744 mb_per_s=71.82 ops_per_s=912.84
```

Future work (tracked in `.personal/finished/wal-bench-todo.md`) includes reintroducing the Criterion-based harness once network access allows pulling the crate.
