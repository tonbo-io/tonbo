# Disk Bench Report — 2025-12-19

Environment: local disk backend (`target/bench-tmp`), `--features test`, concurrency 1, tokio multi-thread runtime. All commands run from repo root.

## Summary
- Write path is bounded by amortized WAL+memtable: ~145k ops/s (74 MB/s) at 512B rows when WAL syncs are batched. Forcing fsync every batch halves throughput (~75k ops/s).
- Read path (per-key predicate scans) is currently very slow: 10k point reads at 512B took ~137s (~73 ops/s) because each lookup scans all data. Larger runs (>50k) become impractical.
- Mixed workload (small CI shape) is lightweight: ~377 ops/s, 48 KB/s; too small to stress paths.
- Compaction scenario with bench-only minor compaction knob shows ingest-bound numbers; compaction remains async on this path.
- Iterator/scan throughput (component) has ample headroom (~488k ops/s) relative to write path; read-path cost in the scenario stems from per-key planning/scan, not raw iterator speed.
- WAL sync policy is confirmed as the throughput dial: fsync=Always is ~109k ops/s; Disabled is ~131k ops/s; batched/default policies keep ingest near memtable rates (~145k ops/s).

## Detailed metrics
- Scenario: sequential_write (deep-disk, 200k x 512B)
  - 145,281 ops/s, 74.4 MB/s, wall 1.376s; WAL syncs=15 (p50 11.7 ms, max 20.1 ms).
- Scenario: compaction (compaction-disk, 200k x 256B, minor compaction enabled)
  - 169,019 ops/s, 43.3 MB/s, wall 1.183s.
- Scenario: read_only (read-disk-10k, 10k x 512B, warmup 1k)
  - 72.6 ops/s, 37.2 KB/s, wall 137.7s; WAL syncs=1. Scaling beyond ~10k is impractical due to O(N^2) per-key scans.
- Scenario: mixed (ci-mixed, 1200 ops, 70% reads, 128B)
  - 376.8 ops/s, 48.2 KB/s, wall 3.184s.
- Component: memtable_insert (200k x 512B)
  - 151,181 ops/s, 77.4 MB/s, wall 1.322s; WAL syncs=13.
- Component: wal_append (fsync=Always, 200k x 512B)
  - 75,336 ops/s, 38.6 MB/s, wall 2.654s; WAL syncs=98.
- Component: sst_encode (200k x 512B)
  - 124,068 ops/s, 63.5 MB/s, wall 1.612s; WAL syncs=16.
- Component: iterator_scan (200k x 512B, scan_all)
  - 488,194 ops/s, 250.0 MB/s, wall 0.409s; WAL syncs during ingest=26.
- Component: wal_append (fsync policy sweep, 200k x 512B)
  - Always: 108,880 ops/s, 55.7 MB/s, wall 1.836s; WAL syncs=98.
  - IntervalBytes(4096): 111,178 ops/s, 56.9 MB/s, wall 1.798s; WAL syncs=98 (small batches).
  - IntervalTime(1ms): 131,880 ops/s, 67.5 MB/s, wall 1.516s; WAL syncs=99.
  - Disabled: 130,663 ops/s, 66.9 MB/s, wall 1.530s; WAL syncs=0.
  - These runs show: forcing many syncs (Always/IntervalBytes) pushes throughput down; relaxing sync frequency (IntervalTime/Disabled) raises throughput toward the memtable range (~151k ops/s) but still below it, reflecting residual WAL enqueue/serialize cost even without fsync.

## Why read-only looks slow but iterator is fast
- The `read_only` scenario issues 10k separate `scan_with_predicate` calls. There is no point index, so each predicate executes a full scan of all rows and then filters. That’s ~10k scans × 10k rows ≈ 100M row visits (~51 GB at 512B/row), plus per-call planning/allocation overhead. The JSON `bytes_per_sec` only counts logical payload once, so it under-reports the real work; wall time reflects the repeated scans (O(N²) behavior).
- The `iterator_scan` component writes once and performs a single `scan_all`, streaming through the data a single time. It measures raw merge/iterator throughput without per-call planning or repeated full scans, so it appears much faster.

## Interpretation
- WAL sync policy is the dominant durability-performance lever: batched syncs keep ingest near memtable limits; per-batch fsync halves throughput.
- Read-only path is bottlenecked by per-request scans/planning (no index); raw iterator speed is not the limiter. Consider batching reads or adding indexed predicate support before using large point-read benches.
- No evidence of compaction or flush backpressure in these runs (queue depth stayed at 1; diagnostics show compaction unsupported on the measured paths).

## Command log (repro)
1. `cargo run -p tonbo-bench-runner -- --profile deep`
   - Produces sequential_write (deep-disk) scenario and all components on disk.
2. `cargo bench --features test --bench tonbo_scenarios -- --config benches/harness/configs/compaction-disk.yaml`
   - Compaction scenario on disk.
3. `cargo bench --features test --bench tonbo_scenarios -- --config benches/harness/configs/read-disk-10k.yaml`
   - Read-only scenario (10k rows) on disk. Note: larger configs hung due to O(N^2) per-key scans.
4. `cargo bench --features test --bench tonbo_scenarios -- --config benches/harness/configs/ci-mixed.yaml`
   - Mixed scenario (small CI shape) on disk.
5. `cargo bench --features test --bench tonbo_components -- --config benches/harness/configs/deep-disk.yaml --component wal` (fsync=Always, default)
6. `TONBO_BENCH_WAL_SYNC=disabled cargo bench --features test --bench tonbo_components -- --config benches/harness/configs/deep-disk.yaml --component wal` (fsync disabled)

Results are under `target/bench-results/<run-id>/...` as shown by the commands.
