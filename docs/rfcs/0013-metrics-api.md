# RFC: Metrics API for Tonbo

- Status: Draft
- Authors: Tonbo team
- Created: 2025-12-22
- Area: Metrics, Benchmarks

## Summary

Define a stable, public metrics API that exposes core storage-engine metrics from day one.
Benchmarks and production monitoring consume the same interface, decoupling benchmark harnesses
from internal refactors and reducing long-term maintenance cost.

## Motivation

- Benchmarks currently rely on internal, bench-only hooks gated by compile-time flags.
- This creates a maintenance burden: internal refactors break benches and reduce metric continuity.
- Mature databases expose stable metrics (RocksDB, PostgreSQL, FoundationDB) that serve both
  benchmarks and production monitoring.
- A stable API lets us run parameter sweeps (e.g., WAL sync policy) and plot trends over time
  without embedding ad-hoc access into engine internals.

## Goals

- Provide a stable metrics snapshot API for Tonbo users and benchmark harnesses.
- Cover core engine subsystems (WAL, flush/compaction, memtable) with well-defined counters.
- Keep the API lightweight and runtime-agnostic (no hard dependency on Prometheus or OTEL).
- Preserve async, non-blocking behavior for metrics reads and updates.

## Non-Goals

- Full observability backend integration (Prometheus/OTEL exporters are optional follow-ups).
- Dashboards or visualization tooling inside this repo.
- Per-query tracing or high-cardinality request logging.

## Design

### API Surface

Expose a public snapshot method on `DB`:

- `DB::metrics_snapshot() -> DbMetricsSnapshot` (async where needed)

Metrics types live under `tonbo::metrics`.

The snapshot includes optional sections for each subsystem. Missing data should be represented
as `None`, not through feature-gated types, so the API shape remains stable.

### Metrics Categories (Current)

| Category | Metrics | Notes |
| --- | --- | --- |
| WAL | queue depth, max queue depth, bytes written, sync ops, append latency | Captures durability and backpressure behavior |
| Flush | flush count, bytes, total duration, min/max duration | Aggregated per flush; no unbounded storage |
| Memtable | entries, inserts, replaces, approx key bytes, entry overhead | Snapshot from in-memory counters |
| Compaction | runs, failures, CAS conflicts, input/output bytes/rows, duration | Major compaction only |
| Read path | plan/scan counts + duration, entries/rows examined/filtered/emitted, batches | Aggregated across scans |
| Object store | read/write ops + bytes, list/head/delete ops, errors | Logical bytes (file size) |
| Cache | hits/misses/evictions/bytes/entries | `supported = false` if cache absent |

Metric names and semantics must remain stable once published.

### Collection Model

- Metrics counters are updated on the hot path using lightweight atomics or small locks.
- No unbounded vectors; all metrics are aggregated counters or bounded summaries.
- Snapshot access is async only when it requires awaiting runtime locks (e.g., WAL metrics).

### Bench Integration

Bench harnesses use only public APIs:

- `ingest`, `scan`, and `metrics_snapshot`
- No bench-only accessors in the engine

This ensures benchmark code does not depend on unstable internals.

### Visibility for Tests

`cfg(test)` remains for unit-test-only helpers. For integration or e2e visibility, use
`feature = "test"` so the same API can be enabled across benches and external tests.

## Alternatives Considered

- Keep bench-only hooks: rejected due to maintenance cost and lack of
  production parity.
- Adopt a Prometheus-only API: rejected because it hardcodes a monitoring backend and is harder
  to use from tests and benchmarks.

## Future Work

- Prometheus and OpenTelemetry exporters as optional features.
- Cache metrics once a cache implementation lands.
- Parameter sweep runner that produces plot-ready datasets directly from metrics snapshots
  (implemented in `tonbo-bench-runner`; Parquet output remains a follow-up).

## Stable Names & Units

Metrics are exposed via `DbMetricsSnapshot` fields with stable names/units suitable for
exporters. Counts are monotonic unless otherwise noted; durations are in microseconds.

| Snapshot Field | Stable Name | Unit |
| --- | --- | --- |
| `wal.queue_depth` | `tonbo.wal.queue_depth` | gauge |
| `wal.max_queue_depth` | `tonbo.wal.max_queue_depth` | gauge |
| `wal.bytes_written` | `tonbo.wal.bytes_written_total` | bytes |
| `wal.sync_operations` | `tonbo.wal.sync_operations_total` | count |
| `wal.append_latency.*` | `tonbo.wal.append_latency_*_us` | microseconds |
| `flush.flush_count` | `tonbo.flush.count_total` | count |
| `flush.total_bytes` | `tonbo.flush.bytes_total` | bytes |
| `flush.total_us` | `tonbo.flush.total_us` | microseconds |
| `flush.max_us` | `tonbo.flush.max_us` | microseconds |
| `flush.min_us` | `tonbo.flush.min_us` | microseconds |
| `memtable.entries` | `tonbo.memtable.entries` | count |
| `memtable.inserts` | `tonbo.memtable.inserts_total` | count |
| `memtable.replaces` | `tonbo.memtable.replaces_total` | count |
| `memtable.approx_key_bytes` | `tonbo.memtable.approx_key_bytes` | bytes |
| `memtable.entry_overhead` | `tonbo.memtable.entry_overhead_bytes` | bytes |
| `compaction.runs` | `tonbo.compaction.runs_total` | count |
| `compaction.failures` | `tonbo.compaction.failures_total` | count |
| `compaction.cas_conflicts` | `tonbo.compaction.cas_conflicts_total` | count |
| `compaction.input_ssts` | `tonbo.compaction.input_ssts_total` | count |
| `compaction.output_ssts` | `tonbo.compaction.output_ssts_total` | count |
| `compaction.input_bytes` | `tonbo.compaction.input_bytes_total` | bytes |
| `compaction.output_bytes` | `tonbo.compaction.output_bytes_total` | bytes |
| `compaction.input_rows` | `tonbo.compaction.input_rows_total` | count |
| `compaction.output_rows` | `tonbo.compaction.output_rows_total` | count |
| `compaction.total_us` | `tonbo.compaction.total_us` | microseconds |
| `compaction.max_us` | `tonbo.compaction.max_us` | microseconds |
| `compaction.min_us` | `tonbo.compaction.min_us` | microseconds |
| `read_path.plan_count` | `tonbo.read.plan_count_total` | count |
| `read_path.plan_total_us` | `tonbo.read.plan_total_us` | microseconds |
| `read_path.plan_max_us` | `tonbo.read.plan_max_us` | microseconds |
| `read_path.scan_count` | `tonbo.read.scan_count_total` | count |
| `read_path.scan_total_us` | `tonbo.read.scan_total_us` | microseconds |
| `read_path.scan_max_us` | `tonbo.read.scan_max_us` | microseconds |
| `read_path.entries_seen` | `tonbo.read.entries_seen_total` | count |
| `read_path.rows_examined` | `tonbo.read.rows_examined_total` | count |
| `read_path.rows_filtered` | `tonbo.read.rows_filtered_total` | count |
| `read_path.rows_emitted` | `tonbo.read.rows_emitted_total` | count |
| `read_path.batches_emitted` | `tonbo.read.batches_emitted_total` | count |
| `object_store.read_ops` | `tonbo.object_store.read_ops_total` | count |
| `object_store.read_bytes` | `tonbo.object_store.read_bytes_total` | bytes |
| `object_store.write_ops` | `tonbo.object_store.write_ops_total` | count |
| `object_store.write_bytes` | `tonbo.object_store.write_bytes_total` | bytes |
| `object_store.list_ops` | `tonbo.object_store.list_ops_total` | count |
| `object_store.head_ops` | `tonbo.object_store.head_ops_total` | count |
| `object_store.delete_ops` | `tonbo.object_store.delete_ops_total` | count |
| `object_store.errors` | `tonbo.object_store.errors_total` | count |
| `cache.entries` | `tonbo.cache.entries` | count |
| `cache.bytes` | `tonbo.cache.bytes` | bytes |
| `cache.hits` | `tonbo.cache.hits_total` | count |
| `cache.misses` | `tonbo.cache.misses_total` | count |
| `cache.evictions` | `tonbo.cache.evictions_total` | count |
