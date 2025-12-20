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

### Metrics Categories (Initial)

| Category | Metrics | Notes |
| --- | --- | --- |
| WAL | queue depth, max queue depth, bytes written, sync ops, append latency | Captures durability and backpressure behavior |
| Flush | flush count, bytes, total duration, min/max duration | Aggregated per flush; no unbounded storage |
| Memtable | entries, inserts, replaces, approx key bytes, entry overhead | Snapshot from in-memory counters |
| Compaction | placeholder (unsupported) | Defined but optional until implemented |

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
- Compaction, read-path, and cache metrics once subsystems stabilize.
- Parameter sweep runner that produces plot-ready datasets directly from metrics snapshots
  (implemented in `tonbo-bench-runner`; Parquet output remains a follow-up).
