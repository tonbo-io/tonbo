# RFC: Layered Observability Metrics Across `aisle`, `fusio`, and Tonbo

- Status: Draft
- Authors: Tonbo team
- Created: 2026-03-12
- Area: Observability, Query, Storage I/O

## Summary

Complement RFC 0012 (logging and tracing) with a layered observability
proposal: `aisle`, `fusio`, and Tonbo all emit `tracing` for debugging and
correlation, while metrics remain separate and explicit. `aisle` exposes
prune-quality statistics, `fusio` exposes generic storage-I/O observation
hooks, and Tonbo owns database-level metrics and domain classification. The
design keeps all three libraries independently usable, avoids global
initialization or exporter side effects, and gives Tonbo one consistent
observability surface for debugging, benchmarking, and production operations.

## Motivation

Tonbo currently has three related observability problems:

- Slow or unstable behavior still often requires ad hoc `eprintln!` debugging.
- Benchmark-only probes explain some current scenarios, but they are not part of
  the engine and are therefore not reusable in normal runs.
- Tonbo needs production-safe observability without imposing global loggers,
  metric recorders, or exporters on downstream users of Tonbo, `fusio`, or
  `aisle`.

The current benchmark program already identified the main gap: Tonbo can
observe top-line read latency, but it still cannot cleanly attribute that cost
across prune effectiveness, storage requests, manifest work, WAL, compaction,
and GC.

This RFC proposes a layered ownership model so each library emits the signals
it uniquely understands while staying side-effect-free by default.

## Goals

- Make Tonbo debuggable without source-level print statements.
- Reuse the same observability signals in benchmarks, tests, and production.
- Keep `aisle`, `fusio`, and Tonbo independently usable with no required global
  initialization.
- Preserve the RFC 0012 library contract: Tonbo emits events/spans only and
  never configures global subscribers itself.
- Use traces and metrics for different purposes:
  - traces for debugging, correlation, and contextual explanation,
  - metrics for aggregated timing, operation counts, and error rates.
- Keep metrics explicit and pull-based by default, following Tonbo's existing
  `WalMetrics` and `CompactionMetrics` model.
- Attribute cost at the right layer:
  - prune effectiveness in `aisle`,
  - I/O request cost in `fusio`,
  - database semantics in Tonbo.
- Minimize overhead when observability is disabled or left unconfigured.

## Non-Goals

- Adding a mandatory `metrics` facade dependency to every library in the stack
- Shipping a built-in Prometheus server, OpenTelemetry exporter, or metrics HTTP
  endpoint from any core library
- Defining dashboards or SLOs in this RFC
- Recording high-cardinality metrics keyed by raw object path, primary key, or
  request ID by default
- Replacing RFC 0012; this RFC extends it with metrics and layering semantics

## Design

### Ownership by layer

| Layer | Knows about | Emits / returns |
| --- | --- | --- |
| `aisle` | Predicate pruning, bloom/page-index utility, residual fallback | `tracing` events/spans + `PruneStats` |
| `fusio` | Generic storage operations and outcomes | `tracing` events/spans + `IoObserver` callbacks or equivalent middleware |
| Tonbo | Scan/WAL/manifest/compaction/GC semantics | `tracing` events/spans + pull-based metric snapshots |

The key rule is: each layer emits only the information it can define without
learning Tonbo-specific semantics it does not own.

### Conceptual flow

```text
User operation
    |
    v
+----------------------+
| Tonbo engine         |
| scan / wal /         |
| compaction / gc      |
+----------------------+
    |
    v
+----------------------+
| aisle                |
| prune decisions      |
+----------------------+
    |
    v
+----------------------+
| fusio                |
| open/read/write/list |
| head/remove/CAS      |
+----------------------+
    |
    v
+----------------------+
| LocalFS / S3 / etc   |
+----------------------+
```

Observability follows the same layering:

```text
Tonbo:
  - tracing events/spans
  - ScanMetrics / ManifestMetrics / WalMetrics / CompactionMetrics / GcMetrics

aisle:
  - tracing events/spans
  - PruneStats

fusio:
  - tracing events/spans
  - generic I/O observation callbacks
```

### Emission model without side effects

This is the most important contract of the proposal.

#### Logs and traces

- `aisle`, `fusio`, and Tonbo may all emit `tracing` events and spans.
- None of these libraries may call `tracing_subscriber::init`, install a global
  logger, or start a background exporter.
- Applications choose subscribers, formatting, filtering, and export sinks.
- `aisle` and `fusio` should not emit Tonbo-specific fields or semantics.

Tracing is primarily for:

- contextual debugging,
- async correlation,
- explaining unusual or slow behavior,
- backend- or algorithm-specific diagnostics.

#### Metrics

- Metrics remain explicit and pull-based by default.
- No library in this stack should register a global metrics recorder.
- No library should start a metrics server or background exporter.
- Downstream applications may poll snapshots or adapt them into Prometheus,
  OpenTelemetry, or custom dashboards outside the core libraries.

Metrics are primarily for:

- aggregated time accounting,
- operation counts,
- error counts/rates,
- benchmark artifact generation,
- operator polling and dashboards.

#### Defaults

- The default configuration is no-op and side-effect-free.
- If a user does nothing, there should be no global initialization, no extra
  worker tasks, and no observable behavior change aside from negligible branch
  checks.

### Technical proposal for `aisle`

`aisle` should expose prune-quality data as plain returned data, not as
process-global metrics.

Proposed contract:

- Emit `tracing` spans/events around pruning work and prune fallbacks.
- Add a `PruneStats` struct to the prune result surface or a parallel
  `*_with_stats` API.
- `PruneStats` is plain data with no recorder or subscriber dependency.
- Callers can ignore the stats with zero semantic change.

Suggested fields:

| Field | Meaning |
| --- | --- |
| `row_groups_total` | Total row groups considered |
| `row_groups_selected` | Row groups retained after pruning |
| `row_groups_pruned` | Row groups eliminated |
| `bloom_consulted` | Bloom filters consulted |
| `bloom_positive` | Bloom said possible match |
| `bloom_negative` | Bloom ruled out match |
| `page_index_used` | Page index pruning was used |
| `residual_required` | Caller must apply full residual filter later |

Why plain returned data:

- `aisle` remains independently usable.
- Users who do not care about metrics pay no integration cost.
- Tonbo can aggregate these stats into scan metrics and benchmark artifacts.

Why `tracing` too:

- prune behavior is difficult to debug from aggregates alone,
- residual fallback and bloom/page-index decisions need contextual explanation,
- `tracing` is library-safe as long as `aisle` does not initialize subscribers.

### Technical proposal for `fusio`

`fusio` should expose generic I/O observation hooks or middleware around the
`Fs` / `FsCas` / `ObjectHead` boundary.

Proposed contract:

- Emit `tracing` spans/events for retries, failures, slow operations, and
  backend-specific diagnostics.
- Add an optional observer interface, for example `IoObserver`, or a generic
  middleware wrapper in `fusio`.
- Observation is explicit and opt-in.
- Default observer is no-op.
- `fusio` should remain generic: it records storage operation identity and
  outcome, but it does not derive Tonbo concepts such as WAL, SST, manifest, or
  GC.

Generic operation timing alone is insufficient for caller-side domain
attribution. An observation such as "read 32 KiB in 4 ms" does not tell Tonbo
whether the operation belonged to WAL replay, Parquet scan, manifest loading, or
GC unless `fusio` also returns enough identity for correlation.

Therefore, the observer payload must include:

- operation kind,
- duration,
- bytes transferred when meaningful,
- result / retry / error classification,
- the accessed path or object key when available,
- an optional opaque caller-supplied correlation token.

The path or object key allows callers such as Tonbo to classify storage objects
according to their own layout conventions. The opaque correlation token allows
higher layers to associate multiple storage operations with one logical request
without teaching `fusio` database semantics.

Suggested observed operations:

| Kind | Notes |
| --- | --- |
| `open` | File/object open |
| `read` | Read or range-read |
| `write` | Write or append |
| `list` | Prefix listing |
| `head` | Metadata/head lookup |
| `remove` | Delete/remove |
| `cas_load` | Conditional-load / tagged load |
| `cas_put` | Conditional-put / CAS publish |

Suggested observed fields:

| Field | Meaning |
| --- | --- |
| `op` | Operation kind |
| `path` | Accessed file/object path when available |
| `bytes` | Bytes transferred when meaningful |
| `duration_ns` | End-to-end operation duration |
| `success` | Success/failure outcome |
| `retries` | Retry count if operation retried internally |
| `error_kind` | Stable error classification when failed |
| `correlation_id` | Optional opaque caller-supplied token |

Why observer hooks instead of global metrics:

- `fusio` remains independently usable for non-Tonbo consumers.
- Users opt in explicitly.
- Tonbo can attach its own aggregator or wrapper without forcing exporter
  choices on all `fusio` users.

Why `tracing` too:

- storage debugging often needs contextual failure and retry information,
- metrics answer "how often" and "how much", but not "what happened on this
  operation",
- `fusio` can provide backend-level diagnostics without imposing sink choices.

### Technical proposal for Tonbo

Tonbo owns the database meaning and therefore owns the primary operator-facing
metrics and the top-level structured log events/spans.

Tonbo should add an explicit observability attachment surface, for example:

- an optional `ObservabilityConfig` on `DbBuilder`, or
- explicit metric/observer handles passed into builders and components.

The attachment surface must remain optional and default to no-op behavior.

Tonbo metrics should remain pull-based snapshots, similar to current
`WalMetrics` and `CompactionMetrics`.

Suggested Tonbo metric families:

| Family | Example fields |
| --- | --- |
| `ScanMetrics` | snapshot/plan/build/merge/package timing, rows scanned/returned, prune stats |
| `ManifestMetrics` | head fetch count/latency, decode latency, CAS attempts, CAS retries |
| `WalMetrics` | queue depth, bytes written, sync count, durable latency totals |
| `CompactionMetrics` | backlog, bytes in/out, job duration, CAS retries, obsolete outputs |
| `GcMetrics` | obsolete bytes pending, obsolete objects pending, reclaim latency, retention-blocked bytes |

Tonbo is also responsible for path/domain classification. Generic `fusio` I/O
events are not enough for operators. Tonbo must map them into stable database
domains such as:

- `wal`
- `sst_data`
- `sst_delete`
- `manifest_version`
- `manifest_catalog`
- `manifest_gc`
- `other`

This classification belongs in Tonbo because only Tonbo understands its storage
layout and object naming conventions.

### Traces vs metrics

This proposal intentionally treats traces and metrics as complementary, not
interchangeable.

Use traces for:

- debugging correctness or performance problems,
- reconstructing the causal path of one operation,
- correlating retries, residual fallbacks, CAS conflicts, and slow stages,
- explaining surprising behavior during development and incident analysis.

Use metrics for:

- where time is spent in aggregate,
- how many operations happened,
- how many errors happened,
- how much data moved,
- how a benchmark or workload trends over time.

Hot paths should prefer metrics for always-on aggregation. `tracing` on hot
paths should focus on spans, failures, retries, slow-operation thresholds, or
other contextual events that stay useful without turning into a firehose.

### Emission sketches (non-normative)

The proposal is intentionally explicit and opt-in.

#### `aisle`

`aisle` should emit `tracing` and expose prune stats as returned data:

```rust,ignore
let result = pruner.prune_async_with_stats(&mut provider).await?;
let selection = result.selection;
let stats = result.stats;
```

or equivalently:

```rust,ignore
let (selection, stats) = pruner.prune_async_with_stats(&mut provider).await?;
```

If a caller ignores `stats`, behavior is unchanged.

#### `fusio`

`fusio` should emit `tracing` and expose generic observation through explicit
hooks or middleware:

```rust,ignore
let observer = Arc::new(MyIoObserver::default());
let fs = fs.with_observer(observer);
```

or:

```rust,ignore
let fs = ObservedFs::new(inner_fs, observer);
```

If a caller does not attach an observer, behavior is unchanged.

#### Tonbo

Tonbo should attach metrics/observers explicitly through its builder or
component config:

```rust,ignore
let metrics = Arc::new(TonboMetrics::default());
let cfg = ObservabilityConfig::new(Arc::clone(&metrics));

let db = DbBuilder::from_schema_key_name(schema, "id")?
    .observability(cfg)
    .build()
    .await?;
```

Logs and traces remain host-controlled:

```rust,ignore
fn main() {
    tracing_subscriber::fmt().with_env_filter("info,tonbo=debug").init();
}
```

Tonbo still does not initialize the subscriber itself.

### Independent library usage

This RFC requires that each library stays independently usable.

Examples:

- An `aisle` user can call the old API or ignore `PruneStats`.
- A `fusio` user can avoid attaching any observer and see no side effects.
- A Tonbo user can build a database without observability configuration and see
  no global loggers, no metrics recorders, and no exporters.
- A benchmark can attach the same metric handles Tonbo uses in production and
  serialize snapshots to JSON without special benchmark-only plumbing.

### Relationship to RFC 0012

RFC 0012 remains the contract for logging and tracing:

- Tonbo emits `tracing` events and spans
- Tonbo does not initialize global subscribers
- applications configure sinks independently

This RFC adds the corresponding metric story:

- metric state is explicit and pull-based
- upstream libraries expose data or callbacks, not global side effects
- Tonbo binds the layers together into a database-level surface
- upstream libraries may emit `tracing`, but they do not configure sinks

### Rollout plan

#### Phase 1: `aisle` prune stats

- Add `tracing` around prune operations and fallbacks
- Add `PruneStats` as plain returned data
- Integrate into Tonbo scan-path profiling and benchmark artifacts

#### Phase 2: `fusio` generic I/O observer

- Add `tracing` for retries, failures, and slow operations
- Add opt-in observation hooks or middleware
- Keep payload generic and backend-agnostic

#### Phase 3: Tonbo observability surface

- Add optional observability attachment on builders
- Add `ScanMetrics`, `ManifestMetrics`, and `GcMetrics`
- Extend existing `WalMetrics` where needed
- Keep `CompactionMetrics` as the reference pattern for structured snapshots

#### Phase 4: Benchmark and operational integration

- Replace benchmark-local probes with runtime metrics where possible
- Emit benchmark artifacts from the same metric surfaces used in normal runs
- Document production integration patterns alongside RFC 0012 examples

### Success criteria

- A slow scan can be explained without code-local print statements.
- Benchmark artifacts reuse production metric surfaces instead of bespoke
  benchmark-only instrumentation where feasible.
- Tonbo users who do not opt into observability see no global side effects.
- `aisle` and `fusio` remain independently useful libraries for non-Tonbo users.
- The design scales to GC benchmarking and production GC debugging without
  changing the fundamental contracts.

## Alternatives Considered

### Tonbo-only instrumentation

Tonbo could keep all instrumentation locally by wrapping `Fs` and extending
benchmark probes.

Why not as the full answer:

- It duplicates generic work that belongs in `fusio`.
- It cannot surface prune-quality information as cleanly as `aisle` can.
- It encourages benchmark-specific plumbing instead of reusable observability.

This remains acceptable as a short-term fallback if upstream work blocks.

### Upstream-only instrumentation

The project could try to solve observability entirely inside `aisle` and
`fusio`, leaving Tonbo as a passive consumer.

Why not:

- Tonbo still owns the semantics operators care about.
- Upstream libraries should not be forced to understand Tonbo-specific domains.
- Tonbo-specific metrics such as WAL durable-ack lag, compaction backlog, and
  reclaim delay cannot be defined upstream.

### Global metrics facade in every library

Each library could add a `metrics` facade dependency and emit directly to a
global recorder.

Why not:

- It conflicts with the no-side-effects library goal.
- It couples core libraries to recorder configuration concerns.
- It makes embedded and test usage harder to control.

## Future Work

- Optional adapter crates or examples that export Tonbo snapshots to Prometheus,
  OpenTelemetry metrics, or structured JSON
- Sampling and redaction policy for high-cardinality debugging traces
- WASM-specific guidance for observation sinks and subscriber configuration
- Remote/service deployment patterns that combine trace correlation and metric
  polling across workers
