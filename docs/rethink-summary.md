# Rethinking Tonbo — Dev Branch Summary

<aside>
Build a new Arrow-first core alongside the existing code, then cut over incrementally ("strangler" migration). Don't rewrite the whole repo from zero, and don't refactor legacy paths in-place.
</aside>

Rethinking Tonbo pivots the engine to a single Arrow‑first, runtime columnar core with native `RecordBatch` ingest and pushdown-capable read APIs. It sheds legacy surfaces that failed user expectations, making composite keys, schema versioning, and a clear path to durability and snapshot isolation first-class. The result is a simpler, interoperable DBaaS foundation—one storage truth (indexed columnar segments), better performance and UX for Python/JS users, and clearer evolution without backward-compatibility baggage.

## Goals

- **Unblock adoption:** Modernize read/write APIs so users can adopt Tonbo without workarounds; reduce tight coupling between interfaces and internals.
- **Native Arrow ingest:** Accept Arrow `RecordBatch` as a first-class input format across interfaces.
- **Pushdown reads:** Provide a logical expression API with filter/projection/limit (and basic ordering/ranging) pushdown.
- **Composite primary keys:** Make single and composite keys first-class with consistent range semantics across modes.
- **Schema evolution:** Support schema versioning, validation, and a clear compatibility/deprecation policy visible to clients.
- **Durability & isolation:** Provide durability and snapshot-consistent reads suitable for production workloads.
- **DBaaS readiness:** Meet multi-tenant needs (auth, quotas), observability (per-dataset metrics, schema/version tracking), and operational controls.
- **Performance targets:** High-throughput batch ingest, predictable low-latency point/range reads, and bounded memory usage.
- **Interoperability:** Arrow-friendly I/O and SDK ergonomics for Python/JS ecosystems; straightforward import/export.

## Non-goals

- **Backward compatibility:** Not required; prior Tonbo APIs/formats may be dropped.
- **Algorithm rewrites:** No mandate to fully redesign compaction or related strategies (review and adjust only as needed).
- **Legacy client bindings:** No refactor of existing Python/JS bindings; they may be deprecated and removed.

## Why & Why Now

- **Write API shift:** First-class Arrow `RecordBatch` ingest—decomposing batches into per-row inserts is wasteful. The legacy dynamic “row” type is unnecessary; runtime schema should be natively `RecordBatch`.
- **Read API pushdown:** Expose a logical expression API with filter/projection/limit pushdown. The legacy path couldn’t push down, forcing broader scans and materialization.
- **Internal model realignment:** Memtable/segment structures must match the new APIs: columnar-first ingestion, per-segment key indexes, last-writer dedup via merged scans, and background compaction. The old row-centric mutable doesn’t fit batch ingest.
- **Composite keys & schema evolution:** Adding composite primary keys and better runtime-schema handling on the legacy architecture is overly complex and error-prone; a clean redesign lowers coupling and risk.
- **DBaaS alignment:** Users already produce Arrow batches in Python/JS; supporting native `RecordBatch` now improves interop, performance, and developer experience.
- **Cost of delay:** Retrofitting later compounds technical debt and migration risk; refactoring around a unified, Arrow-first core simplifies APIs and future MVCC/WAL work.

## Use Cases

| **Id** | **Who (mock)** | **What they do** | **Current stack** | **What they want** | **How Tonbo helps** |
| --- | --- | --- | --- | --- | --- |
| 1 | swing.io | Build public cloud on the edge. | Delta Lake for logs (two-tier append/merge workaround). | 1) Append-friendly data lakehouse 2) Serverless scale with cheap read replicas | High-performance serverless log store with Arrow-native analytics. |
| 2 | Clara Dev | Agentic coding CLI tool. | JSON data without similarity retrieval. | 1) Unified context management 2) Proper history retrieval 3) Cross-tool integration | Structured context store with vector-friendly extensions instead of ad hoc JSON. |
| 3 | tab.com | General agent platform. | Turbopuffer for similarity indexing. | Edge-first, self-hosted indexing service to cut latency and cost. | Open-source serverless DB that can be customised for indexing in weeks. |

## Execution Plan

### Phase 1 (Complete)

- [x] Primary key + version columns: primary key encoding via `KeyExtractor`; `_commit_ts` MVCC column; tombstones via delete sidecar.
- [x] In-memory segment + index: `ImmutableSegment` with Arrow record batches; PK index for dedup; projection support.
- [x] Range scan + filter + order/limit: `ScanBuilder` with predicates, projection, limit; PK ascending order via `MergeStream`.
- [x] On-disk files + index + stats: Parquet SSTs with `SsTableStats`; manifest tracks SST entries per level.
- [x] Log + publish + recovery: batch WAL with frame-level writes; manifest publish via CAS; `recover_with_wal` tested end-to-end.

### Phase 2

- [x] Streaming API: `ScanBuilder` streams Arrow batches; unified scan path for memory and disk segments via `MergeStream`.
- [ ] Multi-table basics: internal `CatalogCodec` exists; per-table manifest entries work; public admin APIs pending.
- [x] Snapshots: 64-bit `Timestamp` as snapshot token; `snapshot_at()` and `list_versions()` APIs work; manifest-backed durability.
- [ ] Checkpoints: manifest-integrated version tracking works; named checkpoints with tags pending (#554).
- [ ] Stats + planner: table/column stats computed at flush time; scan planning exists; Parquet page-level pruning pending (#551).

## Component-Level Changes

### Memtable

- **Previous:** Row MVCC (`SkipMap<Ts<Key>>, Option<R>>`) with per-row WAL; dynamic rows via owned `DynRecord`; scans iterate (key, ts) order with limited pushdown.
- **Problems:** Not Arrow-first (row splitting, copies); no column vectors or per-column stats; `O(log N)` insert cost; difficult composite keys/schema evolution.
- **Target:** Columnar ingestion into indexed, read-only segments; optional write buffer for immediate reads; tombstones in buffer and segments; API exposes `range`/`predicate`/`projection`/`limit`; policies to seal/coalesce segments with metrics.

### SSTable

- **Previous:** Flush iterates `SkipMap` in key/timestamp order; per-batch BTree index keyed by `Ts<Key>`; tombstones via `Option<R>`; L0 physical order fixed at write time.
- **Problems:** Sort-on-write adds CPU; row-ordered flush blocks `RecordBatch` attach; ts-in-key complicates MVCC; no uniform pushdown across RAM/SST.
- **Target:** Parquet/Arrow IPC per SST plus sidecar PK index; unsorted L0 with external PK index (sort in major compaction); MVCC columns (`_commit_ts`, `_tombstone`); batch-level WAL with idempotency; compaction rewrites with PK sort/dedup/tombstone fold; stats drive pushdown.

### Row/Size Estimation

- **Previous:** No table-level stats; no persisted per-segment stats.
- **Current:** `StagedTableStats` computes rows, bytes, key bounds, commit_ts range at flush time; stats stored in SST descriptors and manifest.
- **Remaining:** Column-level NDV/histograms; public stats API; query planner integration.

### Multi-table (Column-Family Style)

- **Previous:** Single-table process; no catalog; duplicated caches.
- **Current:** `CatalogCodec` in manifest supports multi-table registration; per-table `TableId` and manifest entries; `register_table()` with schema validation.
- **Remaining:** Public catalog APIs; shared resource pools; quotas and isolation; per-table access control.

### Snapshot & Checkpoint

- **Previous:** In-memory `read_ts` only; no durable checkpoints; u32 wrap risk.
- **Current:** u64 `Timestamp`; `snapshot_at()` and `list_versions()` APIs; manifest-backed snapshots survive restart; uniform MVCC columns (`_commit_ts`); WAL floor tracking.
- **Remaining:** Named checkpoints with tags (#554); reader registry for GC watermarks (#547); version retention policies.

## Risks

- **Small segments:** enforce seal thresholds; micro-merge in background; admission control on segment count.
- **ORDER BY cost on unsorted L0:** heap-based k-way merge with early `LIMIT`; fall back to compaction if needed.
- **Object-store atomicity:** avoid rename assumptions; write data/index, validate checksums, commit manifest as source of truth; use idempotent `batch_id`.
- **MVCC complexity:** start single-writer or coarse commit allocator; expand once WAL/manifest hardened.
- **Stats drift:** compute `min/max/nulls` eagerly; NDV/histograms opportunistically; bound CPU with sampling; cache aggregates in manifest.

## Status Update (December 2025)

| Component | Status | Evidence |
| --- | --- | --- |
| Manifest Core | Complete | `TonboManifest` wraps catalog+version stores; flush publish path includes SST and WAL refs with CAS. |
| Recovery Pipeline | Complete | WAL replay restores state with ordering hints; `recover_with_wal` path tested end-to-end. |
| GC & Watermarks | In progress | WAL floor tracking and `prune_wal_segments_below_floor` implemented; GC worker for SST deletion pending (#547). |
| Table Catalog & Admin Surface | In progress | Internal `CatalogCodec` exists; public admin APIs not yet exported. |
| Scan Planning | In progress | `ScanPlan` with `plan_scan()` implemented; SST/page-level pruning pending (#551). |
| Merge Executor | Complete | `MergeStream` + `PackageStream` perform MVCC-aware k-way merges; plan/execute scan path wired. |
| SST Integration | Complete | `SstableScan` streams Parquet data and delete sidecars with MVCC filtering. |
| Projection & Filters | In progress | Predicates are applied post-merge/materialize; no page/row-group pruning or pushdown yet. |
| Minor Flush Pipeline | Complete | Parquet writer emits data + MVCC + delete sidecar files; manifest publication runs after flush. |
| Stats & PK Index Build | In progress | `StagedTableStats` tracks key/ts bounds and bytes; stats written to SST descriptors; PK sidecar index format TBD. |
| Leveled Compaction Planner | In progress | `LeveledCompactionPlanner` implemented with level selection logic; background scheduling pending (#545). |
| Major Compaction Executor | In progress | `CompactionExecutor` and `SstableMerger` work; manifest CAS with retry implemented; background loop pending (#550). |
| Observability & Tuning | In progress | Memtable stats drive sealing; WAL writer tracks queue-depth/sync counters; comprehensive metrics pending (#552). |
| Snapshot & Time Travel | Complete | `snapshot_at(timestamp)` and `list_versions()` public APIs work against manifest-backed snapshots. |
| Reader Registry | Not started | Required for GC watermark computation; tracked in #547. |
| Tombstone Pruning | In progress | Tombstones tracked in immutable segments and delete sidecars; compaction-time pruning pending (#549). |
| Conflict Detection & Idempotency | Complete | Transaction commit locks keys, checks mutable/immutable conflicts, batches WAL tickets. |
| WAL Replay Hardening | Complete | Replayer honors manifest WAL floors; recovery bumps commit clocks; pinned segment tests pass. |

This table is the concise requirements tracker referenced throughout the migration; see GitHub issues #545-556 for detailed epic tracking.
