# Rethinking Tonbo — Dev Branch Summary

<aside>
💡 Build a new Arrow-first core alongside the existing code, then cut over incrementally (“strangler” migration). Don’t rewrite the whole repo from zero, and don’t refactor legacy paths in-place.
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

### Phase 1

- Primary key + version columns: define primary key encoding; append `_commit_ts` and `_tombstone` columns; choose in-memory segment layout; define filter format.
- In-memory segment + index: build a read-only in-memory segment with a primary-key index; attach Arrow record batches; remove duplicates inside each batch; skip unused columns.
- Range scan + filter + order/limit: add range scans; run column-based filters; support `ORDER BY` primary key ascending; stop early when `LIMIT` is reached.
- On-disk files + index + stats: write Arrow files (IPC format) plus a separate primary-key index file; record per-segment stats; define the manifest record format.
- Log + publish + recovery: add a batch write-ahead log that can be replayed safely; publish data and index together in one step on the local file system; on restart, rebuild state from the manifest and the log; run an end-to-end durability test.

### Phase 2

- Streaming API: stream Arrow batches over HTTP; add flow control and chunk size; use one scan path for both memory and disk segments.
- Multi-table basics: add a small table catalog and request routing; per-table directories; simple usage limits.
- Snapshots: add a snapshot token (64-bit); keep an in-process list of active readers; make scans respect the safe cutoff (no deletions yet).
- Checkpoints: write and list named checkpoints (pin file IDs, commit time, schema version, stats); integrate with the manifest.
- Stats + planner: aggregate table and column stats; feed rows and row width to the Postgres FDW; stabilize, document, and run basic benchmarks.

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

- **Previous:** No table-level stats; no persisted per-segment stats; PG planner blind to row counts.
- **Target:** Table stats (rows, bytes, avg width); column stats (`min`, `max`, `null_count`, later NDV/histograms); unified across memory + SST; FDW integration; expose via admin API.

### Multi-table (Column-Family Style)

- **Previous:** Single-table process; no catalog; duplicated caches; 1:1 endpoints.
- **Target:** Many tables per process; shared resources (threadpools, caches, fusio handles); catalog APIs; per-table WAL/manifest; quotas and isolation; per-table access control & schema/version policy.

### Snapshot & Checkpoint

- **Previous:** In-memory `read_ts` only; manifest compaction after N edits; recovery replays per-row WAL; no durable checkpoints.
- **Problems:** u32 wrap risk; no snapshot token; no reader registry; slow recovery; no batch idempotency.
- **Target:** u64 commit/read timestamps; serialized snapshot tokens; uniform MVCC columns; reader registry for GC watermarks; manifest-integrated checkpoints (pin SST IDs, commit_ts, schema version, WAL HWM, stats); GC never deletes checkpointed SSTs; prune only when `end_ts <= watermark`.

## Risks

- **Small segments:** enforce seal thresholds; micro-merge in background; admission control on segment count.
- **ORDER BY cost on unsorted L0:** heap-based k-way merge with early `LIMIT`; fall back to compaction if needed.
- **Object-store atomicity:** avoid rename assumptions; write data/index, validate checksums, commit manifest as source of truth; use idempotent `batch_id`.
- **MVCC complexity:** start single-writer or coarse commit allocator; expand once WAL/manifest hardened.
- **Stats drift:** compute `min/max/nulls` eagerly; NDV/histograms opportunistically; bound CPU with sampling; cache aggregates in manifest.

## Status Update (October 2025)

| Component | Status | Evidence |
| --- | --- | --- |
| Manifest Core | Not started | No manifest modules; flush returns `SsTable` only (`src/db.rs:215-324`). |
| Recovery Pipeline | Not started | Startup rebuilds purely from WAL replay (`src/db.rs:94-167`). |
| GC & Watermarks | Not started | Watermark tracking or object deletion logic absent across crate. |
| Table Catalog & Admin Surface | Not started | No catalog/admin modules exported (`src/lib.rs:9-41`). |
| Range Planner MVP | Not started | Only `RangeSet` utilities exist; no planner implementation (`src/scan.rs:1-160`). |
| Merge Executor | In progress | MergeStream/ordered merge code missing entirely. |
| SST Integration | Not started | `SsTableReader::collect` returns `Unimplemented` (`src/ondisk/sstable.rs:335-347`). |
| Projection & Filters | Not started | Read path materializes full rows; no projection helpers (`src/inmem/mutable/memtable.rs:42-200`). |
| Minor Flush Pipeline | In progress | Parquet writer stages immutables but lacks publish wiring (`src/ondisk/sstable.rs:180-310`). |
| Stats & PK Index Build | In progress | Writer accumulates stats yet no sidecar PK index persisted (`src/ondisk/sstable.rs:180-310`). |
| Leveled/Range Planner | Not started | Compaction module lacks leveled scheduling or overlap logic (`src/compaction.rs:21-140`). |
| Observability & Tuning | Not started | Metrics types exist but no exported counters/hooks (`src/inmem/mutable/metrics.rs`; `src/wal/metrics.rs`). |
| Snapshot Tokens & Reader Registry | Not started | MVCC module offers timestamps only; no durable snapshots (`src/mvcc/mod.rs:1-120`). |
| Tombstone Pruning | In progress | Immutable segments track tombstones without compaction pruning (`src/inmem/immutable/memtable.rs:18-200`). |
| Conflict Detection & Idempotency | Not started | Transactions/manifest commits unimplemented despite docs. |
| WAL Replay Hardening | In progress | Frame codec & writer exist; rotation/idempotent replay still TODO (`src/wal/writer.rs:180-360`; `src/wal/mod.rs:296-540`). |

This table is the concise requirements tracker referenced throughout the migration; keep it in sync with `docs/overview.md` and the RFCs as implementation progresses.

