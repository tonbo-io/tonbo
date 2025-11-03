# RFC: Hybrid Mutable Memtable, Dynamic Ingest, and Deferred Ordering via Compaction

- Status: Draft
- Authors: Tonbo team
- Created: 2025-08-24
- Area: Storage engine, in-memory structures, Arrow interop

## Summary

Tonbo adopts a hybrid in-memory design that separates the duties of write absorption and scan efficiency:

- Mutable memtable: handles ordering and last-writer-wins semantics with minimal overhead on the write path.
- Immutable memtables: sealed, Arrow-native segments optimized for scans and downstream interoperability.

For OLAP/Arrow-centric workloads, we defer physical ordering of rows to background compaction. Ingest keeps the physical order of incoming data (typed rows or RecordBatches), while logical ordering is provided via per-chunk key indexes and K-way merged scans. Background compaction reorders data into sorted, compressed immutables.

This RFC records the design rationale, API impacts, and next steps.

> **Status update (October 21, 2025):** The current Tonbo codebase runs with
> the dynamic runtime-schema mode only. Compile-time typed dispatch described
> below remains part of the design intent but is temporarily removed from the
> implementation while we simplify the stack.

## Motivation

- Keep write path cheap for micro-batches and trickle inserts.
- Avoid copying string/binary payloads for dynamic ingestion; leverage Arrow buffers.
- Provide fast ordered key scans even before compaction completes.
- Align Tonbo with Arrow-first, OLAP patterns where vectorized scans and background clustering are standard.

## Goals

- Make columnar the single mutable memtable layout (dynamic today, typed ready to return).
- Provide ordered key scans via logical (index-based) ordering without rewriting payloads on insert.
- Enable dynamic ingestion of `RecordBatch` without copying payload cells.
- Prepare for background compaction that sorts and clusters immutable segments.

## Non-Goals

- Immediate row materialization from the mutable memtable (pre-compaction) for all workloads.
- Enforcing total order at ingest time.
- Implementing WAL/SST on-disk persistence in this RFC (planned as follow-up work).

## Design Overview

### Typed vs Dynamic Modes

- Typed (`TypedMode<R>`, currently disabled in code): callers insert typed rows `R: Record`. The mutable buffers rows unsorted; a small index maintains last offsets per key.
- Dynamic (`DynMode`): callers insert Arrow `RecordBatch`es. The mutable attaches each batch (unsorted) and builds a per-chunk key index, avoiding payload copies.

### Mutable Memtable Shape (Common Strategy)

- Append-first ingest:
  - Typed: append rows to an open buffer; update a small last-writer index.
  - Dynamic: attach `Arc<RecordBatch>` chunk; compute keys once and build a sorted `(key, row_idx)` vector per chunk.
- Logical order via per-chunk indexes:
  - For ordered scans, perform a K-way merge across sorted key vectors (newest → oldest), applying last-writer-wins dedup.
- Defer physical ordering:
  - Background compaction merges rows by key (or clustering key), producing sorted, compressed immutables.

### Why Not Require Per-row DynRecord?

- Row shims that clone payloads (for example `Vec<Option<DynCell>>`) still imply copying strings/binary per insert.
- Indexing from a `RecordBatch` allows reading keys once and keeping payloads zero-copy (via `Arc<RecordBatch>`), which is preferable for batch-oriented ingestion.

### Why Not Sort on Insert?

- Sorting on the hot path increases per-insert latency and write amplification.
- Compaction is asynchronous and amortizes sorting cost over larger runs, improving compression and scan performance without blocking writes.

## API Impact

- Scans:
  - `DB::scan_mutable_rows(&RangeSet)` returns values (rows). Key-only scans were removed to align with expectations.
  - Immutable scans continue to use key indexes internally but surface rows to callers.
- Dynamic ingest:
- `DB<DynMode, E>::insert_batch(RecordBatch)` remains; we build per-chunk key indexes and keep the batch payload zero-copy.
  - Optional: add `insert_dyn_key<K: Into<KeyOwned>>(key: K)` for single-key updates without row objects.
- Typed ingest (planned once re-enabled):
  - `DB<TypedMode<R>>::insert(row: R)` appends and updates the last-writer index; optional `insert_key(R::Key)` may be added later.

## Alternatives Considered

- Global BTree index over all mutable keys:
  - Simpler scans but O(log N) per row on ingest; unfavorable for large batches.
- Per-row `DynRecord` (owned or view-based):
  - Owned rows copy payloads; view-based complicates lifetimes and APIs. Not necessary for ordering-only memtable.
- Sorting batches at ingest:
  - Adds latency and write amp; better deferred to compaction.

## Read Path Before Compaction

- Scans: K-way merge across per-chunk indexes (typed open buffer would add a small index once re-enabled), newest → oldest, with last-writer dedup; rows are materialized for callers.
- Data skipping: expose per-chunk stats (min/max, counts) and optional bloom/prefix filters to prune early.
- Row/batch scans (future): when needed, gather rows by offset across chunks to materialize `RecordBatch`es without copying.

## Compaction

- Background task merges multiple chunks to produce sorted immutables (Arrow IPC/Parquet target formats).
- Sorting/clustering key configurable (primary key, time, or composite).
- Applies last-writer-wins, handles tombstones/deletes.
- Produces segment-level stats and filters to accelerate future scans.

## Performance Considerations

- Ingest throughput: O(1) amortized append + O(n log n) per-batch key index build; no payload copies for dynamic.
- Scan performance: ordered key scans via K-way merge; vectorized row scans post-compaction.
- Compression: sort during compaction improves dictionary/RLE encoding.
- Memory: mutable tracks key indexes and references to batches (dynamic) or open rows (typed); payload duplication is avoided on ingest.

## Migration & Compatibility

- Breaking change: key-only scan APIs removed; scanning returns rows.
- Old row-based `MutableMemTable` is replaced by columnar mutable structures and per-chunk indexes.
- Examples updated to demonstrate row scans.

## Drawbacks & Risks

- Pre-compaction, payload materialization from the mutable requires additional gather logic (planned follow-up).
- Memory pinned by `Arc<RecordBatch>` chunks until compaction; mitigated by compaction and chunk sizing.
- Slightly more complex scan implementation (K-way merge) versus a single global map.

## Open Questions

- Deletes/tombstones representation in mutable indexes; impact on merge and compaction.
- Snapshot semantics and reader concurrency across chunk sets.
- Heuristics: auto-rotation/ sealing thresholds (rows/bytes), dead-row ratios, memory budgeting.
- Exact stats/filtering APIs for data skipping.

## Future Work

- WAL + crash recovery; on-disk SST/segment formats (Arrow IPC/Parquet + sidecar index).
- Row/batch scan helpers from mutable indexes (dynamic today, typed once reinstated).
- DataFusion/Polars interop for direct query execution over immutables.
- Background compaction policy (tiered compaction, TTL, clustering strategies).

## Appendix: Module Mapping (as of this RFC)

- `src/inmem/mutable/memtable.rs`:
  - `DynMem`: columnar mutable backed by attached `RecordBatch` chunks and a last-writer index.
  - Typed layout scaffolding is removed for now but the trait surface stays ready for it.
- `src/inmem/immutable/memtable.rs`:
  - Generic immutable memtable with runtime builders (`segment_from_batch_with_*`). Typed builders will return alongside the typed mode.
- `src/db.rs`:
  - `DB<M: Mode, E: Executor + Timer>` currently instantiates `DynMode`; the trait-based structure lets us add a typed mode later without reshaping APIs.
