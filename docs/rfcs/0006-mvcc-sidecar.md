# RFC: MVCC Sidecar Storage for Immutable Segments

- Status: Draft
- Authors: Tonbo storage team
- Created: 2025-10-28
- Area: Storage engine, MVCC, WAL, SSTable, Arrow interoperability

## Summary

- Preserve user-defined Arrow schemas during ingestion, WAL persistence, and SST flushes by moving commit timestamps and tombstone markers into a dedicated MVCC sidecar.
- Introduce explicit sidecar objects (Arrow/Parquet) that travel with each immutable segment and SST, referenced by the manifest.
- Refactor ingestion, WAL encoding, and read path to operate on `(RecordBatch, MvccColumns)` pairs while keeping MVCC semantics (latest-wins, snapshot visibility) unchanged.

## Motivation

- Current implementation appends `_commit_ts`/`_tombstone` columns into user batches (`attach_mvcc_columns`), forcing schemas to allow nulls and leaking system metadata into user-visible Parquet.
- Tombstoned rows are materialized as all-null payloads, violating non-nullable schema expectations and complicating downstream integration.
- `docs/overview.md` describes immutable snapshots as Arrow-native reflections of user data; aligning with that contract reduces friction for external tooling and keeps MVCC metadata implementation-specific.
- Separating MVCC state opens the door to richer metadata (per-row audit info, CDC) without further schema churn.

## Goals

- Immutable segments and SST files expose exactly the user schema; MVCC data is carried alongside but never inside the batch.
- WAL payloads remain stream-friendly and replayable without schema mutation.
- Compaction, recovery, and GC continue to operate over append-only objects via Fusio.
- Minimal runtime overhead for scans and range lookups relative to the current implementation.

## Non-Goals

- Changing timestamp assignment strategy (still monotonic per commit).
- Introducing new transaction semantics or cross-table coordination.
- Redesigning compaction algorithms beyond consuming the new layout.
- Typed/compile-time ingestion pathways (still future work per RFC 0001).

## Background

- `DynMem::seal_into_immutable` currently synthesizes null rows for tombstones and calls `attach_mvcc_columns`, which appends hidden columns to the batch (`src/inmem/mutable/memtable.rs`).
- WAL appends rely on `append_tombstone_column`/`split_tombstone_column` to shuttle the hidden column through Arrow IPC (`src/wal/mod.rs`, `src/wal/frame.rs`).
- SST flushes stream the widened batch into Parquet, so user readers see `_commit_ts`/`_tombstone`, conflicting with schema guarantees outlined in `docs/overview.md` §Data Model/MVCC.

## Proposal

### 1. Immutable representation

- Replace `attach_mvcc_columns` with `bundle_mvcc_sidecar(batch, commit_ts, tombstone)` returning the original `RecordBatch` plus `MvccColumns`.
- Ensure `ImmutableMemTable` stores the original batch and MVCC vectors without modifying the schema; maintain index and iterators as today.

### 2. MVCC sidecar format

- Define `MvccSidecar` schema: `_commit_ts: UInt64 (non-null)`, `_tombstone: Boolean (non-null)`.
- In memory, continue using `MvccColumns` (`Vec<Timestamp>`, `Vec<bool>`).
- Persist sidecar columns as a single-row-group Arrow/Parquet file aligned 1:1 with the data batch.

### 3. WAL changes

- Keep the helpers as WAL-local utilities: `append_mvcc_columns` widens batches for encoding; `split_mvcc_columns` strips MVCC columns immediately after decoding.
- `Wal::append` accepts the user `RecordBatch`, a tombstone slice, and the commit timestamp; it materializes Arrow arrays internally so MVCC metadata stays hidden from callers.
- Frame encoding continues to emit a single Arrow IPC stream with `_commit_ts` and `_tombstone` appended; decode recovers `(RecordBatch, Option<Timestamp>, ArrayRef, ArrayRef)` and drops the widened schema before handing the payload to replay.
- WAL tests and recovery specs assert bitmap-length validation occurs prior to encoding and that MVCC columns never escape the WAL boundary.

### 4. Flush & manifest

- `ParquetTableWriter::stage_immutable` writes two objects per segment:
  - `…/segment.data.parquet` (user schema),
  - `…/segment.mvcc.parquet` (sidecar).
- Extend `SsTableDescriptor` and manifest entries with:
  - `data_uri`,
  - `mvcc_uri`,
  - ~~MVCC stats (rows, tombstone count, min/max commit_ts).~~ no longer required as recorded in SstableStats already.
- Fusio uploads both objects before committing the manifest edit.
- Persist data and mvcc path in manifest using SstEntry.

### 5. Read path & compaction

- Load both files when materializing immutable segments for scans or compaction.
- `ImmutableMemTable::from_storage` reconstructs the in-memory `(RecordBatch, MvccColumns)` pair.
- Range and visibility scans continue to use `MvccColumns`; callers never see `_commit_ts`/`_tombstone` columns.

### 6. Recovery & GC

- WAL replay already yields `(RecordBatch, Vec<bool>)`; apply logic remains identical.
- Manifest-driven GC marks both `data_uri` and `mvcc_uri` for retention/removal atomically.

### 7. API & observability

- Update public ingestion APIs to remove any mention of `_tombstone` columns; tombstone vectors stay explicit parameters.
- Metrics (e.g., tombstone counts) now derive from sidecar stats; adjust exporters accordingly.

## Storage layout updates

```
/sst/L0/000000000000000123.parquet        # user data
/sst/L0/000000000000000123.mvcc.parquet   # sidecar
```

Manifest entry:

```
{ id: 123, level: 0, data_uri: "...123.parquet", mvcc_uri: "...123.mvcc.parquet",
  stats: { rows, bytes, tombstones, min_commit_ts, max_commit_ts } }
```

Sidecar Parquet uses `SNAPPY` compression by default (configurable alongside data compression).

## Compatibility & migration

- New segments written after the rollout use the sidecar layout.
- For existing SSTs (with embedded MVCC columns), provide a background migrator:
  1. Load the legacy batch.
  2. Split hidden columns into `MvccColumns`.
  3. Rewrite to data + sidecar files.
  4. Publish manifest edit removing `_commit_ts`/`_tombstone`.
- During transition, readers detect legacy files by schema inspection and fall back to the old path until migration completes.

## Alternatives considered

1. **Keep hidden columns**: simplest but fails schema-compatibility goal and forces nullability.
2. **Store MVCC in Parquet metadata/row group stats**: metadata lacks per-row fidelity, complicates streaming reads.
3. **Embed tombstone in WAL only**: makes SST reconstruction expensive and breaks crash recovery invariants.

## Open questions

- Should the sidecar support future fields (e.g., `_write_id`, `_op_type`)? Proposal reserves extension slots but defers schema evolution rules.
- Do we enforce identical compression codecs for data and sidecar to simplify deployment?
- What retention policy should GC apply to orphaned sidecars if a manifest edit references one file but not the other (corruption handling)?

## Implementation plan

1. Refactor in-memory and WAL code paths to operate on `(RecordBatch, MvccColumns)` without modifying batches.
2. Introduce sidecar writer/reader utilities and update SST builder + manifest descriptors.
3. Adjust recovery, compaction, and range scans to load sidecars.
4. Update docs (overview, storage layout RFC) and unit/integration tests.
5. Ship a migrator for legacy SSTs and gate enablement behind a feature flag.
6. Flip the feature flag once compatibility tests pass, then clean up legacy helpers.

## Testing strategy

- Extend property tests to ensure sidecar length equals batch rows.
- WAL round-trip tests verifying new IPC encoding.
- SST flush/replay integration tests that write/read both files and assert schema purity.
- Migration tests covering legacy -> sidecar conversions.

## Rollout

- Phase 0: land refactors behind crate-private feature.
- Phase 1: enable in staging, run dual-read validation comparing legacy vs sidecar scans.
- Phase 2: migrate production data, remove feature flag, deprecate `_commit_ts`/`_tombstone` columns.
