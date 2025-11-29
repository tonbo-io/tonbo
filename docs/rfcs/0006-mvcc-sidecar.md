# RFC: MVCC Sidecar Storage for Immutable Segments (superseded)

- Status: Superseded by lean layout (data embeds `_commit_ts`; delete sidecar only)
- Authors: Tonbo storage team
- Created: 2025-10-28
- Area: Storage engine, MVCC, WAL, SSTable, Arrow interoperability

## Summary

This RFC is superseded by the lean SST layout now implemented in-code: `_commit_ts` is stored as a column in the data Parquet, and only a key-only delete sidecar (`.delete.parquet`) is emitted when tombstones exist. The separate MVCC sidecar (`.mvcc.parquet`) described below is no longer used. The historical content is retained for context.

## Motivation

- Current implementation appends `_commit_ts`/`_tombstone` columns into user batches (`attach_mvcc_columns`), forcing schemas to allow nulls and leaking system metadata into user-visible Parquet.
- Tombstoned rows are materialized as all-null payloads (or, after the mutable refactor, synthesized only during sealing) solely to satisfy the existing immutable/SST layout.
- `docs/overview.md` describes immutable snapshots as Arrow-native reflections of user data; aligning with that contract reduces friction for external tooling and keeps MVCC metadata implementation-specific.
- Separating MVCC state opens the door to richer metadata (per-row audit info, CDC) without further schema churn.

## Goals

- Immutable segments and SST files expose exactly the user schema; delete metadata is carried alongside in a key-only sidecar, while `_commit_ts` stays in `MvccColumns` for upserts.
- WAL payloads stay Arrow-native and replayable without schema mutation beyond the existing `_commit_ts` columns.
- Compaction, recovery, and GC continue to operate over append-only objects via Fusio.
- Minimal runtime overhead for scans and range lookups relative to the current implementation.

## Non-Goals

- Changing timestamp assignment strategy (still monotonic per commit).
- Introducing new transaction semantics or cross-table coordination.
- Redesigning compaction algorithms beyond consuming the new layout.
- Typed/compile-time ingestion pathways (still future work per RFC 0001).

## Background

- `DynMem::seal_into_immutable` currently synthesizes null rows for tombstones and calls `attach_mvcc_columns`, which appends hidden columns to the batch (`src/inmem/mutable/memtable.rs`).
- WAL appends rely on `append_commit_column`/`split_commit_column` to shuttle the hidden column through Arrow IPC (`src/wal/mod.rs`, `src/wal/frame.rs`).
- SST flushes stream the widened batch into Parquet, so user readers see `_commit_ts`/`_tombstone`, conflicting with schema guarantees outlined in `docs/overview.md` §Data Model/MVCC.

## Proposal

### 1. Immutable representation

- Keep `bundle_mvcc_sidecar(batch, commit_ts, tombstone)` for upserts so `_commit_ts` continues to travel outside the user schema, but stop duplicating tombstones in the row batch. The data batch stays untouched and MVCC vectors carry timestamps only.
- Track delete intents as dedicated `DeleteBatch { keys: RecordBatch (primary-key schema), commit_ts: UInt64Array }` values. `ImmutableMemTable` stores both the upsert `(RecordBatch, MvccColumns)` pair and a list of delete batches.

### 2. Delete sidecar format

- Define `DeleteSidecar` schema: `<primary-key columns>, _commit_ts: UInt64 (non-null)`.
- Persist deletes as Arrow/Parquet artifacts, one per immutable segment (and later per SST), aligned 1:1 with the segment. Upsert MVCC columns continue to mirror the existing `_commit_ts` sidecar.

### 3. WAL changes

- WAL row payloads stay as they are today: `RowPayload` batches still carry `_commit_ts` in their MVCC columns; delete batches use the new `KeyDelete` row mode (already implemented) and never fabricate value columns.
- Replay produces `(RecordBatch, MvccColumns)` for upserts and `DeleteBatch` for tombstones, matching the in-memory layout.

### 4. Flush & manifest

- `ParquetTableWriter::stage_immutable` writes two (eventually three) artifacts per segment:
  - `…/segment.data.parquet` (user schema),
  - `…/segment.mvcc.parquet` (unchanged `_commit_ts` sidecar for upserts),
  - `…/segment.delete.parquet` (new key-only delete sidecar).
- Extend `SsTableDescriptor` and manifest entries with a `delete_uri` alongside the existing `data_uri`/`mvcc_uri`.
- Fusio uploads all artifacts before committing the manifest edit; GC treats them atomically.

### 5. Read path & compaction

- Load both the MVCC sidecar (timestamps) and the delete sidecar when materializing immutables/SSTs. Range scans consult the delete sidecar to suppress tombstoned versions without ever seeing placeholder rows.
- Compaction merges delete sidecars the same way it merges data files, applying the latest-wins rule based on `_commit_ts`.

### 5. Read path & compaction

- Load both files when materializing immutable segments for scans or compaction.
- `ImmutableMemTable::from_storage` reconstructs the in-memory `(RecordBatch, MvccColumns)` pair.
- Range and visibility scans continue to use `MvccColumns`; callers never see `_commit_ts`/`_tombstone` columns.

### 6. Recovery & GC

- WAL replay already yields `(RecordBatch, Vec<bool>)` and `DeleteBatch`; apply logic remains identical to the mutable implementation.
- Manifest-driven GC marks `data_uri`, `mvcc_uri`, and `delete_uri` for retention/removal atomically.

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

1. (Done) Refactor mutable/WAL code paths to keep deletes key-only while leaving `_commit_ts` handling unchanged.
2. Introduce delete sidecar writer/reader utilities and update `SsTableBuilder` + manifest descriptors with `delete_uri`.
3. Teach `ImmutableMemTable`/SST readers to load delete sidecars and merge them with upsert batches during scans.
4. Update docs (overview, storage layout RFC) and unit/integration tests to reflect the tombstone sidecar.
5. Ship a migrator that rewrites existing SSTs by extracting tombstones from their data batches into delete sidecars.
6. Flip the feature flag once compatibility tests pass, then remove the legacy placeholder-row shim.

## Testing strategy

- Extend property tests to ensure sidecar length equals batch rows.
- WAL round-trip tests verifying new IPC encoding.
- SST flush/replay integration tests that write/read both files and assert schema purity.
- Migration tests covering legacy -> sidecar conversions.

## Rollout

- Phase 0: land refactors behind crate-private feature.
- Phase 1: enable in staging, run dual-read validation comparing legacy vs sidecar scans.
- Phase 2: migrate production data, remove feature flag, deprecate `_commit_ts`/`_tombstone` columns.
