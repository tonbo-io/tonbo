# RFD 0005: SSTable Skeleton

- Status: Implementing
- Authors: Tonbo team
- Created: 2025-10-24
- Area: SsTable, Compaction

## Summary

Tonbo’s dynamic mode now ships a working on-disk module (`ondisk::sstable`) that turns sealed immutables into Parquet SSTables and streams them back with MVCC-aware merge. The data file appends `_commit_ts` to user columns; tombstones live in an optional key-only delete sidecar. WAL ids and stats are captured for manifest/compaction, and the scan path merges data + delete streams to enforce latest-wins. This RFD describes the current implementation and the remaining gaps (pruning, richer stats, caching).

## Motivation

* Minor compaction needs a deterministic way to turn sealed in-memory runs into durable artifacts before version-set logic lands.
* Aligning our dynamic-first implementation with Tonbo’s historical Parquet SST layout keeps future typed modes and the manifest story compatible.
* Documenting the plumbing now avoids signature churn once we wire IO and range scans.

## Non-Goals

* Redesigning manifest/version management (covered in RFC 0007).
* Advanced page-level pruning/caching policies (tracked separately).

## Current State

### Config + Identity

SST configuration captures the Arrow schema, target level, compression choice, storage root, optional key extractor, and a guardrail for merge iteration. Descriptors carry the table id + level, optional stats (rows/bytes/key + commit_ts bounds/tombstones), optional WAL identifiers, and the resolved data/delete paths. Paths are level-scoped (`L{level}/{id}.parquet` plus optional `L{level}/{id}.delete.parquet`).

### Builder Skeleton

Builder is responsible for:
- staging immutables,
- optionally attaching WAL ids gathered from sealed segments,
- aggregating MVCC-aware stats (min/max key, commit_ts range, row/tombstone counts, bytes),
- writing Parquet data with `_commit_ts` appended, plus a key-only delete sidecar when tombstones exist,
- returning a descriptor with stats/paths/WAL ids or failing fast on empty input or MVCC sidecar mismatches.
Future work: page-size tuning, page indexes, and richer stats.

### DB Integration

Flush entry point:
- drains sealed immutables, attaches WAL ids when available, writes Parquet (data + optional delete sidecar), and publishes manifest edits as one atomic step;
- on success, drains the sealed immutables and their WAL ranges; on error, in-memory state remains intact.

### Compaction Helper

```rust
pub struct MinorCompactor { .. }

impl MinorCompactor {
    pub fn new(segment_threshold: usize, target_level: usize, start_id: u64) -> Self;
    pub async fn maybe_compact<M, E>(
        &self,
        db: &mut DB<M, E>,
        cfg: Arc<SsTableConfig>,
    ) -> Result<Option<SsTable<M>>, SsTableError>;
}
```

* Provides a simple segment-count based trigger that generates `SsTableDescriptor`s and invokes `flush_immutables_with_descriptor`.
* Returns `Some(SsTable)` when a flush completes, exposing descriptor stats + WAL IDs; leaves immutables intact if the builder reports an error.
* Intended as a starter orchestrator; smarter policies can swap in later or wrap this helper.

### Read Path Placeholders

Read path:
- streams Parquet data plus optional delete sidecar,
- merges by primary key + commit_ts (latest-wins, delete wins on tie),
- applies projection before materialization; MVCC filtering happens during the merge.
Delete sidecar schema: primary-key columns + `_commit_ts` only; data file carries user columns + `_commit_ts`.
Current gaps: no row-group/page pruning; projection indices are positional (exclude `_commit_ts`).

## Delta vs. Legacy Tonbo (`main`)

| Area | Legacy Tonbo | Current state | Notes |
| --- | --- | --- | --- |
| Writer | Parquet writer with range filters and LRU caching | Async Parquet writer; appends `_commit_ts`, optional delete sidecar; records stats + WAL ids | Page-size tuning/index pruning still TODO |
| Store wrapper | `ParquetStore` newtype | Inlined store config with level-scoped paths | Paths `L{level}/{id}.parquet` and optional `.delete.parquet` |
| Reader | Async `SsTable::scan/get` returning `SsTableScan` stream | Streaming merge of data + delete with MVCC filtering | No row-group/page pruning yet |
| DB flush | Handled by compaction pipeline in legacy code | Flush writes Parquet and publishes manifest edits | Uses WAL refs when available |
| Compaction helper | Policy-driven scheduler | `MinorCompactor` with segment threshold | Major compaction handled separately (RFC 0011) |

## Execution Plan

1. **Pruning and page tuning**
   * Add row-group/page pruning and optional page-size overrides tuned for object storage.
2. **Stats & planning**
   * Persist richer stats (column-level NDV/histograms) and feed them into scan/compaction planners.
3. **Caching/read ergonomics**
   * Add reader-side caching hooks (LRU) and selection-vector based projection to trim IO.
4. **Validation**
   * Maintain round-trip tests for data + delete sidecars; extend integration to cover WAL replay -> flush -> manifest publish -> read.

## Hints

* **Path Layout** – Level directories with `{:020}.parquet` data files plus optional `{:020}.delete.parquet`.
* **MVCC contract** – `_commit_ts` lives in the data file; deletes are key-only sidecars; latest-wins enforced during streaming merge.
* **Stats Scope** – `SsTableStats` already records rows/bytes/key + commit bounds/tombstones; keep it extensible for planner heuristics.
* **Schema Fingerprints** – Leave schema identity tracking to the manifest/catalog layer; SST descriptors reference the Arrow schema carried by `SsTableConfig`.

## References

* Legacy Tonbo `src/ondisk/sstable.rs` for full Parquet implementation.
* `docs/rfcs/0001-hybrid-mutable-and-compaction.md` – outlines mutable sealing and minor compaction triggers.
* This branch’s `src/ondisk/sstable.rs` & `src/compaction.rs` for the live scaffolding and helper implementations.
