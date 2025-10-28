# RFD 0005: SSTable Skeleton (Dynamic Mode)

- Status: Draft
- Authors: Tonbo team
- Created: 2025-10-24
- Area: SsTable, Compaction

## Summary

Tonbo’s dynamic mode now owns an on-disk module (`ondisk::sstable`) that sketches out the write/read surfaces for Parquet-backed sorted string tables. The goal of this RFD is to capture the current scaffolding, align it with the legacy Tonbo design on `main`, and enumerate the remaining pieces required to turn sealed immutables into durable SSTables. This draft also calls out the forthcoming MVCC sidecar plan from RFC 0006 so the writer/manifest work tracks that direction.

## Motivation

* Minor compaction needs a deterministic way to turn sealed in-memory runs into durable artifacts before version-set logic lands.
* Aligning our dynamic-first implementation with Tonbo’s historical Parquet SST layout keeps future typed modes and the manifest story compatible.
* Documenting the plumbing now avoids signature churn once we wire IO and range scans.

## Non-Goals

* Implementing the actual Parquet writer/reader (streaming + caching) – this RFD tracks API scaffolding only.
* Designing the version set or manifest reconciliation – a follow-up RFD will cover level management once SST files exist.

## Current State

### Config + Identity

```rust
pub struct SsTableConfig {
    schema: SchemaRef,
    target_level: usize,
    compression: SsTableCompression,
    fs: Arc<dyn DynFs>,
    root: Path,
}

pub struct SsTableDescriptor {
    id: SsTableId,
    level: usize,
    approximate_stats: Option<SsTableStats>,
}
```

* Mirrors `main/src/ondisk/sstable.rs`, but parameterised for dynamic mode.
* `SsTableCompression` currently exposes `None | Zstd`; default matches main.
* `fs` + `root` inline the former `ParquetStore` wrapper.
* `SsTableDescriptor` now captures optional WAL IDs alongside enriched stats for manifest consumers.

### Builder Skeleton

```rust
pub struct SsTableBuilder<M: Mode> { .. }

impl<M: Mode> SsTableBuilder<M> {
    pub(crate) fn add_immutable(&mut self, seg: &Immutable<M>) -> Result<(), SsTableError>;
    pub async fn finish(self) -> Result<SsTable<M>, SsTableError>;
}
```

* Tracks target descriptor/config and aggregates MVCC-aware stats through `StagedTableStats` (min/max key, commit horizon, tombstone count).
* `finish` streams the user batch through `AsyncArrowWriter`, records byte size, and returns an `SsTable` handle populated with the enriched `SsTableStats`. A follow-up will add the MVCC sidecar writer described in RFC 0006.
* `SsTableError::NoImmutableSegments` still guards empty flush attempts.
* Future: extend writer with page indexes/compression tuning.

### DB Integration

```rust
impl<M: Mode, E: Executor + Timer> DB<M, E> {
    pub async fn flush_immutables_with_descriptor(
        &self,
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> Result<SsTable<M>, SsTableError>;
}
```

* Minor compaction entry point – owner of the sealed `immutables` deque.
* Drains the in-memory runs into the builder, attaches collected WAL IDs, and clears them once the staged flush succeeds; immutables stay untouched on error.

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

* `SsTableReader`, `SsTableScanPlan`, and `SsTableStream` exist but always return `SsTableError::Unimplemented`.
* API surface aligns with main: open via config + descriptor, plan scans with ranges, MVCC timestamp, and optional `Predicate<M::Key>`.

## Delta vs. Legacy Tonbo (`main`)

| Area | Legacy Tonbo | Current Scaffold | Notes |
| --- | --- | --- | --- |
| Writer | Real Parquet writer with range filters and LRU caching | `SsTableBuilder::add_immutable` collects stats; `finish` returns stub `SsTable` | Need to port `AsyncWriter`, compute byte sizes, and persist MVCC metadata via the forthcoming sidecar. |
| Store wrapper | `ParquetStore` newtype | Inlined `fs` + `root` on `SsTableConfig` | Simpler config; future manifest can still derive full paths. |
| Reader | Async `SsTable::scan/get` returning `SsTableScan` stream | Placeholder `SsTableReader`/`SsTableStream` | Requires row filter + ordering support once IO lands. |
| DB flush | Handled by compaction pipeline in `main` | `DB::flush_immutables_with_descriptor` drains immutables and returns staged descriptor | Real writer + WAL/plumbing will extend this; manifest work can observe descriptors today. |

| Compaction helper | Policy-driven scheduler in legacy code | `MinorCompactor` with segment threshold | Acts as a placeholder orchestrator until version-set logic lands. |

## Execution Plan

1. **Implement Writer IO**
   * `ParquetTableWriter` performs real Parquet writes (via `AsyncArrowWriter`) and records enriched stats today; extend it to emit the aligned MVCC sidecar per RFC 0006, then iterate on page indexes & compression tuning.
2. **Hook Minor Compaction**
   * `DB::flush_immutables_with_descriptor` drains immutables on success and propagates WAL IDs into the descriptor.
   * `MinorCompactor` provides a baseline orchestrator; richer policies can replace it.
3. **Reader + Row Filter**
   * Port `get_range_filter` logic from main to dynamic `KeyDyn`.
   * Implement `SsTableReader::open` using fusio LRU cache.
   * Materialize `SsTableScanPlan::execute` -> Parquet stream + selection vector.
4. **Testing & Validation**
   * Unit tests cover immutable MVCC ranges, builder stats, DB flush guards, and the compactor path.
   * Still pending: Parquet round-trip tests once IO lands; integration covering WAL replay + flush + manifest hand-off.
5. **Docs/Manifest Follow-up**
   * Update `0004-storage-layout` once SST files land on disk.
   * Draft RFD for version-set/manifest interactions (leveling, metadata).

## Hints

* **Path Layout** – Match Tonbo main: write SSTables under `<level>/<sstable_id>.parquet` so the directory structure lines up with the existing compaction tooling.
* **MVCC Sidecar** – For each data file, produce `<level>/<sstable_id>.mvcc.parquet` carrying `_commit_ts`/`_tombstone`; manifest descriptors reference both URIs atomically.
* **Stats Scope** – Record byte size first to support leveled compaction heuristics, but keep `SsTableStats` open for additional metrics (row counts, bloom filters, custom policy hooks) so bespoke compactors can plug in later.
* **Schema Fingerprints** – Leave schema identity tracking to the manifest/catalog layer; SST descriptors will reference entries maintained by the future manifest component.

## References

* Legacy Tonbo `src/ondisk/sstable.rs` for full Parquet implementation.
* `docs/rfcs/0001-hybrid-mutable-and-compaction.md` – outlines mutable sealing and minor compaction triggers.
* This branch’s `src/ondisk/sstable.rs` & `src/compaction.rs` for the live scaffolding and helper implementations.
