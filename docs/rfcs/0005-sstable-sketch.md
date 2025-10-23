# RFD 0005: SSTable Skeleton (Dynamic Mode)

- Status: Draft
- Authors: Tonbo team
- Created: 2025-10-24
- Area: SsTable, Compaction

## Summary

Tonbo’s dynamic mode now owns an on-disk module (`ondisk::sstable`) that sketches out the write/read surfaces for Parquet-backed sorted string tables. The goal of this RFD is to capture the current scaffolding, align it with the legacy Tonbo design on `main`, and enumerate the remaining pieces required to turn sealed immutables into durable SSTables.

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

### Builder Skeleton

```rust
pub struct SsTableBuilder<M: Mode> { .. }

impl<M: Mode> SsTableBuilder<M> {
    pub(crate) fn add_immutable(&mut self, seg: &Immutable<M>) -> Result<(), SsTableError>;
    pub async fn finish(self) -> Result<SsTable<M>, SsTableError>;
}
```

* Tracks target descriptor/config; returns `SsTableError::Unimplemented` today.
* `SsTableError::NoImmutableSegments` guards empty flush attempts.
* Future: stream immutable rows into a Parquet writer (`AsyncArrowWriter` as on main) using the config’s schema and compression.

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
* For now iterates all immutables into the builder and returns the `Unimplemented` error; next step replaces that with real IO.

### Read Path Placeholders

* `SsTableReader`, `SsTableScanPlan`, and `SsTableStream` exist but always return `SsTableError::Unimplemented`.
* API surface aligns with main: open via config + descriptor, plan scans with ranges, MVCC timestamp, and optional `Predicate<M::Key>`.

## Delta vs. Legacy Tonbo (`main`)

| Area | Legacy Tonbo | Current Scaffold | Notes |
| --- | --- | --- | --- |
| Writer | Real Parquet writer with range filters and LRU caching | `SsTableBuilder::add_immutable` + `finish` stubs | Need to port `AsyncWriter`, page index filters, and composite key handling. |
| Store wrapper | `ParquetStore` newtype | Inlined `fs` + `root` on `SsTableConfig` | Simpler config; future manifest can still derive full paths. |
| Reader | Async `SsTable::scan/get` returning `SsTableScan` stream | Placeholder `SsTableReader`/`SsTableStream` | Requires row filter + ordering support once IO lands. |
| DB flush | Handled by compaction pipeline in `main` | `DB::flush_immutables_with_descriptor` entry point | Minor compaction will call this once writer is real. |

## Execution Plan

1. **Implement Writer IO**
   * Introduce `ParquetTableWriter` inside `ondisk::sstable`.
   * Serialize MVCC columns and index metadata; collect `SsTableStats`.
   * Return a populated `SsTable<M>` (descriptor + stats) from `finish`.
2. **Hook Minor Compaction**
   * Extend `DB::flush_immutables_with_descriptor` to drain immutables once the SST file is written.
   * Surface resulting descriptor to compaction coordinator (future version set).
3. **Reader + Row Filter**
   * Port `get_range_filter` logic from main to dynamic `KeyDyn`.
   * Implement `SsTableReader::open` using fusio LRU cache.
   * Materialize `SsTableScanPlan::execute` -> Parquet stream + selection vector.
4. **Testing & Validation**
   * Parquet round-trip unit tests (write -> scan -> collect).
   * Integration test proving WAL replay + flush produces on-disk SST.
5. **Docs/Manifest Follow-up**
   * Update `0004-storage-layout` once SST files land on disk.
   * Draft RFD for version-set/manifest interactions (leveling, metadata).

## Hints

* **Path Layout** – Match Tonbo main: write SSTables under `<level>/<sstable_id>.parquet` so the directory structure lines up with the existing compaction tooling.
* **Stats Scope** – Record byte size first to support leveled compaction heuristics, but keep `SsTableStats` open for additional metrics (row counts, bloom filters, custom policy hooks) so bespoke compactors can plug in later.
* **Schema Fingerprints** – Leave schema identity tracking to the manifest/catalog layer; SST descriptors will reference entries maintained by the future manifest component.

## References

* Legacy Tonbo `src/ondisk/sstable.rs` for full Parquet implementation.
* `docs/rfcs/0001-hybrid-mutable-and-compaction.md` – outlines mutable sealing and minor compaction triggers.
* This branch’s `src/ondisk/sstable.rs` for the live scaffold described above.
