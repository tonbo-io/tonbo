# RFC 0008: Zero-Copy Key View

- Status: Accepted
- Authors: Tonbo team
- Created: 2025-11-01
- Area: In-memory, Transactions, WAL

## Summary

Tonbo originally materialised primary keys into bespoke enums (`KeyComponentRaw`, `KeyComponentOwned`) whenever it ingested a `RecordBatch` row or replayed WAL entries. That pipeline duplicated Arrow projection logic, reimplemented comparison semantics, and cloned variable-width data on every hop. This RFC specifies—and the implementation now ships—a typed-arrow-dyn backed key path: `KeyRow` wraps `DynRowRaw` views projected directly from Arrow batches, while `KeyOwned` stores the durable `DynRowOwned` form used by WAL and manifest code. The design keeps comparisons, hashing, and ordering inside the shared typed-arrow helpers, validates eligible key columns, and unifies mutable tables, MVCC indices, and durability layers around the same zero-copy representation.

## Problem Statement

1. **Hot-path cloning** — Prior to this migration the mutable table, transaction staging map, and lock manager operated on bespoke `KeyComponent*` enums, eagerly cloning every key component. On composite or high-cardinality keys this dominated ingest cost and memory churn.
2. **Inconsistent ownership semantics** — Borrowed adapters (`KeyViewRaw`) and owned forms (`KeyComponentOwned`) implemented ordering separately yet still re-materialised data for WAL/manifest codecs, making it unclear which modules could remain zero-copy.
3. **Difficult schema guarantees** — Without validation, users could declare key columns with Arrow logical types we cannot order or hash deterministically, risking inconsistent range scans and MVCC comparisons.
4. **Unsafe duplication** — Multiple modules reimplemented Arrow comparisons manually, increasing the surface for bugs once transactions introduce long-lived references.

## Goals

1. Provide a lightweight key view that borrows directly from Arrow buffers and becomes the default representation inside mutable structures, lock maps, and transactional staging.
2. Preserve deterministic ordering, hashing, and equality for all supported key columns so reads, deduplication, and MVCC checks behave exactly as today.
3. Retain an owned key form only where durability demands it (WAL, manifest, checkpoints), paying the cloning cost at commit/replay boundaries instead of per row.
4. Validate table schemas up front, rejecting key columns whose Arrow logical types cannot support total ordering or stable hashing.

## Non-goals

- Redesigning WAL frame formats beyond substituting the new owned key encoding.
- Solving concurrent mutation conflicts; the RFC only delivers the key representation required by locking and transactional layers.
- Adding new storage backends or changing Parquet/Arrow file formats.

## Background

- The initial `dev` branch design ingested Arrow `RecordBatch` values, stored them in mutable batches, and indexed them via `BTreeMap<KeyDyn, Vec<VersionLoc>>`. The refactor replaces that structure with `BTreeMap<KeyRow, Vec<VersionLoc>>`, where `KeyRow` is a thin wrapper around `typed_arrow_dyn::DynRowRaw`.
- Transactions, staged mutations, and future snapshot readers now share the same key view because every borrowed form ultimately references a `DynRowRaw`.
- We have already moved Arrow projection and ownership logic into the shared `typed-arrow-dyn` crate (`DynProjection`, `DynRowRaw`, `DynRowOwned`), making key materialisation the remaining major copy on the write path.
- Related RFCs (`0002-wal`, `0007-manifest`) assume we can serialize keys deterministically; this RFC ensures we keep that invariant once we switch to the typed-arrow-backed representations.

## Proposed Design

### Type Overview

The refactor unifies key handling around typed-arrow-dyn:

- `KeyRow` — lightweight wrapper around `typed_arrow_dyn::DynRowRaw`. Each `DynRowRaw` owns the schema `Fields` plus a vector of `DynCellRaw` values that borrow Arrow buffers via `NonNull` pointers. `KeyRow` adds Tonbo’s comparison, hashing, and heap-size helpers.
- `KeyOwned` — durable counterpart backed by `typed_arrow_dyn::DynRowOwned`. Only variable-width data is cloned; fixed-width scalars remain inline. Conversions to/from `KeyRow` reuse typed-arrow-dyn’s `from_raw` / `as_raw` helpers.
- `KeyTsViewRaw` / `KeyTsOwned` — composite `(key, commit_ts)` wrappers that pair the above representations with MVCC timestamps without re-implementing ordering.
- `KeyProjection` — dynamic extractor implemented in terms of `typed_arrow_dyn::DynProjection` and `DynRowView`. Projection builds `DynRowRaw` rows and immediately wraps them in `KeyRow`, keeping Tonbo free of Arrow-specific pointer plumbing.

### Schema Validation

During table configuration (e.g., `DynModeConfig::new`), Tonbo validates that every declared key column belongs to the supported Arrow logical set:

- Fixed-width: `Bool`, `Int*`, `UInt*`, `Float*` (with documented NaN ordering), `Decimal*`, `Timestamp`, `Date*`, `Time*`, `Duration`.
- Variable-width: `Utf8` / `LargeUtf8`, `Binary` / `LargeBinary`, `FixedSizeBinary`.
- Dictionary columns whose value type is one of the above.
- Struct / tuple composites composed solely of key-safe fields.

Unsupported types (lists, maps, unions, nested arrays without a defined total order) cause schema creation to fail fast. Validation lives alongside the dynamic mode builder so users receive actionable errors when configuring tables.

### Borrowed Representation and Safety

`KeyRow` holds on to a `DynRowRaw` that references the originating batch buffers. `DynMem` retains every ingested `RecordBatch` (`BatchAttachment`), ensuring those buffers stay alive as long as any `KeyRow` exists in the index. Ordering, equality, and hashing delegate to shared helpers (`dyn_rows_cmp`, `dyn_rows_equal`, `dyn_rows_hash`) built atop typed-arrow-dyn’s `DynCellRaw`; Tonbo no longer reimplements per-type comparison logic.

### Owned Representation & Durability

`KeyOwned` (via `DynRowOwned`) forms the durability boundary. `KeyRow::to_owned()` calls `DynRowOwned::from_raw`, and `KeyOwned::from_key_row()` reverses the conversion. WAL persistence, manifest bounds, range encoding, and serde all build on this shared owned form. Replay reconstructs `KeyRow` from decoded batches using the same projections, so borrowed and owned paths share semantics.

### Integration Touchpoints

- **Mutable table (`DynMem`)** — indexes rows in `BTreeMap<KeyTsViewRaw, BatchRowLoc>`. MVCC operations, range scans, and compaction plumb `KeyRow` through the system.
- **Transactions and lock manager** — staging uses `KeyRow`; commit-time durability calls `KeyRow::to_owned()` exactly once. Lock maps keyed by `KeyRow` avoid redundant cloning.
- **WAL** — append reuses Arrow batches directly; replay rebuilds `KeyRow` via `DynProjection`, and only the durable tail uses `KeyOwned`.
- **Manifest / SSTs** — sealing converts key bounds to `KeyOwned` for persistence while keeping in-memory structures on `KeyRow`.
- **Testing and tooling** — regression coverage exercises scalar/composite keys, dictionary payloads, float NaNs, and serde round-trips to ensure borrowed/owned parity.

## Implementation Plan

1. **typed-arrow-dyn extensions**
   - Add projection helpers (`DynProjection::project_row_view/project_row_raw`), owned row conversions, and serde support so Arrow runtime logic lives in one crate.
2. **Tonbo key wrappers**
   - Introduce `KeyRow`, `KeyOwned`, and `KeyTs*` newtypes backed by typed-arrow dynamic rows; port comparison/hash helpers and heap accounting.
3. **Integration updates**
   - Swap extractors, mutable/immutable memtables, WAL encode/replay, and SST/manifest key handling to the new wrappers while deleting legacy enums.
4. **Docs & tests**
   - Refresh user-facing docs and migration notes, and add regression coverage (KeyRow↔KeyOwned round-trips, WAL composite replay, serde JSON fixtures).

## Validation Strategy

- Unit tests exercising `KeyRow` comparison/hash semantics, serde round-trips, and `KeyRow` ↔ `KeyOwned` conversions across scalar and composite keys.
- WAL replay regression tests covering composite keys to prove the encode/decode pipeline stays consistent.
- Benchmarks comparing ingest throughput and allocation counts before/after the refactor to confirm the expected savings.
- Property tests (QuickCheck-style) for ordering consistency across supported Arrow types, especially floats and dictionary payloads.

## Risks

- **Raw pointer lifetime drift** — `DynRowRaw` stores `NonNull` pointers into Arrow buffers; dropping a batch before its keys would dangle. Mitigation: retain batches alongside the index and add debug assertions in conversion code.
- **Schema incompatibility** — rejecting unsupported key types may break existing schemas. Mitigation: document the requirements and provide migration guidance.
- **typed-arrow-dyn divergence** — upstream changes to dynamic row semantics could break Tonbo. Mitigation: pin versions and keep regression tests that cover ordering/hash equivalence.
- **Float ordering surprises** — documenting NaN ordering and providing tests/examples is essential to avoid user confusion.

## Alternatives Considered

1. **Row-ID indexes** — store row IDs in the mutable index and rely on Arrow array comparisons during lookups. Rejected because it requires scanning Arrow arrays on every lookup, undermining ingest performance for point/range queries.
2. **Reference-counted owned keys** — keep owned buffers in an `Arc<[u8]>` shared across modules. Rejected because it still pays cloning costs per row and complicates WAL encoding logic.
3. **Retain bespoke `KeyComponent*` enums** — continue maintaining Tonbo-specific enums for borrowed/owned keys. Rejected because it duplicates Arrow projection logic, requires manual unsafe code, and diverges from the shared typed-arrow-dyn semantics now used across crates.

## Open Questions

1. Should we forbid floating-point keys entirely instead of defining total ordering semantics?
2. How should dictionary evolution (changing dictionary values across batches) be handled—normalize to the value at view construction or store dictionary IDs?
3. Do we want to eventually support list/map keys? If so, what canonical ordering should they use?
4. What additional typed-arrow-dyn features (e.g., nested struct/list support, stable heap accounting) are required before we can re-enable richer key schemas?

## References

- `docs/overview.md` — Arrow-first architecture and key semantics.
- `docs/rethink-summary.md` — migration plan motivating Arrow-native ingestion.
- `docs/rfcs/0002-wal.md`, `docs/rfcs/0007-manifest.md` — downstream modules relying on deterministic key encoding.
- `docs/transaction-status.md` — ongoing transactional work that depends on the zero-copy key view.
