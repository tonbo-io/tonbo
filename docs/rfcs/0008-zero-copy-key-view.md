# RFC 0008: Zero-Copy Key View

- Status: Accepted
- Authors: Tonbo team
- Created: 2025-11-01
- Area: In-memory, Transactions, WAL

## Summary

Tonbo originally materialised primary keys into an owned `KeyDyn` value whenever it ingested a `RecordBatch` row or replayed WAL entries. That approach kept the implementation simple but forced copies of every variable-width component (strings, binary blobs), inflated write latency, and complicated future transactional and snapshot work. This RFC specifies—and the implementation now ships—a zero-copy key view that is the hot-path representation for mutable indexes while retaining an owned form only where durability requires it. The design introduces borrow-based key projections, schema validation rules for eligible key columns, and integration across the mutable table, WAL, manifest, and lock manager.

## Problem Statement

1. **Hot-path cloning** — Prior to this migration the mutable table, transaction staging map, and lock manager operated on `KeyDyn`, eagerly cloning every key component. On composite or high-cardinality keys this dominated ingest cost and memory churn.
2. **Inconsistent ownership semantics** — Keys are cloned in-memory yet re-owned again when encoded for the WAL or manifest, making it unclear which modules may borrow versus copy.
3. **Difficult schema guarantees** — Without validation, users can declare key columns with Arrow logical types we cannot order or hash deterministically, risking inconsistent range scans and MVCC comparisons.
4. **Unsafe duplication** — Several modules plan to retain both borrowed and owned keys, increasing the surface for bugs once transactions introduce long-lived references.

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

- The initial `dev` branch design ingested Arrow `RecordBatch` values, stored them in mutable batches, and indexed them via `BTreeMap<KeyDyn, Vec<VersionLoc>>`. The adopted implementation replaces that structure with raw key views (`BTreeMap<KeyViewRaw, Vec<VersionLoc>>`) so no hot-path cloning occurs.
- Transactions, staged mutations, and future snapshot readers will share the same key representation; the current owned-only approach blocks that work because it clones on every access.
- We have already removed `DynRow` in favour of Arrow-native ingestion, making key materialisation the remaining major copy on the write path.
- Related RFCs (`0002-wal`, `0007-manifest`) assume we can serialize keys deterministically; this RFC ensures we keep that invariant once we switch to borrowed representations.

## Proposed Design

### Type Overview

We introduce two complementary types:

- `KeyView<'batch>` — safe, borrow-based projection over the key columns for a given `RecordBatch` row. It copies fixed-width scalars and borrows slices for variable-width components. The lifetime `'batch` ties the view to the batch that owns the Arrow buffers.
- `KeyOwned` — owned counterpart used when keys must live beyond the batch lifetime. It stores `KeyComponentOwned` values that mirror the borrowed view, cloning only the variable-width bytes it needs (shared through `Arc<String>` / `Arc<Vec<u8>>`).

Internally, `KeyView<'batch>` delegates to:

- `KeyViewRaw` — lifetime-free storage that keeps raw pointers (`NonNull<u8>` plus length metadata) for variable-width components and plain scalars for fixed-width ones. Construction requires an unsafe block guarded by a pinned owner so the underlying buffers never move.
- `KeyComponentRaw` — enum covering all supported Arrow logical types for key columns (fixed-width scalars, UTF-8/Binary variants, FixedSizeBinary, dictionary values resolved to their payload, and struct/tuple composites).

`KeyViewRaw` is the single source of truth for comparison, ordering, and hashing. The safe `KeyView<'batch>` façade and the owned `KeyOwned` wrapper both delegate to the raw representation so borrowed and durable forms share identical semantics.

### Schema Validation

During table configuration (e.g., `DynModeConfig::new`), Tonbo validates that every declared key column belongs to the supported Arrow logical set:

- Fixed-width: `Bool`, `Int*`, `UInt*`, `Float*` (with documented NaN ordering), `Decimal*`, `Timestamp`, `Date*`, `Time*`, `Duration`.
- Variable-width: `Utf8` / `LargeUtf8`, `Binary` / `LargeBinary`, `FixedSizeBinary`.
- Dictionary columns whose value type is one of the above.
- Struct / tuple composites composed solely of key-safe fields.

Unsupported types (lists, maps, unions, nested arrays without a defined total order) cause schema creation to fail fast. Validation lives alongside the dynamic mode builder and updates `docs/overview.md` accordingly.

### Ordering, Equality, and Hashing

`KeyComponentRaw` implements `Eq`, `Ord`, and `Hash`, recursing through composite components. Strings and binary values compare lexicographically on borrowed slices. Floats adopt a total order that treats all NaNs as greater than non-NaNs and orders NaNs by IEEE bit pattern. Timestamps incorporate timezone data where present. These semantics are documented and tested so range scans, deduplication, and MVCC comparisons remain deterministic.

`KeyView::to_owned()` transforms a view into `KeyOwned` for serialization. WAL writers and manifest snapshots call this at commit time; WAL replay and recovery rebuild views from the owned representation before reinserting rows into the mutable table.

### Lifetime and Unsafe Boundaries

Two strategies were considered:

1. **Pinned owner** — wrap each batch plus its key index in a `Pin<Box<_>>`, allowing the module to promote component borrows to `'static` inside a single unsafe constructor. The public API remains lifetime-free while the batch’s destructor guarantees the index drops before buffers move.
2. **Explicit lifetimes** — thread `'batch` lifetimes through the index and mutable table types so the compiler enforces correct usage at every call site.

We recommend the pinned owner approach for Tonbo’s MVP: it confines `unsafe` to the module that owns batches, avoids propagating lifetimes through the transactional API surface, and simplifies trait bounds for lock managers and range-set helpers. The RFC mandates documenting the invariants inside the module (batch never moves after construction; drop order releases views before buffers).

### Integration Touchpoints

- **Mutable table (`DynMem`)** — replace the `BTreeMap<KeyDyn, Vec<VersionLoc>>` with an index keyed by `KeyView<'static>` created via the pinned batch owner. Batches store both the Arrow `RecordBatch` and the index in a single structure so drops happen together. Range scans, conflict detection, and MVCC pruning adapt to operate on borrowed keys.
- **Transactions and lock manager** — staged mutations store `KeyView` handles while the transaction is active. When committing, they convert to `KeyOwned` once while appending WAL frames. Lock maps keyed by `KeyView` avoid per-lock cloning.
- **WAL** — writer submissions keep borrowing batches + MVCC sidecars; frames stay Arrow-only and recovery rebuilds `KeyViewRaw` from the decoded batch using the new `KeyComponentOwned` helpers before reinserting into the mutable table.
- **Manifest** — when flushing immutables, we continue to write owned key bounds (`min_key`, `max_key`) by converting the view to its owned form at flush time.
- **Testing and tooling** — add equality/ordering regression tests covering composite keys, dictionary columns, timestamps with timezones, and float NaN handling.

## Implementation Plan

1. **Type foundation**
   - Add `KeyComponentRaw`, `KeyViewRaw`, `KeyView<'batch>`, and `KeyOwned` in a new `src/key/` module.
   - Implement constructors from `RecordBatch` plus unit tests for ordering and hashing semantics.
2. **Schema validation**
   - Extend `DynModeConfig` (and any other entry points) with key-type validation and update docs on supported key types.
3. **Mutable table refactor**
   - Introduce a pinned batch owner (`PinnedBatchIndex`) that holds the `RecordBatch` and `BTreeMap<KeyView<'static>, RowId>` (or equivalent). Update range scans, locking, and transactional staging to use the view.
   - Remove eager key cloning from the mutable table and transaction staging.
4. **WAL/manifest adjustments**
   - Convert WAL append paths to call `KeyView::to_owned()` only at commit.
   - Update WAL replay to rebuild `KeyView` before inserting into the mutable table.
   - Ensure manifest flushes continue to record owned key bounds.
5. **Documentation and tests**
   - Document the new key rules in `docs/overview.md` and transaction status docs.
   - Add integration tests covering ingest, conflict detection, WAL replay, and manifest recovery with the new key view.

## Validation Strategy

- Extend unit tests to cover `KeyView` comparison, hashing, owned conversions, and dictionary handling.
- Add WAL replay regression tests that persist `KeyOwned` and rebuild views during recovery.
- Benchmarks comparing ingest throughput before/after the refactor to ensure we capture the expected allocation savings.
- Fuzz tests (or QuickCheck-style) for ordering consistency across supported Arrow types.

## Risks

- **Unsafe lifetime management** — misuse of pinned batches can lead to dangling pointers. Mitigation: isolate `unsafe` constructors, add debug assertions that the batch pointer matches the buffer address, and write drop-order tests.
- **Schema incompatibility** — rejecting unsupported key types may break existing schemas. Mitigation: document the requirements and provide migration guidance.
- **Owned representation drift** — if `KeyComponentOwned` diverges from the borrowed layout, WAL replay could fail. Mitigation: keep round-trip tests ensuring `KeyView::to_owned()` and the reverse `KeyViewRaw::from_owned()` conversions agree.
- **Float ordering surprises** — documenting NaN ordering is essential to avoid user confusion.

## Alternatives Considered

1. **Row-ID indexes** — store row IDs in the mutable index and rely on Arrow array comparisons during lookups. Rejected because it requires scanning Arrow arrays on every lookup, undermining ingest performance for point/range queries.
2. **Reference-counted owned keys** — keep owned buffers in an `Arc<[u8]>` shared across modules. Rejected because it still pays cloning costs per row and complicates WAL encoding logic.
3. **Typed Arrow dyn crates** — depend directly on `typed-arrow-dyn` for key materialisation. Rejected because we need precise control over unsafe lifetimes and ordering semantics tailored to Tonbo’s MVCC and manifest requirements.

## Open Questions

1. Should we forbid floating-point keys entirely instead of defining total ordering semantics?
2. How should dictionary evolution (changing dictionary values across batches) be handled—normalize to the value at view construction or store dictionary IDs?
3. Do we want to eventually support list/map keys? If so, what canonical ordering should they use?
4. Are there compaction or snapshot scenarios where we must pin batches across async tasks, and how do we enforce that with the proposed pinned owner?

## References

- `docs/overview.md` — Arrow-first architecture and key semantics.
- `docs/rethink-summary.md` — migration plan motivating Arrow-native ingestion.
- `docs/rfcs/0002-wal.md`, `docs/rfcs/0007-manifest.md` — downstream modules relying on deterministic key encoding.
- `docs/transaction-status.md` — ongoing transactional work that depends on the zero-copy key view.
