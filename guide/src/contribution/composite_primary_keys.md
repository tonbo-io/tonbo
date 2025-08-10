# RFC: Composite Primary Keys

This document outlines a practical, incremental plan to add composite (multi-column) primary key support to Tonbo while maintaining backward compatibility. It explains design goals, changes required across the codebase, and a step-by-step implementation and validation plan.

## Goals

- Support multi-column primary keys with lexicographic ordering of PK components.
- Preserve existing single-column PK behavior and public APIs (backward compatible).
- Keep zero-copy reads and projection pushdown guarantees for PK columns.
- Ensure on-disk layout (Parquet) remains sorted by `_ts` then PK(s), with statistics/bloom filters enabled for PK columns.
- Make it easy to use via the `#[derive(Record)]` macro by allowing multiple `#[record(primary_key)]` fields.

## Non-Goals (for this RFC)

- Foreign keys, cascades, or relational constraints.
- Secondary indexes.
- Schema migrations for existing data files.
- Composite keys in dynamic records in the first phase (can be added subsequently).

## High-Level Design

1) Schema trait changes (completed)

- Now: `Schema` exposes `primary_key_indices()` and `primary_key_path()`.
- `primary_key_index()` was removed in favor of the slice-based `primary_key_indices()`.
- Additive helper: `primary_key_paths_and_sorting()` returns all PK column paths plus sorting columns.
- For single-column PKs, implementations return a one-element slice from `primary_key_indices()`.

2) Composite key type(s)

- Introduce a composite key in `src/record/key/composite/` with lexicographic `Ord`:
  - Option A (preferred): The macro generates a record-specific key struct, e.g., `UserKey { k1: u64, k2: String }` and `UserKeyRef<'r> { ... }`.
  - Option B (interim): Provide generic tuple implementations for `(K1, K2)`, `(K1, K2, K3)`, … up to a small N. Each implements `Key` and `KeyRef` with lexicographic `Ord`, plus `Encode`/`Decode`, `Hash`, `Clone`.
- For string/bytes components, `KeyRef` holds borrowed forms, mirroring current single-PK behavior.

3) Macro updates (tonbo_macros)

- Allow multiple `#[record(primary_key)]` fields. Order of appearance in struct determines comparison order (later we can add `order = i` if needed).
- Generate:
  - Record-specific key struct and ref struct (Option A), or map to tuple (Option B).
  - `type Key = <GeneratedKey>` in `Schema` impl.
  - `fn key(&self) -> <GeneratedKeyRef>` in `Record` impl.
  - `fn primary_key_indices(&self) -> Vec<usize>` in `Schema` impl (indices are offset by 2 for `_null`, `_ts`).
- Ensure `RecordRef::from_record_batch` and projection logic always keep all PK columns, even if they are not listed in the projection.
- Keep encoding/arrays builders unchanged in signature; they already append values per-field.

4) Projections and read paths

- Replace single-index assumptions with multi-index collections:
  - Use `[0, 1] ∪ primary_key_indices()` to build fixed projections in `src/lib.rs` and `src/transaction.rs`.
  - In all `RecordRef::projection` usages, ensure all PK columns are always retained (already implied by fixed mask).

5) Parquet writer configuration

- In `DbOption::new`, use `primary_key_paths_and_sorting()` to:
  - Enable stats and bloom filters for each PK column path via `.set_column_statistics_enabled()` and `.set_column_bloom_filter_enabled()` (invoke once per path).
  - Set sorting columns as `[ SortingColumn(_ts, …), SortingColumn(pk1, …), SortingColumn(pk2, …), … ]`.

6) Dynamic records (phase 2)

- Extend `DynSchema` to track `primary_indices: Vec<usize>` in metadata (replacing the single `primary_key_index`).
- Update `DynRecordRef::new` and readers to honor multiple PK indices.
- Define a composite key wrapper for `Value`/`ValueRef` (or generate a per-dyn-schema composite type if feasible). Initially out-of-scope for phase 1.

## Step-by-Step Plan

Phase 1: Core plumbing (single-PK stays working)

1. Extend `Schema` trait
   - Add `primary_key_indices()` and `primary_key_paths_and_sorting()` with default impls wrapping existing methods.
   - Update call sites in `DbOption::new`, `src/lib.rs`, and `src/transaction.rs` to use the plural forms.
   - Acceptance: All tests pass; no behavior change for single-PK users.

2. Fixed projection refactor
   - Replace single `primary_key_index` usage with iteration over `primary_key_indices()` to construct `fixed_projection` = `[0, 1] ∪ PKs`.
   - Acceptance: Existing tests and scan/get projections still behave identically for single-PK.

3. Parquet writer properties
   - Replace single `primary_key_path()` usage with plural variant to configure stats, bloom filters, and sorting columns for `_ts` plus all PK components.
   - Acceptance: Files write successfully; read paths unchanged.

Phase 2: Macro + key types

4. Composite key data structure
   - Implement composite key(s) in `src/record/key/composite/` with `Encode`/`Decode`, `Ord`, `Hash`, `Key`/`KeyRef`.
   - Start with tuples `(K1, K2)`, `(K1, K2, K3)` etc. (Option B) for faster delivery; later switch default macro to per-record key type (Option A).
   - Acceptance: Unit tests confirm lexicographic ordering and encode/decode round-trip for composite keys.

5. Update `#[derive(Record)]`
   - Allow multiple `#[record(primary_key)]` fields and generate:
     - `type Key = (<K1>, <K2>, …)` (Option B) or `<RecordName>Key` (Option A).
     - `fn key(&self) -> (<K1Ref>, <K2Ref>, …)`.
     - `fn primary_key_indices(&self) -> Vec<usize>` with +2 offset.
     - Ensure `from_record_batch` and projection retain all PK columns.
   - Acceptance: trybuild tests covering multi-PK compile and run; single-PK tests unchanged.

6. Integration tests
   - Add end-to-end tests: insert/get/remove, range scans, projection, and ordering on 2+ PK fields (e.g., `tenant_id: u64, name: String`).
   - Acceptance: All new tests pass.

Phase 3: Dynamic records (optional)

7. `DynSchema` multi-PK
   - Store `primary_indices` metadata; update dynamic arrays/refs to keep all PK columns in projections.
   - Provide a composite `ValueRef` key wrapper for in-memory operations.
   - Acceptance: dynamic tests mirroring integration scenarios pass.

## Code Touchpoints

- Traits/APIs: `src/record/mod.rs` (Schema), `src/option.rs` (DbOption::new)
- Read paths: `src/lib.rs` (get/scan/package), `src/transaction.rs` (get/scan)
- Macro codegen: `tonbo_macros/src/record.rs`, `tonbo_macros/src/keys.rs`, `tonbo_macros/src/data_type.rs`
- Key types: `src/record/key/composite/`
- Dynamic (phase 3): `src/record/dynamic/*`

## Testing Strategy

- Unit tests:
  - Composite key `Ord`, `Eq`, `Hash`, `Encode`/`Decode` round-trip.
  - `Schema` default impl compatibility.
- trybuild tests:
  - Multiple `#[record(primary_key)]` in a struct compiles and generates expected APIs.
  - Reject nullable PK components.
- Integration tests:
  - Insert/get/remove by composite key; range scans across composite key ranges; projection keeps PK columns.
  - WAL/compaction unaffected (basic smoke tests).
- (Optional) Property tests: ordering equivalence vs. native tuple lexicographic ordering when Option B is used.

## Backward Compatibility & Migration

- All existing single-PK code continues to work without changes due to default-impl fallbacks.
- Users opting into composite PKs need only annotate multiple fields with `#[record(primary_key)]`.
- No on-disk migration is required for existing tables; new tables with composite PKs will write Parquet sorting columns for all PK components.

## Risks and Mitigations

- API surface increase: keep new APIs additive with conservative defaults.
- Projection bugs: comprehensive tests to ensure PK columns are always included.
- Performance: lexicographic compare is standard; Arrow array lengths are uniform, so no extra bounds checks needed.
- Dynamic records complexity: staged to a later phase to avoid blocking initial delivery.

## Example (target macro UX)

```rust
#[derive(Record, Debug)]
pub struct User {
    #[record(primary_key)]
    pub tenant_id: u64,
    #[record(primary_key)]
    pub name: String,
    pub email: Option<String>,
    pub age: u8,
}

// Generated (conceptually):
// type Key = (u64, String);
// fn key(&self) -> (u64, &str);
// fn primary_key_indices(&self) -> Vec<usize> { vec![2, 3] }
```

## Delivery Checklist

- [ ] Add Schema plural APIs and refactor call sites.
- [ ] Implement composite key types (tuples first).
- [ ] Enable multiple PK fields in macro; generate composite key/ref and PK indices.
- [ ] Update projection logic to retain all PK columns.
- [ ] Configure Parquet sorting/statistics for all PK components.
- [ ] Add unit/trybuild/integration tests.
- [ ] Update user guide (mention composite PK support and examples).
