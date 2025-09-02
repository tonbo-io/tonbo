# Repository Guidelines

## Project Structure & Module Organization

- `src/`: Core Rust library.
  - `db.rs`: Generic `DB<M: Mode>` with typed vs dynamic modes via types.
    - `TypedMode<R: Record>`: compile‑time schema, `ingest(R)`.
    - `DynMode`: runtime Arrow schema, `ingest(RecordBatch)`.
    - Shared: `approx_mutable_bytes` (mutable key memory metrics).
  - `inmem/`: In‑memory memtables.
    - `mutable/`
      - `memtable.rs`: columnar mutable with last‑writer key index (typed and dynamic).
      - `metrics.rs`, `key_size.rs`: lightweight metrics and key heap‑size estimates.
      - `mod.rs`: module glue and re‑exports (`KeyHeapSize`).
    - `immutable/`
      - `memtable.rs`: unified generic `ImmutableMemTable<K, S>` + `ImmutableScan` iterator.
        - Helpers: `segment_from_rows<R, I>(rows)`; `segment_from_batch_with_*` for dynamic.
      - `arrays.rs`: typed‑arrow arrays/builder wrapper for typed immutables.
      - `keys.rs`: zero‑copy owning key types for string/binary (buffer‑backed).
  - `record/`: Records and batch key extraction.
    - `mod.rs`: `Record` trait (typed‑arrow schema + key split/join/key_at).
    - `ext_macros.rs`: derive‑time hooks/macros for ergonomic key definitions.
    - `extract/`: runtime schema helpers
      - `mod.rs`: re‑exports public API.
      - `errors.rs`, `key_dyn.rs`, `traits.rs`, `extractors.rs`.
  - `scan.rs`: key range utilities (`KeyRange`, `RangeSet`).
  - `query/`: resolved key‑only expressions and `to_range_set`.
- Unit tests: colocated under `#[cfg(test)]` in modules. Use `tests/` for integration when needed.

## Design Docs & RFCs

- See `docs/rfcs/` for in-repo design notes and proposals. Start with:
  - `docs/rfcs/0001-hybrid-mutable-and-compaction.md` — hybrid mutable design, dynamic ingest, and deferred ordering/compaction.
  - `docs/rfcs/0002-wal.md` — write-ahead log for durability, with frame format, rotation, sync policy, and recovery plan.

## Build, Test, and Development Commands

- Build: `cargo build` — compile the crate.
- Test: `cargo test` — run unit/integration tests.
- Lint: `cargo clippy -D warnings` — enforce warnings as errors.
- Format: `cargo +nightly fmt --all` — format code.
- Docs: `cargo doc --open` — generate and open rustdoc.

Pre-submit routine (every source adjustment):
- Run `cargo test` and `cargo clippy -D warnings` locally before pushing.
- Fix formatting with `cargo +nightly fmt --all`.

## Coding Style & Naming Conventions

- Rust 2024 edition; format with `rustfmt` (via `cargo fmt`).
- Names: types/traits CamelCase, functions/variables snake_case, modules snake_case.
- Visibility: prefer crate‑private; document public APIs. Crate denies `missing_docs`.
- Keep modules small and cohesive; avoid one‑letter identifiers except indices.
- Do not need any kinds of backward compatibility, neat code first.

## Testing Guidelines

- Tests must be deterministic and offline (no network or timing flakiness).
- Name tests in snake_case; group related assertions for clarity.

## Commit & Pull Request Guidelines

- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`.
- PRs: include a brief rationale, build/test steps, and updated tests/docs when behavior changes.
- Keep PRs small and focused; avoid unrelated refactors or dependency churn.

## Security & Configuration Tips

- Do not commit secrets or machine‑specific absolute paths.
- If path dependencies differ on your machine, adjust locally; do not commit environment‑specific changes.

## Current Architecture Notes

- Unified immutable design:
  - One generic `ImmutableMemTable<K, S>` used for both typed and dynamic modes.
  - Typed storage `S = ImmutableArrays<R>`; dynamic storage `S = RecordBatch`.
  - Builders:
    - `segment_from_rows<R, I>(rows)` and `segment_from_arrays<R>(arrays)` for typed.
    - `segment_from_batch_with_extractor/with_key_col/with_key_name` for dynamic.
- `DB<M: Mode>` unifies typed/dynamic dispatch without feature flags:
  - Mode associated types include `Key`, `ImmStore`, and `Mutable`.
  - Scanning APIs return values (rows), not keys.
- Dynamic ergonomics:
  - `KeyDyn` supports From conversions (`&str`, `String`, `&[u8]`, `Vec<u8>`, numbers, bool).
  - `DynKeyExtractor` builds keys from `RecordBatch` rows; helpers validate schema/field types.
- Record extraction is crate‑local; typed‑arrow‑unified stays free of DB key concepts.

## Legacy Tonbo Reference

- Full legacy Tonbo (LSM engine, triggers, compaction, WAL, SSTables) repository path:
  - `/Users/gwo/Idea/seren`
- Useful when cross‑checking prior designs such as the memtable freeze trigger trait (`FreezeTrigger`) and flush/compaction flows.

## Usage Quick Reference

- Typed DB:
  - `let mut db: DB<TypedMode<MyRow>> = DB::new_typed();`
  - `db.ingest(MyRow { .. })?;`
  - `let it = db.scan_mutable_rows(&RangeSet::all()); // Iterator<Item=&MyRow>`
- Dynamic DB:
  - `let mut db: DB<DynMode> = DB::new_dyn_with_key_name(schema, "id")?;`
  - `db.ingest(batch)?;`
  - `let it = db.scan_mutable_rows(&RangeSet::<KeyDyn>::all()); // Iterator<Item=DynRow>`
- Unified immutable builders:
  - Typed: `let imm = inmem::immutable::memtable::segment_from_rows::<MyRow,_>(rows);`
  - Dynamic: `let imm = inmem::immutable::memtable::segment_from_batch_with_key_name(batch, "id")?;`

## Roadmap (high‑level)

- Minor flush from columnar mutable to `ImmutableMemTable` (typed and dynamic paths).
- K‑way merged scan across mutable + immutables with last‑writer‑wins and optional policies.
- WAL + SST layers; versioning; manifest; compaction strategies.
