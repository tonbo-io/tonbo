# Repository Guidelines

## Project Structure & Module Organization

- `src/`: Core Rust library.
  - `db.rs`: Generic `DB<M: Mode>`; only the dynamic mode is wired up today.
    - `DynMode`: runtime Arrow schema, `ingest(RecordBatch)`.
    - Shared: `approx_mutable_bytes` (mutable key memory metrics). The `Mode`
      trait sticks around so a future typed mode can plug back in.
  - `inmem/`: In‑memory memtables.
    - `mutable/`
      - `memtable.rs`: columnar mutable with last-writer key index (dynamic layout only for now).
      - `metrics.rs`, `key_size.rs`: lightweight metrics and key heap-size estimates.
      - `mod.rs`: module glue and re-exports (`KeyHeapSize`).
    - `immutable/`
      - `memtable.rs`: generic `ImmutableMemTable<K, S>` + `ImmutableScan` iterator.
        - Helpers: runtime-only `segment_from_batch_with_*` builders.
      - `keys.rs`: zero-copy owning key types for string/binary (buffer-backed).
  - `record/`: Runtime batch key extraction.
    - `mod.rs`: re-exports dynamic extractors (typed record trait removed for now).
    - `extract/`: runtime schema helpers
      - `mod.rs`: re-exports public API.
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

- Rust 2024 edition; format with `rustfmt` (via `cargo +nightly fmt`).
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

- Immutable design:
  - `ImmutableMemTable<K, S>` stays generic, but today we instantiate it with
    `RecordBatch` storage via the dynamic helpers. The generic shape is kept so
    typed storage can return later.
  - Builders: `segment_from_batch_with_extractor/with_key_col/with_key_name`.
- `DB<M: Mode>` keeps trait-based dispatch without feature flags:
  - Only `DynMode` is implemented; Mode associated types (`Key`, `ImmStore`,
    `Mutable`) remain for future typed modes.
  - Scanning APIs return values (rows), not keys.
- Dynamic ergonomics:
  - `KeyDyn` supports From conversions (`&str`, `String`, `&[u8]`, `Vec<u8>`, numbers, bool).
  - `DynKeyExtractor` builds keys from `RecordBatch` rows; helpers validate schema/field types.
- Record extraction is crate-local; typed-arrow-unified stays free of DB key concepts.

## Legacy Tonbo Reference

- Full legacy Tonbo (LSM engine, triggers, compaction, WAL, SSTables) repository path:
  - `/Users/gwo/Idea/seren`
- Useful when cross‑checking prior designs such as the memtable freeze trigger trait (`FreezeTrigger`) and flush/compaction flows.

-## Usage Quick Reference

- Dynamic DB:
  - `let mut db: DB<DynMode> = DB::new_dyn_with_key_name(schema, "id")?;`
  - `db.ingest(batch)?;`
  - `let it = db.scan_mutable_rows(&RangeSet::<KeyDyn>::all()); // Iterator<Item=DynRow>`
- Immutable builders:
  - Dynamic: `let imm = inmem::immutable::memtable::segment_from_batch_with_key_name(batch, "id")?;`
  - Typed builders will return once compile-time dispatch is reinstated.

## Roadmap (high‑level)

- Minor flush from columnar mutable to `ImmutableMemTable` (dynamic path today; typed to follow once reinstated).
- K‑way merged scan across mutable + immutables with last‑writer‑wins and optional policies.
- WAL + SST layers; versioning; manifest; compaction strategies.
