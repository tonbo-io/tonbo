# Repository Guidelines

## Project Structure

- `src/`: Rust library; see module docs for details.
- Unit tests live inline under `#[cfg(test)]`; add integration tests in `tests/` when needed.
- All filesystem, object-store, and network I/O goes through `fusio`; do not reach for ad-hoc std APIs unless surfaced by `fusio`.

## Development Styles
- We are still in the early stages of the first public release, so incompatible updates are acceptable—feel free to make breaking changes whenever they are the better option.
- Default to an adventurous mindset: bias toward bold refactors and experiments when they move the design forward, and document the rationale so everyone can follow along.

## Design Docs & RFCs

### IMPORTANT
**YOU MUST CHECK RFC BEFORE STARTING RELATED COMPONENT DEVELOPMENT**
**YOU MUST CHECK OVERVIEW AND RETHINK-SUMMARY IF YOU DO NOT READ IT BEFORE**

- Start with `docs/overview.md` for architecture and component context.
  - Key sections for quick lookup:
    - [I/O](docs/overview.md#io) - Fusio runtime abstraction and durability contract.
    - [First-class Object Storage](docs/overview.md#first-class-object-storage) - object-store centric design.
    - [Data Model](docs/overview.md#data-model) - memtables, SSTs, WAL, manifest.
    - [Write Path](docs/overview.md#write-path) - ingest pipeline stages.
    - [Compaction Path](docs/overview.md#compaction-path) - minor/major compaction and GC.
    - [Read Path](docs/overview.md#read-path) - plan/prune vs merge/materialize flow.
    - [MVCC](docs/overview.md#mvcc) - timestamp model and visibility.
    - [Checkpoints, Snapshots & Catalog Versioning](docs/overview.md#checkpoints-snapshots--catalog-versioning) - versioning semantics.
    - [Data & File Formats](docs/overview.md#data--file-formats) - Arrow in memory, Parquet on disk.
    - [Runtime](docs/overview.md#runtime) - executor/back-end portability.
    - [Query](docs/overview.md#query) - plan/scan adapter model.
    - [Manifest](docs/overview.md#manifest) - CAS coordination and version edits.
    - [Serverless Execution Topology](docs/overview.md#serverless-execution-topology) - roles and lifecycle.
- RFC quick reference:
  - `docs/rfcs/0001-hybrid-mutable-and-compaction.md` — mutable/immutable design.
  - `docs/rfcs/0002-wal.md` — write-ahead log format and durability.
  - `docs/rfcs/0003-mvcc.md` — timestamping, visibility, and snapshot rules.
  - `docs/rfcs/0004-storage-layout.md` — directory hierarchy and path schema.
  - `docs/rfcs/0005-sstable-sketch.md` — SSTable skeleton for dynamic mode.
  - `docs/rfcs/0006-mvcc-sidecar.md` — MVCC sidecar storage for immutable segments.
  - `docs/rfcs/0007-manifest.md` — manifest module on top of fusio-manifest.
- Migration overview: we are in the middle of the refactor; read `docs/rethink-summary.md` before diving in to stay aligned with the goals, execution plan, and live status table.

## Build, Test, and Development Commands

- Build: `cargo build` - compile the crate.
- Test: `cargo test` - run unit/integration tests.
- Lint: `cargo clippy -D warnings` - enforce warnings as errors.
- Format: `cargo +nightly fmt --all` - format code.
- Docs: `cargo doc --open` - generate and open rustdoc.

Pre-submit routine (every source adjustment):
- Run `cargo test` and `cargo clippy -D warnings` before pushing.
- Fix formatting with `cargo +nightly fmt --all`.

## Coding Style & Naming Conventions

- Rust 2024 edition; format with `rustfmt` via `cargo +nightly fmt`.
- Names: types/traits CamelCase, functions/variables snake_case, modules snake_case.
- Visibility: prefer crate-private; document public APIs. The crate denies `missing_docs`.
- Default to the as most conservative as possible visibility (`pub(super)`, `pub(crate)`, etc.); only use `pub` when cross-crate access is required.
- Keep modules small and cohesive; avoid single-letter identifiers except indices.
- No backward-compatibility constraints; prioritize clean code.
- As a data infrastructure project, exhaust every effort to use compile-time dispatch and aggressively control memory, file, and network access patterns to maximize database compute/storage efficiency; prefer static dispatch, preallocation, and explicit IO planning over ad-hoc dynamic code paths.
- All filesystem and network calls must be async so we can pipeline IO and avoid blocking executors.
- In runtime paths, favor returning structured errors over panicking; reserve panics for truly unrecoverable invariants and keep best-effort operations on the happy path.

## Testing Guidelines

- Tests must be deterministic and offline.
- Name tests in snake_case; group related assertions for clarity.

## Commit & Pull Request Guidelines

- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`.
- PRs should include rationale, build/test steps, and updated tests/docs when behavior changes.
- Keep PRs tight; avoid unrelated refactors or dependency churn.

## Security & Configuration Tips

- Do not commit secrets or machine-specific absolute paths.
- Adjust local path dependencies without committing environment-specific changes.

## Roadmap (high-level)

- Review `docs/overview.md` for top-level architecture and component expectations.
- Close the WAL loop: await `WalTicket::durable`, add metrics, finalize rotation/sync policy handling.
- Persist sealed immutables to Parquet SSTs and introduce the manifest skeleton for versioned visibility.
- Build the k-way MVCC scan integrating mutable, immutable, and future SST layers with manifest-driven pruning.
