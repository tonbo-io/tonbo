# Repository Guidelines

## Project Structure & Module Organization
- `src/`: Core Tonbo library (LSM tree, records, storage, executors).
- `tests/`: Integration, macro (trybuild), and optional WASM tests.
- `examples/`: Ready-to-run samples (`declare`, `datafusion`, `dynamic`).
- `benches/`: Micro/criterion benchmarks; feature-gated targets.
- Workspace members: `parquet-lru/` and `tonbo_macros/`.
- Language bindings: `bindings/js` (WASM) and `bindings/python` (PyO3).

## Build, Test, and Development Commands
- Build: `cargo build` (add flags via `--features ...`).
  - Examples: `cargo run --example declare --features tokio,bytes`
    | `cargo run --example datafusion --features datafusion`.
- Test: `cargo test` for Rust tests.
  - WASM: `rustup target add wasm32-unknown-unknown` then
    `cargo test --target wasm32-unknown-unknown --features opfs`.
- Benchmarks: `cargo bench --features bench` (general),
  `cargo bench --bench writes --features sled` (criterion).
- Lint/format: `cargo clippy -D warnings` and `cargo +nightly fmt`.
- Toolchain: Rust 1.85 (see `rust-toolchain.toml`), MSRV 1.79.

## Coding Style & Naming Conventions
- Rust 2021; format with repository `rustfmt.toml` (100 col, grouped imports).
- Prefer explicit modules and crate-private visibility where possible.
- Names: types/traits CamelCase, functions/vars snake_case, crates/modules kebab/snake_case.
- Keep public APIs documented; add examples for new features.

## Testing Guidelines
- Add unit tests near code; integration in `tests/` (name `*_test.rs`).
- Macro changes should include trybuild cases under `tests/{success,fail}`.
- When touching async/I/O paths, include Tokio-based tests where feasible.
- Optional storage/features: test with relevant flags (e.g., `datafusion`, `opfs`).

## Commit & Pull Request Guidelines
- Use Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `ci:`, `chore:`.
- PRs should include: clear description, linked issues, tests/benches for behavior or perf,
  docs updates, and instructions to reproduce.
- CI must pass: `cargo fmt`, `clippy`, tests, and benches where applicable.

## Security & Configuration Tips
- Feature flags control backends: `aws`, `opfs`, `tokio`, `datafusion`, `bench`, `sled`, `rocksdb`, `redb`.
- For S3 examples/tests, export `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `BUCKET_NAME`.
- Do not commit secrets; prefer env vars and local config.
