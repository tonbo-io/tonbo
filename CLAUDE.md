# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tonbo is an embedded, persistent key-value database written in Rust that uses Log-Structured Merge Tree (LSM) architecture with Apache Arrow & Parquet for columnar data storage. The project supports multiple language bindings (Python, JavaScript/WASM) and various storage backends.

## Common Development Commands

### Core Rust Development

```bash
# Code quality checks
cargo check                    # Check for compilation errors
cargo clippy --workspace -- -D warnings  # Run linter (fail on warnings)
cargo +nightly fmt             # Format code (must use nightly)
cargo +nightly fmt -- --check  # Check formatting without changing files

# Building
cargo build                    # Standard debug build
cargo build --release          # Release build
cargo build --features bench   # Build with benchmarking features

# Testing
cargo test                     # Run unit tests
cargo test --workspace         # Run all workspace tests including subcrates
cargo test test_name           # Run a specific test
cargo test -- --nocapture      # Show println! output in tests

# Benchmarks
cargo bench --features bench   # Run performance benchmarks

# Documentation
cargo doc --open               # Build and open documentation
```

### Feature-Specific Builds

```bash
# WASM build
cargo build --target wasm32-unknown-unknown --no-default-features --features wasm

# Build without default features
cargo build --no-default-features --features tokio

# DataFusion SQL support
cargo build --features datafusion
```

### Language Bindings

```bash
# Python bindings (requires maturin)
cd bindings/python
maturin develop              # Build and install locally for development
pytest tests/                 # Run Python tests
pytest tests/bench/ --benchmark-only  # Run Python benchmarks

# JavaScript/WASM bindings (requires wasm-pack)
cd bindings/js
wasm-pack build              # Build WASM module
wasm-pack test --chrome --headless  # Run WASM tests in Chrome
```

### Code Coverage

```bash
cargo llvm-cov --workspace --lcov --output-path lcov.info
```

## Architecture Overview

The codebase is organized into several key modules:

- **src/compaction/**: LSM tree compaction strategies (leveled compaction implementation)
- **src/fs/**: File system abstractions supporting multiple backends (local, S3, OPFS)
- **src/inmem/**: In-memory data structures including mutable and immutable memtables
- **src/ondisk/**: On-disk storage using SSTable format with Arrow/Parquet
- **src/record/**: Record types, schemas, and the `#[derive(Record)]` macro system
  - **dynamic/**: Runtime-defined record schemas with Value enum for flexible typing
  - **key/**: Type implementations for primary keys (strings, numbers, timestamps, lists)
- **src/stream/**: Streaming operations for efficient data processing and merging
- **src/version/**: Version management and multi-version concurrency control
- **src/wal/**: Write-ahead logging for durability
- **src/transaction.rs**: Transaction support with optimistic concurrency control
- **src/manifest.rs**: Database manifest management

The project uses procedural macros (in `tonbo_macros/`) to provide a type-safe API where users define their key-value schema using Rust structs.

## Key Development Patterns

1. **Async Runtime**: The project supports both tokio and async-std (tokio is default). Use `#[tokio::test]` for async tests.

2. **Feature Flags**: Important features include:
   - `tokio`: Async filesystem operations (default)
   - `wasm`: WebAssembly support with OPFS backend
   - `datafusion`: SQL query support via Apache DataFusion
   - `aws`: S3 storage backend support
   - `bench`: Enables comparison benchmarks against RocksDB, Sled, and Redb
   - `bytes`: Bytes type support (default)

3. **Type System**: Records must derive from `tonbo::Record` trait. The macro generates necessary serialization and Arrow schema implementations. Dynamic records are also supported for runtime-defined schemas.

4. **Testing**: 
   - Unit tests use `#[tokio::test]` for async code
   - Integration tests in `tests/` directory
   - Macro correctness tests using trybuild in `tests/success/` and `tests/fail/`
   - WASM-specific tests in `tests/wasm.rs`

5. **Error Handling**: Uses custom error types. Always propagate errors appropriately using `?`.

## Working with the Codebase

1. **Adding New Features**: Check feature flags in `Cargo.toml` and ensure proper conditional compilation with `#[cfg(feature = "...")]`.

2. **Modifying Storage Layer**: Changes to on-disk format should maintain backward compatibility or increment version numbers appropriately. The magic number in `src/magic.rs` helps identify file format versions.

3. **Performance**: Run benchmarks before and after changes that might impact performance. Use `cargo bench --features bench` to compare against other embedded databases.

4. **Cross-Platform**: Ensure changes work across all supported platforms (Linux, macOS, Windows) and targets (native, WASM).

## Important Notes

- Rust toolchain version is pinned to 1.85 in `rust-toolchain.toml`
- The project uses fusio crates (v0.4.0) for pluggable I/O and storage backends
- Formatting is strict - always run `cargo +nightly fmt` before committing
- The project follows semantic versioning (currently at 0.3.2)
- Default features include: aws, bytes, tokio, tokio-http, async-trait