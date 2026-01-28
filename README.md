<p align="left">
  <a href="https://tonbo.io">
    <picture>
      <img width="680" src="https://github.com/user-attachments/assets/f6949c40-012f-425e-8ad8-6b3fe11ce672" />
    </picture>
  </a>
</p>

# Tonbo

[![crates.io](https://img.shields.io/crates/v/tonbo.svg)](https://crates.io/crates/tonbo/) [![crates.io](https://img.shields.io/crates/l/tonbo)](https://github.com/tonbo-io/tonbo/blob/main/LICENSE) [![docs.rs](https://img.shields.io/docsrs/tonbo)](https://docs.rs/tonbo/0.4.0-a0/tonbo/) [![ci](https://github.com/tonbo-io/tonbo/actions/workflows/rust.yml/badge.svg?branch=dev)](https://github.com/tonbo-io/tonbo/actions/workflows/rust.yml) [![discord](https://img.shields.io/discord/1270294987355197460?logo=discord)](https://discord.gg/j27XVFVmJM)


**[Website](https://tonbo.io/) | [Rust Doc](https://docs.rs/tonbo/0.4.0-a0/tonbo/) | [Blog](https://tonbo.io/blogs) | [Community](https://discord.gg/j27XVFVmJM)**


Tonbo is an embedded database for serverless and edge runtimes. Your data is stored as Parquet on S3, coordination happens through a manifest, and compute stays fully stateless.

## Why Tonbo?

Serverless compute is stateless, but your data isn't. Tonbo bridges this gap:

- **Async-first**: The entire storage and query engine is fully async, built for serverless and edge environments.
- **No server to manage**: Data lives on S3, coordination via manifest, compute is stateless
- **Arrow-native**: Define rich data type, declarative schemas, query with zero-copy `RecordBatch`
- **Runs anywhere**: Tokio, WASM, edge runtimes, or as a storage engine for building your own data infrastructure.
- **Open formats**: Standard Parquet files readable by any tool

### When to use Tonbo?

- Build serverless or edge applications that need a durable state layer without running a database.
- Store append-heavy or event-like data directly in S3 and query it with low overhead.
- Embed a lightweight MVCC + Parquet storage engine inside your own data infrastructure.
- Run workloads in WASM or Cloudflare Workers that require structured persistence.

## Quick Start

```rust
use tonbo::{db::{AwsCreds, ObjectSpec, S3Spec}, prelude::*};

#[derive(Record)]
struct User {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    name: String,
    score: Option<i64>,
}

// Open on S3
let s3 = S3Spec::new("my-bucket", "data/users", AwsCreds::from_env()?);
let db = DbBuilder::from_schema(User::schema())?
    .object_store(ObjectSpec::s3(s3))?.open().await?;

// Insert
let users = vec![User { id: "u1".into(), name: "Alice".into(), score: Some(100) }];
let mut builders = User::new_builders(users.len());
builders.append_rows(users);
db.ingest(builders.finish().into_record_batch()).await?;

// Query
let filter = Expr::gt("score", ScalarValue::from(80_i64));
let results = db.scan().filter(filter).collect().await?;
```

For local development, use `.on_disk("/tmp/users")?` instead. See [`examples/`](./examples/) for more.

## Installation

```bash
cargo add tonbo@0.4.0-a0 tokio
```

Or add to `Cargo.toml`:

```toml
[dependencies]
tonbo = "0.4.0-a0"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Examples

Run with `cargo run --example <name>`:

- `01_basic`: Define schema, insert, and query in 30 lines
- `02_transaction`: MVCC transactions with upsert, delete, and read-your-writes
- `02b_snapshot`: Consistent point-in-time reads while writes continue
- `03_filter`: Predicates: eq, gt, in, is_null, and, or, not
- `04_s3`: Store Parquet files on S3/R2/MinIO with zero server config
- `05_scan_options`: Projection pushdown reads only the columns you need
- `06_composite_key`: Multi-column keys for time-series and partitioned data
- `07_streaming`: Process millions of rows without loading into memory
- `08_nested_types`: Deep struct nesting + Lists stored as Arrow StructArray
- `09_time_travel`: Query historical snapshots via MVCC timestamps

## Architecture

Tonbo implements a merge-tree optimized for object storage: writes go to WAL → MemTable → Parquet SSTables, with MVCC for snapshot isolation and a manifest for coordination via compare-and-swap:

- **Stateless compute**: A worker only needs to read and update the manifest; no long-lived coordinator is required.
- **Object storage CAS**: The manifest is committed using compare-and-swap on S3, so any function can safely participate in commits.
- **Immutable data**: Data files are write-once Parquet SSTables, which matches the strengths of S3 and other object stores.


See [docs/overview.md](./docs/overview.md) for the full design.

## Documentation

- **User Guide**: [tonbo.io/docs](https://tonbo.io/docs): tutorials and concepts
- **API Reference**: [docs.rs/tonbo](https://docs.rs/tonbo): full Rust API documentation
- **RFCs**: [docs/rfcs/](./docs/rfcs/): design documents for contributors

## Development

### Coverage

Install coverage tooling once:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --version 0.6.12 --locked
```

Run coverage locally:

```bash
cargo llvm-cov --workspace --lcov --output-path lcov.info --summary
```

Generate an HTML report:

```bash
cargo llvm-cov --workspace --html
```

### Project status

Tonbo is currently in **alpha**. APIs may change, and we're actively iterating based on feedback.
We recommend starting with development and non-critical workloads before moving to production.

## Features

**Storage**
- [x] Parquet files on object storage (S3, R2) or local filesystem
- [x] Manifest-driven coordination (CAS commits, no server needed)
- [ ] (in-progress) Remote compaction (offload to serverless functions)
- [ ] (in-progress) Branching (git-like fork and merge for datasets)
- [ ] Time-window compaction strategy

**Schema & Query**
- [x] Arrow-native schemas (`#[derive(Record)]` or dynamic `Schema`)
- [x] Projection pushdown (read only needed columns)
- [x] Zero-copy reads via Arrow RecordBatch
- [ ] (in-progress) Filter pushdown (predicates evaluated at storage layer)

**Backends**
- [x] Local filesystem
- [x] S3 / S3-compatible (R2, MinIO)
- [ ] (in-progress) OPFS (browser storage)

**Runtime**
- [x] Async-first (Tokio)
- [x] Edge runtimes (Deno, Cloudflare Workers)
- [x] WebAssembly
- [ ] (in-progress) io_uring
- [ ] (in-progress) Python async bindings
- [ ] JavaScript/TypeScript async bindings

**Integrations**
- [ ] DataFusion TableProvider
- [ ] Postgres Foreign Data Wrapper

## License

Apache License 2.0. See [LICENSE](./LICENSE) for details.
