<p align="center">
  <a href="https://tonbo.io">
    <picture>
      <img src="https://github.com/user-attachments/assets/f7625788-0e7f-4fb6-80cd-7f0d2306130b" />
    </picture>
  </a>
</p>

# Tonbo

<p align="left">
  <a href="https://github.com/tonbo-io/tonbo" target="_blank">
    <a href="https://github.com/tonbo-io/tonbo/actions/workflows/ci.yml"><img src="https://github.com/tonbo-io/tonbo/actions/workflows/ci.yml/badge.svg" alt="CI"></img></a>
    <a href="https://crates.io/crates/tonbo/"><img src="https://img.shields.io/crates/v/tonbo.svg"></a>
    <a href="https://codecov.io/gh/tonbo-io/tonbo" > <img src="https://codecov.io/gh/tonbo-io/tonbo/graph/badge.svg?token=4AJ8ACDUM3"/></a>
    <a href="https://github.com/tonbo-io/tonbo/blob/main/LICENSE"><img src="https://img.shields.io/crates/l/tonbo"></a>
  </a>
</p>

**[Website](https://tonbo.io/) | [Rust Doc](https://docs.rs/tonbo/latest/tonbo/) | [Blog](https://tonbo.io/blog/introducing-tonbo) | [Community](https://discord.gg/j27XVFVmJM)**


Tonbo is an embedded database for serverless data-intensive applications.
- Arrow-native schemas with rich, typed structures
- Stores data as Parquet directly on object storage
- Fully asynchronous and can run in lots of runtimes: browsers, edge functions, or inside other databases

No server process to manage. Each database is just a manifest on S3, adding more is trivial.

## Quick Start

```rust
#[derive(Record)]
struct User { id: String, name: String, age: i64 }

// Open database on local disk or S3
let db = DbBuilder::from_schema_key_name(User::schema(), "id")?
    .on_disk("/tmp/users")?
    .open().await?;

// Insert
db.ingest(users.into_record_batch()).await?;

// Query
let batches = db.scan()
    .filter(Predicate::gt(col("age"), 18))
    .collect().await?;
```

## Installation

```bash
cargo add tonbo typed-arrow tokio
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

## Documentation

- **User Guide**: [tonbo.io/docs](https://tonbo.io/docs): tutorials and concepts
- **API Reference**: [docs.rs/tonbo](https://docs.rs/tonbo): full Rust API documentation
- **RFCs**: [docs/rfcs/](./docs/rfcs/): design documents for contributors

## Features

**Storage**
- [x] Parquet files on object storage (S3, R2) or local filesystem
- [x] Manifest-driven coordination (CAS commits, no server needed)
- [ ] Remote compaction (offload to serverless functions)
- [ ] Branching (git-like fork and merge for datasets)
- [ ] Time-window compaction strategy

**Schema & Query**
- [x] Arrow-native schemas (`#[derive(Record)]` or dynamic `Schema`)
- [x] Projection pushdown (read only needed columns)
- [x] Zero-copy reads via Arrow RecordBatch
- [ ] Filter pushdown (predicates evaluated at storage layer)
- [ ] More filter / projection / limit / order-by pushdown

**Backends**
- [x] Local filesystem
- [x] S3 / S3-compatible (R2, MinIO)
- [ ] OPFS (browser storage)

**Runtime**
- [x] Async-first (Tokio)
- [x] Edge runtimes (Deno, Cloudflare Workers)
- [x] WebAssembly
- [ ] io_uring
- [ ] Python async bindings
- [ ] JavaScript/TypeScript async bindings

**Integrations**
- [ ] DataFusion TableProvider
- [ ] Postgres Foreign Data Wrapper

## License

Apache License 2.0. See [LICENSE](./LICENSE) for details.
