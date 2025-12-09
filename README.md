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
- Fully asynchronouse and can run in lots of runtimes: browsers, edge functions, or inside other databases

No server process to manage. Each database is just a manifest on S3, adding more is trivial.

## Quick Start

```bash
cargo add tonbo typed-arrow tokio --features tonbo/tokio
```

```rust
use tonbo::{
    db::DbBuilder,
    query::{ColumnRef, Predicate, ScalarValue},
};
use typed_arrow::{
    Record,
    prelude::*,
    schema::{BuildRows, SchemaMeta},
};

// 1. Define your schema as a Rust struct
//    The `Record` derive macro generates Arrow schema automatically
//    Rust types map to Arrow types: String -> Utf8, Option<i64> -> nullable Int64
#[derive(Record)]
struct User {
    id: String,         // primary key
    name: String,
    score: Option<i64>, // nullable field
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Open database with the schema
    //    `from_schema_key_name` specifies which field is the primary key
    //    `.on_disk()` stores data locally; use `.on_s3()` for object storage
    let db = DbBuilder::from_schema_key_name(<User as SchemaMeta>::schema(), "id")?
        .on_disk("/tmp/tonbo_example")?
        .create_dirs(true)
        .open()
        .await?;

    // 3. Insert data within a transaction
    //    Tonbo uses Arrow RecordBatch for efficient columnar storage
    let mut tx = db.begin_transaction().await?;
    let users = vec![
        User { id: "u1".into(), name: "Alice".into(), score: Some(100) },
        User { id: "u2".into(), name: "Bob".into(), score: Some(85) },
    ];
    // Convert Rust structs to Arrow RecordBatch
    let mut builders = User::new_builders(users.len());
    builders.append_rows(users);
    tx.upsert_batch(&builders.finish().into_record_batch())?;
    tx.commit().await?;

    // 4. Query with predicate
    //    Filter data by conditions
    let predicate = Predicate::gt(ColumnRef::new("score"), ScalarValue::from(90_i64));
    let batches = db.scan().filter(predicate).collect().await?;

    // 5. Read results as typed views (zero-copy)
    for batch in &batches {
        for user in batch.iter_views::<User>()?.try_flatten()? {
            println!("{}: {} (score: {:?})", user.id, user.name, user.score);
        }
    }
    Ok(())
}
```

## Features

**Storage**
- [x] Parquet files on object storage (S3, R2) or local filesystem
- [x] LSM-tree with leveled compaction
- [x] Manifest-driven coordination (CAS commits, no server needed)
- [x] Write-ahead log for durability

**Schema & Data**
- [x] Arrow-native schemas (`#[derive(Record)]` or dynamic `Schema`)
- [x] Rich types: strings, integers, floats, timestamps, nested structs
- [x] Composite primary keys
- [x] Nullable fields

**Query**
- [x] Projection pushdown (read only needed columns)
- [x] Two-phase read: plan (prune) → execute (stream)
- [x] Zero-copy reads via Arrow RecordBatch
- [ ] Filter pushdown (predicates evaluated at storage layer)

**Transactions**
- [x] MVCC with snapshot isolation
- [x] Optimistic transactions (begin, upsert, delete, commit)
- [x] Read-your-own-writes within a transaction

**Backends**
- [x] Local filesystem
- [x] S3 / S3-compatible (R2, MinIO)
- [ ] OPFS (browser storage)

**Runtime**
- [x] Async-first (Tokio)
- [x] Edge runtimes (Deno, Cloudflare Workers)
- [ ] WebAssembly
- [ ] Python bindings
- [ ] JavaScript/TypeScript SDK

**Integrations**
- [ ] DataFusion TableProvider
- [ ] Postgres Foreign Data Wrapper

## Documentation

- **User Guide**: [tonbo.io/docs](https://tonbo.io/docs) (coming soon)
- **API Reference**: [docs.rs/tonbo](https://docs.rs/tonbo) (coming soon)
- **Examples**: [examples/](./examples/) — runnable code for common use cases
- **RFCs**: [docs/rfcs/](./docs/rfcs/) — design documents for contributors

## License

Apache License 2.0. See [LICENSE](./LICENSE) for details.
