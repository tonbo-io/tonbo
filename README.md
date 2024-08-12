# Tonbo (WIP)

<p align="left">
  <a href="https://github.com/tonbo-io/tonbo" target="_blank">
    <a href="https://github.com/tonbo-io/tonbo/actions/workflows/ci.yml"><img src="https://github.com/tonbo-io/tonbo/actions/workflows/ci.yml/badge.svg" alt="CI"></img></a>
    <a href="https://crates.io/crates/tonbo/"><img src="https://img.shields.io/crates/v/tonbo.svg"></a>
    <img src="https://img.shields.io/github/stars/tonbo-io/tonbo.svg?style=social" alt="github star"/>
    <img src="https://img.shields.io/github/forks/tonbo-io/tonbo.svg?style=social" alt="github fork"/>
  </a>
</p>

## Introduction

Tonbo is an embedded KV database built on [Apache Arrow & Parquet](https://github.com/apache/arrow-rs), designed to store, filter, and project structured data using LSM Tree.

Our goal is to provide a lean, modern solution for storing data in a tiered storage, which is arranged by RAM, flash, SSD, S3 and any others.

## Features

- [x] Fully asynchronous API.
- [x] Zero-copy rusty API ensuring safety with compile-time type and lifetime checks.
- [x] Vendor-agnostic:
  - [ ] Various usage methods, async runtimes, and file systems:
    - [x] Rust library:
      - [x] [Customizable async runtime and file system](https://github.com/from-the-basement/tonbo/blob/main/src/executor.rs#L5).
      - [x] [Tokio and Tokio fs](https://github.com/tokio-rs/tokio).
      - [ ] [Async-std](https://github.com/async-rs/async-std).
    - [ ] Python library (via [PyO3](https://github.com/PyO3/pyo3) & [pydantic](https://github.com/pydantic/pydantic)):
      - [ ] asyncio (via [pyo3-asyncio](https://github.com/awestlake87/pyo3-asyncio)).
    - [ ] JavaScript library:
      - [ ] WASM and OPFS.
    - [ ] Dynamic library with a C interface.
  - [x] Most lightweight implementation to Arrow / Parquet LSM Trees:
    - [x] Define schema using just Arrow schema and store data in Parquet files.
    - [x] (Optimistic) Transactions.
    - [x] Leveled compaction strategy.
    - [x] Push down filter, limit and projection.
- [ ] Runtime schema definition (*in next release*).
- [ ] SQL (via [Apache DataFusion](https://datafusion.apache.org/)).
- [ ] Fusion storage across RAM, flash, SSD, and remote Object Storage Service (OSS) for each column-family, balancing performance and cost efficiency per data block:
  - [ ] Remote storage (via [Arrow object_store](https://github.com/apache/arrow-rs/tree/master/object_store) or [Apache OpenDAL](https://github.com/apache/opendal)).
  - [ ] Distributed query and compaction.
- [ ] Blob storage (like [BlobDB in RocksDB](https://github.com/facebook/rocksdb/wiki/BlobDB)).

## Example

```rust
use std::ops::Bound;

use futures_util::stream::StreamExt;
use tonbo::{executor::tokio::TokioExecutor, tonbo_record, Projection, DB};

// use macro to define schema of column family just like ORM
// it provides type safety read & write API
#[tonbo_record]
pub struct User {
    #[primary_key]
    name: String,
    email: Option<String>,
    age: u8,
}

#[tokio::main]
async fn main() {
    // pluggable async runtime and I/O
    let db = DB::new("./db_path/users".into(), TokioExecutor::default())
        .await
        .unwrap();

    // insert with owned value
    db.insert(User {
        name: "Alice".into(),
        email: Some("alice@gmail.com".into()),
        age: 22,
    })
    .await
    .unwrap();

    {
        // tonbo supports transaction
        let txn = db.transaction().await;

        // get from primary key
        let name = "Alice".into();

        // get the zero-copy reference of record without any allocations.
        let user = txn
            .get(
                &name,
                // tonbo supports pushing down projection
                Projection::All,
            )
            .await
            .unwrap();
        assert!(user.is_some());
        assert_eq!(user.unwrap().get().age, Some(22));

        {
            let upper = "Blob".into();
            // range scan of
            let mut scan = txn
                .scan((Bound::Included(&name), Bound::Excluded(&upper)))
                .await
                // tonbo supports pushing down projection
                .projection(vec![1])
                .take()
                .await
                .unwrap();
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                assert_eq!(
                    entry.value(),
                    Some(UserRef {
                        name: "Alice",
                        email: Some("alice@gmail.com"),
                        age: Some(22),
                    })
                );
            }
        }

        // commit transaction
        txn.commit().await.unwrap();
    }
}

```

## Contributing to Tonbo
Please feel free to ask any question or contact us on Github Discussions.
