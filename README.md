# MorselDB (WIP)

## Introduction

**MorselDB** is an embedded schema database based on **Arrow** & **Parquet**. Allow structured data to be **Filtered**, **Projected**, and **Stored** in **LSM** Tree.

## Features

- [x] Fully asynchronous API support.
- [x] Zero-copy rusty API ensuring safety with compile-time type and lifetime checks.
- [x] Vendor-agnostic:
  - [ ] Supports various usage methods, async runtimes, and file systems:
    - [x] Rust library:
      - [x] Customizable async runtime and file system support.
      - [x] Tokio and Tokio fs.
      - [ ] Async-std.
    - [ ] Python library (via PyO3):
      - [ ] asyncio (via [pyo3-asyncio](https://github.com/awestlake87/pyo3-asyncio)).
    - [ ] JavaScript library:
      - [ ] WASM and OPFS.
    - [ ] Dynamic library with a C interface.
  - [x] Implements a lightweight and a standard approach to Arrow / Parquet LSM Trees:
    - [x] Define column-family using just Arrow schema and store data in Parquet files.
    - [x] (Optimistic) Transactions.
    - [x] Leveled compaction strategy.
    - [x] Push down filter, limit and projection.
- [ ] Runtime schema definition (*in next release*).
- [ ] Support SQL (via [Apache DataFusion](https://datafusion.apache.org/)).
- [ ] Fusion storage across RAM, flash, SSD, and remote Object Storage Service (OSS) for each column-family, balancing performance and cost efficiency per data block:
  - [ ] Supports remote storage (via [Arrow object_store](https://github.com/apache/arrow-rs/tree/master/object_store) or [Apache OpenDAL](https://github.com/apache/opendal)).
  - [ ] Supports distributed query and compaction.
- [ ] Blob storage (like [BlobDB in RocksDB](https://github.com/facebook/rocksdb/wiki/BlobDB)).

## Example

```rust
use std::ops::Bound;

use futures_util::stream::StreamExt;
use morseldb::{executor::tokio::TokioExecutor, record, Projection, DB};

// use macro to define schema of column family just like ORM
// it provides type safety read & write API
#[record]
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
    });

    {
        // morseldb supports transaction
        let mut txn = db.transaction().await;

        // get from primary key
        let name = "Alice".into();

        // get the zero-copy reference of record without any allocations.
        let user = txn.get(
            &name,
            // morseldb supports pushing down projection
            Projection::All
        ).await.unwrap();
        assert!(user.is_some());
        assert_eq!(user.unwrap().get().age, Some(22));

        {
            let upper = "Blob".into();
            // range scan of
            let mut scan = txn
                .scan((Bound::Included(&name), Bound::Excluded(&upper)))
                .await
                // morseldb supports pushing down projection
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

## Contributing to MorselDB
Please feel free to ask any question or contact us on Github Discussions.
