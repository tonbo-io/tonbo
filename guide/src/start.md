# Getting started
<!-- toc -->

## Installation

### Prerequisite
To get started using tonbo you should make sure you have [Rust](https://www.rust-lang.org/tools/install) installed on your system. If you haven't alreadly done yet, try following the instructions [here](https://www.rust-lang.org/tools/install).

### Installation

To use local disk as storage backend, you should import [tokio](https://github.com/tokio-rs/tokio) crate and enable "tokio" feature (enabled by default) in the *Cargo.toml* file.

```toml
tokio = { version = "1", features = ["full"] }
tonbo = { git = "https://github.com/tonbo-io/tonbo" }
```

If you want to use tonbo in browser(use OPFS as storage backend), you should disable "*tokio*" feature and enable "*wasm*" feature(As "*tokio*" is enabled by default, you should also disable `default-features`). If you want to use S3 as backend, you also should enable "*wasm-http*" feature.

```toml
tonbo = { git = "https://github.com/tonbo-io/tonbo", default-features = false, features = [
    "wasm",
    "wasm-http",
] }
```
## Using Tonbo

### Defining Schema

Tonbo provides ORM-like macro for ease of use, you can use `Record` macro to define schema of column family. Tonbo will generate all relevant code for you at compile time.

```rust
use tonbo::Record;

#[derive(Record, Debug)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
}
```

- `Record`: Declare this struct as a Tonbo Schema
- `#[record(primary_key)]`: Declare this key as primary key. Compound primary key is not supported now.
- `Option` type represents this field can be null, otherwise it can not be null.

Now, Tonbo support these types:

- Number type: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
- Boolean type: `bool`
- String type: `String`
- Bytes: `bytes::Bytes`

### Creating database

After define you schema, you can create `DB` with a customized `DbOption`

```rust
use std::fs;
use fusio::path::Path;
use tonbo::{executor::tokio::TokioExecutor, DbOption, DB};

#[tokio::main]
async fn main() {
    // make sure the path exists
    fs::create_dir_all("./db_path/users").unwrap();

    let options = DbOption::new(
        Path::from_filesystem_path("./db_path/users").unwrap(),
        &UserSchema,
    );
    let db = DB::<User, TokioExecutor>::new(options, TokioExecutor::current(), UserSchema)
        .await
        .unwrap();
}
```

`UserSchema` is a struct that tonbo generates for you in the compile time, so you do not need to import it.  One thing you need to pay attention to is: you should **make sure the path exists** before creating `DBOption`.

> **Note:** If you use tonbo in WASM, you should use `Path::from_opfs_path` rather than `Path::from_filesystem_path`.
>

### Operations on Database

After creating `DB`, you can execute `insert`, `remove`, `get` and other operations now. But remember that you will get a **`UserRef` instance** that implements `RecordRef` trait rather than the `User`, if you get record from tonbo. This is a struct that tonbo generates for you in the compile time. It may look like:

```rust
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct UserRef<'r> {
    pub name: &'r str,
    pub email: Option<&'r str>,
    pub age: Option<u8>,
}
impl RecordRef for UserRef<'_> {
    // ......
}
```

### Insert

`DB::insert` receives a `Record` instance which is the instance of struct you defined with `#[derive(Record)]`.

```rust
db.insert(User { /* ... */ }).await.unwrap();
```

### Remove
`DB::remove` receives a `Key` which is the type of `#[record(primary_key)]`. This method will remove the record that specified by the given `Key`.

```rust
db.remove("Alice".into()).await.unwrap();
```

### Get
`DB::get` receives a `Key` and process the record with a closure that receives a `TransactionEntry`. You can use `TransactionEntry::get` to get the record which is the type of `RecordRef`.

```rust
let age = db.get(&"Alice".into(),
    |entry| {
        // entry.get() will get a `UserRef`
        let user = entry.get();
        println!("{:#?}", user);
        user.age
    })
    .await
    .unwrap();
```
### Scan
Like `DB::get`, `DB::scan` receives a closure that process `TransactionEntry`. The difference is that `DB::scan` receives a range of `Key`s and process all data that satisfied with the closure.

```rust
let lower = "Alice".into();
let upper = "Bob".into();
let stream = db
    .scan(
        (Bound::Included(&lower), Bound::Excluded(&upper)),
        |entry| {
            let record_ref = entry.get();

            record_ref.age
        },
    )
    .await;
let mut stream = std::pin::pin!(stream);
while let Some(data) = stream.next().await.transpose().unwrap() {
    // ...
}
```

#### Using transaction

Tonbo supports transaction. You can also push down filter, limit and projection operators in query.

```rust
// create transaction
let txn = db.transaction().await;

let name = "Alice".into();

txn.insert(User { /* ... */ });
let user = txn.get(&name, Projection::All).await.unwrap();

let upper = "Blob".into();
// range scan of user
let mut scan = txn
    .scan((Bound::Included(&name), Bound::Excluded(&upper)))
    .take()
    .await
    .unwrap();

while let Some(entry) = scan.next().await.transpose().unwrap() {
    let data = entry.value(); // type of UserRef
    // ......
}
```

### Persistence
As Tonbo uses LSM(Log-Structured-Merge Tree) as the underlying data structure, some data are in the memory(mem). If you want to persist these data, you can use the `flush` method.

If WAL is enabled, the data will be persisted to disk automatically. But as tonbo has buffer for WAL by default, you need to call `flush_wal` method if you want to ensure that all the data will be recovered. If you don not want to use buffer for WAL, you can disable it by setting `wal_buffer_size` to 0.

```rust
let options = DbOption::new(
    Path::from_filesystem_path("./db_path/users").unwrap(),
    &UserSchema,
).wal_buffer_size(0);
```

If you don't want to use WAL, you can disable it by setting the `DbOption::disable_wal`. But please ensure that losing data is acceptable for you.
```rust
let options = DbOption::new(
    Path::from_filesystem_path("./db_path/users").unwrap(),
    &UserSchema,
).disable_wal(true);
```

> **Note**: If you disable WAL, there is nothing to do with `flush_wal`. You need to call `flush` method to persist the memory data.
>
> If you enable WAL and set `wal_buffer_size` to 0, you do not need to call `flush_wal` method, since WAL will be flushed to disk before writing.

### Using in S3

If you want to use Tonbo in S3, you can configure `DbOption` to specify which part of the data to store in S3 and which part to store in local disk. Here is an example:

```rust
let s3_option = FsOptions::S3 {
    bucket: "bucket".to_string(),
    credential: Some(AwsCredential {
        key_id: "key_id".to_string(),
        secret_key: "secret_key".to_string(),
        token: None,
    }),
    endpoint: None,
    sign_payload: None,
    checksum: None,
    region: Some("region".to_string()),
};
let options = DbOption::new(
    Path::from_filesystem_path("./db_path/users").unwrap(),
    &UserSchema,
).level_path(2, "l2", s3_option.clone())
).level_path(3, "l3", s3_option);
```

In this example, the data of level 2 and level 3 will be stored in S3 and the rest of the data will be stored in local disk. If there are data in level 2 and level 3, you can find them in S3 like this:

```bash
s3://bucket/l2/
├── xxx.parquet
├── ......
s3://bucket/l3/
├── xxx.parquet
├── ......
```

For more configuration options, please refer to the [Configuration](./usage/conf.md) section.

## What next?
- To learn more about tonbo in Rust or in WASM, you can refer to [Tonbo API](./usage/tonbo.md)
- To use tonbo in python, you can refer to [Python API](./usage/python.md)
- To learn more about tonbo in brower, you can refer to [WASM API](./usage/wasm.md)
- To learn more configuration about tonbo, you can refer to [Configuration](./usage/conf.md)
- There are some data structures for runtime schema, you can use them to [expole tonbo](./usage/advance.md). You can also refer to our [python](https://github.com/tonbo-io/tonbo/tree/main/bindings/python), [wasm](https://github.com/tonbo-io/tonbo/tree/main/bindings/js) bindings and [Tonbolite(a SQLite extension)](https://github.com/tonbo-io/tonbolite)
- To learn more about tonbo by examples, you can refer to [examples](https://github.com/tonbo-io/tonbo/tree/main/examples)
