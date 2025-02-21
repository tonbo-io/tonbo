## Installation

To get started using tonbo you should make sure you have Rust installed on your system. If you haven't alreadly done yet, try following the instructions [here](https://www.rust-lang.org/tools/install).

## Adding dependencies

```toml
fusio = { git = "https://github.com/tonbo-io/fusio.git", rev = "216eb446fb0a0c6e5e85bfac51a6f6ed8e5ed606", package = "fusio", version = "0.3.3", features = [
  "dyn",
  "fs",
] }
tokio = { version = "1", features = ["full"] }
tonbo = { git = "https://github.com/tonbo-io/tonbo" }
```

## Defining Schema

You can use `Record` macro to define schema of column family just like ORM. Tonbo will generate all relevant files for you at compile time.

```rust
use tonbo::Record;

#[derive(Record, Debug)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
    bytes: Bytes,
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

## Create DB

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

`UserSchema` is a struct that tonbo generates for you in the compile time, so you do not need to import it.

## Read/Write data

After create `DB`, you can execute `insert`, `remove`, `get` now. But remember that you will get a `UserRef` object rather than the `User`, if you get record from tonbo. This is a struct that tonbo generates for you in the compile time.

```rust
db.insert(User {
    name: "Alice".into(),
    email: Some("alice@gmail.com".into()),
    age: 22,
})
.await
.unwrap();

let age = db
    .get(&"Alice".into(), |entry| {
        // entry.get() will get a `UserRef`
        let user = entry.get();
        println!("{:#?}", user);
        user.age
    })
    .await
    .unwrap();
assert!(age.is_some());
assert_eq!(age, Some(22));
```

## Using transaction

Tonbo supports transaction. You can also push down filter, limit and projection operators in query.

```rust
let txn = db.transaction().await;

// get from primary key
let name = "Alice".into();

// get the zero-copy reference of record without any allocations.
let user = txn.get(&name, Projection::All).await.unwrap();

let upper = "Blob".into();
// range scan of user
let mut scan = txn
    .scan((Bound::Included(&name), Bound::Excluded(&upper)))
    // tonbo supports pushing down projection
    .projection(vec![1])
    // push down limitation
    .limit(1)
    .take()
    .await
    .unwrap();

while let Some(entry) = scan.next().await.transpose().unwrap() {
    assert_eq!(
        entry.value(),
        Some(UserRef {
            name: "Alice",
            email: Some("alice@gmail.com"),
            age: None,
        })
    );
}
```

## Using S3 backends

Tonbo supports various storage backends, such as OPFS, S3, and maybe more in the future. You can use `DbOption::level_path` to specify which backend to use.

For local storage, you can use `FsOptions::Local` as the parameter. And you can use `FsOptions::S3` for S3 storage. After create `DB`, you can then operator it like normal.

```rust
use fusio::{path::Path, remotes::aws::AwsCredential};
use fusio_dispatch::FsOptions;
use tonbo::{executor::tokio::TokioExecutor, DbOption, DB};

#[tokio::main]
async fn main() {
    let fs_option = FsOptions::S3 {
        bucket: "wasm-data".to_string(),
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

    let options = DbOption::new(Path::from_filesystem_path("s3_path").unwrap(), &UserSchema)
        .level_path(2, "l2", fs_option);

    let db = DB::<User, TokioExecutor>::new(options, TokioExecutor::current(), UserSchema)
        .await
        .unwrap();
}
```
