
# Using under Wasm

This is the Wasm example of tonbo showing how to use tonbo under Wasm.

## `Cargo.toml`

Since only limited features of tokio can be used in wasm, we need to disable tokio and use `wasm` feature in tonbo.

```toml
fusio = { git = "https://github.com/tonbo-io/fusio.git", rev = "216eb446fb0a0c6e5e85bfac51a6f6ed8e5ed606", package = "fusio", version = "0.3.3", features = [
  "dyn",
  "fs",
] }
tonbo = { git = "https://github.com/tonbo-io/tonbo", default-features = false, features = ["wasm"] }
```

## Create DB

Tonbo provide [OPFS(origin private file system)](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system) as storage backend, but the path is a little different. You should use `Path::from_opfs_path` or `Path::parse` rather than `Path::from_filesystem_path` and it is not permitted to use paths that temporarily step outside the sandbox with something like `../foo` or `./bar`.

```rust
use fusio::path::Path;
use tonbo::{executor::opfs::OpfsExecutor, DbOption, DB};

async fn main() {

    let options = DbOption::new(
        Path::from_opfs_path("db_path/users").unwrap(),
        &UserSchema,
    );
    let db = DB::<User, OpfsExecutor>::new(options, OpfsExecutor::new(), UserSchema)
        .await
        .unwrap();
}
```

## Operations on DB

After create `DB` instance, you can operate it as usual

```rust
let txn = db.transaction().await;

// get from primary key
let name = "Alice".into();

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
