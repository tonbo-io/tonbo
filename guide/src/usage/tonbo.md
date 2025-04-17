# Tonbo API

#### DbOption
`DbOption` is a struct that contains configuration options for the database. Here are some configuration options you can set:

```rust
// Creates a new `DbOption` instance with the given path and schema.
// The path is the default path that the database will use.
async fn new(option: DbOption, executor: E, schema: R::Schema) -> Result<Self, DbError<R>>;

// Sets the path of the database.
fn path(self, path: impl Into<Path>) -> Self;

/// disable the write-ahead log. This may risk of data loss during downtime
pub fn disable_wal(self) -> Self;

/// Maximum size of WAL buffer, default value is 4KB
/// If set to 0, the WAL buffer will be disabled.
pub fn wal_buffer_size(self, wal_buffer_size: usize) -> Self;
```

If you want to learn more about `DbOption`, you can refer to the [Configuration section](conf.md).

> **Note:** You should make sure the path exists before creating `DBOption`.


#### Executor

Tonbo provides an `Executor` trait that you can implement to execute asynchronous tasks. Tonbo has implemented `TokioExecutor`(for local disk) and `OpfsExecutor`(for WASM) for users. You can also customize yourself Executor, here is an example implementation of the `Executor` trait:

```rust
pub struct TokioExecutor {
    handle: Handle,
}

impl TokioExecutor {
    pub fn current() -> Self {
        Self {
            handle: Handle::current(),
        }
    }
}

impl Executor for TokioExecutor {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + MaybeSend + 'static,
    {
        self.handle.spawn(future);
    }
}
```

### Query

You can use `get` method to get a record by key and you should pass a closure that takes a `TransactionEntry` instance and returns a `Option` type. You can use `TransactionEntry::get` to get a `UserRef` instance. This `UserRef` instance is a struct that tonbo generates for you. All fields except primary key are `Option` type, because you may not have set them when you create the record.

You can use `scan` method to scan all records that in the specified range. `scan` method will return a `Stream` instance and you can iterate all records by using this stream.

```rust
/// get the record with `key` as the primary key and process it using closure `f`
let age = db.get(&"Alice".into(),
    |entry| {
        // entry.get() will get a `UserRef`
        let user = entry.get();
        println!("{:#?}", user);
        user.age
    })
    .await
    .unwrap();

let mut scan = db
    .scan((Bound::Included(&name), Bound::Excluded(&upper)))
    .await
    .unwrap();
while let Some(entry) = scan.next().await.transpose().unwrap() {
    let data = entry.value(); // type of UserRef
    // ......
}
```
### Insert/Remove

You can use `db.insert(record)` or `db.insert_batch(records)` to insert new records into the database and use `db.remove(key)` to remove a record from the database. Here is an example of updating the state of database:
```rust
let user = User {
    name: "Alice".into(),
    email: Some("alice@gmail.com".into()),
    age: 22,
    bytes: Bytes::from(vec![0, 1, 2]),
};

/// insert a single tonbo record
db.insert(user).await.unwrap();

/// insert a sequence of data as a single batch
db.insert_batch("Alice".into()).await.unwrap();

/// remove the specified record from the database
db.remove("Alice".into()).await.unwrap();
```
### Transaction
Tonbo supports transactions when using a `Transaction`. You can use `db.transaction()` to create a transaction, and use `txn.commit()` to commit the transaction.

Note that Tonbo provides optimistic concurrency control to ensure data consistency which means that if a transaction conflicts with another transaction when committing, Tonbo will fail with a `CommitError`.

Here is an example of how to use transactions:
```rust
// create transaction
let txn = db.transaction().await;

let name = "Alice".into();

txn.insert(User { /* ... */ });
let _user = txn.get(&name, Projection::Parts(vec!["email", "bytes"])).await.unwrap();

let upper = "Blob".into();
// range scan of user
let mut scan = txn
    .scan((Bound::Included(&name), Bound::Excluded(&upper)))
    // tonbo supports pushing down projection
    .projection(&["email", "bytes"])
    // push down limitation
    .limit(1)
    .take()
    .await
    .unwrap();

while let Some(entry) = scan.next().await.transpose().unwrap() {
    let data = entry.value(); // type of UserRef
    // ......
}
```
#### Query
Transactions support easily reading the state of keys that are currently batched in a given transaction but not yet committed.

You can use `get` method to get a record by key, and `get` method will return a `UserRef` instance. This `UserRef` instance is a struct that tonbo generates for you in the compile time. All fields except primary key are `Option` type, because you may not have set them when you create the record. You can also pass a `Projection` to specify which fields you want to get. `Projection::All` will get all fields, `Projection::Parts(Vec<&str>)` will get only primary key, `email` and `bytes` fields(other fields will be `None`).

You can use `scan` method to scan all records that in the specified range. `scan` method will return a `Scan` instance. You can use `take` method to get a `Stream` instance and iterate all records that satisfied. Tonbo also supports pushing down filters and projections. You can use `Scan::projection(vec!["id", "email"])` to specify which fields you want to get and use `Scan::limit(10)` to limit the number of records you want to get.

```rust
let txn = db.transaction().await;

let _user = txn.get(&name, Projection::Parts(vec!["email"])).await.unwrap();

let mut scan_stream = txn
    .scan((Bound::Included(&name), Bound::Excluded(&upper)))
    // tonbo supports pushing down projection
    .projection(&["email", "bytes"])
    // push down limitation
    .limit(10)
    .take()
    .await
    .unwrap();
while let Some(entry) = scan_stream.next().await.transpose().unwrap() {
    let data = entry.value(); // type of UserRef
    // ......
}
```

#### Insert/Remove
You can use `txn.insert(record)` to insert a new record into the database and use `txn.remove(key)` to remove a record from the database. Tonbo will use a B-Tree to store all data that you modified(insert/remove). All your modifications will be committed to the database when only you call `txn.commit()` successfully. If conflict happens, Tonbo will return an error and all your modifications will be rollback.

Here is an example of how to use transaction to update the state of database:

```rust

let mut txn = db.transaction().await;
txn.insert(User {
    id: 10,
    name: "John".to_string(),
    email: Some("john@example.com".to_string()),
});
txn.remove("Alice".into());
txn.commit().await.unwrap();
```



After create `DB`, you can execute `insert`, `remove`, `get` and other operations now. But remember that you will get a **`UserRef` instance** rather than the `User`, if you get record from tonbo. This is a struct that tonbo generates for you in the compile time. It may look like:

## Using S3 backends

Tonbo supports various storage backends, such as OPFS, S3, and maybe more in the future. Tonbo wiil use local storage by default. If you want to use S3 storage for specific level, you can use `DbOption::level_path(FsOptions::S3)` so that all files in that level will be pushed to S3.

```rust
use tonbo::option::{ AwsCredential, FsOptions, Path };
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

If you want to persist metadata files to S3, you can configure `DbOption::base_fs` with `FsOptions::S3{...}`. This will enable Tonbo to upload metadata files and WAL files to the specified S3 bucket.

> **Note**: This will not guarantee the latest metadata will be uploaded to S3. If you want to ensure the latest WAL is uploaded, you can use `DB::flush_wal`. If you want to ensure the latest metadata is uploaded, you can use `DB::flush` to trigger upload manually. If you want tonbo to trigger upload more frequently, you can adjust `DbOption::version_log_snapshot_threshold` to a smaller value. The default value is 200.

See more details in [Configuration](./conf.md#manifest-configuration).

> **Note**: If you want to use S3 in WASM, please configure CORS rules for the bucket before using. Here is an example of CORS configuration:
>```json
> [
>     {
>         "AllowedHeaders": [
>             "*"
>         ],
>         "AllowedMethods": [
>             "GET",
>             "PUT",
>             "DELETE",
>             "HEAD"
>         ],
>         "AllowedOrigins": [
>             "*"
>         ],
>         "ExposeHeaders": []
>     }
> ]
> ```
> For more details, please refer to [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html).
