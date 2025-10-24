# Explore Tonbo

<!-- toc -->

Tonbo provide `DynRecord` to support dynamic schema. We have been using it to build Python and WASM bindings for Tonbo. You can find the source code [here](https://github.com/tonbo-io/tonbo/tree/main/bindings).

Except using it in Python and WASM bindings for Tonbo, we have also used it to build a SQLite extension, [TonboLite](https://github.com/tonbo-io/tonbolite). This means that you can do more interesting things with tonbo such as building a PostgreSQL extension and integrating with datafusio.


## DynRecord

`DynRecord` is just like the schema you defined by `#[derive(Record)]`, but the fields are not known at compile time. Therefore, before using it, you need to pass the schema and value by yourself. Here is the constructor of the `DynSchema`, the schema of `DynRecord`:

```rust
// constructor of DynSchema
pub fn new(schema: Vec<ValueDesc>, primary_index: usize) -> DynSchema;

// constructor of ValueDesc
pub fn new(name: String, datatype: DataType, is_nullable: bool) -> ValueDesc;
```
- `ValueDesc`: represents a field of schema, which contains field name, field type.
  - `name`: represents the name of the field.
  - `datatype`: represents the data type of the field.
  - `is_nullable`: represents whether the field can be nullable.
- `primary_index`: represents the index of the primary key field in the schema.


```rust
pub fn new(values: Vec<Value>, primary_index: usize) -> DynRecord;

pub fn new(
    datatype: DataType,
    name: String,
    value: Arc<dyn Any + Send + Sync>,
    is_nullable: bool,
) -> Value;

```

- `Value`: represents a field of schema and its value, which contains a field description and the value.
  - `datatype`: represents the data type of the field.
  - `name`: represents the name of the field.
  - `is_nullable`: represents whether the field is nullable.
  - `value`: represents the value of the field.
- `primary_index`: represents the index of the primary key field in the schema.

Now, tonbo support these types for dynamic schema:

| Tonbo type | Rust type |
| --- | --- |
| `UInt8`/`UInt16`/`UInt32`/`UInt64` | `u8`/`u16`/`u32`/`u64` |
| `Int8`/`Int16`/`Int32`/`Int64` | `i8`/`i16`/`i32`/`i64` |
| `Boolean` | `bool` |
| `String` | `String` |
| `Bytes` | `Vec<u8>` |


It allows you to define a schema at runtime and use it to create records. This is useful when you need to define a schema dynamically or when you need to define a schema that is not known at compile time.

## Operations
After creating `DynSchema`, you can use tonbo just like before. The only difference is that what you insert and get is the type of `DynRecord` and `DynRecordRef`.

If you compare the usage with compile-time schema version, you will find that the usage is almost the same. The difference can be summarized into the following 5 points.
- Use `DynSchema` to replace `xxxSchema`(e.g. `UserSchema`)
- Use `DynRecord` instance to replace the instance you defined with `#[derive(Record)]`
- All you get from database is `DynRecordRef` rather than `xxxRef`(e.g. `UserRef`)
- Use `Value` as the `Key` of `DynRecord`. For example, you should pass a `Value` instance the `DB::get` method.
- The value of `Value` should be the type of `Arc<Option<T>>` if the column can be nullable.

But if you look at the code, you will find that both `DynSchema` and `xxxSchema` implement the `Schema` trait , both `DynRecord` and `xxxRecord` implement the `Record` trait and both `DynRecordRef` and `xxxRecordRef` implement the `RecordRef` trait. So there is only two difference between them

### Create Database
```rust
#[tokio::main]
async fn main() {
    // make sure the path exists
    fs::create_dir_all("./db_path/users").unwrap();

    // build DynSchema
    let descs = vec![
        ValueDesc::new("name".to_string(), DataType::String, false),
        ValueDesc::new("email".to_string(), DataType::String, false),
        ValueDesc::new("age".to_string(), DataType::Int8, true),
    ];
    let schema = DynSchema::new(descs, 0);

    let options = DbOption::new(
        Path::from_filesystem_path("./db_path/users").unwrap(),
        &schema,
    );

    let db = DB::<DynRecord, TokioExecutor>::new(options, TokioExecutor::default(), DynSchema)
        .await
        .unwrap();
}
```

If you want to learn more about `DbOption`, you can refer to the [Configuration section](conf.md).

> **Note:** You should make sure the path exists before creating `DBOption`.

### Insert

You can use `db.insert(record)` or `db.insert_batch(records)` to insert new records into the database just like before. The difference is that you should build insert a `DynRecord` instance.

Here is an example of how to build a `DynRecord` instance:

```rust
let mut columns = vec![
    Value::new(
        DataType::String,
        "name".to_string(),
        Arc::new("Alice".to_string()),
        false,
    ),
    Value::new(
        DataType::String,
        "email".to_string(),
        Arc::new("abc@tonbo.io".to_string()),
        false,
    ),
    Value::new(
        DataType::Int8,
        "age".to_string(),
        Arc::new(Some(i as i8)),
        true,
    ),
];
let record = DynRecord::new(columns, 0);
```
- `Value::new` will create a new `Value` instance, which represents the value of the column in the schema. This method receives three parameters:
  - datatype: the data type of the field in the schema
  - name: the name of the field in the schema
  - value: the value of the column. This is the type of `Arc<dyn Any>`. But please be careful that **the value should be the type of `Arc<Option<T>>` if the column can be nullable**.
  - nullable: whether the value is nullable

```rust
/// insert a single tonbo record
db.insert(record).await.unwrap();
```

### Remove
You and use `db.remove(key)` to remove a record from the database. This method receives a `Key`, which is the primary key of the record. But all columns in the record is a `Value`, so you can not use it like `db.remove("Alice".into()).await.unwrap();`. Instead, you should pass a `Value` to `db.remove`.

```rust
let key = Value::new(
    DataType::String,
    "name".to_string(),
    Arc::new("Alice".to_string()),
    false,
);

db.remove(key).await.unwrap();
```

### Query

You can use `get` method to get a record by key and you should pass a closure that takes a `TransactionEntry` instance and returns a `Option` type. You can use `TransactionEntry::get` to get a `DynRecordRef` instance.

You can use `scan` method to scan all records that in the specified range. `scan` method will return a `Stream` instance and you can iterate all records by using this stream.

```rust
/// get the record with `key` as the primary key and process it using closure `f`
let age = db.get(key,
    |entry| {
        // entry.get() will get a `DynRecordRef`
        let record_ref = entry.get();
        println!("{:#?}", record_ref);
        record_ref.age
    })
    .await
    .unwrap();

let mut scan = db
    .scan((Bound::Included(&lower_key), Bound::Excluded(&upper_key)))
    .await
    .unwrap();
while let Some(entry) = scan.next().await.transpose().unwrap() {
    let data = entry.value(); // type of DynRecordRef
    // ......
}
```

### Transaction
Tonbo supports transactions when using a `Transaction`. You can use `db.transaction()` to create a transaction, and use `txn.commit()` to commit the transaction.

Note that Tonbo provides optimistic concurrency control to ensure data consistency which means that if a transaction conflicts with another transaction when committing, Tonbo will fail with a `CommitError`.

Here is an example of how to use transactions:
```rust
// create transaction
let txn = db.transaction().await;

let name = Value::new(
    DataType::String,
    "name".to_string(),
    Arc::new("Alice".to_string()),
    false,
);
let upper = Value::new(
    DataType::String,
    "name".to_string(),
    Arc::new("Bob".to_string()),
    false,
);

txn.insert(DynRecord::new(/* */));
let _record_ref = txn.get(&name, Projection::Parts(vec!["email", "bytes"])).await.unwrap();

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
    let data = entry.value(); // type of DynRecordRef
    // ......
}
```

For more detail about transactions, please refer to the [Transactions](../transactions.md) section.

## Using S3 backends

Using S3 as the backend storage is also similar to the usage of [compile-time version](./tonbo.md#using-s3-backends).

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

    let descs = vec![
        ValueDesc::new("name".to_string(), DataType::String, false),
        ValueDesc::new("email".to_string(), DataType::String, false),
        ValueDesc::new("age".to_string(), DataType::Int8, true),
    ];
    let schema = DynSchema::new(descs, 0);
    let options = DbOption::new(Path::from_filesystem_path("s3_path").unwrap(), &schema)
        .level_path(2, "l2", fs_option);


    let db = DB::<DynRecord, TokioExecutor>::new(options, TokioExecutor::default(), schema)
        .await
        .unwrap();
}
```
