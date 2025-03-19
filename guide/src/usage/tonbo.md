

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
