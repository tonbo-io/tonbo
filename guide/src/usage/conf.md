# Configuration

<!-- toc -->

Tonbo provides a configuration struct `DbOption` for setting up the database. This section will introduce the configuration options available in Tonbo.

## Path Configuration

Tonbo will use local disk as the default storage option(For local is the tokio file, for wasm is the OPFS). If you want to change the default storage backends  `DbOption::base_path`.

```rust
pub fn base_fs(mut self, base_fs: FsOptions) -> DbOption;
```

`FsOptions` is the configuration options for the file system. Tonbo provides two kinds of file system options: `FsOptions::Local` and `FsOptions::S3`.
- `FsOptions::Local`: This is required the feature `tokio`/`wasm` to be enabled.
- `FsOptions::S3{...}`: This is required the feature `aws` and `tokio-http`/`wasm-http` to be enabled. You can use this `FsOptions` to configure the S3 storage.

```rust
pub enum FsOptions {
    #[cfg(any(feature = "tokio", feature = "wasm"))]
    Local,
    #[cfg(feature = "aws")]
    S3 {
        bucket: String,
        credential: Option<AwsCredential>,
        endpoint: Option<String>,
        region: Option<String>,
        sign_payload: Option<bool>,
        checksum: Option<bool>,
    },
}

#[derive(Debug, Clone)]
pub struct AwsCredential {
    /// AWS_ACCESS_KEY_ID
    pub key_id: String,
    /// AWS_SECRET_ACCESS_KEY
    pub secret_key: String,
    /// AWS_SESSION_TOKEN
    pub token: Option<String>,
}
```
- `bucket`: The S3 bucket
- `credential`: The credential configuration for S3
  - `key_id`: The S3 access key
  - `secret_key`: The S3 secret access key
  - `token`: is the security token for the aws S3
- `endpoint`: The S3 endpoint
- `region`: The S3 region
- `sign_payload`: Whether to sign payload for the aws S3
- `checksum`: Whether to enable checksum for the aws S3


If you want to set specific storage options for SSTables, you can use `DbOption::level_path`. This method allows you to specify the storage options for each level of SSTables. If you don't specify the storage options for a level, Tonbo will use the default storage options(that is base fs).

```rust
pub fn level_path(
    mut self,
    level: usize,
    path: Path,
    fs_options: FsOptions,
) -> Result<DbOption, ExceedsMaxLevel>;
```

## Manifest Configuration

Manifest is used to store the metadata of the database. Whenever the compaction is triggered, the manifest will be updated accordingly. But when time goes by, the manifest file will become large, which will increase the time of recovery. Tonbo will rewrite the manifest file if metadata too much, you can use `DbOption::version_log_snapshot_threshold` to configure

```rust
pub fn version_log_snapshot_threshold(self, version_log_snapshot_threshold: u32) -> DbOption;
```

If you want to persist metadata files to S3, you can configure `DbOption::base_fs` with `FsOptions::S3{...}`. This will enable Tonbo to upload metadata files and WAL files to the specified S3 bucket.

> **Note**: This will not guarantee the latest metadata will be uploaded to S3. If you want to ensure the latest metadata is uploaded, you can use `DB::flush` to trigger upload manually. If you want tonbo to trigger upload more frequently, you can adjust `DbOption::version_log_snapshot_threshold` to a smaller value. The default value is 200.

## WAL Configuration

Tonbo use WAL(Write-ahead log) to ensure data durability and consistency. It is a mechanism that ensures that data is written to the log before being written to the database. This helps to prevent data loss in case of a system failure.

Tonbo also provides a buffer to improve performance. If you want to flush wal buffer, you can call `DbOption::flush_wal`. The default buffer size is 4KB. But If you don't want to use wal buffer, you can set the buffer to 0.

```rust
pub fn wal_buffer_size(self, wal_buffer_size: usize) -> DbOption;
```

If you don't want to use WAL, you can disable it by setting the `DbOption::disable_wal`. But please ensure that losing data is acceptable for you.

```rust
pub fn disable_wal(self) -> DbOption;
```

## Compaction Configuration

When memtable reaches the maximum size, we will turn it into a immutable which is read only memtable. But when the number of immutable table reaches the maximum size, we will compact them to SSTables. You can set the `DbOption::immutable_chunk_num` to control the number of files for compaction.
```rust
/// len threshold of `immutables` when minor compaction is triggered
pub fn immutable_chunk_num(self, immutable_chunk_num: usize) -> DbOption;
```

When the number of files in level L exceeds its limit, we also compact them in a background thread. Tonbo use the `major_threshold_with_sst_size` and `level_sst_magnification` to determine when to trigger major compaction. The calculation is as follows:

\\[ major\\_threshold\\_with\\_sst\\_size * level\\_sst\\_magnification^{level} \\]

`major_threshold_with_sst_size` is default to 4 and `level_sst_magnification` is default to 10, which means that the default trigger threshold for level1 is 40 files and 400 for level2.

You can adjust the `major_threshold_with_sst_size` and `level_sst_magnification` to control the compaction behavior.

```rust
/// threshold for the number of `parquet` when major compaction is triggered
pub fn major_threshold_with_sst_size(self, major_threshold_with_sst_size: usize) -> DbOption

/// magnification that triggers major compaction between different levels
pub fn level_sst_magnification(self, level_sst_magnification: usize) -> DbOption;
```

You can also change the default SSTable size by setting the `DbOption::max_sst_file_size`, but we found that the default size is good enough for most use cases.
```rust
/// Maximum size of each parquet
pub fn max_sst_file_size(self, max_sst_file_size: usize) -> DbOption
```

## SSTable Configuration

Tonbo use [parquet](https://github.com/apache/parquet-rs) to store data which means you can set `WriterProperties` for parquet file. You can use `DbOption::write_parquet_option` to set specific settings for Parquet.

```rust
/// specific settings for Parquet
pub fn write_parquet_option(self, write_parquet_properties: WriterProperties) -> DbOption
```

Here is an example of how to use `DbOption::write_parquet_option`:

```rust
let db_option = DbOption::default().write_parquet_option(
    WriterProperties::builder()
        .set_compression(Compression::LZ4)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_enabled(true)
        .build(),
);
```
