# FAQ

## Failed to run custom build command for `ring` in macOS
Apple Clang is a fork of Clang that is specialized to Apple's wishes. It doesn't support wasm32-unknown-unknown. You need to download and use llvm.org Clang instead. You can refer to this [issue](https://github.com/briansmith/ring/issues/1824) for more information.

```bash
brew install llvm
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
```

## Why my data is not recovered and the size of log file and WAL file is 0?

As Tonbo uses buffer for WAL, so it may not be persisted before exiting. You can use `DB::flush_wal` to ensure WAL is persisted or use `DB::flush` to trigger compaction manually.

If you don't want to use WAL buffer, you can set `DbOption::wal_buffer_size` to 0. See more details in [Configuration](./conf.md#wal-configuration).

## How to persist metadata files to S3? / Why metadata files are not persisted in serverless environment like AWS Lambda

If you want to persist metadata files to S3, you can configure `DbOption::base_fs` with `FsOptions::S3{...}`. This will enable Tonbo to upload metadata files and WAL files to the specified S3 bucket.

> **Note**: This will not guarantee the latest metadata will be uploaded to S3. If you want to ensure the latest WAL is uploaded, you can use `DB::flush_wal`. If you want to ensure the latest metadata is uploaded, you can use `DB::flush` to trigger upload manually. If you want tonbo to trigger upload more frequently, you can adjust `DbOption::version_log_snapshot_threshold` to a smaller value. The default value is 200.

See more details in [Configuration](./conf.md#manifest-configuration).
