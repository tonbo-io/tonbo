# Integration Test Harnesses

## `s3_smoke`

- **What**: Exercises Tonbo's S3 object-store path end-to-end using LocalStack.
- **How to run**:
  1. Ensure Docker is available.
  2. From the repo root, run `./tests/s3_smoke.sh`.
     - The script starts LocalStack, provisions the bucket, seeds the required
       `TONBO_S3_*` environment variables, and executes
       `cargo run --example __s3_smoke_tmp`.
  3. On success the script prints `SMOKE_OK`; on failure it tails the relevant
     LocalStack logs for debugging.
- **Alternative**: If you already have an S3-compatible endpoint up, export the
  `TONBO_S3_*` variables yourself and run
  `cargo test --features s3-smoke --test s3_smoke`.

The `s3_smoke` test is gated behind the `s3-smoke` feature, so the regular test
suite will skip it unless explicitly enabled.
