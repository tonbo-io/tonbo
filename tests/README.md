# Integration Test Harnesses

## `s3_smoke`

- **What**: Exercises Tonbo's S3 object-store path end-to-end using LocalStack.
- **How to run**:
  1. Ensure Docker is available.
 2. From the repo root, run `./tests/s3_smoke.sh`.
    - The script starts LocalStack, provisions the bucket, seeds the required
      `TONBO_S3_*` environment variables, and runs
      `cargo test --features s3-smoke --test s3_smoke`.

    | Variable                | Description                              | Default when using script |
    |-------------------------|------------------------------------------|---------------------------|
    | `TONBO_S3_ENDPOINT`     | HTTP endpoint for the S3-compatible API  | `http://localhost:4566`   |
    | `TONBO_S3_BUCKET`       | Bucket name used for the smoke run       | `tonbo-smoke`             |
    | `TONBO_S3_REGION`       | Region reported to fusio                 | `us-east-1`               |
    | `TONBO_S3_ACCESS_KEY`   | Access key ID                            | `test`                    |
    | `TONBO_S3_SECRET_KEY`   | Secret access key                        | `test`                    |
    | `TONBO_S3_SESSION_TOKEN`| Optional session token (only if needed)  | unset                     |

    The script derives the defaults from its own `AWS_*` variables. Override any of
    them before running the script to point at an existing deployment.

    The smoke test writes a single batch, so expect a fresh prefix such as
    `smoke-<timestamp>/` containing `wal/wal-000…0.tonwal` and `wal/state.json` in
    your bucket when it finishes.
  3. On success it prints "Smoke test complete." and tails recent LocalStack
     logs; on failure it exits non-zero and shows a longer log tail for
     debugging.
- **Alternative**: If you already have an S3-compatible endpoint up, export the
  `TONBO_S3_*` variables yourself and run
  `cargo test --features s3-smoke --test s3_smoke`.

The `s3_smoke` test is gated behind the `s3-smoke` feature, so the regular test
suite will skip it unless explicitly enabled.
