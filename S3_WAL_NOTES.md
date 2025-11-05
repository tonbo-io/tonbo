# Tonbo ↔ Fusio S3 WAL Notes

Context for the temporary adjustments in `src/wal/storage.rs` and
`src/wal/writer.rs` while we wait for append + sync support in
`fusio::impls::remotes::aws::fs::AmazonS3`.

- **Truncate-required opens:** Fusio’s S3 adapter rejects `OpenOptions` without
  `truncate(true)`. Tonbo now detects `FileSystemTag::S3` and sets truncate when
  opening WAL segments (`src/wal/storage.rs:48`). This is safe because segments
  are newly created per rotation.
- **No fsync equivalent:** There is no durability sync primitive yet. The WAL
  writer treats S3 the same as the in-memory backend when `WalSyncPolicy` asks
  for a sync (`src/wal/writer.rs:888`). Until fusio exposes fsync semantics,
  durability depends on S3’s PUT guarantees.
- **Size probes return 404:** Calling `size()` on a freshly opened segment may
  surface a 404 (LocalStack + S3 both behave this way). The writer now treats
  those errors as a zero-length file (`src/wal/writer.rs:369`, `src/wal/writer.rs:811`).
- **Smoke coverage:** Run `./tests/s3_smoke.sh` (LocalStack harness) or enable
  the `s3-smoke` feature for `tests/s3_smoke.rs` to exercise this path end-to-end.
- **Action item:** When fusio gains real append + fsync support, roll back these
  guards and re-enable strict sync enforcement before shipping compaction that
  assumes durable WAL persistence.

Addressed upstream fusio commit:
https://github.com/tonbo-io/fusio/blob/8562447100577a996e7e5305f392169a8748693d/fusio/src/impls/remotes/aws/options.rs
