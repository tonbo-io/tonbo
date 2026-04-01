# Local vs S3 First Pass (March 30, 2026)

This note captures the first local-versus-standard-S3 comparison run from the
current benchmark branch after adding topology metadata and switching the
object-store benchmark path off the broken probed wrapper.

## Environment

- runner host: `futuristic-mind-laughs`
- runner topology: non-EC2 remote host
- bucket: `tonbo-smoke-euc1-232814779190-1772115011`
- bucket region: `eu-central-1`
- network path: public internet
- S3 Express status on this host:
  - not runnable for the intended comparison
  - this host is not EC2
  - AWS CLI directory-bucket discovery failed to connect to
    `https://s3express-control.eu-central-1.amazonaws.com/`

## Benchmark Shape

- scenario: `read_compaction_quiesced`
- rows per batch: `64`
- ingest batches base: `640`
- artifact iterations: `8`
- criterion sample size: `10`

## What Changed Before These Runs

The original object-store benchmark path used `ProbedFs<AmazonS3>` inside the
DB builder. On March 30, 2026, that path was shown to produce invalid
object-store artifacts:

- `setup.rows_per_scan = 0`
- `summary.rows_processed = 0`
- `summary.io.read_ops = 0`

A focused public-API S3 reopen-with-compaction test passed on the native
object-store path, so the benchmark was switched back to the native
`object_store(...)` builder path for correctness. This restores valid row and
latency results for standard S3, but object-store request counters remain
untrustworthy for now because the broken probe wrapper is no longer in the DB
read path.

## Valid Scale 1 Result

Artifacts:

- local:
  `target/tonbo-bench/compaction_local-1774884026526-2253191.json`
- standard S3:
  `target/tonbo-bench/compaction_local-1774884042845-2253870.json`
- matrix report:
  `target/tonbo-bench/reports/remote-first-pass-s1-native-s3-20260330-152025.md`

### Local (`scale=1`)

- mean latency: `41.19 ms`
- p95 latency: `47.31 ms`
- rows processed: `16,384`

### Standard S3 (`scale=1`)

- mean latency: `1.678 s`
- p95 latency: `1.726 s`
- rows processed: `16,384`

Observed ratio:

- mean: about `40.7x` slower than local
- p95: about `36.5x` slower than local

Read-path split on standard S3:

- mean prepare: about `1.652 s`
- mean consume: about `25.7 ms`
- mean snapshot stage alone: about `537 ms`

Interpretation:

- On this host, the branch is dominated by fixed object-store setup cost for
  this scenario, not by row consumption after the stream is open.
- The standard-S3 `scale=1` result is valid enough to guide the next round of
  topology work.

## Scale 4 Status

Artifacts:

- local:
  `target/tonbo-bench/compaction_local-1774884424641-2262147.json`
- standard S3 rerun with longer compaction wait:
  `target/tonbo-bench/compaction_local-1774884882909-2283436.json`
- matrix report:
  `target/tonbo-bench/reports/remote-first-pass-s4-native-s3-rerun-20260330-153441.md`

### Local (`scale=4`)

- mean latency: `67.98 ms`
- rows processed: `16,384`

### Standard S3 (`scale=4`)

- mean latency: `644.18 ms`
- p95 latency: `699.00 ms`
- but `setup.rows_per_scan = 0`
- and `summary.rows_processed = 0`

Interpretation:

- The `scale=4` standard-S3 cell is not trustworthy yet.
- Extending the compaction wait timeout from `20 s` to `120 s` allowed the run
  to finish, but did not restore visible rows after compaction.
- That means `scale=4` remains blocked on a benchmark-specific visibility issue
  in this environment and should not be used for conclusions.

## Cleanup

All benchmark S3 prefixes used during this session were removed after the runs.

## Practical Handoff For EC2

What is already known:

- standard S3 `scale=1` is valid on a non-EC2 host
- standard S3 `scale=4` is not yet valid on this host
- S3 Express could not be exercised from this host

What to do next on EC2:

1. Run `ec2_s3` first with `scale=1` using the current harness and topology
   metadata fields.
2. If `scale=1` is valid, retry `scale=4` on EC2 before spending time on
   Express.
3. Only then run S3 Express from an instance in the same AZ as the directory
   bucket.
4. Treat object-store `read_ops` / `bytes_read` as unsupported until the probe
   wrapper is fixed; use latency and row visibility as the primary evidence.
