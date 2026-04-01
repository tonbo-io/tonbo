# EC2 Same-Host Topology Follow-up (2026-03-31)

This note captures the final same-host EC2 comparison for the current branch,
plus the S3 Express investigation outcome.

## Environment

- EC2 instance id: `i-0029361111abba15e`
- instance type: `c7i.large`
- region: `eu-central-1`
- AZ ID: `euc1-az1`
- standard S3 bucket: `tonbo-smoke-euc1-232814779190-1772115011`
- standard S3 bucket region: `eu-central-1`
- network path used for the standard S3 cells: public regional endpoint

## Read Compaction Quiesced: Same-Host EC2 Comparison

Artifacts:

- local `scale=1`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959138050-20408.json`
- local `scale=4`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959154810-20444.json`
- standard S3 `scale=1`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774950219198-9849.json`
- standard S3 `scale=4`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774954499953-19743.json`

### Scale 1

| Cell | Mean | p95 | Rows Processed | Mean Prepare | Mean Consume |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ec2 local, scale=1` | `28.88 ms` | `29.30 ms` | `24,576` | `2.19 ms` | `26.68 ms` |
| `ec2 standard S3, scale=1` | `747.19 ms` | `793.70 ms` | `24,576` | `717.81 ms` | `29.36 ms` |

Observed ratio:

- mean: `25.9x`
- p95: `27.1x`

### Scale 4

| Cell | Mean | p95 | Rows Processed | Mean Prepare | Mean Consume |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ec2 local, scale=4` | `81.18 ms` | `82.58 ms` | `24,576` | `3.61 ms` | `77.55 ms` |
| `ec2 standard S3, scale=4` | `1263.28 ms` | `1284.84 ms` | `24,576` | `1166.56 ms` | `96.68 ms` |

Observed ratio:

- mean: `15.6x`
- p95: `15.6x`

### What Changed To Make `scale=4` Valid

The earlier EC2 `scale=4` standard-S3 artifact was invalid because the
benchmark harness accepted a reopened measurement DB while it still observed
`0` visible rows. The harness now:

- drops the compaction-driving DB handle,
- reopens a fresh measurement DB,
- computes the exact expected visible row count from the benchmark workload,
- polls until the reopened measurement DB reaches that row count,
- fails explicitly if visibility never catches up before timeout.

This proved the problem was benchmark-harness-side acceptance of an invalid
measurement state, not a demonstrated generic reopen/read regression in Tonbo.

## SWMR `1 GB`: Same-Host EC2 Comparison

Artifacts:

- local `1 GB`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959331220-20736.json`
- standard S3 `1 GB`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959375012-20910.json`

These two runs use the same workload shape:

- `TONBO_SWMR_BENCH_LOGICAL_GB=1`
- `TONBO_SWMR_BENCH_ROWS_PER_BATCH=256`
- `TONBO_SWMR_BENCH_PAYLOAD_BYTES=4096`
- `TONBO_SWMR_BENCH_LIGHT_SCAN_LIMIT=256`
- `TONBO_SWMR_BENCH_HEAVY_SCAN_LIMIT=2048`

One measured SWMR step is:

- one writer burst,
- then four readers:
  - `head_light`
  - `head_heavy`
  - `pinned_light`
  - `pinned_heavy`

Per measured step, this run shape processes:

- writer rows: `3328`
- reader rows total: `4608`
- total rows: `7936`

Results:

| Cell | Mean Step (s) | p95 Step (s) | Throughput | Writer Mean (s) | Head Light (s) | Head Heavy (s) | Pinned Light (s) | Pinned Heavy (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ec2 local, ~1 GB logical` | `0.211` | `0.283` | `37.57 Krows/s` | `0.148` | `0.0076` | `0.0356` | `0.0028` | `0.0176` |
| `ec2 standard S3, ~1 GB logical` | `9.642` | `11.950` | `823 rows/s` | `5.840` | `0.887` | `1.617` | `0.275` | `1.022` |

Observed ratio:

- whole mixed step mean: `45.7x`
- writer mean: `39.5x`
- `head_light`: `117.3x`
- `head_heavy`: `45.4x`
- `pinned_light`: `99.6x`
- `pinned_heavy`: `58.1x`

Correctness outcome:

- both same-host `1 GB` artifacts are validity-clean
- all reader classes reported `valid = true`

Interpretation:

- At `~1 GB`, standard S3 is not just slower in setup-heavy read-only cells; it
  remains dramatically slower in the mixed SWMR workload on the same host.
- The main cost wall is still the writer path, but the reader penalties are
  also large enough to matter materially.
- The branch therefore shows deterioration with larger state and mixed
  workload shape, not only with the smaller read-only directional scenario.

## S3 Express Investigation Outcome

What was ruled out:

- AWS account access is not the blocker.
- Directory-bucket creation is not the blocker.
- S3 Express object operations are not generally broken in the account.

What was proven:

- `eu-central-1` was not usable for the intended Express run from the current
  environments because `s3express-control.eu-central-1.amazonaws.com` did not
  resolve.
- A real S3 Express directory bucket was created in `us-east-1`, AZ ID
  `use1-az6`:
  - `tonbo-bench-use1-az6-20260331--use1-az6--x-s3`
- AWS-side S3 Express worked there:
  - `list-directory-buckets`
  - `create-session`
  - `put-object`
  - `head-object`
  - `get-object`

What still failed:

- Tonbo/Fusio could not use the directory bucket for object-store access.
- The failure mode matched the current client design:
  - path-style custom endpoint construction is incompatible with S3 Express
    object I/O,
  - Express also needs `CreateSession` token handling for those object
    operations.

Practical conclusion:

- S3 Express is not blocked on benchmark setup anymore.
- It is blocked on lower-layer client support and should be treated as a
  follow-up implementation story rather than something this branch can enable
  by more benchmark retries.
