# EC2 S3 Express Cross-Region Enablement Results (2026-04-01)

This note captures the first successful Tonbo benchmark runs against a real S3
Express One Zone directory bucket.

It is primarily an enablement and implication report, not a clean public
performance claim for S3 Express itself, because the only working topology in
this environment was cross-region and cross-AZ.

## Environment

- EC2 instance id: `i-0029361111abba15e`
- instance type: `c7i.large`
- runner region: `eu-central-1`
- runner AZ ID: `euc1-az1`
- directory bucket:
  `tonbo-bench-use1-az6-20260331--use1-az6--x-s3`
- bucket region: `us-east-1`
- bucket AZ ID: `use1-az6`
- object endpoint kind: zonal virtual-hosted
- network path: public internet, cross-region

## What Changed To Make Express Work

The branch needed both lower-layer Fusio work and one benchmark-harness fix.

Fusio side:

- explicit `s3_express` mode
- zonal virtual-hosted endpoint handling
- `CreateSession` acquisition and refresh for object I/O
- Express signing/session-token flow

Tonbo side:

- `S3Spec.s3_express`
- benchmark/test env plumbing for Express mode
- local-path dependency to the working local Fusio checkout during validation

Benchmark harness fix:

- the object-store FS rebuilt for snapshot/cleanup metadata walking now carries
  `spec.s3_express`
- before that fix, the benchmark prep path talked to the Express endpoint with
  the non-Express signing path and failed with `Invalid session token in
  request`

## Artifacts

- Express `read_compaction_quiesced`, `scale=1`:
  `/home/ubuntu/tonbo/target/tonbo-bench/compaction_local-1774974760820-43953.json`
- Express `read_compaction_quiesced`, `scale=4`:
  `/home/ubuntu/tonbo/target/tonbo-bench/compaction_local-1774990219851-46818.json`
- Express `swmr_gb_scale_mixed`, `1 GB logical`:
  `/home/ubuntu/tonbo/target/tonbo-bench/compaction_local-1774991631101-47482.json`

Reference same-host comparison artifacts from March 31:

- local `scale=1`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959138050-20408.json`
- standard S3 `scale=1`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774950219198-9849.json`
- local `scale=4`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959154810-20444.json`
- standard S3 `scale=4`:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774954499953-19743.json`
- local `1 GB` SWMR:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959331220-20736.json`
- standard S3 `1 GB` SWMR:
  `target/tonbo-bench/ec2-euc1/compaction_local-1774959375012-20910.json`

## Read Compaction Quiesced

### Scale 1

| Cell | Mean | p95 | Rows Processed | Mean Prepare | Mean Consume |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ec2 local, scale=1` | `28.88 ms` | `29.30 ms` | `24,576` | `2.19 ms` | `26.68 ms` |
| `ec2 standard S3, scale=1` | `747.19 ms` | `793.70 ms` | `24,576` | `717.81 ms` | `29.36 ms` |
| `ec2 S3 Express, scale=1` | `18.188 s` | `18.890 s` | `24,576` | `18.098 s` | `0.091 s` |

Observed ratio:

- Express vs standard S3 mean: `24.3x` slower
- Express vs standard S3 p95: `23.8x` slower
- Express vs local mean: `629.8x` slower

### Scale 4

| Cell | Mean | p95 | Rows Processed | Mean Prepare | Mean Consume |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ec2 local, scale=4` | `81.18 ms` | `82.58 ms` | `24,576` | `3.61 ms` | `77.55 ms` |
| `ec2 standard S3, scale=4` | `1263.28 ms` | `1284.84 ms` | `24,576` | `1166.56 ms` | `96.68 ms` |
| `ec2 S3 Express, scale=4` | `9.356 s` | `9.651 s` | `24,576` | `9.243 s` | `0.113 s` |

Observed ratio:

- Express vs standard S3 mean: `7.4x` slower
- Express vs standard S3 p95: `7.5x` slower
- Express vs local mean: `115.3x` slower

Interpretation:

- These Express directional cells are overwhelmingly prepare-dominated:
  - `scale=1`: `99.5%` prepare
  - `scale=4`: `98.8%` prepare
- The fact that `scale=4` is materially faster than `scale=1` indicates the
  dominant cost is not row consumption. It is fixed remote setup and request
  establishment overhead in this topology.

## SWMR `1 GB`

Per measured step, this workload processes:

- writer rows: `3328`
- reader rows total: `4608`
- total rows: `7936`

Results:

| Cell | Mean Step (s) | p95 Step (s) | Throughput | Writer Mean (s) | Head Light (s) | Head Heavy (s) | Pinned Light (s) | Pinned Heavy (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ec2 local, ~1 GB logical` | `0.211` | `0.283` | `37.57 Krows/s` | `0.148` | `0.0076` | `0.0356` | `0.0028` | `0.0176` |
| `ec2 standard S3, ~1 GB logical` | `9.642` | `11.950` | `823 rows/s` | `5.840` | `0.887` | `1.617` | `0.275` | `1.022` |
| `ec2 S3 Express, ~1 GB logical` | `77.088` | `84.092` | `102.95 rows/s` | `42.486` | `9.523` | `13.860` | `2.978` | `8.241` |

Observed ratio:

- Express vs standard S3 whole mixed step mean: `8.0x` slower
- Express vs standard S3 writer mean: `7.3x` slower
- Express vs standard S3 `head_light`: `10.7x` slower
- Express vs standard S3 `head_heavy`: `8.6x` slower
- Express vs standard S3 `pinned_light`: `10.8x` slower
- Express vs standard S3 `pinned_heavy`: `8.1x` slower

Correctness outcome:

- the Express `1 GB` SWMR artifact is validity-clean
- all reader classes reported `valid = true`
- the artifact still shows the intended distinction between:
  - mutable `head_light` validation by count and key band
  - stable-shape validation for `head_heavy`, `pinned_light`, and
    `pinned_heavy`

## What These Numbers Mean

What they do prove:

- Tonbo plus local Fusio can now use a real S3 Express directory bucket end to
  end.
- The benchmark harness, smoke path, and `1 GB` SWMR path all work against the
  real bucket.
- The branch now has machine-readable S3 Express artifacts for both directional
  and SWMR scenarios.

What they do not prove:

- They do not show S3 Express outperforming standard S3.
- They do not measure the same-AZ low-latency product story S3 Express is built
  for.

Why not:

- the runner was in `eu-central-1`
- the only reachable working directory bucket was in `us-east-1`
- the bucket AZ ID was `use1-az6`
- the network path was public internet and cross-region

That means the branch's Express numbers should be read as:

- functional enablement succeeded
- this topology is a poor performance fit for Express
- cross-region setup cost dominates so strongly that the Express cells are much
  slower than the same-host same-region standard S3 cells

## Practical Implications For The Branch

- The current branch no longer has an S3 Express capability gap.
- The remaining Express question is deployment/topology, not client support.
- Any product claim about Express value should wait for:
  - same-region EC2 to bucket comparison
  - ideally same-AZ EC2 to directory-bucket comparison
  - a standard-S3 control run from that same host

Until then, the safe conclusion is:

- Express works
- this cross-region topology is substantially worse than standard S3 on the
  same EC2 host
- the observed penalty is dominated by setup/prepare latency rather than row
  consumption
