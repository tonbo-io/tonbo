# EC2 us-east-1 Same-Region Follow-up (2026-04-01)

This note captures the first directly comparable same-host runs between
standard S3 and S3 Express on the same EC2 instance in `us-east-1`.

Unlike the earlier cross-region Express experiment, these runs use:

- one EC2 runner
- one region
- for Express, the same AZ ID as the directory bucket

## Environment

- EC2 instance id: `i-0676cd4c1420b2bfb`
- instance type: `c7i.large`
- runner region: `us-east-1`
- runner AZ name on this account: `us-east-1c`
- runner AZ ID: `use1-az6`
- standard S3 bucket: `tonbo-smoke-use1-232814779190-20260401`
- standard bucket region: `us-east-1`
- S3 Express directory bucket:
  `tonbo-bench-use1-az6-20260331--use1-az6--x-s3`
- directory bucket region: `us-east-1`
- directory bucket AZ ID: `use1-az6`

## Read Compaction Quiesced

Artifacts:

- standard S3 `scale=1`:
  `/home/ubuntu/workspace/tonbo/target/tonbo-bench/compaction_local-1775037685933-15549.json`
- Express `scale=1`:
  `/home/ubuntu/workspace/tonbo/target/tonbo-bench/compaction_local-1775037800005-15703.json`
- standard S3 `scale=4`:
  `/home/ubuntu/workspace/tonbo/target/tonbo-bench/compaction_local-1775038027706-15936.json`
- Express `scale=4`:
  `/home/ubuntu/workspace/tonbo/target/tonbo-bench/compaction_local-1775038376217-16364.json`

Results:

| Cell | Mean | p95 | Rows Processed | Mean Prepare | Mean Consume |
| --- | ---: | ---: | ---: | ---: | ---: |
| `use1 standard S3, scale=1` | `873.05 ms` | `1263.49 ms` | `24,576` | `842.48 ms` | `30.55 ms` |
| `use1 S3 Express same-AZ, scale=1` | `2278.97 ms` | `2621.39 ms` | `24,576` | `2243.56 ms` | `35.39 ms` |
| `use1 standard S3, scale=4` | `2478.59 ms` | `2768.13 ms` | `24,576` | `2331.68 ms` | `146.85 ms` |
| `use1 S3 Express same-AZ, scale=4` | `4071.72 ms` | `4264.24 ms` | `24,576` | `3949.93 ms` | `121.73 ms` |

Observed ratio:

- Express vs standard S3 mean, `scale=1`: `2.61x` slower
- Express vs standard S3 p95, `scale=1`: `2.07x` slower
- Express vs standard S3 mean, `scale=4`: `1.64x` slower
- Express vs standard S3 p95, `scale=4`: `1.54x` slower

Interpretation:

- Moving the runner to the same region and same AZ ID removes the absurd
  cross-region Express penalty from the earlier experiment.
- But Express still does not outperform standard S3 in this Tonbo/Fusio path.
- Both backends remain heavily prepare-dominated:
  - standard `scale=1`: `96.5%` prepare
  - Express `scale=1`: `98.4%` prepare
  - standard `scale=4`: `94.1%` prepare
  - Express `scale=4`: `97.0%` prepare
- Express narrows somewhat at `scale=4`, which suggests a large fixed setup
  cost rather than pure row-consumption cost.

## SWMR `1 GB`

Artifacts:

- standard S3 `1 GB`:
  `/home/ubuntu/workspace/tonbo/target/tonbo-bench/compaction_local-1775039166401-16780.json`
- Express `1 GB`:
  `/home/ubuntu/workspace/tonbo/target/tonbo-bench/compaction_local-1775040119330-17177.json`

Per measured step, this workload processes:

- writer rows: `3328`
- reader rows total: `4608`
- total rows: `7936`

Results:

| Cell | Mean Step (s) | p95 Step (s) | Throughput | Writer Mean (s) | Head Light (s) | Head Heavy (s) | Pinned Light (s) | Pinned Heavy (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `use1 standard S3, ~1 GB logical` | `11.644` | `13.747` | `681.57 rows/s` | `7.571` | `1.034` | `1.690` | `0.289` | `1.060` |
| `use1 S3 Express same-AZ, ~1 GB logical` | `40.162` | `42.338` | `197.60 rows/s` | `21.575` | `4.777` | `8.102` | `0.962` | `4.744` |

Observed ratio:

- Express vs standard S3 whole mixed step mean: `3.45x` slower
- Express vs standard S3 writer mean: `2.85x` slower
- Express vs standard S3 `head_light`: `4.62x` slower
- Express vs standard S3 `head_heavy`: `4.79x` slower
- Express vs standard S3 `pinned_light`: `3.33x` slower
- Express vs standard S3 `pinned_heavy`: `4.48x` slower

Correctness outcome:

- both same-host `1 GB` artifacts are validity-clean
- all reader classes reported `valid = true`

Important setup observation:

- standard S3 preload time: `699.734 s`
- Express preload time: `2331.777 s`

That means the main same-host same-AZ Express loss in this `1 GB` cell is not
coming from a reader-correctness issue. It is dominated by the write-heavy
preload path before measurement even begins.

## Express Debug Probe

A focused debug rerun was executed on the same host for `scale=1` with:

- `FUSIO_S3_EXPRESS_DEBUG=1`

What it showed:

- `CreateSession` count during the whole benchmark run: `1`
- Express list failures observed: `0`

Practical implication:

- the earlier repeated-session bug is no longer the explanation for the same-AZ
  performance gap
- the remaining cost wall is deeper in the current Tonbo/Fusio object-store
  path

## Current Conclusion

What is now proven:

- S3 Express works end to end in Tonbo/Fusio on a same-region, same-AZ host
- the old benchmark-path Express enablement blockers are fixed
- the old repeated-session churn is not the current dominant issue

What is now also proven:

- in the current Tonbo/Fusio path, same-AZ S3 Express is still slower than the
  same-host standard-S3 control on this workload set

Most likely interpretation:

- the benchmark path is paying a large fixed setup/write-path cost in the
  current Express implementation
- that cost dominates more heavily in write-rich shapes than in the smaller
  directional read cell

Recommended next step:

- instrument the write path and object-store setup path before running larger
  matrices:
  - WAL publication
  - SST upload/commit
  - manifest publication
  - object-store open/list/head patterns during setup
