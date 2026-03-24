# Manifest CAS Cost Probe — 2026-03-23

## Scope

This note measures Tonbo's current manifest publication path on top of `fusio-manifest`.
The target is the version-manifest commit path used by:

- flush/minor-compaction publication (`apply_version_edits`)
- transaction-side WAL publication (`apply_version_edits`)
- compaction publication (`apply_version_edits_cas`, same underlying commit path plus an extra Tonbo-side head guard)

The probe intentionally measured the current path rather than proposing a redesign first.

## What Was Measured

Code path under test:

- `src/manifest/driver.rs`
  - `Manifest::apply_version_edits_inner`
  - underlying `fusio-manifest` `session_write -> recover_orphans -> snapshot -> lease create -> segment put -> head CAS put`

Per logical publish, the probe captured:

- logical publish latency
- failed-attempt latency
- retry count / retry amplification
- throughput
- equivalent object-operation volume by counting filesystem/object operations on:
  - head `load_with_tag`
  - head `put_conditional`
  - segment `put_conditional`
  - segment `list`
  - segment metadata `head`
  - lease `put_conditional`
  - lease `list`
  - lease file opens
  - lease removes

## How It Was Measured

- Added an ignored internal probe test:
  - `manifest::driver::tests::manifest_cas_cost_profile`
- Probe command:
  - `cargo test manifest_cas_cost_profile -- --ignored --nocapture`
- Backend variants:
  - `in_memory`: raw in-memory manifest backend
  - `in_memory_rtt1ms`: same backend with a 1 ms delay injected per manifest-object operation to approximate a remote/object-store RTT envelope
  - `local_fs`: local disk via `fusio::disk::LocalFs`
  - `s3`: optional real S3 backend when `TONBO_S3_BUCKET` plus credentials are present in `TONBO_S3_*` or standard `AWS_*` environment variables
- Workload shape:
  - each worker repeatedly publishes a tiny, constant-size edit set:
    - `SetWalSegments` with one segment ref
    - `SetTombstoneWatermark`
  - concurrency levels: `1`, `2`, `4`, `8`
  - commits per worker:
    - `128` for both variants
    - `24` for `local_fs`
    - `24` for the optional real `s3` runs

This isolates the CAS/control-plane cost under contention without adding version-state bloat from large SST sets.

### Real S3 rerun

The probe can reuse AWS CLI SSO credentials on a remote host by exporting temporary credentials into the test process:

```bash
eval "$(aws configure export-credentials --profile tonbo --format env)"
export TONBO_S3_BUCKET=tonbo-smoke-euc1-232814779190-1772115011
export TONBO_S3_REGION=eu-central-1
cargo test manifest_cas_cost_profile -- --ignored --nocapture
```

## Results

### Summary table

| backend | concurrency | logical_commits | throughput ops/s | retries | retry amp | p50 ms | p95 ms | failed-attempt p50 ms | object ops / success |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `in_memory` | 1 | 128 | 1995.5 | 0 | 1.00 | 0.483 | 0.844 | - | 10.99 |
| `in_memory` | 2 | 256 | 1144.3 | 0 | 1.00 | 0.899 | 1.497 | - | 11.00 |
| `in_memory` | 4 | 512 | 545.8 | 184 | 1.36 | 1.791 | 24.229 | 3.371 | 14.93 |
| `in_memory` | 8 | 1024 | 255.7 | 1200 | 2.17 | 4.199 | 128.388 | 6.346 | 25.52 |
| `in_memory_rtt1ms` | 1 | 128 | 52.4 | 0 | 1.00 | 19.139 | 19.548 | - | 10.99 |
| `in_memory_rtt1ms` | 2 | 256 | 50.5 | 136 | 1.53 | 20.250 | 116.272 | 39.437 | 16.67 |
| `in_memory_rtt1ms` | 4 | 512 | 48.3 | 695 | 2.36 | 58.750 | 231.746 | 40.808 | 26.80 |
| `in_memory_rtt1ms` | 8 | 1024 | 44.6 | 2248 | 3.20 | 105.857 | 500.484 | 44.549 | 39.80 |
| `local_fs` | 1 | 24 | 640.3 | 0 | 1.00 | 1.570 | 1.661 | - | 10.96 |
| `local_fs` | 2 | 48 | 430.3 | 2 | 1.04 | 1.692 | 2.179 | 35.721 | 11.40 |
| `local_fs` | 4 | 96 | 369.2 | 14 | 1.15 | 2.425 | 4.491 | 35.887 | 12.53 |
| `local_fs` | 8 | 192 | 286.1 | 157 | 1.82 | 3.625 | 139.545 | 5.914 | 20.21 |
| `s3` | 1 | 24 | 1.6 | 0 | 1.00 | 605.288 | 758.055 | - | 10.96 |
| `s3` | 2 | 48 | 1.6 | 42 | 1.88 | 657.828 | 2918.969 | 598.567 | 20.06 |
| `s3` | 4 | 96 | 1.6 | 273 | 3.84 | 1428.188 | 7908.782 | 632.493 | 41.94 |
| `s3` | 8 | 192 | 1.5 | 1183 | 7.16 | 3914.524 | 13718.838 | 661.040 | 80.73 |

### Detailed observations

Uncontended successful publish:

- Stable base cost was about `11` object operations per successful logical commit.
- Even in-memory, the successful path is not just one CAS:
  - repeated head loads
  - segment list probe for orphan recovery
  - lease create/remove work
  - segment write
  - head CAS write

Contention envelope without added RTT:

- contention starts to matter at `4+` writers
- at `8` writers:
  - throughput drops from `1995.5` to `255.7 ops/s`
  - retry amplification rises to `2.17x`
  - p95 logical latency reaches `128.388 ms`
  - object operations per success rise from `10.99` to `25.52`

Contention envelope with 1 ms per-operation delay:

- uncontended throughput collapses to about `52 ops/s`; this is the expected cost of serial control-plane I/O
- contention becomes the dominant problem quickly
- at `8` writers:
  - throughput is only `44.6 ops/s`
  - retry amplification is `3.20x`
  - p50 logical latency is `105.857 ms`
  - p95 logical latency is `500.484 ms`
  - p99 logical latency reached `1086.464 ms`
  - object operations per success rose to `39.80`

Local filesystem envelope:

- uncontended local publication is materially faster than the remote-like delayed variant:
  - `640.3 ops/s`
  - `1.570 ms` p50
  - `10.96` object ops per success
- contention still hurts, but less severely than the 1 ms delayed variant
- at `8` writers:
  - throughput fell to `286.1 ops/s`
  - retry amplification reached `1.82x`
  - p95 logical latency reached `139.545 ms`
  - object operations per success rose to `20.21`

Real S3 envelope:

- the same uncontended successful path still stayed near `11` object operations per success
- latency reflected raw object-store round trips rather than local control-plane work:
  - `1.6 ops/s`
  - `605.288 ms` p50
  - `758.055 ms` p95
- contention on real S3 is much worse than the local and synthetic-RTT backends
- at `8` writers:
  - throughput stayed flat at `1.5 ops/s`
  - retry amplification reached `7.16x`
  - p50 logical latency reached `3914.524 ms`
  - p95 logical latency reached `13718.838 ms`
  - object operations per success rose to `80.73`
- failed attempts still cost about one object-store round trip budget each:
  - failed-attempt p50 stayed between `598.567 ms` and `661.040 ms` from `2` to `8` writers

### Operation-cost shape

The uncontended path stayed near `11` object operations per success. Under contention:

- `in_memory` `8` writers: `25.52 / 10.99 = 2.32x` operation-cost amplification
- `in_memory_rtt1ms` `8` writers: `39.80 / 10.99 = 3.62x` operation-cost amplification

Most of the extra cost came from retries causing repeated:

- head reloads
- segment list probes
- lease churn
- extra segment publish attempts

## Assessment

For a low-contention next release, the current CAS-based manifest path looks acceptable.
The uncontended successful publish path is straightforward and bounded, and even with a small remote-like delay the single-writer cost stays predictable.

The risk is contention, not the existence of CAS itself.
Once multiple writers overlap on the same manifest head, the dominant failure mode is retry-driven amplification:

- latency tails expand sharply
- throughput flattens or declines instead of scaling
- object-operation cost per successful publish multiplies
- lease/orphan bookkeeping adds extra work, not just the head CAS

The data does not support calling the current path "efficient enough" for production-oriented multi-writer workloads that expect frequent concurrent manifest publication on one table/head.
It does support using the current path for the next release if Tonbo is effectively operating in a single-writer or very low-contention regime.

On local disk specifically, the current path looks acceptable for modest concurrency.
The main production-readiness concern is object-store-style latency plus contention, not local-disk CAS overhead.

## Recommended Follow-ups

Evidence-backed next actions:

1. Add stable runtime metrics for manifest publishes:
   - success/failure counts
   - retry counts
   - end-to-end publish latency
   - object-operation counters where the backend can expose them
2. Put explicit operational guidance around writer concurrency per table/head for the next release.
3. Measure compaction-specific `apply_version_edits_cas` separately after the runtime metrics land; it has an extra Tonbo-side head guard and backoff layer, but shares the same successful commit path.
4. If Tonbo expects shared-head multi-writer publication on object storage, redesign or narrow the coordination contract before calling this production-ready; the current retry envelope on real S3 is too expensive.

## Caveats

- The probe used a small constant-size edit payload, so it isolates CAS/control-plane cost rather than full manifest-state growth.
- `local_fs` required probe-side filtering of temporary segment artifacts and panic containment around disappearing lease files during concurrent lease scans. Those are harness accommodations for the local backend’s current behavior, not product-path changes.
- `in_memory_rtt1ms` is only a directional approximation of remote latency, not a substitute for S3 measurements.
- The real S3 numbers above were collected against bucket `tonbo-smoke-euc1-232814779190-1772115011` in `eu-central-1` using AWS CLI-exported SSO session credentials on 2026-03-23.
