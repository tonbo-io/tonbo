# SST GC Timing Rerun (2026-03-22)

- Date (UTC): `2026-03-22`
- Base commit: `3b22656a5c4d590310c005b128ede1d901f3a962`
- Working tree note: this rerun used the branch-local snapshot-pin / GC changes plus
  benchmark-harness updates in `benches/compaction/common.rs` to remove the deleted
  manifest-retention API and report active snapshot pins instead.
- Backend: `local`
- Kept artifact: `target/tonbo-bench/compaction_local-1774213761099-1116172.json`
- Scenario set:
  - `read_baseline`
  - `read_compaction_quiesced`
  - `read_compaction_quiesced_after_gc`
  - `write_heavy_no_sst_sweep`
  - `write_heavy_with_sst_sweep`
  - `estimate_sweep_candidates`

## Command

```bash
TONBO_BENCH_BACKEND=local \
TONBO_BENCH_DATASET_SCALE=20 \
TONBO_COMPACTION_BENCH_INGEST_BATCHES=640 \
TONBO_COMPACTION_BENCH_ROWS_PER_BATCH=64 \
TONBO_COMPACTION_BENCH_WAL_SYNC=always \
TONBO_COMPACTION_BENCH_ENABLE_READ_WHILE_COMPACTION=0 \
TONBO_COMPACTION_BENCH_ENABLE_WRITE_THROUGHPUT_VS_COMPACTION_FREQUENCY=0 \
TONBO_COMPACTION_BENCH_ARTIFACT_ITERATIONS=4 \
TONBO_COMPACTION_BENCH_CRITERION_SAMPLE_SIZE=10 \
cargo bench -p tonbo --bench compaction_local -- \
  'read_baseline|read_compaction_quiesced|read_compaction_quiesced_after_gc|write_heavy_no_sst_sweep|write_heavy_with_sst_sweep|estimate_sweep_candidates' \
  --nocapture
```

## Primary Result

The kept `scale=20` local rerun still shows GC time clearly, but it also shows that reclaim
already happened before the explicit observation point in the read path and partly overlapped the
write-path observation window:

| Scenario | Sweep Runs | Sweep Duration (ms) | Total Deleted SST Objects | Total Deleted SST Bytes | Deleted Bytes per ms | Deleted Objects per ms | GC Share of Setup | GC Share of Measured Steady-State Time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `read_compaction_quiesced_after_gc` | `36` | `2,498` | `230` | `251,362,341` | `100,625.44` | `0.09` | `3.15%` | `247.14%` |
| `write_heavy_with_sst_sweep` | `3` | `199` | `18` | `19,727,917` | `99,135.26` | `0.09` | `0.22%` | `16.51%` |

## Timing Placement

| Scenario | Sweep Runs Before Explicit Sweep | Sweep Duration Before Explicit Sweep (ms) | Explicit Sweep Runs | Explicit Sweep Duration (ms) | Explicit Sweep Deleted Objects | Explicit Sweep Deleted Bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `read_compaction_quiesced_after_gc` | `36` | `2,498` | `0` | `0` | `0` | `0` |
| `write_heavy_with_sst_sweep` | `2` | `131` | `1` | `68` | `6` | `6,576,034` |

The read-focused scenario still points to sweep work happening before the explicit observation
point. The write-focused scenario now shows one additional sweep run during the observation window,
but not as foreground delete work reported by the explicit sweep call itself.

## Physical SST Footprint With vs Without Prior GC

To make reclaim volume easier to interpret, the table below reconstructs the implied physical SST
footprint at the observation point if the already-completed background sweeps had not run yet:

`implied physical SST without prior GC = physical SST at observation point + cumulative deleted SST
bytes/objects before the explicit observation point`

| Scenario | Current Physical SST Objects | Current Physical SST Bytes | Deleted Before Explicit Sweep (Objects) | Deleted Before Explicit Sweep (Bytes) | Implied Physical SST Objects Without Prior GC | Implied Physical SST Bytes Without Prior GC | GC Reduction vs Implied No-GC Footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `read_compaction_quiesced_after_gc` | `6` | `6,486,182` | `230` | `251,362,341` | `236` | `257,848,523` | `97.48%` |
| `write_heavy_with_sst_sweep` | `196` | `214,772,250` | `12` | `13,151,883` | `208` | `227,924,133` | `5.77%` |

For the write-focused scenario, the physical SST footprint then fell further during the observation
window to `191` objects / `209,269,754` bytes even though the explicit sweep call itself reported
zero deleted objects.

## Setup vs Steady-State

| Scenario | Setup Time (ms) | Measured Steady-State Time (ms) | Mean Latency (ms) |
| --- | ---: | ---: | ---: |
| `read_compaction_quiesced` | `82,813.446` | `1,021.403` | `255.351` |
| `read_compaction_quiesced_after_gc` | `79,401.932` | `1,010.743` | `252.686` |
| `write_heavy_no_sst_sweep` | `78,662.434` | `1,271.527` | `317.881` |
| `write_heavy_with_sst_sweep` | `90,190.690` | `1,205.507` | `301.377` |

Latency deltas in the paired GC scenarios remained small:

| Comparison | Mean Latency Delta |
| --- | ---: |
| `read_compaction_quiesced_after_gc` vs `read_compaction_quiesced` | `-1.04%` |
| `write_heavy_with_sst_sweep` vs `write_heavy_no_sst_sweep` | `-5.19%` |

## Conclusions

### Read GC scenario

- Reclaim volume was still material: background sweeps deleted `251,362,341` bytes across `230`
  SST objects.
- Without that prior GC, the read-focused scenario would have been carrying about `257.8 MB` of
  physical SST data instead of `6.5 MB` at the observation point.
- Reclaim duration was measurable but still small relative to setup time: `2,498 ms` total, or
  `3.15%` of setup wall-clock time.
- Sweep efficiency remained high in storage terms: `100,625.44 bytes/ms`.
- The paired steady-state read latency still does not establish a material foreground GC win:
  `read_compaction_quiesced_after_gc` was only `1.04%` faster than `read_compaction_quiesced`.
- The sweep work was not on the measured foreground path in this artifact: all `36` sweep runs had
  already happened before the explicit observation point, and the explicit sweep itself did no
  deletion work.

### Write GC scenario

- Reclaim volume was larger than in the prior report: cumulative sweeps deleted `19,727,917` bytes
  across `18` SST objects, with `13,151,883` bytes / `12` objects already reclaimed before the
  explicit observation point.
- Relative to setup time, reclaim cost remained very small: `199 ms` total, or `0.22%` of setup
  wall-clock time.
- Sweep efficiency was similar to the read scenario: `99,135.26 bytes/ms`.
- At the explicit observation point, prior background GC reduced the implied physical SST footprint
  from about `227.9 MB` to `214.8 MB`; the footprint then dropped further to `209.3 MB` during the
  observation window.
- The steady-state write companion moved in the other direction from the previous report:
  `write_heavy_with_sst_sweep` was `5.19%` faster than `write_heavy_no_sst_sweep`. That is still
  too small and noisy to treat as a clear GC effect on its own, especially because Criterion did
  not detect a significant change for either write scenario.
- Unlike the read scenario, one additional sweep run was recorded during the observation window
  (`68 ms`, `6` objects, `6,576,034` bytes), but the explicit sweep call itself still reported no
  direct delete work. The artifact therefore points to sweep activity overlapping setup/observation
  rather than the measured steady-state foreground operation.
