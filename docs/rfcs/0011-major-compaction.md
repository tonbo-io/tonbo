# RFC 0011: Major Compaction MVP

- Status: Implementing (scaffold landed on `compaction/iterator-merge`)
- Authors: Tonbo storage team
- Created: 2025-11-06
- Area: Compaction, Manifest, SST

## Summary

Major compaction is now wired end-to-end on this branch: the planner emits a task, the DB resolves manifest entries into SST descriptors, the executor performs a latest-wins k-way merge, and manifest edits are published with WAL retention hints. Outputs are written as new Parquet artifacts (data + mvcc + optional delete sidecars) under level-scoped paths; inputs are removed and marked obsolete for GC.

## Current State vs `dev`

- New executor scaffolding (`src/compaction/executor.rs`) with `CompactionExecutor`, `CompactionJob`, `CompactionOutcome`, and a local implementation that calls an SSTable merger.
- DB orchestrator (`DB::run_compaction_task` in `src/db/mod.rs`) now performs plan → resolve → execute → apply manifest edits and carries GC hints plus WAL floor propagation.
- SSTable layer (`src/ondisk/sstable.rs`) gained `SsTableMerger`, merge sources/streams, and helpers to build level-aware Parquet/MVCC/delete outputs and stats.
- Manifest domain (`src/manifest/domain.rs`) records tombstone watermarks and WAL segment sets produced by compaction outcomes.
- Minor compaction driver untouched; major compaction operates on existing SST layout and manifests.

## Goals

- Turn planner tasks into durable manifest updates (Add/Remove SSTs) with WAL floor hints.
- Enforce MVCC correctness: latest-commit wins; tombstones suppress older versions.
- Produce Parquet outputs (data + mvcc + optional delete sidecar) with accurate stats/bounds.
- Keep the flow object-storage friendly: write-new-only, CAS manifest, no in-place mutation.
- Emit GC hints (obsolete SST ids, WAL floor) for a follow-on GC worker.

## Non-Goals (MVP)

- Advanced planner heuristics (size-tiering, overlap-aware LSM tuning).
- Compaction leasing/coordination across processes.
- GC worker implementation; this RFC only emits hints.

## Design

### Flow

1. Planner (`LeveledCompactionPlanner`) selects a `CompactionTask` (source/target level, inputs, optional key range).
2. DB resolves task inputs from the manifest into `SsTableDescriptor`s (paths, stats, `wal_ids`).
3. Executor (`CompactionExecutor` / `LocalCompactionExecutor`) allocates target descriptors, invokes `SsTableMerger`, and streams outputs.
4. Outcome builds `VersionEdit`s: `RemoveSsts` (inputs + obsolete ids), `AddSsts` (outputs with stats + wal_ids), `SetWalSegments` (segment set or wal_floor fallback), and optional `SetTombstoneWatermark` (max commit_ts in outputs).
5. Manifest applies edits atomically; WAL GC uses the published floor; GC hints list obsolete SST ids for a later worker.

### Merge semantics

- Each source stream yields keyed batches; merge orders by key, then `commit_ts` desc.
- Latest-wins per key; tombstones trump older data with lower/equal `commit_ts`.
- Stats captured: visible rows, bytes (best-effort), tombstones, min/max key, min/max `_commit_ts`; WAL ids from inputs are deduped onto outputs.

### Outputs and layout

- Outputs use level-scoped paths: `L{level}/{id}.parquet`, `L{level}/{id}.mvcc.parquet`, and `L{level}/{id}.delete.parquet` (delete sidecar written only when rows exist).
- Files are always write-new (+create/truncate) with no renames; manifest CAS publishes visibility.
- Output batches carry:
  - Data Parquet (user schema).
  - MVCC sidecar Parquet (`_commit_ts`).
  - Optional delete sidecar Parquet (key-only + `_commit_ts`).

### Interfaces with other components

- **Manifest:** CompactionOutcome emits `VersionEdit`s; `VersionState` stores WAL segments, WAL floor, and tombstone watermark derived from merged outputs. Manifest head advances monotonically; DB prunes WAL below manifest floor after apply.
- **WAL:** Executor may surface a new segment set; DB falls back to existing manifest WAL segments or floor to keep retention safe even if executor omits WAL info.
- **Read path:** Readers consume the same SST format; merged outputs already encode latest-wins/tombstones in Parquet + MVCC/delete sidecars to keep visibility rules consistent for merge-scans.
- **Write path:** Minor compaction and flush continue to emit SSTs with data/MVCC/delete sidecars; major compaction rewrites them without changing ingest semantics.
- **GC:** Obsolete SST ids (inputs + already-removed ids) are surfaced as hints; actual deletion/GC worker remains future work.

**Concrete interface map**

| Area | Types / functions | Notes |
| --- | --- | --- |
| Planning | `LeveledCompactionPlanner`, `CompactionTask` | Picks source/target levels + inputs; optional key range. |
| Resolution | `DB::resolve_compaction_inputs` | Turns manifest `SstEntry` into `SsTableDescriptor` (paths, stats, wal_ids). |
| Execution | `CompactionExecutor`, `LocalCompactionExecutor`, `SsTableMerger` | Executor allocates targets, merges via latest-wins; returns `CompactionOutcome`. |
| Manifest apply | `CompactionOutcome::to_version_edits`, `DB::run_compaction_task` | Builds `AddSsts` / `RemoveSsts` / `SetWalSegments` / `SetTombstoneWatermark`; applies via manifest CAS. |
| WAL retention | `CompactionOutcome.wal_segments|wal_floor`, `DB::prune_wal_segments_below_floor` | Uses executor-provided segments or manifest floor fallback to keep WAL GC safe. |
| Read path | `SsTableReader::into_stream` (Parquet + mvcc + delete) | Readers keep consuming merged outputs with same sidecar schema. |
| GC hints | `CompactionOutcome.obsolete_sst_ids` | Inputs surfaced as GC candidates; actual delete worker TBD. |

### Modules and files touched

- `src/compaction/executor.rs`: executor trait, job/outcome structs, local executor backed by `SsTableMerger`.
- `src/compaction.rs`: module exports and minor compactor (unchanged behaviour) to co-exist with major compaction.
- `src/db/mod.rs`: plan/resolve/execute/apply pipeline, WAL floor propagation, GC hints.
- `src/ondisk/sstable.rs`: merge sources/merger, path builders (`L{level}/{id}.*`), Parquet writers, stats aggregation.
- `src/manifest/domain.rs`: version state stores tombstone watermark and WAL segment sets from compaction outcomes.

## Testing

- Unit: merge ordering/dedup/tombstones, manifest edit assembly (Add/Remove/WAL), missing-path validation, outcome fallback to WAL floor.
- Integration: end-to-end compaction over overlapping SSTs (`run_compaction_task_e2e_merges_and_updates_manifest`) validates latest-wins, manifest updates, and readable merged output; planner no-op path returns `None`.

## Future Work

- Planner overlap/size heuristics and L0 fan-in caps.
- Streaming/iterator-based merger (current executor buffers inputs) and remote/distributed executors.
- Compaction leasing/coordination.
- GC plan manifest + worker wired to compaction hints.
- Metrics/telemetry for compaction latency, bytes rewritten, and tombstone pruning rates.
