# RFC: Storage Layout & Filesystem Hierarchy

- Status: Draft
- Authors: Tonbo team
- Created: 2025-10-21
- Area: Storage, WAL, Recovery

## Summary

Define the on-disk directory hierarchy for a Tonbo database instance so that write-ahead logging, future SSTables (data files plus delete sidecars), and catalog metadata share a consistent root. The layout is expressed in terms of `fusio::path::Path` so it maps across local filesystems, remote object stores, or any custom `DynFs` implementation.

## Motivation

- Recovery and initialization require predictable paths for WAL segments, SSTables, and manifests
- Object storage backends lack directory semantics; a clear path schema ensures portability across local filesystems and remote stores
- Reserving paths upfront avoids churn when new features (SST levels, manifests, spill files) land
- Separating concerns (WAL vs SST vs manifest) simplifies retention, compaction, and GC logic

## Goals

- Provide a stable directory/filename schema that `DB` can rely on during initialization and recovery.
- Ensure WAL recovery happens automatically when the DB is pointed at an existing root.
- Reserve locations for forthcoming SSTables, manifests, and mutable spill checkpoints so later features do not churn path semantics.

## Non-goals

- Implement the SSTable layer or manifest format.
- Commit to specific retention policies or compaction strategies.
- Encode tenant/multi-DB topologies (caller chooses the root path).

## Layout Overview

Given a database root `root: Arc<Path>`, Tonbo will create and manage the following subpaths:

```
root/
  wal/
    wal-<seq>.tonwal      // monotonic start sequence per segment
    state.json            // optional small manifest: last_seq, last_commit_ts
  sst/
    L0/                       // reserved for future levelled SST layout
      ...<id>.parquet         // user data file (includes _commit_ts column)
      ...<id>.delete.parquet  // key-only delete sidecar (optional)
    staging/                  // scratch for builds/compactions
  manifest/
    catalog/                  // fusio-manifest catalog namespace
      head.json
      segments/
      checkpoints/
      leases/
    version/                  // fusio-manifest version/GC namespace
      head.json
      segments/
      checkpoints/
      leases/
    gc/                       // fusio-manifest GC plan namespace
      head.json
      segments/
      checkpoints/
      leases/
```

All paths are created through `fusio` APIs; the layout makes no assumptions about POSIX semantics beyond directory hierarchy support.

### WAL directory (`root.join("wal")`)

- **Segments**: `wal-<start_seq>.tonwal`, where `<start_seq>` is the first frame sequence stored in the file (zero-padded decimal). The writer rotates files at the configured size/time threshold.
- **State file**: `state.json` (small JSON blob) records `last_segment_seq` (highest fully sealed segment start sequence), `last_frame_seq` (highest frame sequence emitted), and `last_commit_ts` (highest MVCC commit timestamp observed). This file is optional during MVP but reserved so WAL rotation, retention, and recovery can avoid scanning all segments when metadata is reliable.
- `wal::WalStorage::ensure_dir` creates the directory and the state file stub as needed.

### SST directory (`root.join("sst")`)

- Placeholder for immutable runs once SSTables land. Subdirectories `L0/` (ingest), `L1/`..`Ln/` (compacted levels), and `staging/` (writer scratch) keep compaction bookkeeping localized. The RFC reserves the names; concrete formats arrive in future SST RFCs.
- Each SSTable ID resolves to one required object under its level: `<id>.parquet` for user rows (with `_commit_ts` embedded). When tombstones exist, an optional `<id>.delete.parquet` sidecar holds key + `_commit_ts` rows. Paths are published atomically via the manifest.

### Manifest directory (`root.join("manifest")`)

- Reserved exclusively for the `fusio-manifest` subsystem. Tonbo now creates three independent prefixes under this directory:
  - `manifest/catalog/...` stores the catalog manifest (logical table metadata, schema fingerprints, retention knobs) with its own `head.json`, `segments/`, `checkpoints/`, and `leases/` directories.
  - `manifest/version/...` stores the version manifest (table heads, committed versions, WAL floors, future GC plans) with the same sub-structure.
- `manifest/gc/...` stores GC plans produced by compaction/GC orchestration, keeping deletion plans isolated from catalog/version churn.
- The multi-prefix layout lets us replicate or compact catalog metadata without touching high-churn version edits (and vice versa) while keeping every manifest path opaque to Tonbo code outside the manifest module.

## DB Initialization Flow

When a caller constructs a DB with `DB::new_dyn_with_root(schema, extractor, executor, root: Arc<Path>, cfg: WalConfig)`, the following steps occur:

1. Provision the layout via the builder (or a `DbPaths` helper) so `wal/`, `sst/`, and `manifest/{version,catalog,gc}` exist under `root`.
2. Call `WalStorage::ensure_dir(&paths.wal)` to create the WAL directory and associated state file if missing.
3. If any WAL segments exist, reopen the manifest under `root/manifest`, register (or look up) the logical table to obtain its `TableId`, and invoke the manifest-aware recovery helper. This ensures catalog metadata already persisted under the root is reused rather than silently replaced. The recovery routine updates `commit_clock` from either the state file or replayed events.

The same flow applies to typed modes once they return; only the ingest adapter changes.

## Recovery Notes

- During recovery, `Replayer::scan` enumerates segments under `wal/` in lexical order (`wal-00000000000000000001.tonwal`, ...). If `state.json` is present and trusted, it provides the last durable frame/commit metadata; otherwise, replay scans until the first invalid frame per RFC 0002.
- After replay completes, `commit_clock` is set to `last_commit_ts + 1` so new ingests pick up the correct MVCC timestamp sequence.
- Future work: once the manifest exists, recovery will first consult the latest `manifest/v*.json` to determine which SSTs are durable and how far the WAL can be truncated.

## Open Questions

- Should `state.json` be optional or required for fast start? (Likely optional at MVP; we can gate pruning on its presence.)
- Do we create `mutable/` eagerly, or lazily when spill features arrive? (Leaning lazy creation to avoid empty directories.)
- How do we guarantee atomic updates to `state.json` across backends lacking rename? (Fusio adapters must document the durability guarantees; we may adopt write-then-rename semantics on POSIX.)

## Next Steps

- Implement `DbPaths` helper in code and update constructors to accept `root: Arc<Path>` alongside `WalConfig`.
- Update `WalStorage::ensure_dir` to create `state.json` and expose helpers for listing existing segments.
- Extend the builder-driven recovery flow to leverage the state file once available.
- Document operational guidance in AGENTS.md after the hierarchy lands in code, including the dual-file SST (data with `_commit_ts` + optional delete sidecar) convention.
