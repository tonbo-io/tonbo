# RFC: Storage Layout & Filesystem Hierarchy

- Status: In Progress
- Authors: Tonbo team
- Created: 2025-10-21
- Area: Storage, WAL, Recovery

## Summary

Define the on-disk directory hierarchy for a Tonbo database instance so that write-ahead logging, SSTables (data files plus future MVCC sidecars), and catalog metadata share a consistent root. The layout is expressed in terms of `fusio::path::Path` so it maps across local filesystems, remote object stores, or any custom `DynFs` implementation. The current implementation already writes Parquet SSTables under a level-aware directory tree but still relies on in-memory manifest stores and has not yet introduced the optional WAL state file or mutable spill directory described below.

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
    wal-<seq>.wal          // monotonic start sequence per segment
    state.json             // optional small manifest: last_seq, last_commit_ts (reserved)
  sst/
    L0/                       // ingest / newest level (currently created)
      ...<id>.parquet         // user data file (currently written)
      ...<id>.mvcc.parquet    // MVCC sidecar (planned per RFC 0006)
    staging/                  // scratch for builds/compactions
  manifest/
    ...                       // managed by fusio-manifest
  mutable/
    spill/                    // optional spill files/checkpoints (future)
```

All paths are created through `fusio` APIs; the layout makes no assumptions about POSIX semantics beyond directory hierarchy support.

### WAL directory (`root.join("wal")`)

- **Segments**: `wal-<start_seq>.wal`, where `<start_seq>` is the first frame sequence stored in the file (zero-padded decimal). The writer rotates files at the configured size/time threshold.
- **State file**: `state.json` (small JSON blob) is reserved to record the highest sealed segment sequence and MVCC metadata. The current prototype does not emit this file yet; recovery still scans all segments.
- Directory creation is handled by callers configuring `WalConfig::filesystem`. Automatic bootstrapping of `wal/` and the state file remains future work.

### SST directory (`root.join("sst")`)

- Subdirectories `L0/`, `L1/`, … house SSTables per level. The current writer creates `L<level>/<sst_id>.parquet` and populates it via `AsyncArrowWriter`. MVCC sidecars (`<id>.mvcc.parquet`) and the `staging/` scratch space remain reserved for follow-up work.

### Manifest directory (`root.join("manifest")`)

- Reserved exclusively for the `fusio-manifest` subsystem. The MVP manifest integration currently uses in-memory stores, so no files are written under `manifest/` yet. When durable stores are configured this directory will mirror fusio-manifest’s HEAD/segment/checkpoint layout.

### Mutable spill directory (`root.join("mutable")`)

- Reserved for optional spill files, checkpoints, or crash diagnostics. No code writes to this directory yet.

## DB Initialization Flow

`DB::new` currently accepts an executor and manifest stores but no filesystem root. Callers are responsible for creating `wal/` and `sst/` and for passing those paths through `WalConfig`/`SsTableConfig`. Reintroducing the `DbPaths` helper remains future work so initialization can materialise directories, seed the manifest, and invoke WAL recovery automatically.

## Recovery Notes

- During recovery, `Replayer::scan` enumerates segments under `wal/` in lexical order (`wal-0000000001.wal`, …). Because `state.json` is not yet emitted, the scanner always walks from the oldest segment.
- After replay completes, `commit_clock` is set to `last_commit_ts + 1` so new ingests pick up the correct MVCC timestamp sequence.
- Once the manifest persists to disk, recovery will consult the latest manifest head to determine which SSTs are durable and how far the WAL can be truncated.

## Open Questions

- Should `state.json` be optional or required for fast start? (Likely optional at MVP; we can gate pruning on its presence.)
- Do we create `mutable/` eagerly, or lazily when spill features arrive? (Leaning lazy creation to avoid empty directories.)
- How do we guarantee atomic updates to `state.json` across backends lacking rename? (Fusio adapters must document the durability guarantees; we may adopt write-then-rename semantics on POSIX.)

## Next Steps

- Implement `DbPaths` helper in code and update constructors to accept `root: Arc<Path>` alongside `WalConfig`.
- Update `WalStorage::ensure_dir` to create `state.json` and expose helpers for listing existing segments.
- Extend `DB::recover_with_wal` to leverage the state file once available.
- Document operational guidance in AGENTS.md after the hierarchy lands in code, including the dual-file SST (data + MVCC sidecar) convention.
