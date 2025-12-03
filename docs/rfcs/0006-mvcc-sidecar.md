# RFC: MVCC Storage Layout for Immutable Segments

- Status: Accepted
- Authors: Tonbo storage team
- Created: 2025-10-28
- Updated: 2025-11
- Area: Storage engine, MVCC, SSTable

## Summary

Define the on-disk layout for MVCC metadata in immutable segments and SSTables. Data files embed `_commit_ts` as an appended column, while tombstones are stored in a separate key-only delete sidecar.

## Motivation

- Keep user schema columns cleanly separated from MVCC system columns
- Avoid materializing tombstoned rows as null-filled payloads in the data file
- Enable efficient column pruning—scans that don't need visibility checks can skip `_commit_ts`
- Support delete-heavy workloads without bloating data files

## Goals

- Immutable segments and SST files store `_commit_ts` alongside user data for upserts
- Tombstones live in a dedicated key-only sidecar, not in the data file
- WAL payloads remain Arrow-native and replayable
- Compaction, recovery, and GC operate over append-only objects via Fusio
- Minimal read amplification for scans and range lookups

## Non-Goals

- Changing timestamp assignment strategy (remains monotonic per commit)
- Introducing new transaction semantics or cross-table coordination
- Typed/compile-time ingestion pathways (future work per RFC 0001)

## Design

### Storage Layout

Each SSTable consists of up to two files:

```
/sst/L{level}/{id}.parquet          # data + _commit_ts column
/sst/L{level}/{id}.delete.parquet   # key-only delete sidecar (when tombstones exist)
```

### Data File

The data Parquet contains user schema columns with `_commit_ts: UInt64` appended as the last column. This enables:

- Column pruning to skip `_commit_ts` when visibility filtering is not needed
- Single-file I/O for upsert-only segments
- Alignment with WAL frame layout

### Delete Sidecar

Schema: `<primary-key columns>, _commit_ts: UInt64 (non-null)`

- Only emitted when the segment contains tombstones
- Key-only format avoids storing null value columns for deleted rows
- Enables efficient tombstone lookup during scans without polluting the data file

### WAL Integration

- Upsert frames carry batches with `_commit_ts` column
- Delete frames carry key-only batches for tombstones
- Replay reconstructs both upsert data and tombstone metadata

### Read Path

1. Load data Parquet, extracting `_commit_ts` for visibility checks
2. Load delete sidecar if present
3. Range scans consult both for MVCC filtering
4. Callers never see `_commit_ts` in query results—projection excludes it

### Compaction

- Merge data files with latest-wins semantics based on `_commit_ts`
- Merge delete sidecars alongside data
- Tombstones may be pruned when `commit_ts <= tombstone_watermark` and no live versions exist

### GC

Manifest-driven GC treats data and delete files atomically—both are retained or removed together based on version visibility.

## Alternatives Considered

1. **Three-file layout** (data + mvcc sidecar + delete sidecar): More files increase I/O; Parquet column pruning already handles skipping `_commit_ts`.

2. **Embed tombstones in data file as null rows**: Wastes space, forces nullable schemas, complicates scans.

3. **Store MVCC in Parquet metadata**: Lacks per-row fidelity, complicates streaming reads.

4. **Tombstones in WAL only**: Makes SST reconstruction expensive and breaks crash recovery.

## Comparison with Other Systems

### Apache Iceberg

Iceberg has three delete mechanisms:

| Type | How it works | Pros | Cons |
|------|--------------|------|------|
| Position deletes | file_path + row position | Fast read (exact location) | Requires knowing physical position; deprecated in v3 |
| Equality deletes | Column values that identify rows | Write-friendly (no position needed) | Query penalty—must scan and filter |
| Deletion vectors (v3) | Bitmap per data file | Compact, fast read | Requires file-level tracking |

Tonbo's key-only delete sidecar is similar to Iceberg's **equality deletes**—identifying rows by key values rather than physical position. Iceberg's experience shows equality deletes accumulate and hurt read performance until merged, which informed our decision to track tombstone watermarks for pruning.

### RocksDB

RocksDB takes a different approach:

- **Point tombstones**: Stored inline with data in SST files (key + sequence number)
- **Range tombstones**: Dedicated meta-block within each SST file
- **Compaction**: Tombstones drop only at bottom level when no snapshot references them
- **Trigger heuristics**: Compaction triggered when tombstone ratio exceeds 50%

RocksDB's inline storage avoids extra files but complicates the data format. Their range tombstone support is something Tonbo currently lacks.

### Design Trade-offs

Tonbo's current design optimizes for:
- **Simplicity**: Separate file is easier to reason about than inline storage
- **Write path**: No need to know physical row positions (unlike position deletes)
- **Schema purity**: User data files remain uncontaminated by tombstone markers

Known limitations for future consideration:
- **File proliferation**: Every segment with tombstones creates an extra file; deletion vectors (bitmaps) would be more compact
- **No range deletes**: Point-delete-only; range tombstone support may be needed for bulk delete workloads
- **Watermark semantics**: Tombstone retention must coordinate with snapshot/reader registry to avoid premature pruning
