# RFC 0007: Manifest Integration on Top of `fusio-manifest`

- Status: Implementing
- Authors: Tonbo team
- Created: 2025-10-28
- Area: Storage, Durability, GC

## Summary

Tonbo uses a manifest-backed control plane to define visibility, WAL retention, and version lineage over immutable objects. The manifest builds on `fusio-manifest` (CAS head, append-only segments, checkpoints, leases) to publish SST additions/removals, WAL floors, and catalog metadata. Reads resolve snapshot manifests for MVCC; writes and compaction publish edits atomically.

Manifest data is split across catalog, version, and gc-plan prefixes under `root/manifest`. Catalog owns table identity and schema fingerprints; version owns heads and committed versions (including WAL floors and SST entries); gc-plan stores deletion plans. Separation keeps high-churn version edits isolated from catalog evolution and GC coordination.

## Goals

1. Provide an authoritative manifest per table recording active SSTs, WAL retention cutoffs, schema/versioning information, and aggregated stats.
2. Expose transactional APIs (e.g., `apply_version_edits`, `snapshot_latest`, `list_versions`) backed by fusio-manifest sessions so writers/compactors publish atomically.
3. Tie WAL and SST garbage collection to manifest state by persisting referenced WAL segment ranges and computing safe `wal_floor` watermarks.
4. Support fast crash recovery by loading the manifest HEAD and replaying WAL only above the recorded floor, adopting orphan segments via fusio-manifest.
5. Remain runtime- and backend-agnostic by reusing Fusio traits (`DynFs`, `Executor`, `Timer`) exactly as fusio-manifest expects.

## Non-Goals

- Full multi-tenant catalog features beyond a single namespace
- Redesigning Tonbo's WAL frame format; this RFC only integrates WAL metadata with the manifest

## Background

The architecture requires CAS-published manifests to coordinate stateless readers/writers over object storage. `fusio-manifest` provides backend-agnostic CAS heads, append-only segments, checkpoints, snapshot leases, and retention/GC primitives that map directly onto Tonbo’s durability model.

## Design

### Key Space

Manifest content is partitioned:

- **Catalog keys** — catalog root and per-table metadata (name, schema fingerprint, primary-key layout, retention, schema version).
- **Version keys** — table head (current version pointer, WAL floor), immutable table versions keyed by manifest timestamp, WAL floor records.
- **GC-plan keys** — per-table GC plans describing obsolete SSTs/WAL segments targeted for deletion.

Catalog and version are separate key spaces to isolate high-churn version commits from catalog evolution; gc-plan is isolated so deletion intent does not interfere with either.

### Values

- **Catalog values:** catalog state (known tables, allocator), per-table metadata (name, schema fingerprint, primary-key layout, retention, schema version).
- **Version values:** table head (current version pointer, WAL floor, schema version), version state (levelled SST entries with stats and WAL segments, tombstone watermark), WAL floor records.
- **GC-plan values:** deletion plans for SSTs/WAL segments safe to reclaim.

Each version commit stores a self-contained `VersionState` under its manifest timestamp; manifest ordering defines visibility.

### SST Entry Metadata

Each SST entry in `VersionState` captures fields required for compaction and read-path pruning:

| Field | Purpose |
|-------|---------|
| level | Target level when SST was published |
| min_key / max_key | Lexicographic primary-key bounds |
| min_commit_ts / max_commit_ts | MVCC timestamp range |
| rows, bytes, tombstones | Sizing metrics for compaction planning |
| wal_segments | WAL fragment IDs that produced this SST |

These fields allow compaction and read-path pruning without inspecting Parquet contents.

### Catalog Registration and Table Definitions

Registration mints a `TableId` and records a `TableDefinition` (name, schema fingerprint, primary key columns, retention policy, schema version). If a table already exists under the same name, schema and key layout must match or registration fails. A corresponding head entry is created so writers can publish versions immediately.

### Sessions and Workflow

- **Write path:** assemble a `VersionState` (SST adds/removes, WAL segments, stats, tombstone watermark) and CAS-apply it alongside the updated table head (including WAL floor). CAS success defines global ordering; conflicts are retried with refreshed state.
- **Read path:** fetch table head, then load the referenced `VersionState` and matching catalog metadata to build a snapshot for planning/reads.
- **Recovery:** adopt orphaned manifest segments if present, reload catalog + heads, and replay WAL only at or above the recorded WAL floor.
- **GC:** compute GC plans from retained versions and published floors; gc-plan manifest stores planned deletions to coordinate execution.

### API Surface

The manifest module exposes operations for table lifecycle and version management:

| Operation | Purpose |
|-----------|---------|
| `open` | Initialize manifest from existing catalog and version stores |
| `init_catalog` | Bootstrap empty catalog if none exists |
| `register_table` | Register a new table or validate existing table matches definition |
| `table_meta` | Fetch logical metadata (name, schema, retention) for a table |
| `apply_version_edits` | Atomically publish SST adds/removes and WAL floor updates |
| `snapshot_latest` | Get current version state for read planning |
| `list_versions` | Enumerate retained versions for debugging or time-travel |
| `recover_orphans` | Adopt orphaned segments after crash recovery |
| `compactor` | Access GC/compaction planning APIs |

**Version edits** carry: SST additions, SST removals, WAL segment references, table stats, and tombstone watermark. Callers (flush/compaction) populate edits from SST descriptors and WAL metadata.

### Storage Layout

Manifest data follows RFC 0004 with three prefixes under `root/manifest/`:

| Prefix | Contents |
|--------|----------|
| `catalog/` | Head, segments, checkpoints, leases for table metadata |
| `version/` | Head, segments, checkpoints, leases for version history |
| `gc/` | Head, segments, checkpoints, leases for GC plans |

Each prefix uses fusio-manifest’s standard directory structure and CAS semantics.

## Integration Notes

- Writers and compactors publish SST additions/removals together with WAL segment references; table heads record the minimum WAL sequence (floor) required for recovery.
- Recovery replays WAL at or above the recorded floor and ignores objects not referenced by any manifest version.
- GC consumes manifest state (retained versions + floors) to decide which SSTs/WAL segments can be deleted safely; gc-plan records planned deletions to coordinate execution.

## Risks

- **Manifest bloat:** storing full `VersionState` per commit may grow segment size; acceptable for MVP, but keep structs versioned for future compression or delta encoding.
- **Serde compatibility:** changes to manifest structs must remain backward-compatible. Embed `format_version` fields to gate migrations.
- **GC coordination:** deleting WAL segments before manifest commits reflect the floor causes data loss. Mitigate by driving GC exclusively through manifest state and active leases.
- **Executor integration:** fusio-manifest expects `Executor + Timer`; ensure Tonbo threads its executor (`BlockingExecutor` or Tokio) into `ManifestContext`.

## Open Questions

1. Schema evolution storage: do we keep schema history inside `VersionState` or a separate key? (Current plan: `TableMeta` stores active schema; `VersionState` records `schema_version` pointer.)
2. Retention defaults: how many versions/time should Tonbo retain? Suggested default: keep last 5 versions or 24 hours; configurable per table.
3. Checkpoint strategy: when compaction writes SSTs, do we also emit fusio-manifest checkpoints? Deferred to the compaction RFC.

## References

- Tonbo docs overview (`docs/overview.md`) — describes manifest-driven architecture.
- Tonbo storage layout RFC (`docs/rfcs/0004-storage-layout.md`) — reserves `manifest/` directory.
- fusio-manifest codebase (`/Users/xing/Idea/fusio/fusio-manifest/src/…`) and RFD (`/Users/xing/Idea/fusio/docs/fusio-manifest-rfd.md`).
