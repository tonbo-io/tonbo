# RFC 0007: Manifest Integration on Top of `fusio-manifest`

- Status: Draft
- Authors: Tonbo team
- Created: 2025-10-28
- Area: Storage, Durability, GC

## Summary

Tonbo’s current `dev` branch accepts Arrow `RecordBatch` ingest, persists batches through the async WAL (`src/wal/mod.rs`, `src/wal/writer.rs`), and can flush sealed immutables into Parquet-backed SSTables (`src/ondisk/sstable.rs`). However, there is no manifest/ version-set layer to define visibility, coordinate WAL reclamation, or accelerate recovery. The existing documentation (`docs/overview.md`) calls for CAS-published manifests, yet the implementation lacks any manifest modules.

This RFC introduces a Tonbo-specific manifest module built on the `fusio-manifest` crate under the same workspace (`/Users/xing/Idea/fusio/fusio-manifest`). The module will capture Tonbo’s table metadata, SST versions, and WAL segment lifecycles using fusio-manifest’s serializable key–value transactions, snapshots, and GC APIs. By grounding the manifest on fusio-manifest we align Tonbo’s durability story with the Arrow-first engine and unlock downstream read-path, compaction, and MVCC work.

The implementation now splits responsibilities across **two fusio-manifest instances** rooted under `root/manifest`: a catalog manifest that owns logical table metadata and a version manifest that owns table heads, committed versions, and WAL retention floors. Each instance has its own codec and directory prefix so catalog replication/evolution never interferes with high-churn version edits.

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

Tonbo's dev branch has Arrow-first mutable/immutable layers and SST writer scaffolding, but lacks manifest modules. The WAL writes durable segments yet has no id allocation or reclamation logic. The architecture overview references manifest-driven CAS updates, snapshot visibility, and GC—none of which exist in code.

This RFC builds on `fusio-manifest`, which provides backend-agnostic CAS HEAD objects, append-only segments, checkpoints, snapshot leases, and retention/GC.

## Design

### Key Space

The manifest uses two separate key spaces to isolate catalog metadata from high-churn version edits:

**Catalog manifest keys:**

| Key | Purpose |
|-----|---------|
| CatalogRoot | Global catalog state (known tables, ID allocator) |
| TableMeta | Per-table metadata (name, schema fingerprint, PK layout, retention) |

**Version manifest keys:**

| Key | Purpose |
|-----|---------|
| TableHead | Current head pointer per table (latest version, WAL floor) |
| TableVersion | Versioned state snapshots keyed by `(table_id, manifest_ts)` |
| WalFloor | Lowest retained WAL segment reference per table |

This separation allows catalog replication/evolution without touching high-frequency version commits.

### Values

**Catalog manifest values:**
- `CatalogState`: Known tables plus allocator state
- `TableMeta`: Table name, schema fingerprint, PK layout, retention settings, schema version

**Version manifest values:**
- `TableHead`: Schema version, latest WAL floor, last manifest transaction timestamp
- `VersionState`: SST entries, WAL segment references, tombstone watermark, aggregated stats
- `WalFloor`: Lowest WAL segment still referenced by any retained version

Each version commit stores a full `VersionState` under its `TableVersion` key. The manifest timestamp defines global ordering.

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

The catalog manifest mints `TableId`s and guarantees schema compatibility. Registration happens through a `TableDefinition` payload containing:

- `name`: logical table identifier. The DB builder defaults to `tonbo-default` but exposes `.table_name(...)` so callers can supply custom names.
- `schema_fingerprint`: SHA-256 of the canonicalized Arrow schema (field ordering preserved, map/object keys sorted). Each `Mode` implements the new `CatalogDescribe` trait to compute this deterministically from its config.
- `primary_key_columns`: ordered list of key column names derived from the mode’s `KeyProjection::key_indices`.
- `retention`: optional overrides for version-count/TTL retention knobs.
- `schema_version`: explicit monotonic counter for schema evolution.

`TonboManifest::register_table` now orchestrates catalog + version bootstrap:

1. Ensure the catalog root exists (`init_catalog`).
2. Look up an existing table by name. If schema fingerprint, key layout, version number, or retention differ, return `ManifestError::CatalogConflict`; otherwise reuse the prior `TableId`.
3. Persist/return `TableMeta` and create the matching `TableHead` entry inside the version manifest so writers can immediately publish versions.

This split lets us replicate catalog metadata (e.g., to serve discovery APIs) at a slower cadence than high-frequency version commits while still keeping schema checks manifest-backed.

### Sessions and Workflow

- **Write path:** `TonboManifest::apply_version_edits` opens a fusio `WriteSession`, loads `TableHead`, assembles `VersionState` from the SST flush (including WAL segment refs), stages:
  - `put(TableVersion {...}, new_version)`
  - `put(TableHead {...}, updated_head_with_wal_floor)`
  and commits. CAS success defines global order.
- **Read path:** `snapshot_latest` opens a `ReadSession`, fetches `TableHead`, reads the `VersionState` stored at `last_manifest_txn`, and pairs the result with `TableMeta` fetched from the catalog manifest so callers receive both physical (versions) and logical (schema/name) metadata in a single `TableSnapshot`.
- **Recovery:** On open, call `manifest.recover_orphans()` (fusio-manifest handles adopting contiguous segments). Load `CatalogState` and each `TableHead` to rebuild table descriptors; replay WAL only above the manifest’s `wal_floor`.
- **GC:** `TonboManifest::compute_gc_plan` wraps fusio-manifest’s compactor GC APIs; plans delete SST objects once versions expire and advance `WalFloor` so WAL segments < floor can be truncated.

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

Manifest data follows RFC 0004 with two prefixes under `root/manifest/`:

| Prefix | Contents |
|--------|----------|
| `catalog/` | HEAD pointer, segments, checkpoints, leases for table metadata |
| `version/` | HEAD pointer, segments, checkpoints, leases for version history |

This separation allows catalog replication/checkpointing without touching the heavier version log, and vice versa. Both prefixes use fusio-manifest's standard directory structure.

## Initial Integration Step

The first task for the development team is to wire Tonbo’s existing SST flush and WAL layers into the manifest so WAL segments gain explicit lifecycle management.

1. **Capture WAL segment refs during flush.**
   - `DB::flush_immutables_with_descriptor` already gathers `wal_ids` via `SsTableDescriptor::with_wal_ids` (`src/ondisk/sstable.rs:216-242`). Convert those into `WalSegmentRef { seq, file_id, first_frame, last_frame }`.
   - Add a temporary helper (`wal::manifest_ext`) to fetch the active segment’s first/last frame numbers when sealing.
2. **Call `TonboManifest::apply_version_edits`.**
   - After successful Parquet write, construct the `VersionEdit` payload with the new `SstEntry` and WAL refs (pass as a single-element slice until batching is required).
   - Update `TableHead.wal_floor` to the minimum WAL sequence still referenced by retained versions.
3. **Compute and persist `wal_floor`.**
   - For MVP, `wal_floor = last_retained_wal_ref.seq`. Later iterations will consider retention and multiple versions.
4. **Teach WAL GC to respect the floor.**
   - Expose `TonboManifest::wal_floor(table_id)` so WAL cleanup can remove segments with `seq < wal_floor`.
   - Ensure recovery only replays WAL frames at or above the floor.

Once this integration lands, WAL space is bounded, recovery gains a durable high-water mark, and future manifest features (snapshots, compaction GC) have a solid foundation.

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
