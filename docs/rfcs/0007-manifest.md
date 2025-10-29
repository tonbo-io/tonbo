# RFC 0007: Manifest Integration on Top of `fusio-manifest`

- Status: Draft
- Authors: Tonbo team
- Created: 2025-10-28
- Area: Storage, Durability, GC

## Summary

Tonbo’s current `dev` branch accepts Arrow `RecordBatch` ingest, persists batches through the async WAL (`src/wal/mod.rs`, `src/wal/writer.rs`), and can flush sealed immutables into Parquet-backed SSTables (`src/ondisk/sstable.rs`). However, there is no manifest/ version-set layer to define visibility, coordinate WAL reclamation, or accelerate recovery. The existing documentation (`docs/overview.md`) calls for CAS-published manifests, yet the implementation lacks any manifest modules.

This RFC introduces a Tonbo-specific manifest module built on the `fusio-manifest` crate under the same workspace (`/Users/xing/Idea/fusio/fusio-manifest`). The module will capture Tonbo’s table metadata, SST versions, and WAL segment lifecycles using fusio-manifest’s serializable key–value transactions, snapshots, and GC APIs. By grounding the manifest on fusio-manifest we align Tonbo’s durability story with the Arrow-first engine and unlock downstream read-path, compaction, and MVCC work.

## Goals

1. Provide an authoritative manifest per table recording active SSTs, WAL retention cutoffs, schema/versioning information, and aggregated stats.
2. Expose transactional APIs (e.g., `apply_version_edits`, `snapshot_latest`, `list_versions`) backed by fusio-manifest sessions so writers/compactors publish atomically.
3. Tie WAL and SST garbage collection to manifest state by persisting referenced WAL segment ranges and computing safe `wal_floor` watermarks.
4. Support fast crash recovery by loading the manifest HEAD and replaying WAL only above the recorded floor, adopting orphan segments via fusio-manifest.
5. Remain runtime- and backend-agnostic by reusing Fusio traits (`DynFs`, `Executor`, `Timer`) exactly as fusio-manifest expects.

## Non-goals

- Deliver full multi-tenant catalog features beyond a single namespace.
- Redesign Tonbo’s WAL frame format; the RFC only integrates WAL metadata with the manifest.

## Background

- Tonbo’s dev branch has Arrow-first mutable/immutable layers and SST writer scaffolding, but `src/lib.rs:9-41` shows no manifest module.
- The WAL writes durable `wal-*.tonwal` segments yet lacks id allocation or reclamation logic beyond rotation (`src/wal/storage.rs`, `src/wal/writer.rs`).
- `docs/overview.md` repeatedly references manifest-driven CAS updates, snapshot visibility, and GC, none of which exist in code.
- `fusio-manifest` implements a backend-agnostic manifest with CAS HEAD objects, append-only segments, checkpoints, snapshot leases, and retention/GC (see `/Users/xing/Idea/fusio/fusio-manifest/src/manifest.rs`, `session.rs`, `compactor.rs` and the RFD in `docs/fusio-manifest-rfd.md`).

## Design Overview

### Key Space

Manifest keys must satisfy fusio-manifest’s `Serialize + PartialOrd + Eq + Hash` requirements. We define:

```
ManifestKey =
  CatalogRoot |
  TableMeta { table_id } |
  TableHead { table_id } |
  TableVersion { table_id, version } |
  WalFloor { table_id } |
  GcPlan { table_id }
```

Keys encode as compact tuples (e.g., `("table", id, "head")`) to keep segment payloads small.

### Values

Values are serde-serializable structs stored as the manifest’s record payloads:

- `CatalogState`: known tables and allocator state.
- `TableMeta`: table name, schema fingerprint, PK layout, retention settings.
- `TableHead`: current version id, schema version, next SST id, `wal_floor`, last manifest txn.
- `VersionState`: version metadata—commit timestamp, per-level vectors of `SstEntry`, referenced `WalSegmentRef { seq, file_id, first_frame, last_frame }`, tombstone watermark, aggregated stats.

Each version commit stores a full `VersionState` under `TableVersion { table_id, version }`. The manifest’s append-only segments serve as the edit log; no separate delta key is required at MVP.

`SstEntry` embeds an `SsTableDescriptor` capturing fields required by every compaction strategy. Levels are derived from the index within `VersionState.ssts`, where `ssts[level]` enumerates the SSTs published at a given level. Each entry records:

- `min_key` / `max_key`: lexicographic primary-key bounds.
- `min_commit_ts` / `max_commit_ts`: earliest and latest MVCC timestamps present.
- `rows`, `bytes`, `tombstones`: sizing metrics collected at flush time.
- `wal_segments`: the WAL fragment identifiers (`wal-*.tonwal`) that produced the SST (mirrors `SsTableDescriptor::wal_ids` in `src/ondisk/sstable.rs`).

Additional strategy-specific metadata can ride in an extensible `extra` map, but these core fields must be present so leveled/tiered compaction and read-path pruning can reason about the file without inspecting Parquet contents.

### Sessions and Workflow

- **Write path:** `Manifest::apply_version_edits` opens a fusio `WriteSession`, loads `TableHead`, reads the current `VersionState`, applies the incoming edits (including WAL segment refs), stages:
  - `put(TableVersion {...}, new_version)`
  - `put(TableHead {...}, updated_head_with_wal_floor)`
  and commits. CAS success defines global order.
- **Read path:** `snapshot_latest` opens a `ReadSession`, fetches `TableHead`, reads the referenced `VersionState`, and returns a snapshot struct containing both the fusio snapshot token and Tonbo version id.
- **Recovery:** On open, call `manifest.recover_orphans()` (fusio-manifest handles adopting contiguous segments). Load `CatalogState` and each `TableHead` to rebuild table descriptors; replay WAL only above the manifest’s `wal_floor`.
- **GC:** `Manifest::compute_gc_plan` wraps fusio-manifest’s compactor GC APIs; plans delete SST objects once versions expire and advance `WalFloor` so WAL segments < floor can be truncated.

### API Surface

The manifest module exposes:

```rust
pub struct Manifest { inner: fusio_manifest::manifest::Manifest<ManifestKey, ManifestValue, ...> }

impl Manifest {
    pub async fn open(stores: Stores, ctx: Arc<ManifestContext<..., ...>>) -> Result<Self>;
    pub async fn apply_version_edits(&self, table: TableId, edits: &[VersionEdit]) -> Result<ManifestTxn>;
    pub async fn snapshot_latest(&self, table: TableId) -> Result<CatalogSnapshot>;
    pub async fn list_versions(&self, table: TableId, limit: usize) -> Result<Vec<VersionState>>;
    pub async fn recover_orphans(&self) -> Result<usize>;
    pub fn compactor(&self) -> TonboCompactor;
}
```

`VersionEdit` is an enum of additive/removal operations (`AddSsts`, `RemoveSsts`, `SetWalSegments`, `SetTombstoneWatermark`). Callers queue edits in the desired order, allowing the manifest writer to derive the next `VersionState` deterministically from the current state.

### Storage Layout

Manifest data follows RFC 0004 (`docs/rfcs/0004-storage-layout.md`): `root/manifest/HEAD.json`, `root/manifest/segments/seg-<seq>.json`, checkpoints, and GC plan docs. fusio-manifest already provides stores compatible with Fusio `DynFs`.

## Initial Integration Step

The first task for the development team is to wire Tonbo’s existing SST flush and WAL layers into the manifest so WAL segments gain explicit lifecycle management.

1. **Capture WAL segment refs during flush.**
   - `DB::flush_immutables_with_descriptor` already gathers `wal_ids` via `SsTableDescriptor::with_wal_ids` (`src/ondisk/sstable.rs:216-242`). Convert those into `WalSegmentRef { seq, file_id, first_frame, last_frame }`.
   - Add a temporary helper (`wal::manifest_ext`) to fetch the active segment’s first/last frame numbers when sealing.
2. **Call `Manifest::apply_version_edits`.**
   - After successful Parquet write, assemble the appropriate `VersionEdit` list (e.g., add new SSTs, set WAL refs, update tombstones).
   - Update `TableHead.wal_floor` to the minimum WAL sequence still referenced by retained versions.
3. **Compute and persist `wal_floor`.**
   - For MVP, `wal_floor = last_retained_wal_ref.seq`. Later iterations will consider retention and multiple versions.
4. **Teach WAL GC to respect the floor.**
   - Expose `Manifest::wal_floor(table_id)` so WAL cleanup can remove segments with `seq < wal_floor`.
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
