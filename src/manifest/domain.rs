use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashSet},
};

use fusio::path::Path;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    id::{FileId, FileIdGenerator},
    manifest::{ManifestError, ManifestResult, VersionEdit},
    mvcc::Timestamp,
    ondisk::sstable::{SsTableId, SsTableStats},
};

/// Identifier associated with a physical Tonbo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(Ulid);

impl TableId {
    /// Create a new identifier using the provided file-id allocator.
    pub(crate) fn new(generator: &FileIdGenerator) -> Self {
        Self(generator.generate())
    }
}

impl From<Ulid> for TableId {
    fn from(value: Ulid) -> Self {
        Self(value)
    }
}

impl From<TableId> for Ulid {
    fn from(value: TableId) -> Self {
        value.0
    }
}

/// Keys stored by the catalog manifest instance.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum CatalogKey {
    /// Global catalog state, including allocator metadata.
    Root,
    /// Static metadata describing a table.
    TableMeta {
        /// Identifier of the table described by this entry.
        table_id: TableId,
    },
}

/// Values stored by the catalog manifest instance.

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub(crate) enum CatalogValue {
    /// Catalog state payload.
    Catalog(CatalogState),
    /// Table metadata payload.
    TableMeta(TableMeta),
}

/// Keys tracked by the version manifest instance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum VersionKey {
    /// Mutable head pointer for a table containing the latest version info.
    TableHead {
        /// Identifier of the table whose head is being stored.
        table_id: TableId,
    },
    /// Immutable state for a specific table version keyed by manifest timestamp.
    TableVersion {
        /// Table identifier.
        table_id: TableId,
        /// Manifest transaction timestamp associated with this entry.
        manifest_ts: Timestamp,
    },
    /// Lowest WAL sequence that must be retained.
    WalFloor {
        /// Table identifier.
        table_id: TableId,
    },
}

/// Values tracked by the version manifest instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub(crate) enum VersionValue {
    /// Table head payload.
    TableHead(TableHead),
    /// Immutable version payload.
    TableVersion(VersionState),
    /// WAL retention floor payload.
    WalFloor(WalSegmentRef),
}

/// Keys tracked by the GC-plan manifest instance.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum GcPlanKey {
    /// Stored garbage-collection plan for a table.
    Table {
        /// Table identifier.
        table_id: TableId,
    },
}

/// Values tracked by the GC-plan manifest instance.

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub(crate) enum GcPlanValue {
    /// Table garbage-collection plan payload.
    Plan(GcPlanState),
}

/// Why a committed manifest version is still protected from SST reclamation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtectedVersionKind {
    /// The version currently referenced by the table head.
    Head,
    /// A live in-process snapshot depends on this historical manifest version.
    ActiveSnapshotPin,
}

/// Manifest version that currently protects one or more SST objects from GC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProtectedVersionRef {
    manifest_ts: Timestamp,
    kind: ProtectedVersionKind,
}

impl ProtectedVersionRef {
    pub(crate) fn new(manifest_ts: Timestamp, kind: ProtectedVersionKind) -> Self {
        Self { manifest_ts, kind }
    }

    pub(crate) fn manifest_ts(&self) -> Timestamp {
        self.manifest_ts
    }

    pub(crate) fn kind(&self) -> ProtectedVersionKind {
        self.kind
    }
}

/// Kind of physical SST object protected by the current manifest root set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtectedObjectKind {
    /// Primary SST Parquet file.
    Data,
    /// Optional delete-sidecar Parquet file.
    DeleteSidecar,
}

/// Physical SST object path protected from GC by one or more manifest versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProtectedSstObject {
    path: Path,
    sst_id: SsTableId,
    level: u32,
    kind: ProtectedObjectKind,
}

impl ProtectedSstObject {
    pub(crate) fn new(
        path: Path,
        sst_id: SsTableId,
        level: u32,
        kind: ProtectedObjectKind,
    ) -> Self {
        Self {
            path,
            sst_id,
            level,
            kind,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[allow(dead_code)]
    pub(crate) fn sst_id(&self) -> &SsTableId {
        &self.sst_id
    }

    #[allow(dead_code)]
    pub(crate) fn level(&self) -> u32 {
        self.level
    }

    #[allow(dead_code)]
    pub(crate) fn kind(&self) -> ProtectedObjectKind {
        self.kind
    }
}

/// Deterministic current root set for SST GC decisions.
///
/// In the current single-process model the protected set is the union of the table HEAD and any
/// manifest versions pinned by live in-process snapshots. Physical SST deletion must treat any
/// object referenced by this set as live.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CurrentRootSet {
    protected_versions: Vec<ProtectedVersionRef>,
    protected_objects: Vec<ProtectedSstObject>,
    active_snapshot_version_count: usize,
}

impl CurrentRootSet {
    pub(crate) fn new(
        protected_versions: Vec<ProtectedVersionRef>,
        protected_objects: Vec<ProtectedSstObject>,
        active_snapshot_version_count: usize,
    ) -> Self {
        Self {
            protected_versions,
            protected_objects,
            active_snapshot_version_count,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn protected_versions(&self) -> &[ProtectedVersionRef] {
        &self.protected_versions
    }

    #[allow(dead_code)]
    pub(crate) fn protected_objects(&self) -> &[ProtectedSstObject] {
        &self.protected_objects
    }

    pub(crate) fn protected_version_count(&self) -> usize {
        self.protected_versions.len()
    }

    pub(crate) fn protected_object_count(&self) -> usize {
        self.protected_objects.len()
    }

    pub(crate) fn active_snapshot_version_count(&self) -> usize {
        self.active_snapshot_version_count
    }

    pub(crate) fn contains_path(&self, path: &Path) -> bool {
        self.protected_objects
            .binary_search_by(|protected| protected.path().as_ref().cmp(path.as_ref()))
            .is_ok()
    }

    pub(crate) fn from_versions(
        head_manifest_ts: Option<Timestamp>,
        active_pins: &[Timestamp],
        versions: &[VersionState],
    ) -> Self {
        let active_pin_set = active_pins.iter().copied().collect::<BTreeSet<_>>();
        let mut protected_versions = Vec::with_capacity(
            active_pin_set
                .len()
                .saturating_add(usize::from(head_manifest_ts.is_some())),
        );
        let mut seen_paths = BTreeSet::new();
        let mut protected_objects = Vec::new();

        for version in versions {
            let manifest_ts = version.commit_timestamp();
            let is_head = Some(manifest_ts) == head_manifest_ts;
            let is_active_pin = active_pin_set.contains(&manifest_ts);
            if !is_head && !is_active_pin {
                continue;
            }
            let kind = if is_head {
                ProtectedVersionKind::Head
            } else {
                ProtectedVersionKind::ActiveSnapshotPin
            };
            protected_versions.push(ProtectedVersionRef::new(manifest_ts, kind));

            for (level, bucket) in version.ssts().iter().enumerate() {
                let level = u32::try_from(level).unwrap_or(u32::MAX);
                for entry in bucket {
                    let data_path_key = entry.data_path().as_ref().to_owned();
                    if seen_paths.insert(data_path_key) {
                        protected_objects.push(ProtectedSstObject::new(
                            entry.data_path().clone(),
                            entry.sst_id().clone(),
                            level,
                            ProtectedObjectKind::Data,
                        ));
                    }
                    if let Some(delete_path) = entry.delete_path() {
                        let delete_path_key = delete_path.as_ref().to_owned();
                        if seen_paths.insert(delete_path_key) {
                            protected_objects.push(ProtectedSstObject::new(
                                delete_path.clone(),
                                entry.sst_id().clone(),
                                level,
                                ProtectedObjectKind::DeleteSidecar,
                            ));
                        }
                    }
                }
            }
        }

        protected_versions.sort_by_key(|version| {
            (
                match version.kind() {
                    ProtectedVersionKind::Head => 0u8,
                    ProtectedVersionKind::ActiveSnapshotPin => 1u8,
                },
                std::cmp::Reverse(version.manifest_ts()),
            )
        });
        protected_objects.sort_by(|lhs, rhs| lhs.path().as_ref().cmp(rhs.path().as_ref()));

        Self::new(protected_versions, protected_objects, active_pin_set.len())
    }
}

impl TryFrom<CatalogValue> for CatalogState {
    type Error = ManifestError;

    fn try_from(value: CatalogValue) -> Result<Self, Self::Error> {
        match value {
            CatalogValue::Catalog(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<CatalogValue> for TableMeta {
    type Error = ManifestError;

    fn try_from(value: CatalogValue) -> Result<Self, Self::Error> {
        match value {
            CatalogValue::TableMeta(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<VersionValue> for TableHead {
    type Error = ManifestError;

    fn try_from(value: VersionValue) -> Result<Self, Self::Error> {
        match value {
            VersionValue::TableHead(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<VersionValue> for VersionState {
    type Error = ManifestError;

    fn try_from(value: VersionValue) -> Result<Self, Self::Error> {
        match value {
            VersionValue::TableVersion(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<VersionValue> for WalSegmentRef {
    type Error = ManifestError;

    fn try_from(value: VersionValue) -> Result<Self, Self::Error> {
        match value {
            VersionValue::WalFloor(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<GcPlanValue> for GcPlanState {
    type Error = ManifestError;

    fn try_from(value: GcPlanValue) -> Result<Self, Self::Error> {
        match value {
            GcPlanValue::Plan(payload) => Ok(payload),
        }
    }
}

/// Snapshot of catalog-level metadata stored in the manifest.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct CatalogState {
    /// Mapping of table identifiers to their catalog entries.
    pub tables: BTreeMap<TableId, TableCatalogEntry>,
    /// Monotonic counter used to derive human-friendly table ordinals.
    pub next_table_ordinal: u64,
}

/// Entry describing a table inside the catalog root.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableCatalogEntry {
    /// Logical representation of the table.
    pub logical_table_name: String,
}

/// Static metadata for a Tonbo table.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMeta {
    /// Identifier of the table.
    pub table_id: TableId,
    /// Human-readable name of the table.
    pub name: String,
    /// Schema fingerprint used for compatibility validation.
    pub schema_fingerprint: String,
    /// Columns participating in the primary key.
    pub primary_key_columns: Vec<String>,
    /// Monotonic schema version number.
    pub schema_version: u32,
}

/// Input payload used when registering a table in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDefinition {
    /// Human-readable name of the table.
    pub name: String,
    /// Schema fingerprint used for compatibility validation.
    pub schema_fingerprint: String,
    /// Columns participating in the primary key.
    pub primary_key_columns: Vec<String>,
    /// Monotonic schema version number.
    pub schema_version: u32,
}

/// Mutable head view for a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableHead {
    /// Identifier of the table.
    pub table_id: TableId,
    /// Current schema version.
    pub schema_version: u32,
    /// Lowest WAL sequence that must be retained.
    pub wal_floor: Option<WalSegmentRef>,
    /// Last manifest transaction identifier applied to the head.
    pub last_manifest_txn: Option<Timestamp>,
}

/// Immutable version payload describing a committed state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionState {
    /// Table identifier.
    pub(super) table_id: TableId,
    /// Commit timestamp when this version was created.
    pub(crate) commit_timestamp: Timestamp,
    /// SST entries materialised for this version, grouped by compaction level.
    pub(crate) ssts: Vec<Vec<SstEntry>>,
    /// WAL segments referenced by this version.
    #[serde(default)]
    pub(super) wal_segments: Vec<WalSegmentRef>,
    /// Floor WAL fragment referenced by this version (derived from `wal_segments`).
    pub(super) wal_floor: Option<WalSegmentRef>,
    /// Upper bound for tombstones included in this version.
    pub(super) tombstone_watermark: Option<u64>,
}

impl VersionState {
    pub(crate) fn empty(table_id: TableId) -> Self {
        Self {
            table_id,
            commit_timestamp: Timestamp::new(0),
            ssts: Vec::new(),
            wal_segments: Vec::new(),
            wal_floor: None,
            tombstone_watermark: None,
        }
    }

    pub(crate) fn apply_edits(&mut self, edits: &[VersionEdit]) -> ManifestResult<()> {
        for edit in edits {
            match edit {
                VersionEdit::AddSsts { level, entries } => self.add_ssts(*level, entries)?,
                VersionEdit::RemoveSsts { level, sst_ids } => self.remove_ssts(*level, sst_ids)?,
                VersionEdit::SetWalSegments { segments } => self.set_wal_segments(segments),
                VersionEdit::SetTombstoneWatermark { watermark } => {
                    self.tombstone_watermark = Some(*watermark);
                }
            }
        }

        for bucket in &mut self.ssts {
            bucket.sort_by_key(|entry| entry.sst_id.raw());
        }
        self.trim_trailing_empty_levels();
        self.normalise_wal_segments();

        Ok(())
    }

    fn add_ssts(&mut self, level: u32, entries: &[SstEntry]) -> ManifestResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let bucket = self.ensure_level(level);
        for entry in entries {
            bucket.retain(|existing| existing.sst_id() != entry.sst_id());
            bucket.push(entry.clone());
        }
        Ok(())
    }

    fn remove_ssts(&mut self, level: u32, sst_ids: &[SsTableId]) -> ManifestResult<()> {
        if sst_ids.is_empty() {
            return Ok(());
        }
        let index = level as usize;
        if index >= self.ssts.len() {
            return Err(ManifestError::Invariant(
                "attempted to remove SSTs from a missing level",
            ));
        }
        let bucket = &mut self.ssts[index];
        let targets: HashSet<SsTableId> = sst_ids.iter().cloned().collect();
        bucket.retain(|entry| !targets.contains(entry.sst_id()));
        Ok(())
    }

    fn ensure_level(&mut self, level: u32) -> &mut Vec<SstEntry> {
        let index = level as usize;
        if self.ssts.len() <= index {
            self.ssts.resize_with(index + 1, Vec::new);
        }
        &mut self.ssts[index]
    }

    fn trim_trailing_empty_levels(&mut self) {
        while self.ssts.last().is_some_and(|entries| entries.is_empty()) {
            self.ssts.pop();
        }
    }
    pub(crate) fn commit_timestamp(&self) -> Timestamp {
        self.commit_timestamp
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn table_id(&self) -> &TableId {
        &self.table_id
    }

    pub(crate) fn ssts(&self) -> &[Vec<SstEntry>] {
        &self.ssts
    }

    pub(crate) fn wal_segments(&self) -> &[WalSegmentRef] {
        &self.wal_segments
    }

    #[allow(dead_code)]
    pub(crate) fn contains_sst(&self, sst_id: &SsTableId) -> bool {
        self.ssts
            .iter()
            .any(|bucket| bucket.iter().any(|entry| entry.sst_id() == sst_id))
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn wal_floor(&self) -> Option<&WalSegmentRef> {
        self.wal_floor.as_ref()
    }

    pub(crate) fn cloned_wal_floor(&self) -> Option<WalSegmentRef> {
        self.wal_floor.clone()
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn tombstone_watermark(&self) -> Option<u64> {
        self.tombstone_watermark
    }

    pub(crate) fn set_commit_timestamp(&mut self, ts: Timestamp) {
        self.commit_timestamp = ts;
    }

    fn set_wal_segments(&mut self, segments: &[WalSegmentRef]) {
        self.wal_segments = segments.to_vec();
    }

    fn normalise_wal_segments(&mut self) {
        if self.wal_segments.is_empty() {
            // No WAL fragments were supplied, so clear the cached floor.
            self.wal_floor = None;
            return;
        }

        self.wal_segments.sort_by(WalSegmentRef::cmp);
        self.wal_floor = self.wal_segments.first().cloned();
    }
}

/// Descriptor of an SST entry committed into a version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SstEntry {
    #[serde(with = "path_serde")]
    data_path: Path,
    #[serde(with = "path_serde_option")]
    delete_path: Option<Path>,
    sst_id: SsTableId,
    stats: Option<SsTableStats>,
    wal_segments: Option<Vec<FileId>>,
}

impl PartialEq for SstEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sst_id == other.sst_id
    }
}

impl SstEntry {
    pub(crate) fn new(
        sst_id: SsTableId,
        stats: Option<SsTableStats>,
        wal_segments: Option<Vec<FileId>>,
        data_path: Path,
        delete_path: Option<Path>,
    ) -> Self {
        Self {
            data_path,
            delete_path,
            sst_id,
            stats,
            wal_segments,
        }
    }

    pub(crate) fn sst_id(&self) -> &SsTableId {
        &self.sst_id
    }

    pub(crate) fn stats(&self) -> Option<&SsTableStats> {
        self.stats.as_ref()
    }

    pub(crate) fn wal_segments(&self) -> Option<&[FileId]> {
        self.wal_segments.as_deref()
    }

    pub(crate) fn data_path(&self) -> &Path {
        &self.data_path
    }

    pub(crate) fn delete_path(&self) -> Option<&Path> {
        self.delete_path.as_ref()
    }
}

/// Reference to WAL fragments that produced a version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WalSegmentRef {
    /// Monotonic WAL sequence number.
    seq: u64,
    /// Identifier of the WAL file.
    file_id: FileId,
    /// First frame index (inclusive).
    first_frame: u64,
    /// Last frame index (inclusive).
    last_frame: u64,
}

impl WalSegmentRef {
    pub(crate) fn new(seq: u64, file_id: FileId, first_frame: u64, last_frame: u64) -> Self {
        Self {
            seq,
            file_id,
            first_frame,
            last_frame,
        }
    }

    pub(crate) fn cmp(lhs: &Self, rhs: &Self) -> Ordering {
        lhs.seq
            .cmp(&rhs.seq)
            .then_with(|| lhs.first_frame.cmp(&rhs.first_frame))
            .then_with(|| lhs.last_frame.cmp(&rhs.last_frame))
            .then_with(|| lhs.file_id.cmp(&rhs.file_id))
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn file_id(&self) -> &FileId {
        &self.file_id
    }

    pub(crate) fn first_frame(&self) -> u64 {
        self.first_frame
    }

    pub(crate) fn last_frame(&self) -> u64 {
        self.last_frame
    }
}

/// Garbage-collection candidates for a table.
///
/// This state is a staging queue for a future sweeper, not the correctness authority
/// for SST deletion. Physical SST reclamation must re-check the current manifest root
/// set and may only delete candidates that are unreachable from that root set.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct GcPlanState {
    /// SST identifiers and paths proposed for removal.
    pub obsolete_ssts: Vec<GcSstRef>,
    /// WAL sequence numbers that can be truncated.
    pub obsolete_wal_segments: Vec<WalSegmentRef>,
}

/// Summary of how a staged GC plan relates to the current manifest root set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GcAuthorizationSummary {
    /// Total staged SST candidates currently recorded in the GC plan.
    pub staged_sst_candidates: u64,
    /// SST candidates that are currently unreachable and therefore safe to sweep.
    pub authorized_sst_candidates: u64,
    /// SST candidates still protected by the current root set.
    pub blocked_sst_candidates: u64,
    /// WAL candidates staged alongside the SST plan.
    pub obsolete_wal_segments: u64,
    /// Manifest versions currently contributing to the protection root set.
    pub protected_versions: u64,
    /// Unique manifest versions currently pinned by live in-process snapshots.
    pub active_snapshot_versions: u64,
    /// Physical SST objects currently protected by the manifest root set.
    pub protected_sst_objects: u64,
}

impl GcPlanState {
    /// Merge additional staged GC candidates into this plan without duplicating entries.
    pub(crate) fn merge(&mut self, other: Self) {
        self.obsolete_ssts.extend(other.obsolete_ssts);
        self.obsolete_wal_segments
            .extend(other.obsolete_wal_segments);
        self.normalize_in_place();
    }

    /// Remove SST candidates that were already reclaimed from the staged plan.
    pub(crate) fn remove_sst_candidates(&mut self, reclaimed: &[GcSstRef]) {
        if reclaimed.is_empty() {
            return;
        }

        let reclaimed_keys = reclaimed
            .iter()
            .map(GcSstRef::dedupe_key)
            .collect::<BTreeSet<_>>();
        self.obsolete_ssts
            .retain(|candidate| !reclaimed_keys.contains(&candidate.dedupe_key()));
        self.normalize_in_place();
    }

    /// Authorize the staged SST candidate set against the current manifest root set.
    ///
    /// The returned plan keeps only candidates that are currently unreachable and therefore still
    /// eligible for a future sweeper.
    #[allow(dead_code)]
    pub(crate) fn authorize_with_root_set(mut self, root_set: &CurrentRootSet) -> Self {
        self.obsolete_ssts.retain(|candidate| {
            !root_set.contains_path(&candidate.data_path)
                && candidate
                    .delete_path
                    .as_ref()
                    .is_none_or(|path| !root_set.contains_path(path))
        });
        self
    }

    /// Compute authorization counts for the staged plan against the current root set.
    pub(crate) fn authorization_summary(
        &self,
        root_set: &CurrentRootSet,
    ) -> GcAuthorizationSummary {
        let authorized_sst_candidates = self
            .obsolete_ssts
            .iter()
            .filter(|candidate| {
                !root_set.contains_path(&candidate.data_path)
                    && candidate
                        .delete_path
                        .as_ref()
                        .is_none_or(|path| !root_set.contains_path(path))
            })
            .count() as u64;
        let staged_sst_candidates = self.obsolete_ssts.len() as u64;

        GcAuthorizationSummary {
            staged_sst_candidates,
            authorized_sst_candidates,
            blocked_sst_candidates: staged_sst_candidates.saturating_sub(authorized_sst_candidates),
            obsolete_wal_segments: self.obsolete_wal_segments.len() as u64,
            protected_versions: root_set.protected_version_count() as u64,
            active_snapshot_versions: root_set.active_snapshot_version_count() as u64,
            protected_sst_objects: root_set.protected_object_count() as u64,
        }
    }

    /// Split staged SST candidates into authorized and still-blocked subsets.
    pub(crate) fn split_sst_candidates(
        self,
        root_set: &CurrentRootSet,
    ) -> (Vec<GcSstRef>, Vec<GcSstRef>) {
        let mut authorized = Vec::new();
        let mut blocked = Vec::new();
        for candidate in self.obsolete_ssts {
            if !root_set.contains_path(&candidate.data_path)
                && candidate
                    .delete_path
                    .as_ref()
                    .is_none_or(|path| !root_set.contains_path(path))
            {
                authorized.push(candidate);
            } else {
                blocked.push(candidate);
            }
        }
        (authorized, blocked)
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.obsolete_ssts.is_empty() && self.obsolete_wal_segments.is_empty()
    }

    fn normalize_in_place(&mut self) {
        self.obsolete_ssts
            .sort_by(|lhs, rhs| lhs.cmp_for_dedupe(rhs));
        self.obsolete_ssts
            .dedup_by(|lhs, rhs| lhs.dedupe_key() == rhs.dedupe_key());

        self.obsolete_wal_segments.sort_by(WalSegmentRef::cmp);
        self.obsolete_wal_segments
            .dedup_by(|lhs, rhs| WalSegmentRef::cmp(lhs, rhs).is_eq());
    }
}

/// Reference to an SST candidate staged for future deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GcSstRef {
    /// Identifier of the SST.
    pub id: SsTableId,
    /// Level where the SST resided.
    pub level: u32,
    /// Data path for the SST.
    #[serde(with = "path_serde")]
    pub data_path: Path,
    /// Optional delete sidecar path for the SST.
    #[serde(with = "path_serde_option")]
    pub delete_path: Option<Path>,
}

impl GcSstRef {
    fn cmp_for_dedupe(&self, other: &Self) -> Ordering {
        self.data_path
            .as_ref()
            .cmp(other.data_path.as_ref())
            .then_with(|| {
                self.delete_path
                    .as_ref()
                    .map(Path::as_ref)
                    .cmp(&other.delete_path.as_ref().map(Path::as_ref))
            })
            .then_with(|| self.level.cmp(&other.level))
            .then_with(|| self.id.raw().cmp(&other.id.raw()))
    }

    fn dedupe_key(&self) -> (String, Option<String>, u32, u64) {
        (
            self.data_path.as_ref().to_owned(),
            self.delete_path
                .as_ref()
                .map(|path| path.as_ref().to_owned()),
            self.level,
            self.id.raw(),
        )
    }
}

mod path_serde {
    use serde::{Deserializer, Serializer};

    use super::*;

    pub fn serialize<S>(value: &Path, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(value.as_ref())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Path, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Path::parse(raw).map_err(serde::de::Error::custom)
    }
}

mod path_serde_option {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::*;

    pub fn serialize<S>(value: &Option<Path>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(path) => serializer.serialize_some(path.as_ref()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Path>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Option::<String>::deserialize(deserializer)?;
        match raw {
            Some(value) => Path::parse(value)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}
