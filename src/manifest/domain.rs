use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    time::Duration,
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
pub(crate) struct TableId(Ulid);

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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum GcPlanKey {
    /// Stored garbage-collection plan for a table.
    Table {
        /// Table identifier.
        table_id: TableId,
    },
}

/// Values tracked by the GC-plan manifest instance.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub(crate) enum GcPlanValue {
    /// Table garbage-collection plan payload.
    Plan(GcPlanState),
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
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct CatalogState {
    /// Mapping of table identifiers to their catalog entries.
    pub tables: BTreeMap<TableId, TableCatalogEntry>,
    /// Monotonic counter used to derive human-friendly table ordinals.
    pub next_table_ordinal: u64,
}

/// Entry describing a table inside the catalog root.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableCatalogEntry {
    /// Logical representation of the table.
    pub logical_table_name: String,
}

/// Static metadata for a Tonbo table.
#[allow(dead_code)]
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
    /// Optional retention policy overrides.
    pub retention: Option<TableRetionConfig>,
    /// Monotonic schema version number.
    pub schema_version: u32,
}

/// Retention policy knobs for a table.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableRetionConfig {
    /// Maximum number of committed versions to retain.
    pub max_versions: u64,
    /// Maximum age to retain versions and their WAL segments.
    pub max_ttl: Duration,
}

impl Default for TableRetionConfig {
    fn default() -> Self {
        Self {
            max_versions: 5,
            max_ttl: Duration::from_secs(12 * 60 * 60),
        }
    }
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
pub(crate) struct VersionState {
    /// Table identifier.
    pub(super) table_id: TableId,
    /// Commit timestamp in milliseconds since the Unix epoch.
    pub(super) commit_timestamp: Timestamp,
    /// SST entries materialised for this version, grouped by compaction level.
    pub(super) ssts: Vec<Vec<SstEntry>>,
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

    pub(super) fn apply_edits(&mut self, edits: &[VersionEdit]) -> ManifestResult<()> {
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

    #[cfg(test)]
    pub(crate) fn table_id(&self) -> &TableId {
        &self.table_id
    }

    #[cfg(test)]
    pub(crate) fn ssts(&self) -> &[Vec<SstEntry>] {
        &self.ssts
    }

    #[cfg(test)]
    pub(crate) fn wal_segments(&self) -> &[WalSegmentRef] {
        &self.wal_segments
    }

    #[cfg(test)]
    pub(crate) fn wal_floor(&self) -> Option<&WalSegmentRef> {
        self.wal_floor.as_ref()
    }

    pub(crate) fn cloned_wal_floor(&self) -> Option<WalSegmentRef> {
        self.wal_floor.clone()
    }

    #[cfg(test)]
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
            // Preserve the previously persisted floor when no WAL lineage change was supplied.
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
    #[serde(with = "path_serde")]
    mvcc_path: Path,
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
        mvcc_path: Path,
    ) -> Self {
        Self {
            data_path,
            mvcc_path,
            sst_id,
            stats,
            wal_segments,
        }
    }

    pub(crate) fn sst_id(&self) -> &SsTableId {
        &self.sst_id
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> Option<&SsTableStats> {
        self.stats.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn wal_segments(&self) -> Option<&[FileId]> {
        self.wal_segments.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn data_path(&self) -> &Path {
        &self.data_path
    }

    #[cfg(test)]
    pub(crate) fn mvcc_path(&self) -> &Path {
        &self.mvcc_path
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn first_frame(&self) -> u64 {
        self.first_frame
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn last_frame(&self) -> u64 {
        self.last_frame
    }
}

/// Garbage-collection plan for a table.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct GcPlanState {
    /// SST identifiers that can be removed.
    obsolete_sst_ids: Vec<SsTableId>,
    /// WAL sequence numbers that can be truncated.
    obsolete_wal_segments: Vec<WalSegmentRef>,
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
