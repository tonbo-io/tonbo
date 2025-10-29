//! Manifest domain types and Tonbo-specific wrapper around `fusio-manifest`.
//!
//! This module provides the strongly typed key/value pairs Tonbo stores inside
//! `fusio-manifest` together with a convenience wrapper that wires the
//! underlying stores together and exposes higher-level APIs such as
//! `apply_version_edits` and `snapshot_latest`.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use fusio_manifest::{
    BlockingExecutor, CheckpointStore, HeadStore, LeaseStore, SegmentIo,
    manifest::Manifest as FusioManifest,
    retention::DefaultRetention,
    snapshot::{ScanRange, Snapshot},
    types::Error as FusioManifestError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    fs::{FileId, generate_file_id},
    mvcc::Timestamp,
    ondisk::sstable::{SsTableId, SsTableStats},
};

mod version;
pub use version::VersionEdit;

/// Error type surfaced by Tonbo's manifest layer.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// Error originating from the underlying `fusio-manifest` crate.
    #[error(transparent)]
    Backend(#[from] FusioManifestError),
    /// Tonbo-specific invariant violation detected while manipulating manifest records.
    #[error("invariant violation: {0}")]
    Invariant(&'static str),
}

/// Convenience result alias for manifest operations.
pub type ManifestResult<T> = Result<T, ManifestError>;

/// Identifier associated with a physical Tonbo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(FileId);

impl TableId {
    /// Create a new identifier using a freshly generated file identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(generate_file_id())
    }

    /// Wrap an existing file identifier.
    #[must_use]
    pub fn from_file_id(value: FileId) -> Self {
        Self(value)
    }

    /// Access the underlying file identifier.
    #[must_use]
    pub fn as_file_id(self) -> FileId {
        self.0
    }
}

impl Default for TableId {
    fn default() -> Self {
        Self::new()
    }
}

/// Identifier associated with a manifest version of a table.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct VersionId(u64);

impl VersionId {
    /// Construct a version identifier from a raw integer.
    #[must_use]
    pub const fn from(value: u64) -> Self {
        Self(value)
    }

    /// Access the raw version value.
    #[must_use]
    pub const fn inner(self) -> u64 {
        self.0
    }

    /// Return the next `VersionId`, incremented by exactly one.
    ///
    /// # Panics
    /// Panics if incrementing would overflow `u64`.
    pub fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("VersionId overflow"))
    }
}

/// Logical manifest keys tracked inside `fusio-manifest`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ManifestKey {
    /// Global catalog state, including allocator metadata.
    CatalogRoot,
    /// Static metadata describing a table.
    TableMeta {
        /// Identifier of the table described by this entry.
        table_id: TableId,
    },
    /// Mutable head pointer for a table containing the latest version info.
    TableHead {
        /// Identifier of the table whose head is being stored.
        table_id: TableId,
    },
    /// Immutable state for a specific table version.
    TableVersion {
        /// Table identifier.
        table_id: TableId,
        /// Version identifier associated with this entry.
        version: VersionId,
    },
    /// Lowest WAL sequence that must be retained.
    WalFloor {
        /// Table identifier.
        table_id: TableId,
    },
    /// Stored garbage-collection plan for a table.
    GcPlan {
        /// Table identifier.
        table_id: TableId,
    },
}

/// Serde-serializable manifest value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub enum ManifestValue {
    /// Catalog state payload.
    Catalog(CatalogState),
    /// Table metadata payload.
    TableMeta(TableMeta),
    /// Table head payload.
    TableHead(TableHead),
    /// Immutable version payload.
    TableVersion(VersionState),
    /// WAL retention floor payload.
    WalFloor(WalSegmentRef),
    /// Table garbage-collection plan payload.
    GcPlan(GcPlanState),
}

// TryFrom implementations for ManifestValue to extract inner payloads
impl TryFrom<ManifestValue> for CatalogState {
    type Error = ManifestError;
    fn try_from(value: ManifestValue) -> Result<Self, Self::Error> {
        match value {
            ManifestValue::Catalog(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<ManifestValue> for TableMeta {
    type Error = ManifestError;
    fn try_from(value: ManifestValue) -> Result<Self, Self::Error> {
        match value {
            ManifestValue::TableMeta(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<ManifestValue> for TableHead {
    type Error = ManifestError;
    fn try_from(value: ManifestValue) -> Result<Self, Self::Error> {
        match value {
            ManifestValue::TableHead(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<ManifestValue> for VersionState {
    type Error = ManifestError;
    fn try_from(value: ManifestValue) -> Result<Self, Self::Error> {
        match value {
            ManifestValue::TableVersion(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<ManifestValue> for WalSegmentRef {
    type Error = ManifestError;
    fn try_from(value: ManifestValue) -> Result<Self, Self::Error> {
        match value {
            ManifestValue::WalFloor(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

impl TryFrom<ManifestValue> for GcPlanState {
    type Error = ManifestError;
    fn try_from(value: ManifestValue) -> Result<Self, Self::Error> {
        match value {
            ManifestValue::GcPlan(payload) => Ok(payload),
            _ => Err(ManifestError::Invariant("manifest value type mismatch")),
        }
    }
}

/// Snapshot of catalog-level metadata stored in the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CatalogState {
    /// Mapping of table identifiers to their catalog entries.
    pub tables: BTreeMap<TableId, TableCatalogEntry>,
    /// Monotonic counter used to derive human-friendly table ordinals.
    pub next_table_ordinal: u64,
}

/// Entry describing a table inside the catalog root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCatalogEntry {
    /// Logical representation of the table.
    pub logical_table_name: String,
}

impl TableCatalogEntry {
    /// Build a new entry from the provided logical table name.
    #[must_use]
    pub fn new(logical_table_name: impl Into<String>) -> Self {
        Self {
            logical_table_name: logical_table_name.into(),
        }
    }
}

/// Static metadata for a Tonbo table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMeta {
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRetionConfig {
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
pub struct TableHead {
    /// Identifier of the table.
    pub table_id: TableId,
    /// Current committed version, if any.
    pub current_version: Option<VersionId>,
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
    pub table_id: TableId,
    /// Version identifier.
    pub version_id: VersionId,
    /// Commit timestamp in milliseconds since the Unix epoch.
    pub commit_timestamp: Timestamp,
    /// SST entries materialised for this version, grouped by compaction level.
    pub ssts: Vec<Vec<SstEntry>>,
    /// Floor WAL fragment referenced by this version.
    pub wal_floor: Option<WalSegmentRef>,
    /// Upper bound for tombstones included in this version.
    pub tombstone_watermark: Option<u64>,
    // TODO: Add aggregated statistics for all SSTs
}

impl VersionState {
    fn empty(table_id: TableId) -> Self {
        Self {
            table_id,
            version_id: VersionId::from(0),
            commit_timestamp: Timestamp::new(0),
            ssts: Vec::new(),
            wal_floor: None,
            tombstone_watermark: None,
        }
    }
}

/// Descriptor of an SST entry committed into a version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstEntry {
    /// SST identifier
    pub sst_id: SsTableId,
    /// Mandatory statistics for the SST.
    pub stats: Option<SsTableStats>,
    /// Mandatory WAL segments composed to build the SST.
    pub wal_segments: Option<Vec<FileId>>,
}

impl PartialEq for SstEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sst_id == other.sst_id
    }
}

/// Reference to WAL fragments that produced a version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentRef {
    /// Monotonic WAL sequence number.
    pub seq: u64,
    /// Identifier of the WAL file.
    pub file_id: FileId,
    /// First frame index (inclusive).
    pub first_frame: u64,
    /// Last frame index (inclusive).
    pub last_frame: u64,
}

/// Garbage-collection plan for a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GcPlanState {
    /// SST identifiers that can be removed.
    pub obsolete_sst_ids: Vec<SsTableId>,
    /// WAL sequence numbers that can be truncated.
    pub obsolete_wal_segments: Vec<WalSegmentRef>,
}

/// Result of loading the latest state for a table.
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    /// Underlying fusio snapshot guarding read leases.
    pub manifest_snapshot: Snapshot,
    /// Current table head
    pub head: TableHead,
    /// Most recent committed version for the table, if any.
    pub latest_version: Option<VersionState>,
}

/// Bundle of storage backends required by the manifest.
#[derive(Debug)]
pub struct Stores<HS, SS, CS, LS> {
    /// Store used for the manifest head CAS object.
    pub head: HS,
    /// Store used for manifest segments.
    pub segment: SS,
    /// Store used for manifest checkpoints.
    pub checkpoint: CS,
    /// Store used for snapshot leases.
    pub lease: LS,
}

impl<HS, SS, CS, LS> Stores<HS, SS, CS, LS> {
    /// Construct a new bundle from the provided stores.
    #[must_use]
    pub fn new(head: HS, segment: SS, checkpoint: CS, lease: LS) -> Self {
        Self {
            head,
            segment,
            checkpoint,
            lease,
        }
    }
}

/// Manifest wrapper that stores Tonbo-specific key/value pairs and exposes higher-level APIs.
pub struct Manifest<HS, SS, CS, LS>
where
    HS: HeadStore + Send + Sync + 'static,
    SS: SegmentIo + Send + Sync + 'static,
    CS: CheckpointStore + Send + Sync + 'static,
    LS: LeaseStore + Send + Sync + 'static,
{
    inner: FusioManifest<
        ManifestKey,
        ManifestValue,
        HS,
        SS,
        CS,
        LS,
        BlockingExecutor,
        DefaultRetention,
    >,
}

impl<HS, SS, CS, LS> Manifest<HS, SS, CS, LS>
where
    HS: HeadStore + Send + Sync + 'static,
    SS: SegmentIo + Send + Sync + 'static,
    CS: CheckpointStore + Send + Sync + 'static,
    LS: LeaseStore + Send + Sync + 'static,
{
    /// Construct a new manifest wrapper from the provided stores and context.
    #[must_use]
    pub fn open(
        stores: Stores<HS, SS, CS, LS>,
        ctx: Arc<fusio_manifest::context::ManifestContext<DefaultRetention, BlockingExecutor>>,
    ) -> Self {
        Self {
            inner: FusioManifest::new_with_context(
                stores.head,
                stores.segment,
                stores.checkpoint,
                stores.lease,
                ctx,
            ),
        }
    }

    /// Borrow the underlying fusio-manifest instance.
    #[must_use]
    pub const fn inner(
        &self,
    ) -> &FusioManifest<
        ManifestKey,
        ManifestValue,
        HS,
        SS,
        CS,
        LS,
        BlockingExecutor,
        DefaultRetention,
    > {
        &self.inner
    }

    /// Dissolve the wrapper, returning the raw fusio-manifest value.
    #[must_use]
    pub fn into_inner(
        self,
    ) -> FusioManifest<ManifestKey, ManifestValue, HS, SS, CS, LS, BlockingExecutor, DefaultRetention>
    {
        self.inner
    }

    /// Apply a sequence of edits, atomically publishing a new table version together with head
    /// metadata.
    pub async fn apply_version_edits(
        &self,
        table: TableId,
        edits: &[VersionEdit],
    ) -> ManifestResult<Timestamp> {
        if edits.is_empty() {
            return Err(ManifestError::Invariant("no version edits provided"));
        }

        let mut session = self.inner.session_write().await?;

        let head_key = ManifestKey::TableHead { table_id: table };
        let mut head = match session.get(&head_key).await? {
            Some(value) => {
                Self::validate_manifest_key_value(&head_key, &value)?;
                TableHead::try_from(value)?
            }
            None => {
                session.end().await?;
                return Err(ManifestError::Invariant(
                    "table head must exist before applying edits",
                ));
            }
        };

        let mut state = if let Some(current_version) = head.current_version {
            let version_key = ManifestKey::TableVersion {
                table_id: table,
                version: current_version,
            };
            match session.get(&version_key).await? {
                Some(value) => {
                    Self::validate_manifest_key_value(&version_key, &value)?;
                    VersionState::try_from(value)?
                }
                None => {
                    session.end().await?;
                    return Err(ManifestError::Invariant(
                        "version referenced by table head is missing",
                    ));
                }
            }
        } else {
            VersionState::empty(table)
        };

        version::apply_edits(&mut state, edits)?;

        // Update table version and commit timestamp
        let new_version_id = state.version_id.next();
        let next_txn = state.commit_timestamp.next();
        state.version_id = new_version_id;
        state.commit_timestamp = next_txn;
        let wal_floor = state.wal_floor.clone();

        session.put(
            ManifestKey::TableVersion {
                table_id: table,
                version: new_version_id,
            },
            ManifestValue::TableVersion(state),
        );

        // Update table head with updated wal floor
        head.current_version = Some(new_version_id);
        head.last_manifest_txn = Some(next_txn);
        head.wal_floor = wal_floor.clone();

        session.put(head_key, ManifestValue::TableHead(head));

        // Update WAL floor
        if let Some(floor) = wal_floor {
            session.put(
                ManifestKey::WalFloor { table_id: table },
                ManifestValue::WalFloor(floor),
            )
        }

        session.commit().await?;
        Ok(next_txn)
    }

    /// Load the latest manifest state for the given table.
    pub async fn snapshot_latest(&self, table: TableId) -> ManifestResult<TableSnapshot> {
        let session = self.inner.session_read().await?;
        let manifest_snapshot = session.snapshot().clone();

        let head_key = ManifestKey::TableHead { table_id: table };
        let head = match session.get(&head_key).await? {
            Some(value) => {
                Self::validate_manifest_key_value(&head_key, &value)?;
                TableHead::try_from(value)?
            }
            None => {
                return Err(ManifestError::Invariant(
                    "Head cannot be empty for snapshot",
                ));
            }
        };

        let version_key = ManifestKey::TableVersion {
            table_id: table,
            version: head.current_version.unwrap_or_default(),
        };
        let latest_version = match session.get(&version_key).await? {
            Some(value) => {
                Self::validate_manifest_key_value(&version_key, &value)?;
                Some(VersionState::try_from(value)?)
            }
            None => None,
        };

        session.end().await?;
        Ok(TableSnapshot {
            manifest_snapshot,
            head,
            latest_version,
        })
    }

    /// List committed versions for the table, ordered from newest to oldest.
    pub async fn list_versions(
        &self,
        table: TableId,
        limit: usize,
    ) -> ManifestResult<Vec<VersionState>> {
        let session = self.inner.session_read().await?;
        let mut versions: Vec<VersionState> = Vec::new();

        // TODO: This is inefficient; fusio-manifest should support limit for range scan.
        let entries = match session
            .scan_range(ScanRange {
                start: Some(ManifestKey::TableVersion {
                    table_id: table,
                    version: VersionId::from(0),
                }),
                end: Some(ManifestKey::TableVersion {
                    table_id: table,
                    version: VersionId(u64::MAX),
                }),
            })
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                session.end().await?;
                return Err(e.into());
            }
        };

        for (key, value) in entries {
            Self::validate_manifest_key_value(&key, &value)?;
            if let ManifestValue::TableVersion(state) = value {
                versions.push(state);
            }
        }
        session.end().await?;

        versions.sort_by(|a, b| b.version_id.inner().cmp(&a.version_id.inner()));
        if limit > 0 && versions.len() > limit {
            versions.truncate(limit);
        }

        Ok(versions)
    }

    /// Attempt to adopt orphaned manifest segments (delegates to fusio-manifest).
    pub async fn recover_orphans(&self) -> ManifestResult<usize> {
        self.inner
            .recover_orphans()
            .await
            .map_err(ManifestError::from)
    }

    /// Return a compactor bound to the same stores and context.
    #[must_use]
    pub fn compactor(
        &self,
    ) -> fusio_manifest::compactor::Compactor<
        ManifestKey,
        ManifestValue,
        HS,
        SS,
        CS,
        LS,
        BlockingExecutor,
        DefaultRetention,
    > {
        self.inner.compactor()
    }

    /// Fetch the persisted WAL floor for a table.
    pub async fn wal_floor(&self, table: TableId) -> ManifestResult<Option<WalSegmentRef>> {
        let session = self.inner.session_read().await?;
        let key = ManifestKey::WalFloor { table_id: table };
        let floor = match session.get(&key).await? {
            Some(value) => {
                Self::validate_manifest_key_value(&key, &value)?;
                Some(WalSegmentRef::try_from(value)?)
            }
            None => None,
        };
        session.end().await?;
        Ok(floor)
    }

    fn validate_manifest_key_value(
        key: &ManifestKey,
        value: &ManifestValue,
    ) -> Result<(), ManifestError> {
        match (key, value) {
            (ManifestKey::CatalogRoot, ManifestValue::Catalog(_)) => Ok(()),
            (ManifestKey::TableMeta { .. }, ManifestValue::TableMeta(_)) => Ok(()),
            (ManifestKey::TableHead { .. }, ManifestValue::TableHead(_)) => Ok(()),
            (ManifestKey::TableVersion { .. }, ManifestValue::TableVersion(_)) => Ok(()),
            (ManifestKey::WalFloor { .. }, ManifestValue::WalFloor(_)) => Ok(()),
            (ManifestKey::GcPlan { .. }, ManifestValue::GcPlan(_)) => Ok(()),
            _ => Err(ManifestError::Invariant("manifest key/value type mismatch")),
        }
    }
}

impl<HS, SS, CS, LS> Clone for Manifest<HS, SS, CS, LS>
where
    HS: HeadStore + Send + Sync + Clone + 'static,
    SS: SegmentIo + Send + Sync + Clone + 'static,
    CS: CheckpointStore + Send + Sync + Clone + 'static,
    LS: LeaseStore + Send + Sync + Clone + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fusio::mem::fs::InMemoryFs;
    use fusio_manifest::{
        BackoffPolicy, BlockingExecutor, CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl,
        ManifestContext, SegmentStoreImpl,
    };

    use super::*;

    type TestManifest = Manifest<
        HeadStoreImpl<InMemoryFs>,
        SegmentStoreImpl<InMemoryFs>,
        CheckpointStoreImpl<InMemoryFs>,
        LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
    >;

    fn in_memory_manifest() -> TestManifest {
        let fs = InMemoryFs::new();
        let head = HeadStoreImpl::new(fs.clone(), "head.json");
        let segment = SegmentStoreImpl::new(fs.clone(), "segments");
        let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
        let timer = BlockingExecutor;
        let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), timer);
        let context = Arc::new(ManifestContext::new(BlockingExecutor));
        Manifest::open(Stores::new(head, segment, checkpoint, lease), context)
    }

    async fn prime_table_head(manifest: &TestManifest, table_id: TableId) {
        let head = TableHead {
            table_id,
            current_version: None,
            schema_version: 1,
            wal_floor: None,
            last_manifest_txn: None,
        };

        let mut session = manifest
            .inner()
            .session_write()
            .await
            .expect("failed to open write session for head setup");
        session.put(
            ManifestKey::TableHead { table_id },
            ManifestValue::TableHead(head),
        );
        session
            .commit()
            .await
            .expect("failed to commit initial table head");
    }

    #[tokio::test]
    async fn apply_version_edits_snapshot_latest_and_list_versions_happy_path() {
        let manifest = in_memory_manifest();
        let table_id = TableId::new();
        prime_table_head(&manifest, table_id).await;

        let wal_segment = WalSegmentRef {
            seq: 42,
            file_id: generate_file_id(),
            first_frame: 0,
            last_frame: 10,
        };
        let sst_level0_a = SstEntry {
            sst_id: SsTableId::new(7),
            stats: Some(SsTableStats::default()),
            wal_segments: Some(vec![wal_segment.file_id.clone()]),
        };
        let sst_level0_b = SstEntry {
            sst_id: SsTableId::new(8),
            stats: Some(SsTableStats::default()),
            wal_segments: Some(vec![generate_file_id()]),
        };
        let sst_level1 = SstEntry {
            sst_id: SsTableId::new(21),
            stats: Some(SsTableStats::default()),
            wal_segments: Some(vec![generate_file_id()]),
        };

        // Simulate flush of immutables
        let edits = vec![
            VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_level0_a.clone(), sst_level0_b.clone()],
            },
            VersionEdit::SetWalSegment {
                segment: wal_segment.clone(),
            },
            VersionEdit::SetTombstoneWatermark { watermark: 99 },
        ];
        let first_txn = manifest
            .apply_version_edits(table_id, &edits)
            .await
            .expect("apply_version_edits should succeed");
        assert_eq!(
            first_txn,
            Timestamp::new(1),
            "first manifest transaction should increment timestamp"
        );

        // Assert snapshot_latest works
        let snapshot = manifest
            .snapshot_latest(table_id)
            .await
            .expect("snapshot_latest should succeed");
        let latest_version = snapshot
            .latest_version
            .expect("latest version should exist after applying edits");
        assert_eq!(latest_version.table_id, table_id);
        assert_eq!(
            latest_version.version_id,
            VersionId::from(1),
            "first published version should have id 1"
        );
        assert_eq!(
            latest_version.commit_timestamp,
            Timestamp::new(1),
            "commit timestamp should match returned value"
        );
        assert_eq!(latest_version.ssts.len(), 1);
        assert_eq!(latest_version.ssts[0].len(), 2);
        assert!(
            latest_version.ssts[0].contains(&sst_level0_a)
                && latest_version.ssts[0].contains(&sst_level0_b)
        );
        assert_eq!(latest_version.wal_floor, Some(wal_segment.clone()));
        assert_eq!(
            latest_version.tombstone_watermark,
            Some(99),
            "tombstone watermark should be captured"
        );
        assert_eq!(
            snapshot.head.current_version,
            Some(VersionId::from(1)),
            "table head should point at the first committed version"
        );
        assert_eq!(
            snapshot.head.wal_floor,
            Some(wal_segment.clone()),
            "wal floor should track the lowest wal segment"
        );

        // Assert list version works
        let listed_versions = manifest
            .list_versions(table_id, 10)
            .await
            .expect("list_versions should succeed");
        assert_eq!(listed_versions.len(), 1);
        assert_eq!(listed_versions[0], latest_version);

        // Now Simulate a compaction where it delete some sst files
        let new_wal_segment = WalSegmentRef {
            seq: 128,
            file_id: generate_file_id(),
            first_frame: 11,
            last_frame: 42,
        };
        let removal_edits = vec![
            VersionEdit::RemoveSsts {
                level: 0,
                sst_ids: vec![sst_level0_a.sst_id.clone(), sst_level0_b.sst_id.clone()],
            },
            VersionEdit::AddSsts {
                level: 1,
                entries: vec![sst_level1.clone()],
            },
            VersionEdit::SetWalSegment {
                segment: new_wal_segment.clone(),
            },
            VersionEdit::SetTombstoneWatermark { watermark: 111 },
        ];
        let second_txn = manifest
            .apply_version_edits(table_id, &removal_edits)
            .await
            .expect("apply_version_edits should succeed when removing");
        assert_eq!(
            second_txn,
            Timestamp::new(2),
            "second manifest transaction should advance timestamp again"
        );

        // Assert
        let snapshot_after = manifest
            .snapshot_latest(table_id)
            .await
            .expect("snapshot_latest should succeed after removal");
        let updated_version = snapshot_after
            .latest_version
            .expect("latest version should exist after removal edit");
        assert_eq!(updated_version.version_id, VersionId::from(2));
        assert_eq!(updated_version.commit_timestamp, Timestamp::new(2));
        assert_eq!(updated_version.ssts.len(), 2);
        assert!(
            updated_version
                .ssts
                .first()
                .is_some_and(|level| level.is_empty()),
            "level 0 should be empty after deletions"
        );
        assert_eq!(
            updated_version.ssts.get(1),
            Some(&vec![sst_level1.clone()]),
            "level 1 should contain the newly added SST"
        );
        assert_eq!(
            updated_version.wal_floor,
            Some(new_wal_segment.clone()),
            "wal floor should update to the latest segment when provided"
        );
        assert_eq!(
            updated_version.tombstone_watermark,
            Some(111),
            "second commit should refresh watermark"
        );
        assert_eq!(
            snapshot_after.head.wal_floor,
            Some(new_wal_segment.clone()),
            "table head should reflect the newest wal segment"
        );
        assert_eq!(
            snapshot_after.head.current_version,
            Some(VersionId::from(2)),
            "head should point at the newest version after removal"
        );

        // Assert
        let listed_versions_after = manifest
            .list_versions(table_id, 10)
            .await
            .expect("list_versions should succeed after removal");
        assert_eq!(
            listed_versions_after.len(),
            2,
            "both committed versions should be discoverable"
        );
        assert_eq!(listed_versions_after[0].version_id, VersionId::from(2));
        assert_eq!(listed_versions_after[1].version_id, VersionId::from(1));
        assert_eq!(
            listed_versions_after[0].tombstone_watermark,
            Some(111),
            "newest version should expose latest watermark"
        );
        assert_eq!(
            listed_versions_after[0].wal_floor,
            Some(new_wal_segment.clone()),
            "newest version should expose updated wal segment"
        );
        assert!(
            listed_versions_after[0]
                .ssts
                .get(1)
                .is_some_and(|level| level == &vec![sst_level1.clone()]),
            "new aggregate should advertise level 1 SSTs"
        );

        let limited_versions = manifest
            .list_versions(table_id, 1)
            .await
            .expect("list_versions should allow limiting");
        assert_eq!(limited_versions.len(), 1);
        assert_eq!(
            limited_versions[0].version_id,
            VersionId::from(2),
            "limit should return only the newest version"
        );
        assert_eq!(
            limited_versions[0].wal_floor,
            Some(new_wal_segment),
            "limited view should still include latest wal segment ref"
        );
    }

    #[tokio::test]
    async fn apply_version_edits_failure() {
        let manifest = in_memory_manifest();
        let table_id = TableId::new();
        let err = manifest
            .apply_version_edits(
                table_id,
                &[VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![SstEntry {
                        sst_id: SsTableId::new(11),
                        stats: Some(SsTableStats::default()),
                        wal_segments: Some(vec![generate_file_id()]),
                    }],
                }],
            )
            .await
            .expect_err("applying edits without a head should fail");
        match err {
            ManifestError::Invariant(msg) => assert_eq!(
                msg, "table head must exist before applying edits",
                "expected invariant violation when head is missing"
            ),
            other => panic!("unexpected error variant: {other:?}"),
        }

        let empty_err = manifest
            .apply_version_edits(table_id, &[])
            .await
            .expect_err("applying empty edit batch should fail");
        match empty_err {
            ManifestError::Invariant(msg) => assert_eq!(
                msg, "no version edits provided",
                "expected invariant violation when edits are empty"
            ),
            other => panic!("unexpected error variant: {other:?}"),
        }

        let versions = manifest
            .list_versions(table_id, 10)
            .await
            .expect("listing versions without a head should succeed");
        assert!(
            versions.is_empty(),
            "no versions should be returned for an uninitialised head"
        );
    }
}
