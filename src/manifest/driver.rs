use std::sync::Arc;

use fusio_manifest::{
    BlockingExecutor, CheckpointStore, HeadStore, LeaseStore, SegmentIo,
    compactor::Compactor,
    context::ManifestContext,
    manifest::Manifest as FusioManifest,
    retention::DefaultRetention,
    snapshot::{ScanRange, Snapshot},
    types::Error as FusioManifestError,
};
use thiserror::Error;

use super::{
    VersionEdit,
    codec::{ManifestCodec, VersionCodec},
    domain::{TableHead, TableId, VersionKey, VersionState, VersionValue, WalSegmentRef},
};
use crate::mvcc::Timestamp;

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
pub(crate) type ManifestResult<T> = Result<T, ManifestError>;

/// Result of loading the latest state for a table.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct TableSnapshot {
    /// Underlying fusio snapshot guarding read leases.
    pub manifest_snapshot: Snapshot,
    /// Current table head.
    pub head: TableHead,
    /// Most recent committed version for the table, if any.
    pub latest_version: Option<VersionState>,
}

/// Bundle of storage backends required by the manifest.
///
/// Each manifest instance should receive store handles that already point at the physical
/// directory or bucket allocated for that instance. Supplying distinct stores allows callers to
/// isolate manifest families on disk or in object storage; sharing the same stores will co-locate
/// them while still keeping key spaces separated at the type level.
#[derive(Debug)]
pub(crate) struct Stores<HS, SS, CS, LS> {
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
    pub(crate) fn new(head: HS, segment: SS, checkpoint: CS, lease: LS) -> Self {
        Self {
            head,
            segment,
            checkpoint,
            lease,
        }
    }
}

/// Manifest wrapper parameterized by the codec describing its key/value types.
pub(crate) struct Manifest<C, HS, SS, CS, LS>
where
    C: ManifestCodec,
    HS: HeadStore + Send + Sync + 'static,
    SS: SegmentIo + Send + Sync + 'static,
    CS: CheckpointStore + Send + Sync + 'static,
    LS: LeaseStore + Send + Sync + 'static,
{
    inner: FusioManifest<C::Key, C::Value, HS, SS, CS, LS, BlockingExecutor, DefaultRetention>,
}

impl<C, HS, SS, CS, LS> Manifest<C, HS, SS, CS, LS>
where
    C: ManifestCodec,
    HS: HeadStore + Send + Sync + 'static,
    SS: SegmentIo + Send + Sync + 'static,
    CS: CheckpointStore + Send + Sync + 'static,
    LS: LeaseStore + Send + Sync + 'static,
{
    /// Construct a new manifest wrapper from the provided stores and context.
    #[must_use]
    pub(super) fn open(
        stores: Stores<HS, SS, CS, LS>,
        ctx: Arc<ManifestContext<DefaultRetention, BlockingExecutor>>,
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

    /// Access the underlying compactor for advanced merging workflows.
    #[allow(dead_code)]
    pub(crate) fn compactor(
        &self,
    ) -> Compactor<C::Key, C::Value, HS, SS, CS, LS, BlockingExecutor, DefaultRetention> {
        self.inner.compactor()
    }
}

impl<HS, SS, CS, LS> Manifest<VersionCodec, HS, SS, CS, LS>
where
    HS: HeadStore + Send + Sync + 'static,
    SS: SegmentIo + Send + Sync + 'static,
    CS: CheckpointStore + Send + Sync + 'static,
    LS: LeaseStore + Send + Sync + 'static,
{
    /// Apply a sequence of edits, atomically publishing a new table version together with head
    /// metadata.
    pub(crate) async fn apply_version_edits(
        &self,
        table: TableId,
        edits: &[VersionEdit],
    ) -> ManifestResult<Timestamp> {
        if edits.is_empty() {
            return Err(ManifestError::Invariant("no version edits provided"));
        }

        let mut session = self.inner.session_write().await?;

        let head_key = VersionKey::TableHead { table_id: table };
        let mut head = match session.get(&head_key).await? {
            Some(value) => {
                <VersionCodec as ManifestCodec>::validate_key_value(&head_key, &value)?;
                TableHead::try_from(value)?
            }
            None => {
                session.end().await?;
                return Err(ManifestError::Invariant(
                    "table head must exist before applying edits",
                ));
            }
        };

        let mut state = if let Some(last_txn) = head.last_manifest_txn {
            let version_key = VersionKey::TableVersion {
                table_id: table,
                manifest_ts: last_txn,
            };
            match session.get(&version_key).await? {
                Some(value) => {
                    <VersionCodec as ManifestCodec>::validate_key_value(&version_key, &value)?;
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

        state.apply_edits(edits)?;

        // Update table version and commit timestamp
        let next_txn = state.commit_timestamp().next();
        state.set_commit_timestamp(next_txn);
        let wal_floor = state.cloned_wal_floor();

        session.put(
            VersionKey::TableVersion {
                table_id: table,
                manifest_ts: next_txn,
            },
            VersionValue::TableVersion(state),
        );

        // Update table head with updated wal floor
        head.last_manifest_txn = Some(next_txn);
        head.wal_floor = wal_floor.clone();

        session.put(head_key, VersionValue::TableHead(head));

        // Update WAL floor
        if let Some(floor) = wal_floor {
            session.put(
                VersionKey::WalFloor { table_id: table },
                VersionValue::WalFloor(floor),
            )
        }

        session.commit().await?;
        Ok(next_txn)
    }

    #[allow(dead_code)]
    pub(crate) async fn snapshot_latest(&self, table: TableId) -> ManifestResult<TableSnapshot> {
        let session = self.inner.session_read().await?;
        let manifest_snapshot = session.snapshot().clone();

        let head_key = VersionKey::TableHead { table_id: table };
        let head = match session.get(&head_key).await? {
            Some(value) => {
                <VersionCodec as ManifestCodec>::validate_key_value(&head_key, &value)?;
                TableHead::try_from(value)?
            }
            None => {
                return Err(ManifestError::Invariant(
                    "Head cannot be empty for snapshot",
                ));
            }
        };

        let latest_version = if let Some(last_txn) = head.last_manifest_txn {
            let version_key = VersionKey::TableVersion {
                table_id: table,
                manifest_ts: last_txn,
            };
            match session.get(&version_key).await? {
                Some(value) => {
                    <VersionCodec as ManifestCodec>::validate_key_value(&version_key, &value)?;
                    Some(VersionState::try_from(value)?)
                }
                None => None,
            }
        } else {
            None
        };

        session.end().await?;
        Ok(TableSnapshot {
            manifest_snapshot,
            head,
            latest_version,
        })
    }

    pub(crate) async fn init_table_head(
        &self,
        table_id: TableId,
        head: TableHead,
    ) -> ManifestResult<()> {
        let mut session = self.inner.session_write().await?;
        session.put(
            VersionKey::TableHead { table_id },
            VersionValue::TableHead(head),
        );
        session.commit().await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn list_versions(
        &self,
        table: TableId,
        limit: usize,
    ) -> ManifestResult<Vec<VersionState>> {
        let session = self.inner.session_read().await?;
        let mut versions: Vec<VersionState> = Vec::new();

        // TODO: This is inefficient; fusio-manifest should support limit for range scan.
        let entries = match session
            .scan_range(ScanRange {
                start: Some(VersionKey::TableVersion {
                    table_id: table,
                    manifest_ts: Timestamp::MIN,
                }),
                end: Some(VersionKey::TableVersion {
                    table_id: table,
                    manifest_ts: Timestamp::MAX,
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
            <VersionCodec as ManifestCodec>::validate_key_value(&key, &value)?;
            if let VersionValue::TableVersion(state) = value {
                versions.push(state);
            }
        }
        session.end().await?;

        versions.sort_by(|a, b| b.commit_timestamp.cmp(&a.commit_timestamp));
        if limit > 0 && versions.len() > limit {
            versions.truncate(limit);
        }

        Ok(versions)
    }

    #[allow(dead_code)]
    pub(crate) async fn recover_orphans(&self) -> ManifestResult<usize> {
        self.inner
            .recover_orphans()
            .await
            .map_err(ManifestError::from)
    }

    /// Fetch the persisted WAL floor for a table.
    pub(crate) async fn wal_floor(&self, table: TableId) -> ManifestResult<Option<WalSegmentRef>> {
        let session = self.inner.session_read().await?;
        let key = VersionKey::WalFloor { table_id: table };
        let floor = match session.get(&key).await? {
            Some(value) => {
                <VersionCodec as ManifestCodec>::validate_key_value(&key, &value)?;
                Some(WalSegmentRef::try_from(value)?)
            }
            None => None,
        };
        session.end().await?;
        Ok(floor)
    }
}

impl<C, HS, SS, CS, LS> Clone for Manifest<C, HS, SS, CS, LS>
where
    C: ManifestCodec,
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

    use fusio::{mem::fs::InMemoryFs, path::Path};
    use fusio_manifest::{
        BackoffPolicy, BlockingExecutor, CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl,
        ManifestContext, SegmentStoreImpl,
    };

    use super::{super::domain::SstEntry, *};
    use crate::{
        fs::generate_file_id,
        manifest::{bootstrap::init_in_memory_manifest, domain::TableId},
        ondisk::sstable::{SsTableId, SsTableStats},
    };

    type TestManifest = super::super::bootstrap::InMemoryManifest;

    fn bare_manifest() -> TestManifest {
        let fs = InMemoryFs::new();
        let head = HeadStoreImpl::new(fs.clone(), "head.json");
        let segment = SegmentStoreImpl::new(fs.clone(), "segments");
        let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
        let timer = BlockingExecutor;
        let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), timer);
        let context = Arc::new(ManifestContext::new(BlockingExecutor));
        Manifest::open(Stores::new(head, segment, checkpoint, lease), context)
    }

    fn test_paths(id: u64) -> (Path, Path) {
        let base = format!("sst/L0/{id:020}");
        let data = Path::parse(format!("{base}.parquet")).expect("parse data path");
        let mvcc = Path::parse(format!("{base}.mvcc.parquet")).expect("parse mvcc path");
        (data, mvcc)
    }

    #[tokio::test]
    async fn apply_version_edits_snapshot_latest_and_list_versions_happy_path() {
        let (manifest, table_id) = init_in_memory_manifest(1).expect("manifest should initialize");

        let wal_segment = WalSegmentRef::new(42, generate_file_id(), 0, 10);
        let (data0a, mvcc0a) = test_paths(7);
        let sst_level0_a = SstEntry::new(
            SsTableId::new(7),
            Some(SsTableStats::default()),
            Some(vec![wal_segment.file_id().clone()]),
            data0a.clone(),
            mvcc0a.clone(),
        );
        let (data0b, mvcc0b) = test_paths(8);
        let sst_level0_b = SstEntry::new(
            SsTableId::new(8),
            Some(SsTableStats::default()),
            Some(vec![generate_file_id()]),
            data0b.clone(),
            mvcc0b.clone(),
        );
        let (data1, mvcc1) = test_paths(21);
        let sst_level1 = SstEntry::new(
            SsTableId::new(21),
            Some(SsTableStats::default()),
            Some(vec![generate_file_id()]),
            data1.clone(),
            mvcc1.clone(),
        );

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
        assert_eq!(latest_version.table_id(), &table_id);
        assert_eq!(
            latest_version.commit_timestamp(),
            Timestamp::new(1),
            "commit timestamp should match returned value"
        );
        assert_eq!(latest_version.ssts().len(), 1);
        assert_eq!(latest_version.ssts()[0].len(), 2);
        assert!(
            latest_version.ssts()[0].contains(&sst_level0_a)
                && latest_version.ssts()[0].contains(&sst_level0_b)
        );
        assert_eq!(latest_version.wal_floor(), Some(&wal_segment));
        let persisted_level0 = &latest_version.ssts()[0];
        let persisted_a = persisted_level0
            .iter()
            .find(|entry| entry.sst_id() == sst_level0_a.sst_id())
            .expect("level 0 should contain first sst");
        assert_eq!(persisted_a.data_path(), &data0a);
        assert_eq!(persisted_a.mvcc_path(), &mvcc0a);
        let persisted_b = persisted_level0
            .iter()
            .find(|entry| entry.sst_id() == sst_level0_b.sst_id())
            .expect("level 0 should contain second sst");
        assert_eq!(persisted_b.data_path(), &data0b);
        assert_eq!(persisted_b.mvcc_path(), &mvcc0b);
        assert_eq!(
            latest_version.tombstone_watermark(),
            Some(99),
            "tombstone watermark should be captured"
        );
        assert_eq!(
            snapshot.head.last_manifest_txn,
            Some(Timestamp::new(1)),
            "table head should track the latest manifest txn"
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
        let new_wal_segment = WalSegmentRef::new(128, generate_file_id(), 11, 42);
        let removal_edits = vec![
            VersionEdit::RemoveSsts {
                level: 0,
                sst_ids: vec![sst_level0_a.sst_id().clone(), sst_level0_b.sst_id().clone()],
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
        assert_eq!(updated_version.commit_timestamp(), Timestamp::new(2));
        assert_eq!(updated_version.ssts().len(), 2);
        assert!(
            updated_version
                .ssts()
                .first()
                .is_some_and(|level| level.is_empty()),
            "level 0 should be empty after deletions"
        );
        assert_eq!(
            updated_version.ssts().get(1),
            Some(&vec![sst_level1.clone()]),
            "level 1 should contain the newly added SST"
        );
        let persisted_level1 = updated_version
            .ssts()
            .get(1)
            .expect("level 1 present")
            .iter()
            .find(|entry| entry.sst_id() == sst_level1.sst_id())
            .expect("level 1 entry present");
        assert_eq!(persisted_level1.data_path(), &data1);
        assert_eq!(persisted_level1.mvcc_path(), &mvcc1);
        assert_eq!(
            updated_version.wal_floor(),
            Some(&new_wal_segment),
            "wal floor should update to the latest segment when provided"
        );
        assert_eq!(
            updated_version.tombstone_watermark(),
            Some(111),
            "second commit should refresh watermark"
        );
        assert_eq!(
            snapshot_after.head.wal_floor,
            Some(new_wal_segment.clone()),
            "table head should reflect the newest wal segment"
        );
        assert_eq!(
            snapshot_after.head.last_manifest_txn,
            Some(Timestamp::new(2)),
            "head should point at the newest manifest txn after removal"
        );

        let listed_versions_after = manifest
            .list_versions(table_id, 10)
            .await
            .expect("list_versions should succeed after removal");
        assert_eq!(
            listed_versions_after.len(),
            2,
            "both committed versions should be discoverable"
        );
        assert_eq!(
            listed_versions_after[0].commit_timestamp(),
            Timestamp::new(2),
            "newest version should appear first"
        );
        assert_eq!(
            listed_versions_after[1].commit_timestamp(),
            Timestamp::new(1),
            "oldest version should follow"
        );

        let limited_versions = manifest
            .list_versions(table_id, 1)
            .await
            .expect("list_versions should allow limiting");
        assert_eq!(limited_versions.len(), 1);
        assert_eq!(limited_versions[0].commit_timestamp(), Timestamp::new(2));
    }

    #[tokio::test]
    async fn apply_version_edits_failure() {
        let manifest = bare_manifest();
        let table_id = TableId::new();
        let (failure_data_path, failure_mvcc_path) = test_paths(11);
        let err = manifest
            .apply_version_edits(
                table_id,
                &[VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![SstEntry::new(
                        SsTableId::new(11),
                        Some(SsTableStats::default()),
                        Some(vec![generate_file_id()]),
                        failure_data_path.clone(),
                        failure_mvcc_path.clone(),
                    )],
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
