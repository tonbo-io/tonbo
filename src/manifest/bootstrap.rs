use std::sync::Arc;

#[cfg(test)]
use fusio::mem::fs::InMemoryFs;
use fusio::{
    dynamic::{MaybeSend, MaybeSync},
    executor::{Executor, Timer},
    fs::{Fs, FsCas},
    path::{Path, PathPart},
};
use fusio_manifest::{
    BackoffPolicy, CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl, ManifestContext,
    SegmentStoreImpl, snapshot::Snapshot, types::Error as FusioManifestError,
};
use tracing::instrument;

use super::{
    ManifestFs,
    codec::{CatalogCodec, GcPlanCodec, ManifestCodec, VersionCodec},
    domain::{
        CurrentRootSet, GcAuthorizationSummary, GcPlanState, TableDefinition, TableHead, TableId,
        TableMeta, VersionState, WalSegmentRef,
    },
    driver::{Manifest, ManifestError, ManifestResult, Stores, VersionSnapshot},
    version::VersionEdit,
};
use crate::{
    id::FileIdGenerator,
    mvcc::Timestamp,
    observability::{log_debug, log_info},
};

/// Concrete manifest type alias for a given filesystem and executor.
pub(super) type ManifestInstance<C, FS, E> = Manifest<
    C,
    HeadStoreImpl<FS>,
    SegmentStoreImpl<FS>,
    CheckpointStoreImpl<FS>,
    LeaseStoreImpl<FS, E>,
    E,
>;

/// In-memory manifest type wired against `fusio`'s memory FS for dev/test flows.
#[cfg(all(test, feature = "tokio"))]
pub(super) type InMemoryManifest<E> = ManifestInstance<VersionCodec, InMemoryFs, E>;

/// In-memory catalog manifest type used for catalog-specific tests.
#[cfg(all(test, feature = "tokio"))]
pub(super) type InMemoryCatalogManifest<E> = ManifestInstance<CatalogCodec, InMemoryFs, E>;

/// Snapshot combining catalog metadata with the latest version state.
#[derive(Debug, Clone)]
pub(crate) struct TableSnapshot {
    pub _manifest_snapshot: Snapshot,
    pub head: TableHead,
    pub latest_version: Option<VersionState>,

    pub _table_meta: TableMeta,
}

impl TableSnapshot {
    pub(super) fn from_parts(version: VersionSnapshot, table_meta: TableMeta) -> Self {
        Self {
            _manifest_snapshot: version.manifest_snapshot,
            head: version.head,
            latest_version: version.latest_version,
            _table_meta: table_meta,
        }
    }
}

/// Primary manifest bundle exposed to the rest of the crate.
///
/// This struct bundles three manifest instances (version, catalog, gc_plan) sharing
/// the same filesystem and executor types for static dispatch.
pub(crate) struct TonboManifest<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as Fs>::File: fusio::durability::FileCommit,
{
    version: ManifestInstance<VersionCodec, FS, E>,
    catalog: ManifestInstance<CatalogCodec, FS, E>,
    gc_plan: ManifestInstance<GcPlanCodec, FS, E>,
}

impl<FS, E> Clone for TonboManifest<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as Fs>::File: fusio::durability::FileCommit,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore + Clone,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo + Clone,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore + Clone,
    LeaseStoreImpl<FS, E>: fusio_manifest::LeaseStore + Clone,
{
    fn clone(&self) -> Self {
        Self {
            version: self.version.clone(),
            catalog: self.catalog.clone(),
            gc_plan: self.gc_plan.clone(),
        }
    }
}

impl<FS, E> TonboManifest<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as Fs>::File: fusio::durability::FileCommit,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, E>: fusio_manifest::LeaseStore,
{
    fn new(
        version: ManifestInstance<VersionCodec, FS, E>,
        catalog: ManifestInstance<CatalogCodec, FS, E>,
        gc_plan: ManifestInstance<GcPlanCodec, FS, E>,
    ) -> Self {
        Self {
            version,
            catalog,
            gc_plan,
        }
    }

    pub(crate) async fn apply_version_edits(
        &self,
        table: TableId,
        edits: &[VersionEdit],
    ) -> ManifestResult<Timestamp> {
        self.version.apply_version_edits(table, edits).await
    }

    pub(crate) async fn apply_version_edits_cas(
        &self,
        table: TableId,
        expected_head: Option<Timestamp>,
        edits: &[VersionEdit],
    ) -> ManifestResult<Timestamp> {
        self.version
            .apply_version_edits_cas(table, expected_head, edits)
            .await
    }

    pub(crate) async fn snapshot_latest(&self, table: TableId) -> ManifestResult<TableSnapshot> {
        let version_snapshot = self.version.snapshot_latest(table).await?;
        let table_meta = self.catalog.table_meta(table).await?;
        Ok(TableSnapshot::from_parts(version_snapshot, table_meta))
    }

    /// Capture the latest manifest view, optionally falling back to a cached `TableMeta`
    /// when the catalog entry is missing (e.g. transient object-store visibility gaps).
    pub(crate) async fn snapshot_latest_with_fallback(
        &self,
        table: TableId,
        fallback: &TableMeta,
    ) -> ManifestResult<TableSnapshot> {
        match self.catalog.table_meta(table).await {
            Ok(meta) => {
                let version_snapshot = self.version.snapshot_latest(table).await?;
                Ok(TableSnapshot::from_parts(version_snapshot, meta))
            }
            Err(ManifestError::Invariant("catalog metadata missing for table_id")) => {
                let version_snapshot = self.version.snapshot_latest(table).await?;
                Ok(TableSnapshot::from_parts(
                    version_snapshot,
                    fallback.clone(),
                ))
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn wal_floor(&self, table: TableId) -> ManifestResult<Option<WalSegmentRef>> {
        self.version.wal_floor(table).await
    }

    async fn init_table_head(&self, table_id: TableId, head: TableHead) -> ManifestResult<()> {
        self.version.init_table_head(table_id, head).await
    }

    async fn init_catalog(&self) -> ManifestResult<()> {
        self.catalog.init_catalog_root().await
    }

    #[instrument(
        name = "manifest::register_table",
        skip(self, file_ids, definition),
        fields(component = "manifest")
    )]
    pub(crate) async fn register_table(
        &self,
        file_ids: &FileIdGenerator,
        definition: &TableDefinition,
    ) -> ManifestResult<TableMeta> {
        let meta = self.catalog.register_table(file_ids, definition).await?;
        self.init_table_head(
            meta.table_id,
            TableHead {
                table_id: meta.table_id,
                schema_version: meta.schema_version,
                wal_floor: None,
                last_manifest_txn: None,
            },
        )
        .await?;
        log_info!(
            component = "manifest",
            event = "manifest_table_registered",
            table_id = ?meta.table_id,
            schema_version = meta.schema_version,
        );
        Ok(meta)
    }

    #[cfg(test)]
    pub(crate) async fn record_gc_plan(
        &self,
        table: TableId,
        plan: GcPlanState,
    ) -> ManifestResult<()> {
        self.gc_plan.put_gc_plan(table, plan).await
    }

    pub(crate) async fn merge_gc_plan(
        &self,
        table: TableId,
        plan: GcPlanState,
    ) -> ManifestResult<(Option<GcPlanState>, Option<GcPlanState>)> {
        self.gc_plan
            .update_gc_plan(table, move |current| {
                let mut merged = current.unwrap_or_default();
                merged.merge(plan);
                (!merged.is_empty()).then_some(merged)
            })
            .await
    }

    pub(crate) async fn remove_gc_sst_candidates(
        &self,
        table: TableId,
        reclaimed: &[crate::manifest::GcSstRef],
    ) -> ManifestResult<(Option<GcPlanState>, Option<GcPlanState>)> {
        self.gc_plan
            .update_gc_plan(table, |current| {
                let mut plan = current?;
                plan.remove_sst_candidates(reclaimed);
                (!plan.is_empty()).then_some(plan)
            })
            .await
    }

    pub(crate) async fn peek_gc_plan(&self, table: TableId) -> ManifestResult<Option<GcPlanState>> {
        self.gc_plan.peek_gc_plan(table).await
    }

    /// Take staged GC candidates and authorize SST reclamation against the current root set.
    ///
    /// In the single-process model the latest manifest root set is the only source of truth for
    /// whether an SST is still live. `GcPlanState` remains a hint queue; this helper filters out
    /// any SST candidate that has become reachable again before a sweeper acts on it. Callers that
    /// only process part of the returned plan must re-queue any residual SST or WAL candidates.
    #[allow(dead_code)]
    pub(crate) async fn take_gc_plan_for_authorized_sweep(
        &self,
        table: TableId,
    ) -> ManifestResult<Option<GcPlanState>> {
        self.take_gc_plan_for_authorized_sweep_with_pins(table, &[])
            .await
    }

    pub(crate) async fn take_gc_plan_for_authorized_sweep_with_pins(
        &self,
        table: TableId,
        active_pins: &[Timestamp],
    ) -> ManifestResult<Option<GcPlanState>> {
        let Some(plan) = self.gc_plan.peek_gc_plan(table).await? else {
            return Ok(None);
        };
        let staged_sst_count = plan.obsolete_ssts.len();
        let root_set = self
            .version
            .current_root_set_with_pins(table, active_pins)
            .await?;
        let plan = plan.authorize_with_root_set(&root_set);
        let authorized_sst_count = plan.obsolete_ssts.len();
        log_debug!(
            component = "manifest",
            event = "gc_plan_authorized_for_sweep",
            table_id = ?table,
            protected_versions = root_set.protected_version_count(),
            protected_sst_objects = root_set.protected_object_count(),
            staged_sst_candidates = staged_sst_count,
            authorized_sst_candidates = authorized_sst_count,
            filtered_sst_candidates = staged_sst_count.saturating_sub(authorized_sst_count),
            obsolete_wal_segments = plan.obsolete_wal_segments.len(),
        );
        if plan.is_empty() {
            Ok(None)
        } else {
            Ok(Some(plan))
        }
    }

    /// Inspect the current GC plan against the latest manifest root set without mutating it.
    #[allow(dead_code)]
    pub(crate) async fn inspect_gc_plan_authorization(
        &self,
        table: TableId,
    ) -> ManifestResult<Option<GcAuthorizationSummary>> {
        self.inspect_gc_plan_authorization_with_pins(table, &[])
            .await
    }

    pub(crate) async fn inspect_gc_plan_authorization_with_pins(
        &self,
        table: TableId,
        active_pins: &[Timestamp],
    ) -> ManifestResult<Option<GcAuthorizationSummary>> {
        let Some(plan) = self.gc_plan.peek_gc_plan(table).await? else {
            return Ok(None);
        };
        let root_set = self
            .version
            .current_root_set_with_pins(table, active_pins)
            .await?;
        Ok(Some(plan.authorization_summary(&root_set)))
    }

    /// List committed versions of a table, ordered newest-first.
    ///
    /// Returns up to `limit` versions for time travel queries.
    pub async fn list_versions(
        &self,
        table: TableId,
        limit: usize,
    ) -> ManifestResult<Vec<VersionState>> {
        self.version.list_versions(table, limit).await
    }

    /// Build the current manifest root set that protects SST objects from GC.
    #[allow(dead_code)]
    pub(crate) async fn current_root_set(&self, table: TableId) -> ManifestResult<CurrentRootSet> {
        self.current_root_set_with_pins(table, &[]).await
    }

    #[allow(dead_code)]
    pub(crate) async fn current_root_set_with_pins(
        &self,
        table: TableId,
        active_pins: &[Timestamp],
    ) -> ManifestResult<CurrentRootSet> {
        self.version
            .current_root_set_with_pins(table, active_pins)
            .await
    }

    /// Snapshot a specific historical version by its manifest timestamp.
    ///
    /// Unlike `snapshot_latest` which always returns the current head version,
    /// this method loads the exact version that was committed at `manifest_ts`.
    pub(crate) async fn snapshot_at_version(
        &self,
        table: TableId,
        manifest_ts: Timestamp,
    ) -> ManifestResult<TableSnapshot> {
        let version_snapshot = self.version.snapshot_at_version(table, manifest_ts).await?;
        let table_meta = self.catalog.table_meta(table).await?;
        Ok(TableSnapshot::from_parts(version_snapshot, table_meta))
    }
}

/// Raw helper used by tests needing direct access to the concrete manifest.
#[cfg(all(test, feature = "tokio"))]
pub(super) async fn init_in_memory_manifest_raw<E>(
    schema_version: u32,
    file_ids: &FileIdGenerator,
    executor: E,
) -> ManifestResult<(InMemoryManifest<E>, TableId)>
where
    E: Executor + Timer + Clone + 'static,
    LeaseStoreImpl<InMemoryFs, E>: fusio_manifest::LeaseStore,
{
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "head.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), executor.clone());
    let ctx = Arc::new(ManifestContext::new(executor));
    let manifest = Manifest::open(Stores::new(head, segment, checkpoint, lease), ctx);
    let table_id = TableId::new(file_ids);
    manifest
        .init_table_head(
            table_id,
            TableHead {
                table_id,
                schema_version,
                wal_floor: None,
                last_manifest_txn: None,
            },
        )
        .await?;
    Ok((manifest, table_id))
}

/// Construct an in-memory manifest.
#[cfg(test)]
pub(crate) async fn init_in_memory_manifest<E>(
    executor: E,
) -> ManifestResult<TonboManifest<InMemoryFs, E>>
where
    E: Executor + Timer + Clone + 'static,
    LeaseStoreImpl<InMemoryFs, E>: fusio_manifest::LeaseStore,
    <InMemoryFs as Fs>::File: fusio::durability::FileCommit,
{
    let root = Path::default();
    init_fs_manifest(InMemoryFs::new(), &root, executor).await
}

/// Construct a manifest rooted under `root/manifest` using the provided filesystem backend.
#[instrument(
    name = "manifest::init_fs_manifest",
    skip(fs, root, executor),
    fields(component = "manifest")
)]
pub(crate) async fn init_fs_manifest<FS, E>(
    fs: FS,
    root: &Path,
    executor: E,
) -> ManifestResult<TonboManifest<FS, E>>
where
    FS: super::ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, E>: fusio_manifest::LeaseStore,
    <FS as Fs>::File: fusio::durability::FileCommit,
{
    let manifest_root = append_segment(root, "manifest");
    let version_root = append_segment(&manifest_root, "version");
    let catalog_root = append_segment(&manifest_root, "catalog");
    let gc_root = append_segment(&manifest_root, "gc");

    ensure_manifest_dirs::<FS>(&version_root).await?;
    ensure_manifest_dirs::<FS>(&catalog_root).await?;
    ensure_manifest_dirs::<FS>(&gc_root).await?;

    let version_manifest =
        open_manifest_instance::<FS, VersionCodec, E>(fs.clone(), &version_root, executor.clone());
    let catalog_manifest =
        open_manifest_instance::<FS, CatalogCodec, E>(fs.clone(), &catalog_root, executor.clone());
    let gc_manifest = open_manifest_instance::<FS, GcPlanCodec, E>(fs.clone(), &gc_root, executor);
    let tonbo = TonboManifest::new(version_manifest, catalog_manifest, gc_manifest);
    tonbo.init_catalog().await?;
    log_debug!(component = "manifest", event = "manifest_initialized",);
    Ok(tonbo)
}

/// Convenience wrapper for in-memory manifests (tests/dev).
#[cfg(all(test, feature = "tokio"))]
pub(crate) async fn init_fs_manifest_in_memory<E>(
    fs: InMemoryFs,
    root: &Path,
    executor: E,
) -> ManifestResult<TonboManifest<InMemoryFs, E>>
where
    E: Executor + Timer + Clone + 'static,
{
    init_fs_manifest(fs, root, executor).await
}

async fn ensure_dir_path<FS>(path: &Path) -> ManifestResult<()>
where
    FS: Fs,
{
    FS::create_dir_all(path)
        .await
        .map_err(|err| ManifestError::Backend(FusioManifestError::Io(err)))?;
    Ok(())
}

pub(crate) async fn ensure_manifest_dirs<FS>(base: &Path) -> ManifestResult<()>
where
    FS: Fs,
{
    ensure_dir_path::<FS>(base).await?;
    ensure_dir_path::<FS>(&base.child(PathPart::parse("segments").expect("segments part"))).await?;
    ensure_dir_path::<FS>(&base.child(PathPart::parse("checkpoints").expect("checkpoints part")))
        .await?;
    ensure_dir_path::<FS>(&base.child(PathPart::parse("leases").expect("leases part"))).await?;
    Ok(())
}

fn open_manifest_instance<FS, C, E>(
    fs: FS,
    prefix: &Path,
    executor: E,
) -> ManifestInstance<C, FS, E>
where
    C: ManifestCodec,
    FS: Fs + FsCas + Clone + MaybeSend + MaybeSync + 'static,
    E: Executor + Timer + Clone + 'static,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, E>: fusio_manifest::LeaseStore,
    <FS as Fs>::File: fusio::durability::FileCommit,
{
    let head = HeadStoreImpl::new(
        fs.clone(),
        prefix.child(PathPart::parse("head.json").expect("head")),
    );
    let segment = SegmentStoreImpl::new(
        fs.clone(),
        prefix.child(PathPart::parse("segments").expect("segments")),
    );
    let checkpoint = CheckpointStoreImpl::new(
        fs.clone(),
        prefix.child(PathPart::parse("checkpoints").expect("checkpoints")),
    );
    // LeaseStoreImpl appends the `leases/` suffix internally; pass the manifest prefix so
    // lease records land under `<prefix>/leases/...` alongside other manifest metadata.
    let lease_prefix = prefix.as_ref().to_string();
    let lease = LeaseStoreImpl::new(fs, lease_prefix, BackoffPolicy::default(), executor.clone());
    let ctx = Arc::new(ManifestContext::new(executor));
    Manifest::open(Stores::new(head, segment, checkpoint, lease), ctx)
}

fn append_segment(base: &Path, segment: &str) -> Path {
    if base.as_ref().is_empty() {
        Path::parse(segment).expect("segment path")
    } else {
        base.child(PathPart::parse(segment).expect("segment part"))
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::time::Duration;

    use fusio::{
        disk::LocalFs,
        executor::tokio::TokioExecutor,
        mem::fs::InMemoryFs,
        path::{Path as FusioPath, PathPart},
    };
    use fusio_manifest::LeaseStore;
    use futures::TryStreamExt;

    use super::*;
    use crate::{
        id::FileIdGenerator,
        manifest::{SstEntry, TableDefinition, VersionEdit},
        mvcc::Timestamp,
        ondisk::sstable::{SsTableId, SsTableStats},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn init_disk_manifest_smoke() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = FusioPath::from_filesystem_path(dir.path()).expect("fs path");
        init_fs_manifest(LocalFs {}, &path, TokioExecutor::default())
            .await
            .expect("disk manifest");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lease_store_writes_under_leases_prefix() {
        let fs = InMemoryFs::new();
        let root = FusioPath::default();
        let manifest_root = append_segment(&root, "manifest");
        let version_root = append_segment(&manifest_root, "version");
        ensure_manifest_dirs::<InMemoryFs>(&version_root)
            .await
            .expect("dirs created");

        let executor = TokioExecutor::default();
        let lease_store = LeaseStoreImpl::new(
            fs.clone(),
            version_root.as_ref().to_string(),
            BackoffPolicy::default(),
            executor,
        );
        let lease = lease_store
            .create(1, None, Duration::from_secs(30))
            .await
            .expect("lease created");

        let leases_dir = version_root.child(PathPart::parse("leases").expect("leases part"));
        let files: Vec<_> = fs
            .list(&leases_dir)
            .await
            .expect("list leases dir")
            .try_collect()
            .await
            .expect("collect leases dir entries");

        assert!(
            files
                .iter()
                .any(|meta| meta.path.as_ref().contains(&lease.id.0)),
            "lease documents must be placed under the leases directory"
        );
        assert!(
            files
                .iter()
                .all(|meta| meta.path.as_ref().starts_with(leases_dir.as_ref())),
            "all lease files should live under the leases/ prefix"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manifest_history_remains_queryable_while_root_set_defaults_to_head() {
        let manifest = init_in_memory_manifest(TokioExecutor::default())
            .await
            .expect("manifest");
        let file_ids = FileIdGenerator::default();
        let table = manifest
            .register_table(
                &file_ids,
                &TableDefinition {
                    name: "manifest-history".into(),
                    schema_fingerprint: "fingerprint".into(),
                    primary_key_columns: vec!["id".into()],
                    schema_version: 1,
                },
            )
            .await
            .expect("register table")
            .table_id;

        manifest
            .apply_version_edits(
                table,
                &[VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![SstEntry::new(
                        SsTableId::new(1),
                        Some(SsTableStats::default()),
                        None,
                        FusioPath::from("L0/1.parquet"),
                        None,
                    )],
                }],
            )
            .await
            .expect("first version");
        manifest
            .apply_version_edits(
                table,
                &[
                    VersionEdit::RemoveSsts {
                        level: 0,
                        sst_ids: vec![SsTableId::new(1)],
                    },
                    VersionEdit::AddSsts {
                        level: 1,
                        entries: vec![SstEntry::new(
                            SsTableId::new(2),
                            Some(SsTableStats::default()),
                            None,
                            FusioPath::from("L1/2.parquet"),
                            None,
                        )],
                    },
                ],
            )
            .await
            .expect("second version");
        manifest
            .apply_version_edits(
                table,
                &[
                    VersionEdit::RemoveSsts {
                        level: 1,
                        sst_ids: vec![SsTableId::new(2)],
                    },
                    VersionEdit::AddSsts {
                        level: 2,
                        entries: vec![SstEntry::new(
                            SsTableId::new(3),
                            Some(SsTableStats::default()),
                            None,
                            FusioPath::from("L2/3.parquet"),
                            None,
                        )],
                    },
                ],
            )
            .await
            .expect("third version");

        let versions = manifest
            .list_versions(table, 10)
            .await
            .expect("list versions");
        assert_eq!(
            versions
                .iter()
                .map(VersionState::commit_timestamp)
                .collect::<Vec<_>>(),
            vec![Timestamp::new(3), Timestamp::new(2), Timestamp::new(1)]
        );

        let snapshot = manifest
            .snapshot_at_version(table, Timestamp::new(1))
            .await
            .expect("historical version should remain available");
        assert_eq!(
            snapshot
                .latest_version
                .as_ref()
                .map(VersionState::commit_timestamp),
            Some(Timestamp::new(1))
        );

        let root_set = manifest.current_root_set(table).await.expect("root set");
        assert_eq!(root_set.protected_version_count(), 1);
        assert!(
            !root_set.contains_path(&FusioPath::from("L0/1.parquet")),
            "historical SSTs should not be protected unless a live snapshot pins them",
        );
        assert!(root_set.contains_path(&FusioPath::from("L2/3.parquet")));
    }
}
