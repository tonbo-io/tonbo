use std::sync::Arc;

#[cfg(any(test, feature = "test-helpers"))]
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

use super::{
    ManifestFs,
    codec::{CatalogCodec, GcPlanCodec, ManifestCodec, VersionCodec},
    domain::{
        GcPlanState, TableDefinition, TableHead, TableId, TableMeta, VersionState, WalSegmentRef,
    },
    driver::{Manifest, ManifestError, ManifestResult, Stores, VersionSnapshot},
    version::VersionEdit,
};
use crate::{id::FileIdGenerator, mvcc::Timestamp};

/// Concrete manifest type alias for a given filesystem and executor.
pub(crate) type ManifestInstance<C, FS, E> = Manifest<
    C,
    HeadStoreImpl<FS>,
    SegmentStoreImpl<FS>,
    CheckpointStoreImpl<FS>,
    LeaseStoreImpl<FS, E>,
    E,
>;

/// In-memory manifest type wired against `fusio`'s memory FS for dev/test flows.
#[cfg(all(test, feature = "tokio"))]
pub(crate) type InMemoryManifest<E> = ManifestInstance<VersionCodec, InMemoryFs, E>;

/// In-memory catalog manifest type used for catalog-specific tests.
#[cfg(all(test, feature = "tokio"))]
pub(crate) type InMemoryCatalogManifest<E> = ManifestInstance<CatalogCodec, InMemoryFs, E>;

/// Snapshot combining catalog metadata with the latest version state.
#[derive(Debug, Clone)]
pub(crate) struct TableSnapshot {
    pub _manifest_snapshot: Snapshot,
    pub head: TableHead,
    pub latest_version: Option<VersionState>,

    pub _table_meta: TableMeta,
}

impl TableSnapshot {
    pub(crate) fn from_parts(version: VersionSnapshot, table_meta: TableMeta) -> Self {
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
        Ok(meta)
    }

    pub(crate) async fn record_gc_plan(
        &self,
        table: TableId,
        plan: GcPlanState,
    ) -> ManifestResult<()> {
        self.gc_plan.put_gc_plan(table, plan).await
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) async fn take_gc_plan(&self, table: TableId) -> ManifestResult<Option<GcPlanState>> {
        self.gc_plan.take_gc_plan(table).await
    }
}

/// Raw helper used by tests needing direct access to the concrete manifest.
#[cfg(all(test, feature = "tokio"))]
pub(crate) async fn init_in_memory_manifest_raw<E>(
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
#[cfg(any(test, feature = "test-helpers"))]
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
}
