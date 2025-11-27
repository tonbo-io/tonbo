use std::sync::Arc;

use fusio::{
    fs::{Fs, FsCas},
    mem::fs::InMemoryFs,
    path::{Path, PathPart},
};
use fusio_manifest::{
    BackoffPolicy, BlockingExecutor, CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl,
    ManifestContext, SegmentStoreImpl, snapshot::Snapshot, types::Error as FusioManifestError,
};
use futures::future::BoxFuture;

use super::{
    codec::{CatalogCodec, GcPlanCodec, ManifestCodec, VersionCodec},
    domain::{
        GcPlanState, TableDefinition, TableHead, TableId, TableMeta, VersionState, WalSegmentRef,
    },
    driver::{Manifest, ManifestError, ManifestResult, Stores, VersionSnapshot},
    version::VersionEdit,
};
use crate::{id::FileIdGenerator, mvcc::Timestamp};

/// In-memory manifest type wired against `fusio`'s memory FS for dev/test flows.
#[cfg(test)]
pub(crate) type InMemoryManifest = Manifest<
    VersionCodec,
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
>;

/// In-memory catalog manifest type used for catalog-specific tests.
#[cfg(test)]
pub(crate) type InMemoryCatalogManifest = Manifest<
    CatalogCodec,
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
>;

/// Snapshot combining catalog metadata with the latest version state.
#[derive(Debug, Clone)]
pub(crate) struct TableSnapshot {
    pub manifest_snapshot: Snapshot,
    pub head: TableHead,
    pub latest_version: Option<VersionState>,
    #[allow(dead_code)]
    pub table_meta: TableMeta,
}

impl TableSnapshot {
    fn from_parts(version: VersionSnapshot, table_meta: TableMeta) -> Self {
        Self {
            manifest_snapshot: version.manifest_snapshot,
            head: version.head,
            latest_version: version.latest_version,
            table_meta,
        }
    }
}

/// Trait object abstraction over version manifest backends.
pub(crate) trait VersionRuntime: Send + Sync {
    fn init_table_head<'a>(
        &'a self,
        table_id: TableId,
        head: TableHead,
    ) -> BoxFuture<'a, ManifestResult<()>>;

    fn apply_version_edits<'a>(
        &'a self,
        table: TableId,
        edits: &'a [VersionEdit],
    ) -> BoxFuture<'a, ManifestResult<Timestamp>>;

    fn apply_version_edits_cas<'a>(
        &'a self,
        table: TableId,
        expected_head: Option<Timestamp>,
        edits: &'a [VersionEdit],
    ) -> BoxFuture<'a, ManifestResult<Timestamp>>;

    fn snapshot_latest<'a>(
        &'a self,
        table: TableId,
    ) -> BoxFuture<'a, ManifestResult<VersionSnapshot>>;

    #[allow(dead_code)]
    fn list_versions<'a>(
        &'a self,
        table: TableId,
        limit: usize,
    ) -> BoxFuture<'a, ManifestResult<Vec<VersionState>>>;

    #[allow(dead_code)]
    fn recover_orphans<'a>(&'a self) -> BoxFuture<'a, ManifestResult<usize>>;

    fn wal_floor<'a>(
        &'a self,
        table: TableId,
    ) -> BoxFuture<'a, ManifestResult<Option<WalSegmentRef>>>;
}

/// Trait object abstraction over GC plan manifest backends.
pub(crate) trait GcPlanRuntime: Send + Sync {
    fn put_gc_plan<'a>(
        &'a self,
        table_id: TableId,
        plan: GcPlanState,
    ) -> BoxFuture<'a, ManifestResult<()>>;

    #[allow(dead_code)]
    fn take_gc_plan<'a>(
        &'a self,
        table_id: TableId,
    ) -> BoxFuture<'a, ManifestResult<Option<GcPlanState>>>;
}

/// Trait object abstraction over catalog manifest backends.
pub(crate) trait CatalogRuntime: Send + Sync {
    fn init_catalog_root<'a>(&'a self) -> BoxFuture<'a, ManifestResult<()>>;

    fn register_table<'a>(
        &'a self,
        file_ids: &'a FileIdGenerator,
        definition: &'a TableDefinition,
    ) -> BoxFuture<'a, ManifestResult<TableMeta>>;

    fn table_meta<'a>(&'a self, table: TableId) -> BoxFuture<'a, ManifestResult<TableMeta>>;
}

/// Primary manifest handle exposed to the rest of the crate.
#[derive(Clone)]
pub(crate) struct TonboManifest {
    version: Arc<dyn VersionRuntime>,
    catalog: Arc<dyn CatalogRuntime>,
    gc_plan: Arc<dyn GcPlanRuntime>,
}

impl TonboManifest {
    fn new(
        version: Arc<dyn VersionRuntime>,
        catalog: Arc<dyn CatalogRuntime>,
        gc_plan: Arc<dyn GcPlanRuntime>,
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub(crate) async fn list_versions(
        &self,
        table: TableId,
        limit: usize,
    ) -> ManifestResult<Vec<VersionState>> {
        self.version.list_versions(table, limit).await
    }

    #[allow(dead_code)]
    pub(crate) async fn recover_orphans(&self) -> ManifestResult<usize> {
        self.version.recover_orphans().await
    }

    pub(crate) async fn wal_floor(&self, table: TableId) -> ManifestResult<Option<WalSegmentRef>> {
        self.version.wal_floor(table).await
    }

    async fn init_table_head(&self, table_id: TableId, head: TableHead) -> ManifestResult<()> {
        self.version.init_table_head(table_id, head).await
    }

    pub(crate) async fn init_catalog(&self) -> ManifestResult<()> {
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

    #[allow(dead_code)]
    pub(crate) async fn table_meta(&self, table: TableId) -> ManifestResult<TableMeta> {
        self.catalog.table_meta(table).await
    }

    pub(crate) async fn record_gc_plan(
        &self,
        table: TableId,
        plan: GcPlanState,
    ) -> ManifestResult<()> {
        self.gc_plan.put_gc_plan(table, plan).await
    }

    #[allow(dead_code)]
    pub(crate) async fn take_gc_plan(&self, table: TableId) -> ManifestResult<Option<GcPlanState>> {
        self.gc_plan.take_gc_plan(table).await
    }
}

/// Adapter allowing concrete `Manifest` instances to be used behind the trait object.
pub(crate) struct ManifestHandle<M>(M);

impl<HS, SS, CS, LS> VersionRuntime for ManifestHandle<Manifest<VersionCodec, HS, SS, CS, LS>>
where
    HS: fusio_manifest::HeadStore + Send + Sync + Clone + 'static,
    SS: fusio_manifest::SegmentIo + Send + Sync + Clone + 'static,
    CS: fusio_manifest::CheckpointStore + Send + Sync + Clone + 'static,
    LS: fusio_manifest::LeaseStore + Send + Sync + Clone + 'static,
{
    fn init_table_head<'a>(
        &'a self,
        table_id: TableId,
        head: TableHead,
    ) -> BoxFuture<'a, ManifestResult<()>> {
        Box::pin(async move { self.0.init_table_head(table_id, head).await })
    }

    fn apply_version_edits<'a>(
        &'a self,
        table: TableId,
        edits: &'a [VersionEdit],
    ) -> BoxFuture<'a, ManifestResult<Timestamp>> {
        Box::pin(async move { self.0.apply_version_edits(table, edits).await })
    }

    fn apply_version_edits_cas<'a>(
        &'a self,
        table: TableId,
        expected_head: Option<Timestamp>,
        edits: &'a [VersionEdit],
    ) -> BoxFuture<'a, ManifestResult<Timestamp>> {
        Box::pin(async move {
            self.0
                .apply_version_edits_cas(table, expected_head, edits)
                .await
        })
    }

    fn snapshot_latest<'a>(
        &'a self,
        table: TableId,
    ) -> BoxFuture<'a, ManifestResult<VersionSnapshot>> {
        Box::pin(async move { self.0.snapshot_latest(table).await })
    }

    fn list_versions<'a>(
        &'a self,
        table: TableId,
        limit: usize,
    ) -> BoxFuture<'a, ManifestResult<Vec<VersionState>>> {
        Box::pin(async move { self.0.list_versions(table, limit).await })
    }

    fn recover_orphans<'a>(&'a self) -> BoxFuture<'a, ManifestResult<usize>> {
        Box::pin(async move { self.0.recover_orphans().await })
    }

    fn wal_floor<'a>(
        &'a self,
        table: TableId,
    ) -> BoxFuture<'a, ManifestResult<Option<WalSegmentRef>>> {
        Box::pin(async move { self.0.wal_floor(table).await })
    }
}

/// Adapter allowing GC-plan manifests to participate in the trait object abstraction.
pub(crate) struct GcPlanHandle<M>(M);

impl<HS, SS, CS, LS> GcPlanRuntime for GcPlanHandle<Manifest<GcPlanCodec, HS, SS, CS, LS>>
where
    HS: fusio_manifest::HeadStore + Send + Sync + Clone + 'static,
    SS: fusio_manifest::SegmentIo + Send + Sync + Clone + 'static,
    CS: fusio_manifest::CheckpointStore + Send + Sync + Clone + 'static,
    LS: fusio_manifest::LeaseStore + Send + Sync + Clone + 'static,
{
    fn put_gc_plan<'a>(
        &'a self,
        table_id: TableId,
        plan: GcPlanState,
    ) -> BoxFuture<'a, ManifestResult<()>> {
        Box::pin(async move { self.0.put_gc_plan(table_id, plan).await })
    }

    fn take_gc_plan<'a>(
        &'a self,
        table_id: TableId,
    ) -> BoxFuture<'a, ManifestResult<Option<GcPlanState>>> {
        Box::pin(async move { self.0.take_gc_plan(table_id).await })
    }
}

/// Adapter allowing catalog manifests to participate in the trait object abstraction.
pub(crate) struct CatalogHandle<M>(M);

impl<HS, SS, CS, LS> CatalogRuntime for CatalogHandle<Manifest<CatalogCodec, HS, SS, CS, LS>>
where
    HS: fusio_manifest::HeadStore + Send + Sync + Clone + 'static,
    SS: fusio_manifest::SegmentIo + Send + Sync + Clone + 'static,
    CS: fusio_manifest::CheckpointStore + Send + Sync + Clone + 'static,
    LS: fusio_manifest::LeaseStore + Send + Sync + Clone + 'static,
{
    fn init_catalog_root<'a>(&'a self) -> BoxFuture<'a, ManifestResult<()>> {
        Box::pin(async move { self.0.init_catalog_root().await })
    }

    fn register_table<'a>(
        &'a self,
        file_ids: &'a FileIdGenerator,
        definition: &'a TableDefinition,
    ) -> BoxFuture<'a, ManifestResult<TableMeta>> {
        Box::pin(async move { self.0.register_table(file_ids, definition).await })
    }

    fn table_meta<'a>(&'a self, table: TableId) -> BoxFuture<'a, ManifestResult<TableMeta>> {
        Box::pin(async move { self.0.table_meta(table).await })
    }
}

fn wrap_version_manifest<M>(manifest: M) -> Arc<dyn VersionRuntime>
where
    ManifestHandle<M>: VersionRuntime,
    M: Send + Sync + 'static,
{
    Arc::new(ManifestHandle(manifest))
}

fn wrap_catalog_manifest<M>(manifest: M) -> Arc<dyn CatalogRuntime>
where
    CatalogHandle<M>: CatalogRuntime,
    M: Send + Sync + 'static,
{
    Arc::new(CatalogHandle(manifest))
}

fn wrap_gc_plan_manifest<M>(manifest: M) -> Arc<dyn GcPlanRuntime>
where
    GcPlanHandle<M>: GcPlanRuntime,
    M: Send + Sync + 'static,
{
    Arc::new(GcPlanHandle(manifest))
}

/// Raw helper used by tests needing direct access to the concrete manifest.
#[cfg(test)]
pub(crate) async fn init_in_memory_manifest_raw(
    schema_version: u32,
    file_ids: &FileIdGenerator,
) -> ManifestResult<(InMemoryManifest, TableId)> {
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "head.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), BlockingExecutor);
    let ctx = Arc::new(ManifestContext::new(BlockingExecutor));
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

/// Construct an in-memory manifest and wrap it in the dynamic handle.
pub(crate) async fn init_in_memory_manifest() -> ManifestResult<TonboManifest> {
    let root = Path::default();
    init_fs_manifest(InMemoryFs::new(), &root).await
}

/// Construct a manifest rooted under `root/manifest` using the provided filesystem backend.
pub(crate) async fn init_fs_manifest<FS>(fs: FS, root: &Path) -> ManifestResult<TonboManifest>
where
    FS: Fs + FsCas + Clone + Send + Sync + 'static,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, BlockingExecutor>: fusio_manifest::LeaseStore,
{
    let manifest_root = append_segment(root, "manifest");
    let version_root = append_segment(&manifest_root, "version");
    let catalog_root = append_segment(&manifest_root, "catalog");
    let gc_root = append_segment(&manifest_root, "gc");

    ensure_manifest_dirs::<FS>(&version_root).await?;
    ensure_manifest_dirs::<FS>(&catalog_root).await?;
    ensure_manifest_dirs::<FS>(&gc_root).await?;

    let version_manifest = open_manifest_instance::<FS, VersionCodec>(fs.clone(), &version_root);
    let catalog_manifest = open_manifest_instance::<FS, CatalogCodec>(fs.clone(), &catalog_root);
    let gc_manifest = open_manifest_instance::<FS, GcPlanCodec>(fs, &gc_root);
    let tonbo = TonboManifest::new(
        wrap_version_manifest(version_manifest),
        wrap_catalog_manifest(catalog_manifest),
        wrap_gc_plan_manifest(gc_manifest),
    );
    tonbo.init_catalog().await?;
    Ok(tonbo)
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

async fn ensure_manifest_dirs<FS>(base: &Path) -> ManifestResult<()>
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

#[allow(clippy::type_complexity)]
fn open_manifest_instance<FS, C>(
    fs: FS,
    prefix: &Path,
) -> Manifest<
    C,
    HeadStoreImpl<FS>,
    SegmentStoreImpl<FS>,
    CheckpointStoreImpl<FS>,
    LeaseStoreImpl<FS, BlockingExecutor>,
>
where
    C: ManifestCodec,
    FS: Fs + FsCas + Clone + Send + Sync + 'static,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, BlockingExecutor>: fusio_manifest::LeaseStore,
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
    let lease = LeaseStoreImpl::new(fs, lease_prefix, BackoffPolicy::default(), BlockingExecutor);
    let ctx = Arc::new(ManifestContext::new(BlockingExecutor));
    Manifest::open(Stores::new(head, segment, checkpoint, lease), ctx)
}

fn append_segment(base: &Path, segment: &str) -> Path {
    if base.as_ref().is_empty() {
        Path::parse(segment).expect("segment path")
    } else {
        base.child(PathPart::parse(segment).expect("segment part"))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fusio::{
        disk::LocalFs,
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
        init_fs_manifest(LocalFs {}, &path)
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

        let lease_store = LeaseStoreImpl::new(
            fs.clone(),
            version_root.as_ref().to_string(),
            BackoffPolicy::default(),
            BlockingExecutor,
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
