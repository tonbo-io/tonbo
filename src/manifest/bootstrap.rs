use std::{sync::Arc, time::Duration};

use fusio::{
    dynamic::{MaybeSend, MaybeSync},
    fs::{Fs, FsCas},
    mem::fs::InMemoryFs,
    path::{Path, PathPart},
};
use fusio_manifest::{
    BackoffPolicy, CheckpointStoreImpl, DefaultExecutor, HeadStoreImpl, LeaseHandle,
    LeaseStoreImpl, ManifestContext, NoopExecutor, SegmentStoreImpl, snapshot::Snapshot,
    types::Error as FusioManifestError,
};
#[cfg(not(target_arch = "wasm32"))]
use futures::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures::future::LocalBoxFuture as BoxFuture;

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
    LeaseStoreImpl<InMemoryFs, ManifestExecutor>,
>;

/// In-memory catalog manifest type used for catalog-specific tests.
#[cfg(test)]
pub(crate) type InMemoryCatalogManifest = Manifest<
    CatalogCodec,
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, ManifestExecutor>,
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

type ManifestExecutor = DefaultExecutor;

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
pub(crate) trait VersionRuntime: MaybeSend + MaybeSync {
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
pub(crate) trait GcPlanRuntime: MaybeSend + MaybeSync {
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
pub(crate) trait CatalogRuntime: MaybeSend + MaybeSync {
    fn init_catalog_root<'a>(&'a self) -> BoxFuture<'a, ManifestResult<()>>;

    fn register_table<'a>(
        &'a self,
        file_ids: &'a FileIdGenerator,
        definition: &'a TableDefinition,
    ) -> BoxFuture<'a, ManifestResult<TableMeta>>;

    fn table_meta<'a>(&'a self, table: TableId) -> BoxFuture<'a, ManifestResult<TableMeta>>;
}

/// Lease store abstraction for compaction/GC coordination.
#[allow(dead_code)]
pub(crate) trait LeaseRuntime: Send + Sync {
    fn try_acquire<'a>(
        &'a self,
        snapshot_txn_id: u64,
        ttl: Duration,
    ) -> BoxFuture<'a, ManifestResult<Option<LeaseHandle>>>;

    fn release<'a>(&'a self, lease: LeaseHandle) -> BoxFuture<'a, ManifestResult<()>>;
}

/// Idempotency store abstraction to avoid duplicate work across processes.
pub(crate) trait IdempotencyRuntime: Send + Sync {
    /// Returns true when the key was acquired, false if an unexpired record already exists.
    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, ManifestResult<bool>>;

    /// Release the key after completion.
    fn release<'a>(&'a self, key: &'a str) -> BoxFuture<'a, ManifestResult<()>>;
}

/// Primary manifest handle exposed to the rest of the crate.
#[derive(Clone)]
pub(crate) struct TonboManifest {
    version: Arc<dyn VersionRuntime>,
    catalog: Arc<dyn CatalogRuntime>,
    gc_plan: Arc<dyn GcPlanRuntime>,
    #[allow(dead_code)]
    lease: Arc<dyn LeaseRuntime>,
    idempotency: Arc<dyn IdempotencyRuntime>,
}

impl TonboManifest {
    fn new(
        version: Arc<dyn VersionRuntime>,
        catalog: Arc<dyn CatalogRuntime>,
        gc_plan: Arc<dyn GcPlanRuntime>,
        lease: Arc<dyn LeaseRuntime>,
        idempotency: Arc<dyn IdempotencyRuntime>,
    ) -> Self {
        Self {
            version,
            catalog,
            gc_plan,
            lease,
            idempotency,
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

    #[allow(dead_code)]
    pub(crate) fn lease_runtime(&self) -> Arc<dyn LeaseRuntime> {
        Arc::clone(&self.lease)
    }

    pub(crate) fn idempotency_runtime(&self) -> Arc<dyn IdempotencyRuntime> {
        Arc::clone(&self.idempotency)
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
    HS: fusio_manifest::HeadStore + MaybeSend + MaybeSync + Clone + 'static,
    SS: fusio_manifest::SegmentIo + MaybeSend + MaybeSync + Clone + 'static,
    CS: fusio_manifest::CheckpointStore + MaybeSend + MaybeSync + Clone + 'static,
    LS: fusio_manifest::LeaseStore + MaybeSend + MaybeSync + Clone + 'static,
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
    HS: fusio_manifest::HeadStore + MaybeSend + MaybeSync + Clone + 'static,
    SS: fusio_manifest::SegmentIo + MaybeSend + MaybeSync + Clone + 'static,
    CS: fusio_manifest::CheckpointStore + MaybeSend + MaybeSync + Clone + 'static,
    LS: fusio_manifest::LeaseStore + MaybeSend + MaybeSync + Clone + 'static,
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
    HS: fusio_manifest::HeadStore + MaybeSend + MaybeSync + Clone + 'static,
    SS: fusio_manifest::SegmentIo + MaybeSend + MaybeSync + Clone + 'static,
    CS: fusio_manifest::CheckpointStore + MaybeSend + MaybeSync + Clone + 'static,
    LS: fusio_manifest::LeaseStore + MaybeSend + MaybeSync + Clone + 'static,
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

/// Adapter over a manifest lease store to implement `LeaseRuntime`.
#[allow(dead_code)]
pub(crate) struct LeaseHandleAdapter<LS> {
    store: LS,
}

impl<LS> LeaseHandleAdapter<LS> {
    pub(crate) fn new(store: LS) -> Self {
        Self { store }
    }
}

impl<LS> LeaseRuntime for LeaseHandleAdapter<LS>
where
    LS: fusio_manifest::LeaseStore + Send + Sync + Clone + 'static,
{
    fn try_acquire<'a>(
        &'a self,
        snapshot_txn_id: u64,
        ttl: Duration,
    ) -> BoxFuture<'a, ManifestResult<Option<LeaseHandle>>> {
        Box::pin(async move {
            let lease = self
                .store
                .create(snapshot_txn_id, None, ttl)
                .await
                .map_err(ManifestError::from)?;
            Ok(Some(lease))
        })
    }

    fn release<'a>(&'a self, lease: LeaseHandle) -> BoxFuture<'a, ManifestResult<()>> {
        Box::pin(async move { self.store.release(lease).await.map_err(ManifestError::from) })
    }
}

fn wrap_version_manifest<M>(manifest: M) -> Arc<dyn VersionRuntime>
where
    ManifestHandle<M>: VersionRuntime,
    M: MaybeSend + MaybeSync + 'static,
{
    Arc::new(ManifestHandle(manifest))
}

fn wrap_catalog_manifest<M>(manifest: M) -> Arc<dyn CatalogRuntime>
where
    CatalogHandle<M>: CatalogRuntime,
    M: MaybeSend + MaybeSync + 'static,
{
    Arc::new(CatalogHandle(manifest))
}

fn wrap_gc_plan_manifest<M>(manifest: M) -> Arc<dyn GcPlanRuntime>
where
    GcPlanHandle<M>: GcPlanRuntime,
    M: MaybeSend + MaybeSync + 'static,
{
    Arc::new(GcPlanHandle(manifest))
}

fn wrap_lease_runtime<LS>(store: LS) -> Arc<dyn LeaseRuntime>
where
    LeaseHandleAdapter<LS>: LeaseRuntime,
    LS: Send + Sync + 'static,
{
    Arc::new(LeaseHandleAdapter::new(store))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IdempotencyDoc {
    expires_at_ms: u64,
}

/// Manifest-backed idempotency store using CAS on per-key documents.
pub(crate) struct IdempotencyHandleAdapter<FS> {
    fs: FS,
    prefix: String,
}

impl<FS> IdempotencyHandleAdapter<FS> {
    pub(crate) fn new(fs: FS, prefix: impl Into<String>) -> Self {
        Self {
            fs,
            prefix: prefix.into(),
        }
    }

    fn key_for(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            format!("idempotency/{key}.json")
        } else {
            format!("{}/idempotency/{key}.json", self.prefix)
        }
    }
}

impl<FS> IdempotencyRuntime for IdempotencyHandleAdapter<FS>
where
    FS: Fs + FsCas + Send + Sync + Clone + 'static,
{
    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, ManifestResult<bool>> {
        Box::pin(async move {
            let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            let path = Path::parse(self.key_for(key))
                .map_err(|_| ManifestError::Invariant("idempotency path parse failed"))?;
            let doc = IdempotencyDoc {
                expires_at_ms: now_ms.saturating_add(ttl_ms),
            };
            let body = serde_json::to_vec(&doc)
                .map_err(|_| ManifestError::Invariant("idempotency encode failed"))?;
            match self
                .fs
                .put_conditional(
                    &path,
                    &body,
                    Some("application/json"),
                    None,
                    fusio::fs::CasCondition::IfNotExists,
                )
                .await
            {
                Ok(_) => return Ok(true),
                Err(fusio::Error::PreconditionFailed) => {}
                Err(err) => return Err(ManifestError::Backend(err.into())),
            }

            // Check existing doc; overwrite if expired.
            let (bytes, tag) = match self.fs.load_with_tag(&path).await {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(false),
                Err(err) => return Err(ManifestError::Backend(err.into())),
            };
            let existing: IdempotencyDoc = serde_json::from_slice(&bytes)
                .map_err(|_| ManifestError::Invariant("idempotency decode failed"))?;
            if existing.expires_at_ms > now_ms {
                return Ok(false);
            }
            let body = serde_json::to_vec(&doc)
                .map_err(|_| ManifestError::Invariant("idempotency encode failed"))?;
            match self
                .fs
                .put_conditional(
                    &path,
                    &body,
                    Some("application/json"),
                    None,
                    fusio::fs::CasCondition::IfMatch(tag),
                )
                .await
            {
                Ok(_) => Ok(true),
                Err(fusio::Error::PreconditionFailed) => Ok(false),
                Err(err) => Err(ManifestError::Backend(err.into())),
            }
        })
    }

    fn release<'a>(&'a self, key: &'a str) -> BoxFuture<'a, ManifestResult<()>> {
        Box::pin(async move {
            let path = Path::parse(self.key_for(key))
                .map_err(|_| ManifestError::Invariant("idempotency path parse failed"))?;
            let _ = self.fs.remove(&path).await;
            Ok(())
        })
    }
}

fn wrap_idempotency_runtime<FS>(fs: FS, prefix: impl Into<String>) -> Arc<dyn IdempotencyRuntime>
where
    IdempotencyHandleAdapter<FS>: IdempotencyRuntime,
    FS: Send + Sync + 'static,
{
    Arc::new(IdempotencyHandleAdapter::new(fs, prefix))
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
    let lease = LeaseStoreImpl::new(
        fs,
        "",
        BackoffPolicy::default(),
        ManifestExecutor::default(),
    );
    let ctx = Arc::new(ManifestContext::new(ManifestExecutor::default()));
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
    FS: Fs + FsCas + Clone + MaybeSend + MaybeSync + 'static,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, ManifestExecutor>: fusio_manifest::LeaseStore,
{
    let manifest_root = append_segment(root, "manifest");
    let version_root = append_segment(&manifest_root, "version");
    let catalog_root = append_segment(&manifest_root, "catalog");
    let gc_root = append_segment(&manifest_root, "gc");

    ensure_manifest_dirs::<FS>(&version_root).await?;
    ensure_manifest_dirs::<FS>(&catalog_root).await?;
    ensure_manifest_dirs::<FS>(&gc_root).await?;

    let fs_for_gc = fs.clone();
    let lease_store = LeaseStoreImpl::new(
        fs.clone(),
        version_root.as_ref().to_string(),
        BackoffPolicy::default(),
        NoopExecutor,
    );
    let version_manifest = open_manifest_instance::<FS, VersionCodec>(fs.clone(), &version_root);
    let catalog_manifest = open_manifest_instance::<FS, CatalogCodec>(fs.clone(), &catalog_root);
    let gc_manifest = open_manifest_instance::<FS, GcPlanCodec>(fs_for_gc, &gc_root);
    let lease_runtime = wrap_lease_runtime(lease_store);
    // Keep idempotency records on the manifest filesystem so duplicate triggers are deduped
    // across processes and restarts.
    let idempotency_runtime =
        wrap_idempotency_runtime(fs.clone(), version_root.as_ref().to_string());
    let tonbo = TonboManifest::new(
        wrap_version_manifest(version_manifest),
        wrap_catalog_manifest(catalog_manifest),
        wrap_gc_plan_manifest(gc_manifest),
        lease_runtime,
        idempotency_runtime,
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
    ensure_dir_path::<FS>(&base.child(PathPart::parse("idempotency").expect("idempotency part")))
        .await?;
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
    LeaseStoreImpl<FS, ManifestExecutor>,
>
where
    C: ManifestCodec,
    FS: Fs + FsCas + Clone + MaybeSend + MaybeSync + 'static,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, ManifestExecutor>: fusio_manifest::LeaseStore,
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
    let lease = LeaseStoreImpl::new(
        fs,
        lease_prefix,
        BackoffPolicy::default(),
        ManifestExecutor::default(),
    );
    let ctx = Arc::new(ManifestContext::new(ManifestExecutor::default()));
    Manifest::open(Stores::new(head, segment, checkpoint, lease), ctx)
}

fn append_segment(base: &Path, segment: &str) -> Path {
    if base.as_ref().is_empty() {
        Path::parse(segment).expect("segment path")
    } else {
        base.child(PathPart::parse(segment).expect("segment part"))
    }
}

#[cfg(all(test, feature = "tokio-runtime"))]
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
            ManifestExecutor::default(),
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
