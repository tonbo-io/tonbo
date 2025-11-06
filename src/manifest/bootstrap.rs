use std::sync::Arc;

use fusio::{
    fs::{Fs, FsCas},
    mem::fs::InMemoryFs,
    path::Path,
};
use fusio_manifest::{
    BackoffPolicy, BlockingExecutor, CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl,
    ManifestContext, SegmentStoreImpl, types::Error as FusioManifestError,
};
use futures::{executor::block_on, future::BoxFuture};

use super::{
    codec::VersionCodec,
    domain::{TableHead, TableId, VersionState, WalSegmentRef},
    driver::{Manifest, ManifestError, ManifestResult, Stores, TableSnapshot},
    version::VersionEdit,
};
use crate::mvcc::Timestamp;

/// In-memory manifest type wired against `fusio`'s memory FS for dev/test flows.
pub(crate) type InMemoryManifest = Manifest<
    VersionCodec,
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
>;

/// Trait object abstraction over manifest backends.
pub(crate) trait ManifestRuntime: Send + Sync {
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

    fn snapshot_latest<'a>(
        &'a self,
        table: TableId,
    ) -> BoxFuture<'a, ManifestResult<TableSnapshot>>;

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

/// Primary manifest handle exposed to the rest of the crate.
#[derive(Clone)]
pub(crate) struct TonboManifest {
    inner: Arc<dyn ManifestRuntime>,
}

impl TonboManifest {
    fn new(runtime: Arc<dyn ManifestRuntime>) -> Self {
        Self { inner: runtime }
    }

    pub(crate) async fn apply_version_edits(
        &self,
        table: TableId,
        edits: &[VersionEdit],
    ) -> ManifestResult<Timestamp> {
        self.inner.apply_version_edits(table, edits).await
    }

    pub(crate) async fn snapshot_latest(&self, table: TableId) -> ManifestResult<TableSnapshot> {
        self.inner.snapshot_latest(table).await
    }

    #[allow(dead_code)]
    pub(crate) async fn list_versions(
        &self,
        table: TableId,
        limit: usize,
    ) -> ManifestResult<Vec<VersionState>> {
        self.inner.list_versions(table, limit).await
    }

    #[allow(dead_code)]
    pub(crate) async fn recover_orphans(&self) -> ManifestResult<usize> {
        self.inner.recover_orphans().await
    }

    pub(crate) async fn wal_floor(&self, table: TableId) -> ManifestResult<Option<WalSegmentRef>> {
        self.inner.wal_floor(table).await
    }

    async fn init_table_head(&self, table_id: TableId, head: TableHead) -> ManifestResult<()> {
        self.inner.init_table_head(table_id, head).await
    }
}

/// Adapter allowing concrete `Manifest` instances to be used behind the trait object.
pub(crate) struct ManifestHandle<M>(M);

impl<HS, SS, CS, LS> ManifestRuntime for ManifestHandle<Manifest<VersionCodec, HS, SS, CS, LS>>
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

    fn snapshot_latest<'a>(
        &'a self,
        table: TableId,
    ) -> BoxFuture<'a, ManifestResult<TableSnapshot>> {
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

fn wrap_manifest<M>(manifest: M) -> TonboManifest
where
    ManifestHandle<M>: ManifestRuntime,
    M: Send + Sync + 'static,
{
    TonboManifest::new(Arc::new(ManifestHandle(manifest)))
}

fn bootstrap_manifest<M>(
    manifest: M,
    schema_version: u32,
) -> ManifestResult<(TonboManifest, TableId)>
where
    ManifestHandle<M>: ManifestRuntime,
    M: Send + Sync + 'static,
{
    let table_id = TableId::new();
    let tonbo = wrap_manifest(manifest);
    block_on(async {
        tonbo
            .init_table_head(
                table_id,
                TableHead {
                    table_id,
                    schema_version,
                    wal_floor: None,
                    last_manifest_txn: None,
                },
            )
            .await
    })?;
    Ok((tonbo, table_id))
}

/// Raw helper used by tests needing direct access to the concrete manifest.
pub(crate) fn init_in_memory_manifest_raw(
    schema_version: u32,
) -> ManifestResult<(InMemoryManifest, TableId)> {
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "head.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), BlockingExecutor);
    let ctx = Arc::new(ManifestContext::new(BlockingExecutor));
    let manifest = Manifest::open(Stores::new(head, segment, checkpoint, lease), ctx);
    let table_id = TableId::new();
    block_on(async {
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
            .await
    })?;
    Ok((manifest, table_id))
}

/// Construct an in-memory manifest and wrap it in the dynamic handle.
pub(crate) fn init_in_memory_manifest(
    schema_version: u32,
) -> ManifestResult<(TonboManifest, TableId)> {
    let (manifest, table_id) = init_in_memory_manifest_raw(schema_version)?;
    Ok((wrap_manifest(manifest), table_id))
}

/// Construct a manifest rooted under `root/manifest` using the provided filesystem backend.
pub(crate) fn init_fs_manifest<FS>(
    fs: FS,
    root: &Path,
    schema_version: u32,
) -> ManifestResult<(TonboManifest, TableId)>
where
    FS: Fs + FsCas + Clone + Send + Sync + 'static,
    HeadStoreImpl<FS>: fusio_manifest::HeadStore,
    SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
    CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
    LeaseStoreImpl<FS, BlockingExecutor>: fusio_manifest::LeaseStore,
{
    let base = root.as_ref();
    let manifest_prefix = if base.is_empty() {
        "manifest".to_string()
    } else {
        format!("{base}/manifest")
    };
    ensure_dir::<FS>(&manifest_prefix)?;
    ensure_dir::<FS>(&format!("{manifest_prefix}/segments"))?;
    ensure_dir::<FS>(&format!("{manifest_prefix}/checkpoints"))?;
    ensure_dir::<FS>(&format!("{manifest_prefix}/leases"))?;

    let head_key = format!("{manifest_prefix}/head.json");
    let segments_prefix = format!("{manifest_prefix}/segments");
    let checkpoint_prefix = manifest_prefix.clone();
    let lease_prefix = manifest_prefix.clone();

    let head = HeadStoreImpl::new(fs.clone(), head_key);
    let segment = SegmentStoreImpl::new(fs.clone(), segments_prefix);
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), checkpoint_prefix.clone());
    let lease = LeaseStoreImpl::new(fs, lease_prefix, BackoffPolicy::default(), BlockingExecutor);
    let ctx = Arc::new(ManifestContext::new(BlockingExecutor));
    let manifest = Manifest::open(Stores::new(head, segment, checkpoint, lease), ctx);
    bootstrap_manifest(manifest, schema_version)
}

fn ensure_dir<FS>(path: &str) -> ManifestResult<()>
where
    FS: Fs,
{
    let parsed = Path::from(path);
    block_on(async { FS::create_dir_all(&parsed).await })
        .map_err(|err| ManifestError::Backend(FusioManifestError::Io(err)))?;
    Ok(())
}
