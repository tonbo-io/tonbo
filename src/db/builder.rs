use std::{env, io, sync::Arc, time::Duration};

use fusio::{
    DynFs, Fs,
    dynamic::{MaybeSend, MaybeSync},
    executor::{Executor, Timer},
    fs::FsCas as FusioCas,
    mem::fs::InMemoryFs,
    path::{Path, PathPart},
};
#[cfg(feature = "tokio")]
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor};
use fusio_manifest::{CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl, SegmentStoreImpl};
use thiserror::Error;

use super::{DB, DbInner, MinorCompactionState};
#[cfg(any(test, bench))]
use crate::bench_diagnostics::{BenchDiagnosticsRecorder, BenchObserver};
use crate::{
    compaction::{MinorCompactor, executor::LocalCompactionExecutor, planner::CompactionStrategy},
    extractor::{KeyExtractError, projection_for_columns},
    id::FileIdGenerator,
    manifest::{
        ManifestError, ManifestFs, TonboManifest, bootstrap::ensure_manifest_dirs, init_fs_manifest,
    },
    mode::{DynModeConfig, table_definition},
    ondisk::sstable::SsTableConfig,
    transaction::CommitAckMode,
    wal::{
        WalConfig as RuntimeWalConfig, WalError, WalExt, WalRecoveryMode, WalSyncPolicy,
        state::{FsWalStateStore, WalStateStore},
        storage::WalStorage,
    },
};

/// User-facing overrides for safe WAL tuning knobs.
#[derive(Clone, Default)]
pub struct WalConfig {
    segment_max_bytes: Option<usize>,
    segment_max_age: Option<Option<Duration>>,
    flush_interval: Option<Duration>,
    sync: Option<WalSyncPolicy>,
    recovery: Option<WalRecoveryMode>,
    retention_bytes: Option<Option<usize>>,
    queue_size: Option<usize>,
    wal_dir: Option<Path>,
    segment_backend: Option<Arc<dyn DynFs>>,
    state_store: Option<Option<Arc<dyn WalStateStore>>>,
}

impl WalConfig {
    /// Override the WAL segment size in bytes.
    #[must_use]
    pub fn segment_max_bytes(mut self, bytes: usize) -> Self {
        self.segment_max_bytes = Some(bytes);
        self
    }

    /// Override the maximum age for a WAL segment (or disable with `None`).
    #[must_use]
    pub fn segment_max_age(mut self, age: Option<Duration>) -> Self {
        self.segment_max_age = Some(age);
        self
    }

    /// Override the flush interval for the WAL writer.
    #[must_use]
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = Some(interval);
        self
    }

    /// Override the durability policy applied after each WAL append.
    #[must_use]
    pub fn sync_policy(mut self, policy: WalSyncPolicy) -> Self {
        self.sync = Some(policy);
        self
    }

    /// Override the recovery mode adopted when replaying WAL segments.
    #[must_use]
    pub fn recovery_mode(mut self, mode: WalRecoveryMode) -> Self {
        self.recovery = Some(mode);
        self
    }

    /// Override the soft retention budget for WAL bytes (use `None` to disable).
    #[must_use]
    pub fn retention_bytes(mut self, retention: Option<usize>) -> Self {
        self.retention_bytes = Some(retention);
        self
    }

    /// Override the bounded queue size between clients and the WAL writer.
    #[must_use]
    pub fn queue_size(mut self, size: usize) -> Self {
        self.queue_size = Some(size);
        self
    }

    /// Override the WAL directory resolved by the storage layout.
    #[must_use]
    pub fn wal_dir(mut self, dir: Path) -> Self {
        self.wal_dir = Some(dir);
        self
    }

    /// Override the filesystem implementation backing WAL segments.
    #[must_use]
    pub fn segment_backend(mut self, backend: Arc<dyn DynFs>) -> Self {
        self.segment_backend = Some(backend);
        self
    }

    /// Override the optional WAL state-store binding.
    #[must_use]
    pub fn state_store(mut self, store: Option<Arc<dyn WalStateStore>>) -> Self {
        self.state_store = Some(store);
        self
    }

    fn apply(&self, cfg: &mut RuntimeWalConfig) {
        if let Some(bytes) = self.segment_max_bytes {
            cfg.segment_max_bytes = bytes;
        }
        if let Some(age) = self.segment_max_age {
            cfg.segment_max_age = age;
        }
        if let Some(interval) = self.flush_interval {
            cfg.flush_interval = interval;
        }
        if let Some(policy) = self.sync.clone() {
            cfg.sync = policy;
        }
        if let Some(mode) = self.recovery {
            cfg.recovery = mode;
        }
        if let Some(retention) = self.retention_bytes {
            cfg.retention_bytes = retention;
        }
        if let Some(size) = self.queue_size {
            cfg.queue_size = size;
        }
        if let Some(dir) = self.wal_dir.clone() {
            cfg.dir = dir;
        }
        if let Some(backend) = &self.segment_backend {
            cfg.segment_backend = Arc::clone(backend);
        }
        if let Some(store) = &self.state_store {
            cfg.state_store = store.clone();
        }
    }

    fn merge(&mut self, other: Self) {
        if other.segment_max_bytes.is_some() {
            self.segment_max_bytes = other.segment_max_bytes;
        }
        if other.segment_max_age.is_some() {
            self.segment_max_age = other.segment_max_age;
        }
        if other.flush_interval.is_some() {
            self.flush_interval = other.flush_interval;
        }
        if other.sync.is_some() {
            self.sync = other.sync;
        }
        if other.recovery.is_some() {
            self.recovery = other.recovery;
        }
        if other.retention_bytes.is_some() {
            self.retention_bytes = other.retention_bytes;
        }
        if other.queue_size.is_some() {
            self.queue_size = other.queue_size;
        }
        if other.wal_dir.is_some() {
            self.wal_dir = other.wal_dir;
        }
        if other.segment_backend.is_some() {
            self.segment_backend = other.segment_backend;
        }
        if other.state_store.is_some() {
            self.state_store = other.state_store;
        }
    }
}

pub(super) const DEFAULT_TABLE_NAME: &str = "tonbo-default";

/// Durability classification for storage backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityClass {
    /// In-memory storage; data is lost on process exit.
    Volatile,
    /// Persistent storage with WAL support.
    Durable,
}

impl DurabilityClass {
    fn is_durable(self) -> bool {
        matches!(self, Self::Durable)
    }
}

/// Unconfigured builder state before storage backend selection.
#[derive(Debug, Default)]
pub struct Unconfigured;

/// Generic storage configuration for any filesystem implementing required traits.
///
/// This unified type replaces the previous `InMemoryState`, `DiskDurableState`,
/// and `ObjectDurableState` types. It works with any filesystem that implements
/// `Fs + FusioCas`.
pub struct StorageConfig<FS> {
    fs: Arc<FS>,
    root: Path,
    table_name: Option<String>,
    wal_config: Option<WalConfig>,
    durability: DurabilityClass,
    create_layout: bool,
}

impl<FS> StorageConfig<FS> {
    /// Create a new storage configuration.
    pub fn new(fs: Arc<FS>, root: Path, durability: DurabilityClass) -> Self {
        let wal_config = if durability.is_durable() {
            Some(WalConfig::default())
        } else {
            None
        };
        Self {
            fs,
            root,
            table_name: None,
            wal_config,
            durability,
            create_layout: false,
        }
    }

    /// Set an optional table name.
    #[must_use]
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = Some(name.into());
        self
    }

    /// Set WAL configuration overrides (only meaningful for durable backends).
    #[must_use]
    pub fn with_wal_config(mut self, config: WalConfig) -> Self {
        if let Some(ref mut existing) = self.wal_config {
            existing.merge(config);
        } else if self.durability.is_durable() {
            self.wal_config = Some(config);
        }
        self
    }

    /// Enable automatic creation of the storage layout directories.
    #[must_use]
    pub fn with_create_layout(mut self, enable: bool) -> Self {
        self.create_layout = enable;
        self
    }

    /// Returns the durability class of this storage configuration.
    pub fn durability(&self) -> DurabilityClass {
        self.durability
    }

    /// Returns a reference to the table name if set.
    pub fn table_name(&self) -> Option<&String> {
        self.table_name.as_ref()
    }

    /// Returns a mutable reference to the table name slot.
    pub fn table_name_mut(&mut self) -> &mut Option<String> {
        &mut self.table_name
    }

    /// Returns a reference to the WAL config if present.
    pub fn wal_config(&self) -> Option<&WalConfig> {
        self.wal_config.as_ref()
    }

    /// Returns a mutable reference to the WAL config if present.
    pub fn wal_config_mut(&mut self) -> Option<&mut WalConfig> {
        self.wal_config.as_mut()
    }

    /// Returns whether layout creation is enabled.
    pub fn should_create_layout(&self) -> bool {
        self.create_layout
    }

    /// Execute layout preparation (creating directories) if enabled.
    pub async fn prepare(&self) -> Result<(), DbBuildError>
    where
        FS: Fs,
    {
        if self.create_layout {
            ensure_storage_layout::<FS>(&self.root).await
        } else {
            Ok(())
        }
    }

    /// Build the storage layout from this configuration.
    pub fn layout(&self) -> Result<StorageLayout<FS>, DbBuildError>
    where
        FS: DynFs + FusioCas + 'static,
    {
        if self.root.as_ref().is_empty() {
            return Err(DbBuildError::InvalidPath {
                path: self.root.to_string(),
                reason: "root cannot be empty".into(),
            });
        }
        let cas: Arc<dyn FusioCas> = self.fs.clone();
        Ok(StorageLayout::new(
            self.fs.clone(),
            Some(cas),
            self.root.clone(),
        ))
    }
}

/// Extension helpers for advanced WAL tuning without widening the main builder surface.
pub mod wal_tuning {
    use fusio::{
        DynFs,
        dynamic::{MaybeSend, MaybeSync},
        fs::FsCas as FusioCas,
    };

    use super::{DbBuilder, StorageConfig, WalConfig};

    /// Extension trait exposing WAL overrides for callers that need explicit control.
    pub trait WalConfigExt<FS>: Sized {
        /// Apply a batch of WAL overrides supplied via [`WalConfig`].
        fn wal_config(self, overrides: WalConfig) -> Self;
    }

    impl<FS> WalConfigExt<FS> for DbBuilder<StorageConfig<FS>>
    where
        FS: DynFs + FusioCas + Clone + MaybeSend + MaybeSync + 'static,
    {
        fn wal_config(self, overrides: WalConfig) -> Self {
            DbBuilder::wal_config(self, overrides)
        }
    }
}

/// Ensure required directories exist for storage.
///
/// This creates the standard Tonbo directory layout:
/// - `{root}/wal/` - Write-ahead log segments
/// - `{root}/sst/` - SSTable files
/// - `{root}/manifest/` - Manifest files (version, catalog, gc)
///
/// This function is generic over the filesystem type, allowing it to work
/// with local disk, OPFS, or any other filesystem that implements `Fs`.
async fn ensure_storage_layout<FS>(root: &Path) -> Result<(), DbBuildError>
where
    FS: Fs,
{
    if root.as_ref().is_empty() {
        return Err(DbBuildError::InvalidPath {
            path: root.to_string(),
            reason: "root cannot be empty".into(),
        });
    }

    // Provision required directories via the filesystem's associated functions.
    async fn mk_dir<F: Fs>(path: &Path) -> Result<(), DbBuildError> {
        F::create_dir_all(path)
            .await
            .map_err(|err| DbBuildError::PreparePath {
                path: path.to_string(),
                source: io::Error::other(err.to_string()),
            })
    }

    mk_dir::<FS>(&root.child(PathPart::parse("wal").expect("wal part"))).await?;
    mk_dir::<FS>(&root.child(PathPart::parse("sst").expect("sst part"))).await?;

    let manifest_root = root.child(PathPart::parse("manifest").expect("manifest part"));
    let version_root = manifest_root.child(PathPart::parse("version").expect("version part"));
    let catalog_root = manifest_root.child(PathPart::parse("catalog").expect("catalog part"));
    let gc_root = manifest_root.child(PathPart::parse("gc").expect("gc part"));

    ensure_manifest_dirs::<FS>(&version_root)
        .await
        .map_err(DbBuildError::Manifest)?;
    ensure_manifest_dirs::<FS>(&catalog_root)
        .await
        .map_err(DbBuildError::Manifest)?;
    ensure_manifest_dirs::<FS>(&gc_root)
        .await
        .map_err(DbBuildError::Manifest)?;

    Ok(())
}

/// Builder-style configuration surface for constructing a [`DB`] instance.
///
/// The builder enforces that callers explicitly select a storage backend
/// (in-memory, local disk, or object storage) before the database can be
/// materialised. Fields are intentionally private; use the fluent methods to
/// configure storage, compaction, and durability.
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use fusio::{executor::tokio::TokioExecutor, mem::fs::InMemoryFs};
/// use tonbo::{
///     db::{DB, DbBuilder},
///     schema::SchemaBuilder,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
///
///     let _db: DB<InMemoryFs, TokioExecutor> = DbBuilder::from_schema_key_name(schema, "id")?
///         .in_memory("builder-example")?
///         .build()
///         .await?;
///     Ok(())
/// }
/// ```
pub struct DbBuilder<S = Unconfigured> {
    mode_config: DynModeConfig,
    state: S,
    compaction_strategy: CompactionStrategy,
    compaction_interval: Option<Duration>,
    compaction_loop_cfg: Option<CompactionLoopConfig>,
    minor_compaction: Option<MinorCompactionOptions>,
    #[cfg(any(test, bench))]
    bench_diagnostics: bool,
}

/// Error returned when building a [`DB`] through [`DbBuilder`].
#[derive(Debug, Error)]
pub enum DbBuildError {
    /// No storage backend was selected prior to calling `build`.
    #[error("storage backend not selected")]
    MissingStorage,
    /// The provided storage root could not be parsed.
    #[error("invalid storage path `{path}`: {reason}")]
    InvalidPath {
        /// Path string that failed validation.
        path: String,
        /// Human-readable reason describing the failure.
        reason: String,
    },
    /// Object-store backends are not wired yet.
    #[error("object-store backend support not implemented")]
    UnsupportedObjectStore,
    /// Object-store configuration missing or invalid.
    #[error("object-store configuration error: {reason}")]
    ObjectStoreConfig {
        /// Human-readable explanation of the failure.
        reason: String,
    },
    /// Backend combination not supported on this target/feature set.
    #[error("{backend} backend not supported for this build target")]
    UnsupportedBackend {
        /// Backend label for context (e.g. disk, object-store).
        backend: &'static str,
    },
    /// Mode initialisation failed while building the DB.
    #[error(transparent)]
    Mode(#[from] KeyExtractError),
    /// Manifest initialisation failed while building the DB.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// WAL configuration or recovery failed during builder orchestration.
    #[error(transparent)]
    Wal(#[from] WalError),
    /// Filesystem layout preparation failed.
    #[error("failed to prepare directory `{path}`: {source}")]
    PreparePath {
        /// Path that triggered the failure.
        path: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// High-level durability specification for object-store backed builders.
#[derive(Debug, Clone)]
pub enum ObjectSpec {
    /// Amazon S3 (or compatible) configuration.
    S3(S3Spec),
}

impl ObjectSpec {
    /// Convenience helper to wrap an [`S3Spec`].
    #[must_use]
    pub fn s3(spec: S3Spec) -> Self {
        Self::S3(spec)
    }
}

/// Parameters required to bootstrap an S3-backed builder.
#[derive(Debug, Clone)]
pub struct S3Spec {
    /// Bucket or container name hosting the dataset.
    pub bucket: String,
    /// Prefix/pseudo-directory reserved for the table under that bucket.
    pub prefix: String,
    /// Optional AWS KMS key alias/ARN for server-side encryption.
    pub kms_key: Option<String>,
    /// Credentials used to authenticate with the object store.
    pub credentials: AwsCreds,
    /// Optional custom endpoint (e.g. for MinIO or R2 deployments).
    pub endpoint: Option<String>,
    /// Region to target when constructing the client.
    pub region: Option<String>,
    /// Override for payload signing semantics.
    pub sign_payload: Option<bool>,
    /// Override for checksum enforcement semantics.
    pub checksum: Option<bool>,
    /// Optional flag indicating that the bucket is versioned.
    pub versioned: Option<bool>,
}

impl S3Spec {
    /// Construct a new S3 specification with the required fields.
    #[must_use]
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        credentials: AwsCreds,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: prefix.into(),
            kms_key: None,
            credentials,
            endpoint: None,
            region: None,
            sign_payload: None,
            checksum: None,
            versioned: None,
        }
    }
}

/// AWS credential helper surfaced through the builder API.
#[derive(Debug, Clone)]
pub struct AwsCreds {
    /// Access key identifier used during authentication.
    pub access_key: String,
    /// Secret key paired with the access key identifier.
    pub secret_key: String,
    /// Optional temporary session token.
    pub session_token: Option<String>,
}

impl AwsCreds {
    /// Construct credentials from the provided access and secret keys.
    #[must_use]
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: None,
        }
    }

    /// Construct credentials from the provided key material plus a session token.
    #[must_use]
    pub fn with_session_token(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: Some(token.into()),
        }
    }

    /// Populate credentials from the conventional AWS environment variables.
    pub fn from_env() -> Result<Self, AwsCredsError> {
        let access_key = env::var("AWS_ACCESS_KEY_ID").map_err(|_| AwsCredsError::MissingEnv {
            var: "AWS_ACCESS_KEY_ID",
        })?;
        let secret_key =
            env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| AwsCredsError::MissingEnv {
                var: "AWS_SECRET_ACCESS_KEY",
            })?;
        let session_token = env::var("AWS_SESSION_TOKEN").ok();
        Ok(Self {
            access_key,
            secret_key,
            session_token,
        })
    }
}

/// Error surfaced when credentials cannot be derived from the environment.
#[derive(Debug, Error)]
pub enum AwsCredsError {
    /// Required environment variable was missing during credential discovery.
    #[error("missing AWS credential environment variable `{var}`")]
    MissingEnv {
        /// Name of the missing environment variable.
        var: &'static str,
    },
}

#[derive(Clone)]
struct StorageRoute {
    fs: Arc<dyn DynFs>,
    path: Path,
    cas: Option<Arc<dyn FusioCas>>,
}

#[derive(Clone)]
pub struct StorageLayout<FS> {
    fs: Arc<FS>,
    dyn_fs: Arc<dyn DynFs>,
    cas: Option<Arc<dyn FusioCas>>,
    root: Path,
}

impl<FS> StorageLayout<FS> {
    fn new(fs: Arc<FS>, cas: Option<Arc<dyn FusioCas>>, root: Path) -> Self
    where
        FS: DynFs + 'static,
    {
        let dyn_fs: Arc<dyn DynFs> = fs.clone();
        Self {
            fs,
            dyn_fs,
            cas,
            root,
        }
    }

    fn dyn_fs(&self) -> Arc<dyn DynFs> {
        Arc::clone(&self.dyn_fs)
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn wal_route(&self) -> Result<StorageRoute, DbBuildError> {
        let mut current = self.root.clone();
        let wal = PathPart::parse("wal").map_err(|err| DbBuildError::InvalidPath {
            path: "wal".into(),
            reason: err.to_string(),
        })?;
        current = current.child(wal);
        Ok(StorageRoute {
            fs: Arc::clone(&self.dyn_fs),
            path: current,
            cas: self.cas.clone(),
        })
    }

    fn sst_route(&self) -> Result<StorageRoute, DbBuildError> {
        let mut current = self.root.clone();
        let sst = PathPart::parse("sst").map_err(|err| DbBuildError::InvalidPath {
            path: "sst".into(),
            reason: err.to_string(),
        })?;
        current = current.child(sst);
        Ok(StorageRoute {
            fs: Arc::clone(&self.dyn_fs),
            path: current,
            cas: self.cas.clone(),
        })
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn apply_wal_defaults(&self, cfg: &mut RuntimeWalConfig) -> Result<(), DbBuildError> {
        let route = self.wal_route()?;
        cfg.dir = route.path.clone();
        cfg.segment_backend = Arc::clone(&route.fs);
        cfg.state_store = route
            .cas
            .clone()
            .map(|cas| Arc::new(FsWalStateStore::new(cas)) as Arc<dyn WalStateStore>);
        Ok(())
    }
}

#[allow(clippy::arc_with_non_send_sync)]
fn build_s3_fs(
    spec: S3Spec,
) -> Result<(Arc<fusio::impls::remotes::aws::fs::AmazonS3>, Path), DbBuildError> {
    use fusio::impls::remotes::aws::{credential::AwsCredential, fs::AmazonS3Builder};

    let region = spec.region.clone().unwrap_or_else(|| "us-east-1".into());
    let mut builder = AmazonS3Builder::new(spec.bucket.clone()).region(region);

    if let Some(endpoint) = &spec.endpoint {
        builder = builder.endpoint(endpoint.clone());
    }

    let credential = AwsCredential {
        key_id: spec.credentials.access_key.clone(),
        secret_key: spec.credentials.secret_key.clone(),
        token: spec.credentials.session_token.clone(),
    };
    builder = builder.credential(credential);

    if let Some(sign) = spec.sign_payload {
        builder = builder.sign_payload(sign);
    }

    if let Some(checksum) = spec.checksum {
        builder = builder.checksum(checksum);
    }

    let fs = Arc::new(builder.build());
    let root = if spec.prefix.is_empty() {
        Path::default()
    } else {
        Path::parse(&spec.prefix).map_err(|err| DbBuildError::InvalidPath {
            path: spec.prefix.clone(),
            reason: err.to_string(),
        })?
    };
    Ok((fs, root))
}

async fn wal_segments_exist(cfg: &RuntimeWalConfig) -> Result<bool, DbBuildError> {
    let storage = WalStorage::new(Arc::clone(&cfg.segment_backend), cfg.dir.clone());
    let segments = storage.list_segments().await?;
    Ok(!segments.is_empty())
}

struct ManifestBootstrap<'a, FS> {
    layout: &'a StorageLayout<FS>,
}

impl<'a, FS> ManifestBootstrap<'a, FS> {
    fn new(layout: &'a StorageLayout<FS>) -> Self {
        Self { layout }
    }

    async fn init_manifest<E>(&self, executor: E) -> Result<TonboManifest<FS, E>, DbBuildError>
    where
        FS: ManifestFs<E>,
        E: Executor + Timer + Clone + 'static,
        HeadStoreImpl<FS>: fusio_manifest::HeadStore,
        SegmentStoreImpl<FS>: fusio_manifest::SegmentIo,
        CheckpointStoreImpl<FS>: fusio_manifest::CheckpointStore,
        LeaseStoreImpl<FS, E>: fusio_manifest::LeaseStore,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        init_fs_manifest(
            Arc::as_ref(&self.layout.fs).clone(),
            self.layout.root(),
            executor,
        )
        .await
        .map_err(DbBuildError::Manifest)
    }
}
impl DbBuilder<Unconfigured> {
    pub(super) fn new(mode_config: DynModeConfig) -> Self {
        Self {
            mode_config,
            state: Unconfigured,
            compaction_strategy: CompactionStrategy::default(),
            compaction_interval: None,
            compaction_loop_cfg: None,
            minor_compaction: None,
            #[cfg(any(test, bench))]
            bench_diagnostics: false,
        }
    }

    /// Select the in-memory storage backend, labelling the namespace with the
    /// provided identifier.
    pub fn in_memory(
        self,
        label: impl Into<String>,
    ) -> Result<DbBuilder<StorageConfig<InMemoryFs>>, DbBuildError> {
        let label_str = label.into();
        let root = Path::parse(&label_str).map_err(|err| DbBuildError::InvalidPath {
            path: label_str,
            reason: err.to_string(),
        })?;
        let fs = Arc::new(InMemoryFs::new());
        Ok(DbBuilder {
            mode_config: self.mode_config,
            state: StorageConfig::new(fs, root, DurabilityClass::Volatile),
            compaction_strategy: self.compaction_strategy,
            compaction_interval: self.compaction_interval,
            compaction_loop_cfg: self.compaction_loop_cfg,
            minor_compaction: self.minor_compaction,
            #[cfg(any(test, bench))]
            bench_diagnostics: self.bench_diagnostics,
        })
    }

    /// Select a local filesystem backend rooted at `root`.
    #[must_use = "use the returned DbBuilder to continue configuration"]
    #[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
    pub fn on_disk(
        self,
        root: impl AsRef<std::path::Path>,
    ) -> Result<DbBuilder<StorageConfig<LocalFs>>, DbBuildError> {
        let root_ref = root.as_ref();
        let path =
            Path::from_filesystem_path(root_ref).map_err(|err| DbBuildError::InvalidPath {
                path: root_ref.display().to_string(),
                reason: err.to_string(),
            })?;
        let fs = Arc::new(LocalFs {});
        let state = StorageConfig::new(fs, path, DurabilityClass::Durable).with_create_layout(true);
        Ok(DbBuilder {
            mode_config: self.mode_config,
            state,
            compaction_strategy: self.compaction_strategy,
            compaction_interval: self.compaction_interval,
            compaction_loop_cfg: self.compaction_loop_cfg,
            minor_compaction: self.minor_compaction,
            #[cfg(any(test, bench))]
            bench_diagnostics: self.bench_diagnostics,
        })
    }

    /// Select an object-store backend using the provided specification.
    pub fn object_store(
        self,
        spec: ObjectSpec,
    ) -> Result<DbBuilder<StorageConfig<fusio::impls::remotes::aws::fs::AmazonS3>>, DbBuildError>
    {
        let (fs, root) = match spec {
            ObjectSpec::S3(s3_spec) => build_s3_fs(s3_spec)?,
        };
        Ok(DbBuilder {
            mode_config: self.mode_config,
            state: StorageConfig::new(fs, root, DurabilityClass::Durable),
            compaction_strategy: self.compaction_strategy,
            compaction_interval: self.compaction_interval,
            compaction_loop_cfg: self.compaction_loop_cfg,
            minor_compaction: self.minor_compaction,
            #[cfg(any(test, bench))]
            bench_diagnostics: self.bench_diagnostics,
        })
    }

    /// Create a builder from an Arrow schema and single key column name.
    pub fn from_schema_key_name(
        schema: arrow_schema::SchemaRef,
        key_name: impl Into<String>,
    ) -> Result<Self, DbBuildError> {
        let key = key_name.into();
        let cfg = DynModeConfig::from_key_name(schema, key.as_str()).map_err(DbBuildError::Mode)?;
        Ok(Self::new(cfg))
    }

    /// Create a builder from an Arrow schema and explicit key column indices.
    pub fn from_schema_key_indices(
        schema: arrow_schema::SchemaRef,
        key_indices: Vec<usize>,
    ) -> Result<Self, DbBuildError> {
        let extractor =
            projection_for_columns(schema.clone(), key_indices).map_err(DbBuildError::Mode)?;
        let cfg = DynModeConfig::new(schema, extractor).map_err(DbBuildError::Mode)?;
        Ok(Self::new(cfg))
    }

    /// Create a builder by reading key metadata from the schema.
    ///
    /// This method looks for `tonbo.key` metadata on fields to identify the primary key.
    /// Use `#[metadata(k = "tonbo.key", v = "true")]` on your key field when using
    /// `#[derive(Record)]`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tonbo::db::DbBuilder;
    /// use tonbo::prelude::{Record, SchemaMeta};
    ///
    /// #[derive(Record)]
    /// struct User {
    ///     #[metadata(k = "tonbo.key", v = "true")]
    ///     id: String,
    ///     name: String,
    /// }
    ///
    /// let db = DbBuilder::from_schema(User::schema())?
    ///     .on_disk("/tmp/users")?
    ///     .open()
    ///     .await?;
    /// ```
    pub fn from_schema(schema: arrow_schema::SchemaRef) -> Result<Self, DbBuildError> {
        let cfg = DynModeConfig::from_metadata(schema).map_err(DbBuildError::Mode)?;
        Ok(Self::new(cfg))
    }

    /// Alias for [`from_schema`](Self::from_schema).
    pub fn from_schema_metadata(schema: arrow_schema::SchemaRef) -> Result<Self, DbBuildError> {
        Self::from_schema(schema)
    }
}

impl<FS> DbBuilder<StorageConfig<FS>>
where
    FS: DynFs + FusioCas + Clone + MaybeSend + MaybeSync + 'static,
{
    /// Select a compaction strategy (leveled, tiered, or time-windowed placeholder).
    #[cfg(test)]
    #[must_use]
    pub fn with_compaction_strategy(mut self, strategy: CompactionStrategy) -> Self {
        self.compaction_strategy = strategy;
        self
    }

    /// Configure minor compaction (immutable flush) with a simple segment-count trigger.
    #[cfg(test)]
    #[must_use]
    pub fn with_minor_compaction(
        mut self,
        segment_threshold: usize,
        target_level: usize,
        start_id: u64,
    ) -> Self {
        self.minor_compaction = Some(MinorCompactionOptions {
            segment_threshold,
            target_level,
            start_id,
        });
        self
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn build_minor_compaction_state(
        &self,
        layout: &StorageLayout<FS>,
    ) -> Result<Option<MinorCompactionState>, DbBuildError>
    where
        FS: DynFs + FusioCas + 'static,
    {
        let Some(cfg) = &self.minor_compaction else {
            return Ok(None);
        };
        let route = layout.sst_route()?;
        let extractor = Arc::clone(&self.mode_config.extractor);
        let config = Arc::new(
            SsTableConfig::new(self.mode_config.schema(), route.fs, route.path.clone())
                .with_key_extractor(extractor)
                .with_target_level(cfg.target_level),
        );
        let compactor = MinorCompactor::new(cfg.segment_threshold, cfg.target_level, cfg.start_id);
        Ok(Some(MinorCompactionState::new(compactor, config)))
    }

    /// Attach a stable logical table name enforced at build time.
    #[must_use]
    pub fn table_name(mut self, name: impl Into<String>) -> Self {
        *self.state.table_name_mut() = Some(name.into());
        self
    }

    /// Open the database, recovering from existing state if present.
    ///
    /// For durable backends (on-disk, S3), this will recover from WAL and manifest
    /// if they exist, otherwise initialize a fresh database.
    ///
    /// For volatile backends (in-memory), this always creates a fresh database.
    #[cfg(feature = "tokio")]
    pub async fn open(self) -> Result<DB<FS, TokioExecutor>, DbBuildError>
    where
        FS: ManifestFs<TokioExecutor>,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        let executor = Arc::new(TokioExecutor::default());
        self.open_with_executor(executor).await
    }

    /// Alias for [`open`](Self::open) for backwards compatibility.
    #[cfg(feature = "tokio")]
    pub async fn build(self) -> Result<DB<FS, TokioExecutor>, DbBuildError>
    where
        FS: ManifestFs<TokioExecutor>,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        self.open().await
    }

    /// Open the database using a caller-provided executor implementation.
    ///
    /// For durable backends, this recovers from existing state if present.
    /// For volatile backends, this creates a fresh database.
    pub async fn open_with_executor<E>(self, executor: Arc<E>) -> Result<DB<FS, E>, DbBuildError>
    where
        E: Executor + Timer + Clone + 'static,
        FS: ManifestFs<E>,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        if self.state.durability().is_durable() {
            return self.recover_or_init_with_executor(executor).await;
        }
        // Volatile storage: fresh start (nothing to recover)
        self.state.prepare().await?;
        let layout = self.state.layout()?;
        self.build_with_layout(executor, layout).await
    }

    async fn build_with_layout<E>(
        self,
        executor: Arc<E>,
        layout: StorageLayout<FS>,
    ) -> Result<DB<FS, E>, DbBuildError>
    where
        E: Executor + Timer + Clone + 'static,
        FS: ManifestFs<E> + fusio_manifest::ObjectHead,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        let minor_compaction = self.build_minor_compaction_state(&layout)?;
        let manifest_init = ManifestBootstrap::new(&layout);
        let file_ids = FileIdGenerator::default();
        let table_name = self
            .state
            .table_name()
            .cloned()
            .unwrap_or_else(|| DEFAULT_TABLE_NAME.to_string());
        let table_definition = table_definition(&self.mode_config, &table_name);

        let (schema, delete_schema, commit_ack_mode, mem) =
            self.mode_config.build().map_err(DbBuildError::Mode)?;
        let manifest = manifest_init
            .init_manifest(executor.as_ref().clone())
            .await?;
        let table_meta = manifest
            .register_table(&file_ids, &table_definition)
            .await
            .map_err(DbBuildError::Manifest)?;
        let manifest_table = table_meta.table_id;

        let mut wal_cfg = if self.state.durability().is_durable() {
            let mut cfg = RuntimeWalConfig::default();
            layout.apply_wal_defaults(&mut cfg)?;
            if let Some(overrides) = self.state.wal_config() {
                overrides.apply(&mut cfg);
            }
            Some(cfg)
        } else {
            None
        };

        #[cfg(any(test, bench))]
        let bench_diagnostics = self
            .bench_diagnostics
            .then(|| Arc::new(BenchDiagnosticsRecorder::default()) as Arc<dyn BenchObserver>);

        let mut inner = DbInner::from_components(
            schema,
            delete_schema,
            commit_ack_mode,
            mem,
            layout.dyn_fs(),
            manifest,
            manifest_table,
            table_meta,
            wal_cfg.clone(),
            executor,
            #[cfg(any(test, bench))]
            bench_diagnostics,
        );

        inner.minor_compaction = minor_compaction;

        if let Some(cfg) = wal_cfg.take() {
            inner.enable_wal(cfg).await?;
        }

        if let Some(loop_cfg) = self.compaction_loop_cfg {
            // Temporary shortcut: spawn a local compaction loop for dyn mode using a
            // caller-provided SST config. This should be replaced by a real
            // scheduler/lease + executor selection.
            let planner = self.compaction_strategy.clone().build();
            let exec =
                LocalCompactionExecutor::new(Arc::clone(&loop_cfg.sst_config), loop_cfg.start_id);
            let driver = Arc::new(inner.compaction_driver());
            let handle = driver.spawn_worker(
                Arc::clone(&inner.executor),
                planner,
                exec,
                Some(loop_cfg.sst_config),
                loop_cfg.interval,
                1,
            );
            inner.compaction_worker = Some(handle);
        }

        Ok(DB::from_inner(Arc::new(inner)))
    }
}

impl<S> DbBuilder<S> {
    /// Override the commit acknowledgement mode for transactional writes.
    #[must_use]
    pub fn with_commit_ack_mode(mut self, mode: CommitAckMode) -> Self {
        self.mode_config.commit_ack_mode = mode;
        self
    }

    /// Enable bench-only diagnostics hooks used by scenario benchmarks.
    #[cfg(any(test, bench))]
    #[must_use]
    pub fn enable_bench_diagnostics(mut self) -> Self {
        self.bench_diagnostics = true;
        self
    }
}

impl<FS> DbBuilder<StorageConfig<FS>>
where
    FS: DynFs + FusioCas + Clone + MaybeSend + MaybeSync + 'static,
{
    /// Configure an optional background compaction interval (current-thread Tokio only).
    /// This is a temporary opt-in; a proper scheduler/lease should replace it.
    #[cfg(test)]
    #[must_use]
    pub fn with_compaction_interval(mut self, interval: Duration) -> Self {
        self.compaction_interval = Some(interval);
        self
    }

    /// Enable a background compaction loop on the provided executor. This is a temporary,
    /// dyn-mode-only helper; a proper scheduler/lease should replace it.
    #[cfg(test)]
    #[must_use]
    pub fn with_compaction_loop(
        mut self,
        interval: Duration,
        sst_config: Arc<SsTableConfig>,
        start_id: u64,
    ) -> Self {
        self.compaction_loop_cfg = Some(CompactionLoopConfig {
            interval,
            sst_config,
            start_id,
        });
        self
    }
}

/// Configures automatic compaction loop spawning for dynamic builds.
#[derive(Clone)]
struct CompactionLoopConfig {
    interval: Duration,
    sst_config: Arc<SsTableConfig>,
    start_id: u64,
}

#[derive(Clone, Debug)]
struct MinorCompactionOptions {
    segment_threshold: usize,
    target_level: usize,
    start_id: u64,
}

impl<FS> DbBuilder<StorageConfig<FS>>
where
    FS: DynFs + FusioCas + Clone + MaybeSend + MaybeSync + 'static,
{
    /// Apply a batch of WAL overrides supplied via [`WalConfig`].
    #[must_use]
    pub(crate) fn wal_config(mut self, overrides: WalConfig) -> Self {
        if let Some(ref mut existing) = self.state.wal_config {
            existing.merge(overrides);
        } else if self.state.durability.is_durable() {
            self.state.wal_config = Some(overrides);
        }
        self
    }

    /// Override the WAL segment size without constructing a full override struct.
    #[must_use]
    pub fn wal_segment_bytes(mut self, max_bytes: usize) -> Self {
        if let Some(ref mut cfg) = self.state.wal_config {
            cfg.segment_max_bytes = Some(max_bytes);
        }
        self
    }

    /// Override the WAL sync policy.
    #[must_use]
    pub fn wal_sync_policy(mut self, policy: WalSyncPolicy) -> Self {
        if let Some(ref mut cfg) = self.state.wal_config {
            cfg.sync = Some(policy);
        }
        self
    }

    /// Override the WAL flush interval.
    #[must_use]
    pub fn wal_flush_interval(mut self, interval: Duration) -> Self {
        if let Some(ref mut cfg) = self.state.wal_config {
            cfg.flush_interval = Some(interval);
        }
        self
    }

    /// Override the WAL retention budget (set `None` to disable).
    #[must_use]
    pub fn wal_retention_bytes(mut self, retention: Option<usize>) -> Self {
        if let Some(ref mut cfg) = self.state.wal_config {
            cfg.retention_bytes = Some(retention);
        }
        self
    }

    /// Internal: recover from WAL state if present, otherwise build fresh.
    async fn recover_or_init_with_executor<E>(
        self,
        executor: Arc<E>,
    ) -> Result<DB<FS, E>, DbBuildError>
    where
        E: Executor + Timer + Clone + 'static,
        FS: ManifestFs<E>,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        self.state.prepare().await?;
        let layout = self.state.layout()?;
        let mut wal_cfg = RuntimeWalConfig::default();
        layout.apply_wal_defaults(&mut wal_cfg)?;
        if let Some(overrides) = self.state.wal_config() {
            overrides.apply(&mut wal_cfg);
        }

        if wal_segments_exist(&wal_cfg).await? {
            let manifest_init = ManifestBootstrap::new(&layout);
            let table_name = self
                .state
                .table_name()
                .cloned()
                .unwrap_or_else(|| DEFAULT_TABLE_NAME.to_string());
            let table_definition = table_definition(&self.mode_config, &table_name);
            let file_ids = FileIdGenerator::default();
            let manifest = manifest_init
                .init_manifest(executor.as_ref().clone())
                .await?;
            let table_meta = manifest
                .register_table(&file_ids, &table_definition)
                .await
                .map_err(DbBuildError::Manifest)?;
            let manifest_table = table_meta.table_id;
            let fs_dyn = layout.dyn_fs();
            let minor_compaction = self.build_minor_compaction_state(&layout)?;
            #[cfg(any(test, bench))]
            let bench_diagnostics = self
                .bench_diagnostics
                .then(|| Arc::new(BenchDiagnosticsRecorder::default()) as Arc<dyn BenchObserver>);
            let mut inner = DbInner::recover_with_wal_with_manifest(
                self.mode_config,
                Arc::clone(&executor),
                fs_dyn,
                wal_cfg.clone(),
                manifest,
                manifest_table,
                table_meta,
                #[cfg(any(test, bench))]
                bench_diagnostics,
            )
            .await
            .map_err(DbBuildError::Mode)?;
            inner.minor_compaction = minor_compaction;
            inner.enable_wal(wal_cfg).await?;
            Ok(DB::from_inner(Arc::new(inner)))
        } else {
            self.build_with_layout(executor, layout).await
        }
    }
}
