use std::{env, fs, io::Write, path::Path as StdPath, sync::Arc, time::Duration};

use fusio::{
    DynFs,
    disk::LocalFs,
    executor::{Executor, Timer, tokio::TokioExecutor},
    fs::FsCas as FusioCas,
    mem::fs::InMemoryFs,
    path::{Path, PathPart, path_to_local},
};
use thiserror::Error;

use super::{CompactionLoopHandle, DB, DynMode, Mode};
use crate::{
    compaction::{
        executor::LocalCompactionExecutor,
        planner::{CompactionStrategy, PlannerInitError},
    },
    extractor::{KeyExtractError, projection_for_columns},
    id::FileIdGenerator,
    manifest::{ManifestError, TonboManifest, init_fs_manifest},
    mode::{CatalogDescribe, DynModeConfig},
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

mod sealed {
    pub trait Sealed {}
}

pub(crate) const DEFAULT_TABLE_NAME: &str = "tonbo-default";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityClass {
    Volatile,
    Durable,
}

impl DurabilityClass {
    fn is_durable(self) -> bool {
        matches!(self, Self::Durable)
    }
}

#[derive(Debug, Default)]
pub struct Unconfigured;

pub struct InMemoryState {
    label: String,
    table_name: Option<String>,
}

pub struct DiskDurableState {
    root: Path,
    table_name: Option<String>,
    wal_config: WalConfig,
    create_dirs: bool,
}

pub struct ObjectDurableState {
    spec: ObjectSpec,
    table_name: Option<String>,
    wal_config: WalConfig,
}

impl DiskDurableState {
    fn new(root: Path) -> Self {
        Self {
            root,
            table_name: None,
            wal_config: WalConfig::default(),
            create_dirs: true,
        }
    }
}

fn ensure_disk_layout(root: &Path) -> Result<(), DbBuildError> {
    if root.as_ref().is_empty() {
        return Err(DbBuildError::InvalidPath {
            path: root.to_string(),
            reason: "root cannot be empty".into(),
        });
    }
    let base = path_to_local(root).map_err(|err| DbBuildError::InvalidPath {
        path: root.to_string(),
        reason: err.to_string(),
    })?;
    const REQUIRED_PATHS: &[&str] = &[
        "wal",
        "sst",
        "manifest/catalog",
        "manifest/catalog/segments",
        "manifest/catalog/checkpoints",
        "manifest/catalog/leases",
        "manifest/version",
        "manifest/version/segments",
        "manifest/version/checkpoints",
        "manifest/version/leases",
    ];
    let base = base.as_path();
    for suffix in REQUIRED_PATHS {
        let path = base.join(suffix);
        if let Err(source) = fs::create_dir_all(&path) {
            return Err(DbBuildError::PreparePath {
                path: path.display().to_string(),
                source,
            });
        }
    }

    const EMPTY_HEAD_JSON: &str =
        r#"{"version":1,"checkpoint_id":null,"last_segment_seq":null,"last_txn_id":0}"#;
    let catalog_head = base.join("manifest/catalog/head.json");
    if !catalog_head.exists() {
        write_head_file(&catalog_head, EMPTY_HEAD_JSON)?;
    }
    let version_head = base.join("manifest/version/head.json");
    if !version_head.exists() {
        write_head_file(&version_head, EMPTY_HEAD_JSON)?;
    }

    Ok(())
}

fn write_head_file(path: &StdPath, body: &str) -> Result<(), DbBuildError> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|source| DbBuildError::PreparePath {
            path: path.display().to_string(),
            source,
        })?;
    file.write_all(body.as_bytes())
        .map_err(|source| DbBuildError::PreparePath {
            path: path.display().to_string(),
            source,
        })
}

pub trait StorageState: sealed::Sealed {
    const CLASS: DurabilityClass;

    fn storage_spec(&self) -> StorageBackendSpec;

    fn prepare_layout(&self) -> Result<(), DbBuildError> {
        Ok(())
    }

    fn table_name(&self) -> Option<&String> {
        None
    }

    fn wal_config_ref(&self) -> Option<&WalConfig> {
        None
    }
}

pub trait TableNameConfigurable: StorageState {
    fn table_name_slot(&mut self) -> &mut Option<String>;
}

pub trait DurableStorageState: TableNameConfigurable {
    fn wal_config_mut(&mut self) -> &mut WalConfig;
    fn wal_config(&self) -> &WalConfig;
    fn merge_wal_config(&mut self, overrides: WalConfig) {
        self.wal_config_mut().merge(overrides);
    }
}

impl sealed::Sealed for InMemoryState {}

impl StorageState for InMemoryState {
    const CLASS: DurabilityClass = DurabilityClass::Volatile;

    fn storage_spec(&self) -> StorageBackendSpec {
        StorageBackendSpec::InMemory {
            label: self.label.clone(),
        }
    }

    fn table_name(&self) -> Option<&String> {
        self.table_name.as_ref()
    }
}

impl TableNameConfigurable for InMemoryState {
    fn table_name_slot(&mut self) -> &mut Option<String> {
        &mut self.table_name
    }
}

impl sealed::Sealed for DiskDurableState {}

impl StorageState for DiskDurableState {
    const CLASS: DurabilityClass = DurabilityClass::Durable;

    fn storage_spec(&self) -> StorageBackendSpec {
        StorageBackendSpec::Disk {
            root: self.root.clone(),
        }
    }

    fn prepare_layout(&self) -> Result<(), DbBuildError> {
        if self.create_dirs {
            ensure_disk_layout(&self.root)
        } else {
            Ok(())
        }
    }

    fn table_name(&self) -> Option<&String> {
        self.table_name.as_ref()
    }

    fn wal_config_ref(&self) -> Option<&WalConfig> {
        Some(&self.wal_config)
    }
}

impl TableNameConfigurable for DiskDurableState {
    fn table_name_slot(&mut self) -> &mut Option<String> {
        &mut self.table_name
    }
}

impl DurableStorageState for DiskDurableState {
    fn wal_config_mut(&mut self) -> &mut WalConfig {
        &mut self.wal_config
    }

    fn wal_config(&self) -> &WalConfig {
        &self.wal_config
    }
}

impl sealed::Sealed for ObjectDurableState {}

impl StorageState for ObjectDurableState {
    const CLASS: DurabilityClass = DurabilityClass::Durable;

    fn storage_spec(&self) -> StorageBackendSpec {
        StorageBackendSpec::ObjectStore(self.spec.clone())
    }

    fn table_name(&self) -> Option<&String> {
        self.table_name.as_ref()
    }

    fn wal_config_ref(&self) -> Option<&WalConfig> {
        Some(&self.wal_config)
    }
}

impl TableNameConfigurable for ObjectDurableState {
    fn table_name_slot(&mut self) -> &mut Option<String> {
        &mut self.table_name
    }
}

impl DurableStorageState for ObjectDurableState {
    fn wal_config_mut(&mut self) -> &mut WalConfig {
        &mut self.wal_config
    }

    fn wal_config(&self) -> &WalConfig {
        &self.wal_config
    }
}

/// Builder-style configuration surface for constructing a [`DB`] instance.
///
/// The builder enforces that callers explicitly select a storage backend
/// (in-memory, local disk, or object storage) before the database can be
/// materialised.
pub struct DbBuilder<M, S = Unconfigured>
where
    M: Mode + CatalogDescribe,
{
    mode_config: M::Config,
    state: S,
    compaction_strategy: CompactionStrategy,
    compaction_interval: Option<Duration>,
    compaction_loop_cfg: Option<CompactionLoopConfig>,
}

impl ObjectDurableState {
    fn new(spec: ObjectSpec) -> Self {
        Self {
            spec,
            table_name: None,
            wal_config: WalConfig::default(),
        }
    }
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
    /// Compaction strategy selection is not yet supported.
    #[error(transparent)]
    CompactionPlanner(#[from] PlannerInitError),
}

#[derive(Debug, Clone)]
pub enum StorageBackendSpec {
    InMemory { label: String },
    Disk { root: Path },
    ObjectStore(ObjectSpec),
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
struct StorageBackend {
    dyn_fs: Arc<dyn DynFs>,
    cas: Option<Arc<dyn FusioCas>>,
    root: Path,
    kind: StorageBackendKind,
}

#[derive(Clone)]
#[allow(dead_code)]
enum StorageBackendKind {
    InMemory { fs: Arc<InMemoryFs> },
    Disk { fs: Arc<LocalFs> },
    ObjectStore(ObjectStoreBackend),
}

#[derive(Clone)]
enum ObjectStoreBackend {
    S3 {
        fs: fusio::impls::remotes::aws::fs::AmazonS3,
    },
}

#[derive(Clone)]
struct StorageRoute {
    fs: Arc<dyn DynFs>,
    path: Path,
    cas: Option<Arc<dyn FusioCas>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(
        target_os = "macos",
        ignore = "reqwest client initialisation relies on SystemConfiguration which is blocked in \
                  the macOS sandbox used for tests"
    )]
    fn builds_s3_backend_with_minimal_config() {
        let mut s3 = S3Spec::new("tonbo-test", "tenant-a", AwsCreds::new("access", "secret"));
        s3.endpoint = Some("http://localhost:9000".into());
        s3.region = Some("us-east-2".into());
        s3.sign_payload = Some(true);
        s3.checksum = Some(false);

        let backend = build_object_store_backend(ObjectSpec::s3(s3)).expect("s3 backend");
        match backend.kind {
            StorageBackendKind::ObjectStore(ObjectStoreBackend::S3 { .. }) => {}
            _ => panic!("unexpected backend kind"),
        }
    }
}

struct StorageLayout {
    backend: StorageBackend,
}

impl StorageLayout {
    fn new(spec: StorageBackendSpec) -> Result<Self, DbBuildError> {
        let backend = StorageBackend::from_spec(spec)?;
        Ok(Self { backend })
    }

    fn backend(&self) -> &StorageBackend {
        &self.backend
    }

    fn wal_route(&self) -> Result<StorageRoute, DbBuildError> {
        self.backend.route(StorageClass::Wal)
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StorageClass {
    Wal,
}

impl StorageClass {
    fn components(self) -> &'static [&'static str] {
        match self {
            StorageClass::Wal => &["wal"],
        }
    }
}

impl StorageBackend {
    fn from_spec(spec: StorageBackendSpec) -> Result<Self, DbBuildError> {
        match spec {
            StorageBackendSpec::InMemory { label } => {
                if label.is_empty() {
                    return Err(DbBuildError::InvalidPath {
                        path: label,
                        reason: "label cannot be empty".into(),
                    });
                }
                let raw_fs = Arc::new(InMemoryFs::new());
                let dyn_fs: Arc<dyn DynFs> = raw_fs.clone();
                let cas_fs: Arc<dyn FusioCas> = raw_fs.clone();
                let root = Path::parse(&label).map_err(|err| DbBuildError::InvalidPath {
                    path: label,
                    reason: err.to_string(),
                })?;
                Ok(Self {
                    dyn_fs,
                    cas: Some(cas_fs),
                    root,
                    kind: StorageBackendKind::InMemory { fs: raw_fs },
                })
            }
            StorageBackendSpec::Disk { root } => {
                if root.as_ref().is_empty() {
                    return Err(DbBuildError::InvalidPath {
                        path: root.to_string(),
                        reason: "root cannot be empty".into(),
                    });
                }
                let raw_fs = Arc::new(LocalFs {});
                let dyn_fs: Arc<dyn DynFs> = raw_fs.clone();
                let cas_fs: Arc<dyn FusioCas> = raw_fs.clone();
                Ok(Self {
                    dyn_fs,
                    cas: Some(cas_fs),
                    root,
                    kind: StorageBackendKind::Disk { fs: raw_fs },
                })
            }
            StorageBackendSpec::ObjectStore(spec) => build_object_store_backend(spec),
        }
    }

    fn route(&self, class: StorageClass) -> Result<StorageRoute, DbBuildError> {
        let mut current = self.root.clone();
        for part in class.components() {
            let parsed = PathPart::parse(part).map_err(|err| DbBuildError::InvalidPath {
                path: part.to_string(),
                reason: err.to_string(),
            })?;
            current = current.child(parsed);
        }
        Ok(StorageRoute {
            fs: Arc::clone(&self.dyn_fs),
            path: current,
            cas: self.cas.clone(),
        })
    }

    fn kind(&self) -> &StorageBackendKind {
        &self.kind
    }

    fn root(&self) -> &Path {
        &self.root
    }
}

fn build_object_store_backend(spec: ObjectSpec) -> Result<StorageBackend, DbBuildError> {
    match spec {
        ObjectSpec::S3(params) => build_s3_backend(params),
    }
}

fn build_s3_backend(spec: S3Spec) -> Result<StorageBackend, DbBuildError> {
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

    let fs = builder.build();
    let root = if spec.prefix.is_empty() {
        Path::default()
    } else {
        Path::parse(&spec.prefix).map_err(|err| DbBuildError::InvalidPath {
            path: spec.prefix.clone(),
            reason: err.to_string(),
        })?
    };

    let dyn_fs: Arc<dyn DynFs> = Arc::new(fs.clone());
    let cas_fs: Arc<dyn FusioCas> = Arc::new(fs.clone());

    Ok(StorageBackend {
        dyn_fs,
        cas: Some(cas_fs),
        root,
        kind: StorageBackendKind::ObjectStore(ObjectStoreBackend::S3 { fs }),
    })
}

async fn wal_segments_exist(cfg: &RuntimeWalConfig) -> Result<bool, DbBuildError> {
    let storage = WalStorage::new(Arc::clone(&cfg.segment_backend), cfg.dir.clone());
    let segments = storage.list_segments().await?;
    Ok(!segments.is_empty())
}

struct ManifestBootstrap<'a> {
    layout: &'a StorageLayout,
}

impl<'a> ManifestBootstrap<'a> {
    fn new(layout: &'a StorageLayout) -> Self {
        Self { layout }
    }

    async fn init_manifest(&self) -> Result<TonboManifest, DbBuildError> {
        let backend = self.layout.backend();
        match backend.kind() {
            StorageBackendKind::InMemory { fs } => {
                init_fs_manifest(Arc::as_ref(fs).clone(), backend.root())
                    .await
                    .map_err(DbBuildError::Manifest)
            }
            StorageBackendKind::Disk { fs } => init_fs_manifest(*Arc::as_ref(fs), backend.root())
                .await
                .map_err(DbBuildError::Manifest),
            StorageBackendKind::ObjectStore(ObjectStoreBackend::S3 { fs }) => {
                init_fs_manifest(fs.clone(), backend.root())
                    .await
                    .map_err(DbBuildError::Manifest)
            }
        }
    }
}
impl<M> DbBuilder<M, Unconfigured>
where
    M: Mode + CatalogDescribe,
{
    pub(super) fn new(mode_config: M::Config) -> Self {
        Self {
            mode_config,
            state: Unconfigured,
            compaction_strategy: CompactionStrategy::default(),
            compaction_interval: None,
            compaction_loop_cfg: None,
        }
    }

    /// Select the in-memory storage backend, labelling the namespace with the
    /// provided identifier.
    #[must_use]
    pub fn in_memory(self, label: impl Into<String>) -> DbBuilder<M, InMemoryState> {
        DbBuilder {
            mode_config: self.mode_config,
            state: InMemoryState {
                label: label.into(),
                table_name: None,
            },
            compaction_strategy: self.compaction_strategy,
            compaction_interval: self.compaction_interval,
            compaction_loop_cfg: self.compaction_loop_cfg,
        }
    }

    /// Select a local filesystem backend rooted at `root`.
    #[must_use = "use the returned DbBuilder to continue configuration"]
    pub fn on_disk(
        self,
        root: impl AsRef<std::path::Path>,
    ) -> Result<DbBuilder<M, DiskDurableState>, DbBuildError> {
        let root_ref = root.as_ref();
        let path =
            Path::from_filesystem_path(root_ref).map_err(|err| DbBuildError::InvalidPath {
                path: root_ref.display().to_string(),
                reason: err.to_string(),
            })?;
        Ok(DbBuilder {
            mode_config: self.mode_config,
            state: DiskDurableState::new(path),
            compaction_strategy: self.compaction_strategy,
            compaction_interval: self.compaction_interval,
            compaction_loop_cfg: self.compaction_loop_cfg,
        })
    }

    /// Select an object-store backend using the provided specification.
    #[must_use]
    pub fn object_store(self, spec: ObjectSpec) -> DbBuilder<M, ObjectDurableState> {
        DbBuilder {
            mode_config: self.mode_config,
            state: ObjectDurableState::new(spec),
            compaction_strategy: self.compaction_strategy,
            compaction_interval: self.compaction_interval,
            compaction_loop_cfg: self.compaction_loop_cfg,
        }
    }
}

// Convenience constructors for dynamic mode: build configs from schema + key in one step.
impl DbBuilder<DynMode, Unconfigured> {
    /// Create a dynamic-mode builder from an Arrow schema and single key column name.
    pub fn from_schema_key_name(
        schema: arrow_schema::SchemaRef,
        key_name: impl Into<String>,
    ) -> Result<Self, DbBuildError> {
        let key = key_name.into();
        let cfg = DynModeConfig::from_key_name(schema, key.as_str()).map_err(DbBuildError::Mode)?;
        Ok(Self::new(cfg))
    }

    /// Create a dynamic-mode builder from an Arrow schema and explicit key column indices.
    pub fn from_schema_key_indices(
        schema: arrow_schema::SchemaRef,
        key_indices: Vec<usize>,
    ) -> Result<Self, DbBuildError> {
        let extractor =
            projection_for_columns(schema.clone(), key_indices).map_err(DbBuildError::Mode)?;
        let cfg = DynModeConfig::new(schema, extractor).map_err(DbBuildError::Mode)?;
        Ok(Self::new(cfg))
    }

    /// Create a dynamic-mode builder by reading key metadata from the schema.
    pub fn from_schema_metadata(schema: arrow_schema::SchemaRef) -> Result<Self, DbBuildError> {
        let cfg = DynModeConfig::from_metadata(schema).map_err(DbBuildError::Mode)?;
        Ok(Self::new(cfg))
    }
}

impl<M> DbBuilder<M, DiskDurableState>
where
    M: Mode + CatalogDescribe,
{
    /// Enable or disable provisioning of the on-disk layout prior to build/recovery.
    #[must_use]
    pub fn create_dirs(mut self, enable: bool) -> Self {
        self.state.create_dirs = enable;
        self
    }
}

impl<M, S> DbBuilder<M, S>
where
    M: Mode + CatalogDescribe + 'static,
    S: StorageState + TableNameConfigurable,
{
    /// Select a compaction strategy (leveled, tiered, or time-windowed placeholder).
    #[must_use]
    pub fn with_compaction_strategy(mut self, strategy: CompactionStrategy) -> Self {
        self.compaction_strategy = strategy;
        self
    }

    /// Materialise a [`DB`] using the accumulated builder state.
    pub async fn build(self) -> Result<DB<M, TokioExecutor>, DbBuildError> {
        let executor = Arc::new(TokioExecutor::default());
        if S::CLASS.is_durable() {
            // Default to recovering existing state (manifest + WAL) when present.
            return self.recover_or_init_with_executor(executor).await;
        }
        self.build_with_executor(executor).await
    }

    /// Materialise a [`DB`] using a caller-provided executor implementation.
    pub async fn build_with_executor<E>(self, executor: Arc<E>) -> Result<DB<M, E>, DbBuildError>
    where
        E: Executor + Timer + Send + Sync + 'static,
    {
        if S::CLASS.is_durable() {
            // Durable backends should reuse existing on-disk state when available.
            return self.recover_or_init_with_executor(executor).await;
        }
        self.state.prepare_layout()?;
        let layout = StorageLayout::new(self.state.storage_spec())?;
        self.build_with_layout(executor, layout).await
    }

    async fn build_with_layout<E>(
        self,
        executor: Arc<E>,
        layout: StorageLayout,
    ) -> Result<DB<M, E>, DbBuildError>
    where
        E: Executor + Timer + Send + Sync + 'static,
    {
        let manifest_init = ManifestBootstrap::new(&layout);
        let file_ids = FileIdGenerator::default();
        let table_name = self
            .state
            .table_name()
            .cloned()
            .unwrap_or_else(|| DEFAULT_TABLE_NAME.to_string());
        let table_definition = M::table_definition(&self.mode_config, &table_name);

        let (mode, mem) = M::build(self.mode_config).map_err(DbBuildError::Mode)?;
        let manifest = manifest_init.init_manifest().await?;
        let table_meta = manifest
            .register_table(&file_ids, &table_definition)
            .await
            .map_err(DbBuildError::Manifest)?;
        let manifest_table = table_meta.table_id;

        let mut wal_cfg = if S::CLASS.is_durable() {
            let mut cfg = RuntimeWalConfig::default();
            layout.apply_wal_defaults(&mut cfg)?;
            if let Some(overrides) = self.state.wal_config_ref() {
                overrides.apply(&mut cfg);
            }
            Some(cfg)
        } else {
            None
        };

        let mut db = DB::from_components(
            mode,
            mem,
            manifest,
            manifest_table,
            wal_cfg.clone(),
            file_ids,
            executor,
        );

        if let Some(cfg) = wal_cfg.take() {
            db.enable_wal(cfg).await?;
        }

        let mut db = db;
        if let Some(loop_cfg) = self.compaction_loop_cfg {
            // Temporary shortcut: spawn a local compaction loop for dyn mode using a
            // caller-provided SST config. This should be replaced by a real
            // scheduler/lease + executor selection.
            let planner = self
                .compaction_strategy
                .clone()
                .build()
                .map_err(DbBuildError::CompactionPlanner)?;
            let exec =
                LocalCompactionExecutor::new(Arc::clone(&loop_cfg.sst_config), loop_cfg.start_id);
            let driver = Arc::new(db.compaction_driver());
            let handle = driver.spawn_compaction_worker_local(
                planner,
                exec,
                Some(loop_cfg.sst_config),
                loop_cfg.interval,
                1,
            );
            db.compaction_worker = Some(CompactionLoopHandle {
                _driver: driver,
                handle,
            });
        }

        Ok(db)
    }
}

impl<S> DbBuilder<DynMode, S> {
    /// Override the commit acknowledgement mode for transactional writes.
    #[must_use]
    pub fn with_commit_ack_mode(mut self, mode: CommitAckMode) -> Self {
        self.mode_config.commit_ack_mode = mode;
        self
    }
}

impl<S> DbBuilder<DynMode, S>
where
    S: StorageState,
{
    /// Configure an optional background compaction interval (current-thread Tokio only).
    /// This is a temporary opt-in; a proper scheduler/lease should replace it.
    #[must_use]
    pub fn with_compaction_interval(mut self, interval: Duration) -> Self {
        self.compaction_interval = Some(interval);
        self
    }
}

/// Configures automatic compaction loop spawning for DynMode builds.
#[derive(Clone)]
struct CompactionLoopConfig {
    interval: Duration,
    sst_config: Arc<SsTableConfig>,
    start_id: u64,
}

impl<S> DbBuilder<DynMode, S>
where
    S: StorageState,
{
    /// Enable a background compaction loop (current-thread Tokio). This is a temporary,
    /// dyn-mode-only helper; a proper scheduler/lease should replace it.
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
impl<M, S> DbBuilder<M, S>
where
    M: Mode + CatalogDescribe,
    S: TableNameConfigurable,
{
    /// Attach a stable logical table name enforced at build time.
    #[must_use]
    pub fn table_name(mut self, name: impl Into<String>) -> Self {
        *self.state.table_name_slot() = Some(name.into());
        self
    }
}

impl<M, S> DbBuilder<M, S>
where
    M: Mode + CatalogDescribe + 'static,
    S: DurableStorageState,
{
    /// Apply a batch of WAL overrides supplied via [`WalConfig`].
    ///
    /// ```no_run
    /// # use fusio::executor::tokio::TokioExecutor;
    /// # use tonbo::{
    /// #     db::{DB, DbBuildError, DynMode, WalConfig},
    /// #     mode::DynModeConfig,
    /// #     wal::WalSyncPolicy,
    /// # };
    /// # async fn build(config: DynModeConfig) -> Result<(), DbBuildError> {
    /// let overrides = WalConfig::default()
    ///     .segment_max_bytes(8 * 1024 * 1024)
    ///     .sync_policy(WalSyncPolicy::Always);
    /// let _db = DB::<DynMode, TokioExecutor>::builder(config)
    ///     .on_disk("/data/tonbo".to_string())?
    ///     .wal_config(overrides)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn wal_config(mut self, overrides: WalConfig) -> Self {
        self.state.merge_wal_config(overrides);
        self
    }

    /// Override the WAL segment size without constructing a full override struct.
    #[must_use]
    pub fn wal_segment_bytes(mut self, max_bytes: usize) -> Self {
        self.state.wal_config_mut().segment_max_bytes = Some(max_bytes);
        self
    }

    /// Override the WAL sync policy.
    #[must_use]
    pub fn wal_sync_policy(mut self, policy: WalSyncPolicy) -> Self {
        self.state.wal_config_mut().sync = Some(policy);
        self
    }

    /// Override the WAL flush interval.
    #[must_use]
    pub fn wal_flush_interval(mut self, interval: Duration) -> Self {
        self.state.wal_config_mut().flush_interval = Some(interval);
        self
    }

    /// Override the WAL retention budget (set `None` to disable).
    #[must_use]
    pub fn wal_retention_bytes(mut self, retention: Option<usize>) -> Self {
        self.state.wal_config_mut().retention_bytes = Some(retention);
        self
    }
}

impl<M, S> DbBuilder<M, S>
where
    M: Mode + CatalogDescribe + 'static,
    S: TableNameConfigurable,
{
    /// Attempt to recover from WAL state if present, otherwise build a fresh durable DB.
    pub async fn recover_or_init(self) -> Result<DB<M, TokioExecutor>, DbBuildError> {
        let executor = Arc::new(TokioExecutor::default());
        self.recover_or_init_with_executor(executor).await
    }

    /// Attempt to recover from WAL state using a caller-provided executor.
    pub async fn recover_or_init_with_executor<E>(
        self,
        executor: Arc<E>,
    ) -> Result<DB<M, E>, DbBuildError>
    where
        E: Executor + Timer + Send + Sync + 'static,
    {
        self.state.prepare_layout()?;
        let layout = StorageLayout::new(self.state.storage_spec())?;
        let mut wal_cfg = RuntimeWalConfig::default();
        layout.apply_wal_defaults(&mut wal_cfg)?;
        if let Some(overrides) = self.state.wal_config_ref() {
            overrides.apply(&mut wal_cfg);
        }

        if wal_segments_exist(&wal_cfg).await? {
            let manifest_init = ManifestBootstrap::new(&layout);
            let table_name = self
                .state
                .table_name()
                .cloned()
                .unwrap_or_else(|| DEFAULT_TABLE_NAME.to_string());
            let table_definition = M::table_definition(&self.mode_config, &table_name);
            let file_ids = FileIdGenerator::default();
            let manifest = manifest_init.init_manifest().await?;
            let table_meta = manifest
                .register_table(&file_ids, &table_definition)
                .await
                .map_err(DbBuildError::Manifest)?;
            let manifest_table = table_meta.table_id;
            let mut db = DB::recover_with_wal_with_manifest(
                self.mode_config,
                Arc::clone(&executor),
                wal_cfg.clone(),
                manifest,
                manifest_table,
                file_ids,
            )
            .await
            .map_err(DbBuildError::Mode)?;
            db.enable_wal(wal_cfg).await?;
            Ok(db)
        } else {
            self.build_with_layout(executor, layout).await
        }
    }
}
