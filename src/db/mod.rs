//! Dynamic Arrow-first database surface (`DB`, `DbBuilder`) and runtime wiring.
//!
//! The database is now specialised to the dynamic Arrow `RecordBatch` layout;
//! the earlier `Mode` trait indirection has been removed to simplify the core
//! engine while we focus on a single runtime representation.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use aisle::Pruner;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use fusio::{
    DynFs,
    executor::{Executor, Timer},
    mem::fs::InMemoryFs,
};
use futures::lock::Mutex as AsyncMutex;
use lockable::LockableHashMap;
use wal::SealState;

use crate::compaction::{CompactionHandle, MinorCompactor};
mod builder;
mod compaction;
mod error;
mod scan;
#[cfg(all(test, feature = "tokio"))]
mod tests;
mod wal;

pub use builder::{
    AwsCreds, AwsCredsError, CompactionOptions, DbBuildError, DbBuilder, ObjectSpec, S3Spec,
    WalConfig, wal_tuning,
};
pub use error::DBError;
pub use scan::{DEFAULT_SCAN_BATCH_ROWS, ScanBuilder};
pub(crate) use wal::{TxnWalPublishContext, WalFrameRange};

use crate::{
    extractor::{KeyExtractError, KeyProjection},
    id::FileId,
    inmem::mutable::DynMem,
    key::KeyOwned,
    manifest::{
        ManifestError, ManifestFs, SstEntry, TableId, TableMeta, TonboManifest, VersionEdit,
        WalSegmentRef,
    },
    mvcc::{CommitClock, ReadView, Timestamp},
    ondisk::{
        bloom::{BloomFilterCache, default_bloom_cache},
        metadata::{ParquetMetadataCache, default_parquet_metadata_cache},
        sstable::{SsTable, SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableError},
    },
    transaction::{Snapshot as TxSnapshot, SnapshotError, TransactionDurability, TransactionError},
    wal::{
        WalConfig as RuntimeWalConfig, WalHandle, frame::INITIAL_FRAME_SEQ, manifest_ext,
        replay::Replayer, state::WalStateHandle,
    },
};
pub use crate::{
    inmem::policy::{BatchesThreshold, NeverSeal, SealPolicy},
    mode::DynModeConfig,
    query::{Expr, ScalarValue},
    schema::SchemaBuilder,
    transaction::{CommitAckMode, Transaction},
    wal::WalSyncPolicy,
};

/// Internal shared handle for the database backed by an `Arc`.
pub(crate) type DynDbHandle<FS, E> = Arc<DbInner<FS, E>>;
type PrunerCache = HashMap<String, Arc<Pruner>>;

/// Metadata about a committed database version.
///
/// Each time data is committed (via transaction or ingest), a new version is created
/// with a unique timestamp. Use [`DB::list_versions`] to enumerate historical versions,
/// and [`DB::snapshot_at`] to query the database state at a specific version.
#[derive(Debug, Clone)]
pub struct Version {
    /// The commit timestamp identifying this version.
    pub timestamp: Timestamp,
    /// Number of SST files in this version.
    pub sst_count: usize,
    /// Number of compaction levels with data.
    pub level_count: usize,
}

/// State bundle for opportunistic minor compaction.
struct MinorCompactionState {
    compactor: MinorCompactor,
    config: Arc<SsTableConfig>,
    lock: AsyncMutex<()>,
}

impl MinorCompactionState {
    fn new(compactor: MinorCompactor, config: Arc<SsTableConfig>) -> Self {
        Self {
            compactor,
            config,
            lock: AsyncMutex::new(()),
        }
    }

    async fn maybe_compact<FS, E>(
        &self,
        db: &DbInner<FS, E>,
    ) -> Result<Option<SsTable>, SsTableError>
    where
        FS: ManifestFs<E>,
        E: Executor + Timer + Clone,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        let _guard = self.lock.lock().await;
        self.compactor
            .maybe_compact(db, Arc::clone(&self.config))
            .await
    }
}

/// Database handle with shared ownership.
///
/// `DB` wraps the internal database state in an `Arc`, allowing cheap cloning
/// and concurrent access. This is the primary type users interact with.
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use fusio::{executor::tokio::TokioExecutor, mem::fs::InMemoryFs};
/// use tonbo::{db::DB, schema::SchemaBuilder};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
///     let config = SchemaBuilder::from_schema(Arc::clone(&schema))
///         .primary_key("id")
///         .build()?;
///
///     let db: DB<InMemoryFs, TokioExecutor> = DB::<InMemoryFs, TokioExecutor>::builder(config)
///         .in_memory("example-db")?
///         .build()
///         .await?;
///
///     // Clone is cheap (just Arc clone)
///     let _db2 = db.clone();
///
///     // Begin a transaction
///     let _tx = db.begin_transaction().await?;
///     Ok(())
/// }
/// ```
pub struct DB<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    inner: Arc<DbInner<FS, E>>,
}

impl<FS, E> Clone for DB<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<FS, E> DB<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Create a DB from an inner handle.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn from_inner(inner: Arc<DbInner<FS, E>>) -> Self {
        Self { inner }
    }

    #[cfg(not(test))]
    pub(crate) fn from_inner(inner: Arc<DbInner<FS, E>>) -> Self {
        Self { inner }
    }

    /// Access the inner handle (for internal/testing use).
    #[cfg(test)]
    #[doc(hidden)]
    pub fn inner(&self) -> &Arc<DbInner<FS, E>> {
        &self.inner
    }

    /// Consume the DB and return the inner handle (for testing).
    ///
    /// Panics if there are other references to the inner handle.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn into_inner(self) -> DbInner<FS, E> {
        Arc::try_unwrap(self.inner).unwrap_or_else(|_| panic!("DB has multiple references"))
    }

    /// Begin a read-write transaction.
    ///
    /// The transaction captures a snapshot of the current database state and
    /// allows staging mutations (upserts, deletes) before committing atomically.
    pub async fn begin_transaction(&self) -> Result<Transaction<FS, E>, TransactionError> {
        let handle = Arc::clone(&self.inner);
        let snapshot = handle.begin_snapshot().await?;
        let durability = if handle.wal_handle().is_some() {
            TransactionDurability::Durable
        } else {
            TransactionDurability::Volatile
        };
        let schema = handle.schema.clone();
        let delete_schema = handle.delete_schema.clone();
        let extractor = Arc::clone(handle.extractor());
        let commit_ack_mode = handle.commit_ack_mode;
        Ok(Transaction::new(
            handle,
            schema,
            delete_schema,
            extractor,
            snapshot,
            commit_ack_mode,
            durability,
        ))
    }

    /// Begin constructing a DB through the fluent builder API.
    pub fn builder(config: DynModeConfig) -> DbBuilder {
        DbBuilder::new(config)
    }

    /// Begin a read-only snapshot for queries.
    pub async fn begin_snapshot(&self) -> Result<TxSnapshot, SnapshotError> {
        self.inner.begin_snapshot().await
    }

    /// Ingest a RecordBatch into the database (auto-commit mode).
    pub async fn ingest(&self, batch: RecordBatch) -> Result<(), DBError> {
        self.inner.ingest(batch).await.map_err(DBError::Key)
    }

    /// Ingest a batch along with its tombstone bitmap, routing through the WAL when enabled.
    ///
    /// Each entry in `tombstones` indicates whether the corresponding row should be marked
    /// as deleted (true = delete, false = insert/update).
    pub async fn ingest_with_tombstones(
        &self,
        batch: RecordBatch,
        tombstones: Vec<bool>,
    ) -> Result<(), DBError> {
        self.inner
            .ingest_with_tombstones(batch, tombstones)
            .await
            .map_err(DBError::Key)
    }

    /// Start building a scan query.
    pub fn scan(&self) -> ScanBuilder<'_, FS, E> {
        ScanBuilder::new(&self.inner)
    }

    /// Whether a background compaction worker was spawned for this DB.
    #[cfg(test)]
    pub fn has_compaction_worker(&self) -> bool {
        self.inner.has_compaction_worker()
    }

    /// Open a read-only snapshot pinned to a specific historical timestamp.
    ///
    /// This enables "time travel" queries: you can read the database state as it
    /// existed at any previously committed version. Use [`list_versions`](Self::list_versions)
    /// to discover available timestamps.
    ///
    /// # Example
    /// ```ignore
    /// // Get available versions
    /// let versions = db.list_versions(10).await?;
    ///
    /// // Query at an older version
    /// if let Some(old_version) = versions.last() {
    ///     let snapshot = db.snapshot_at(old_version.timestamp).await?;
    ///     let old_data = snapshot.scan(&db).collect().await?;
    /// }
    /// ```
    pub async fn snapshot_at(&self, timestamp: Timestamp) -> Result<TxSnapshot, SnapshotError> {
        self.inner.snapshot_at(timestamp).await
    }

    /// List committed versions of the database, ordered newest-first.
    ///
    /// Each commit (via transaction or ingest) creates a new version with a unique
    /// timestamp. This method returns metadata about up to `limit` recent versions.
    ///
    /// Use the returned timestamps with [`snapshot_at`](Self::snapshot_at) to query
    /// historical data.
    ///
    /// # Example
    /// ```ignore
    /// // List the 5 most recent versions
    /// let versions = db.list_versions(5).await?;
    /// for v in &versions {
    ///     println!("Version {} has {} SSTs", v.timestamp.get(), v.sst_count);
    /// }
    /// ```
    pub async fn list_versions(&self, limit: usize) -> Result<Vec<Version>, ManifestError> {
        self.inner.list_versions(limit).await
    }
}

impl<E> DB<InMemoryFs, E>
where
    E: Executor + Timer + Clone + 'static,
{
    /// Create a new in-memory DB with the given configuration.
    ///
    /// This is primarily for testing and prototyping.
    #[cfg(test)]
    pub(crate) async fn new(
        config: DynModeConfig,
        executor: Arc<E>,
    ) -> Result<Self, KeyExtractError> {
        let inner = DbInner::new(config, executor).await?;
        Ok(Self::from_inner(Arc::new(inner)))
    }

    /// Create a new in-memory DB with a custom seal policy.
    #[cfg(test)]
    pub(crate) async fn new_with_policy(
        config: DynModeConfig,
        executor: Arc<E>,
        policy: Arc<dyn crate::inmem::policy::SealPolicy + Send + Sync>,
    ) -> Result<Self, KeyExtractError> {
        let mut inner = DbInner::new(config, executor).await?;
        inner.set_seal_policy(policy);
        Ok(Self::from_inner(Arc::new(inner)))
    }
}

type LockMap<K> = Arc<LockableHashMap<K, ()>>;

#[cfg(test)]
fn manifest_error_as_key_extract(err: ManifestError) -> KeyExtractError {
    KeyExtractError::Arrow(ArrowError::ComputeError(format!("manifest error: {err}")))
}

/// Internal database instance bound to a filesystem `FS` and executor `E`.
///
/// Users should interact with [`DB`] instead, which wraps this in an `Arc`.
/// This type is exposed for testing purposes via the `test-helpers` feature.
#[cfg(test)]
#[doc(hidden)]
pub struct DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    pub(crate) schema: SchemaRef,
    pub(crate) delete_schema: SchemaRef,
    pub(crate) commit_ack_mode: CommitAckMode,
    /// Mutable memtable with internal locking and auto-seal support.
    mem: DynMem,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest) plus metadata.
    seal_state: Mutex<SealState>,
    // Sealing policy (pure/lock-free) and last seal timestamp (held inside seal_state)
    policy: Arc<dyn SealPolicy + Send + Sync>,
    // Executor powering async subsystems such as the WAL.
    pub(crate) executor: Arc<E>,
    /// Unified filesystem access for SSTable reads, WAL, and other I/O operations.
    fs: Arc<dyn DynFs>,
    // Optional WAL handle when durability is enabled.
    wal: Option<WalHandle<E>>,
    /// Pending WAL configuration captured before the writer is installed.
    wal_config: Option<RuntimeWalConfig>,
    /// Static table metadata registered in the manifest (cached to survive transient catalog
    /// misses).
    table_meta: TableMeta,
    /// Monotonic commit timestamp assigned to ingests (autocommit path for now).
    commit_clock: CommitClock,
    /// Manifest handle with concrete filesystem type for static dispatch.
    manifest: TonboManifest<FS, E>,
    manifest_table: TableId,
    /// Cached bloom filters for remote SST pruning.
    bloom_cache: Arc<E::Mutex<BloomFilterCache>>,
    /// Cached Aisle pruners keyed by schema fingerprint.
    pruner_cache: Arc<E::Mutex<PrunerCache>>,
    /// Cached Parquet metadata for SST pruning.
    metadata_cache: Arc<E::Mutex<ParquetMetadataCache>>,
    /// WAL frame bounds covering the current mutable memtable, if any.
    mutable_wal_range: Arc<Mutex<Option<WalFrameRange>>>,
    /// Per-key transactional locks (wired once transactional writes arrive).
    _key_locks: LockMap<KeyOwned>,
    /// Optional background compaction worker handle.
    compaction_worker: Option<CompactionHandle<E>>,
    /// Async gate to serialize flushes triggered by minor compaction or manual calls.
    flush_lock: AsyncMutex<()>,
    /// Optional minor compaction hook.
    minor_compaction: Option<MinorCompactionState>,
}

/// Internal database instance bound to a filesystem `FS` and executor `E`.
///
/// Users should interact with [`DB`] instead, which wraps this in an `Arc`.
#[cfg(not(test))]
pub(crate) struct DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    pub(crate) schema: SchemaRef,
    pub(crate) delete_schema: SchemaRef,
    pub(crate) commit_ack_mode: CommitAckMode,
    /// Mutable memtable with internal locking and auto-seal support.
    mem: DynMem,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest) plus metadata.
    seal_state: Mutex<SealState>,
    // Sealing policy (pure/lock-free) and last seal timestamp (held inside seal_state)
    policy: Arc<dyn SealPolicy + Send + Sync>,
    // Executor powering async subsystems such as the WAL.
    pub(crate) executor: Arc<E>,
    /// Unified filesystem access for SSTable reads, WAL, and other I/O operations.
    fs: Arc<dyn DynFs>,
    // Optional WAL handle when durability is enabled.
    wal: Option<WalHandle<E>>,
    /// Pending WAL configuration captured before the writer is installed.
    wal_config: Option<RuntimeWalConfig>,
    /// Static table metadata registered in the manifest (cached to survive transient catalog
    /// misses).
    table_meta: TableMeta,
    /// Monotonic commit timestamp assigned to ingests (autocommit path for now).
    commit_clock: CommitClock,
    /// Manifest handle with concrete filesystem type for static dispatch.
    manifest: TonboManifest<FS, E>,
    manifest_table: TableId,
    /// Cached bloom filters for remote SST pruning.
    bloom_cache: Arc<E::Mutex<BloomFilterCache>>,
    /// Cached Aisle pruners keyed by schema fingerprint.
    pruner_cache: Arc<E::Mutex<PrunerCache>>,
    /// Cached Parquet metadata for SST pruning.
    metadata_cache: Arc<E::Mutex<ParquetMetadataCache>>,
    /// WAL frame bounds covering the current mutable memtable, if any.
    mutable_wal_range: Arc<Mutex<Option<WalFrameRange>>>,
    /// Per-key transactional locks (wired once transactional writes arrive).
    _key_locks: LockMap<KeyOwned>,
    /// Optional background compaction worker handle.
    compaction_worker: Option<CompactionHandle<E>>,
    /// Async gate to serialize flushes triggered by minor compaction or manual calls.
    flush_lock: AsyncMutex<()>,
    /// Optional minor compaction hook.
    minor_compaction: Option<MinorCompactionState>,
}

// SAFETY: DbInner shares internal state behind explicit synchronization.
unsafe impl<FS, E> Send for DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + Send + Sync,
    DynMem: Send + Sync,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
}

// SAFETY: See rationale above for `Send`; read access is synchronized via external locks.
unsafe impl<FS, E> Sync for DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + Send + Sync,
    DynMem: Send + Sync,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
}

impl<FS, E> DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    #[inline]
    fn seal_state_lock(&self) -> MutexGuard<'_, SealState> {
        self.seal_state.lock().expect("seal_state mutex poisoned")
    }

    /// Access the key extractor for this DB.
    pub(crate) fn extractor(&self) -> &Arc<dyn KeyProjection> {
        self.mem.extractor()
    }

    /// Access the delete sidecar key extractor for this DB.
    ///
    /// The delete extractor uses a key-only schema (no value columns),
    /// matching the schema of delete sidecar parquet files.
    pub(crate) fn delete_extractor(&self) -> &Arc<dyn KeyProjection> {
        self.mem.delete_projection()
    }

    /// Access the shared bloom filter cache for pruning.
    pub(crate) fn bloom_cache(&self) -> Arc<E::Mutex<BloomFilterCache>> {
        Arc::clone(&self.bloom_cache)
    }

    /// Access the shared pruner cache for pruning.
    pub(crate) fn pruner_cache(&self) -> Arc<E::Mutex<PrunerCache>> {
        Arc::clone(&self.pruner_cache)
    }

    /// Access the shared Parquet metadata cache for pruning.
    pub(crate) fn metadata_cache(&self) -> Arc<E::Mutex<ParquetMetadataCache>> {
        Arc::clone(&self.metadata_cache)
    }

    /// Acquire a read guard to the mutable memtable (for testing/inspection).
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn mem_read(&self) -> crate::inmem::mutable::memtable::TestMemRef<'_> {
        crate::inmem::mutable::memtable::TestMemRef(&self.mem)
    }

    /// Replace the mutable memtable with one that has specified batch capacity (for testing).
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn set_mem_capacity(&mut self, capacity: usize) {
        self.mem = DynMem::with_capacity(
            self.schema.clone(),
            Arc::clone(self.mem.extractor()),
            Arc::clone(self.mem.delete_projection()),
            capacity,
        );
    }

    /// Seal the mutable memtable and return the sealed segment (for testing).
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn seal_mutable(
        &self,
    ) -> Option<crate::inmem::immutable::memtable::ImmutableMemTable> {
        self.mem.seal_now().expect("seal should not fail")
    }

    #[allow(clippy::too_many_arguments)]
    fn from_components(
        schema: SchemaRef,
        delete_schema: SchemaRef,
        commit_ack_mode: CommitAckMode,
        mem: DynMem,
        fs: Arc<dyn DynFs>,
        manifest: TonboManifest<FS, E>,
        manifest_table: TableId,
        table_meta: TableMeta,
        wal_config: Option<RuntimeWalConfig>,
        executor: Arc<E>,
    ) -> Self {
        Self {
            schema,
            delete_schema,
            commit_ack_mode,
            mem,
            seal_state: Mutex::new(SealState::default()),
            policy: crate::inmem::policy::default_policy(),
            executor,
            fs,
            wal: None,
            wal_config,
            table_meta,
            commit_clock: CommitClock::default(),
            manifest,
            manifest_table,
            bloom_cache: default_bloom_cache::<E>(),
            pruner_cache: Arc::new(E::mutex(HashMap::new())),
            metadata_cache: default_parquet_metadata_cache::<E>(),
            mutable_wal_range: Arc::new(Mutex::new(None)),
            _key_locks: Arc::new(LockableHashMap::new()),
            compaction_worker: None,
            flush_lock: AsyncMutex::new(()),
            minor_compaction: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn recover_with_wal_with_manifest(
        config: DynModeConfig,
        executor: Arc<E>,
        fs: Arc<dyn DynFs>,
        wal_cfg: RuntimeWalConfig,
        manifest: TonboManifest<FS, E>,
        manifest_table: TableId,
        table_meta: TableMeta,
    ) -> Result<Self, KeyExtractError> {
        Self::recover_with_wal_inner(
            config,
            executor,
            fs,
            wal_cfg,
            manifest,
            manifest_table,
            table_meta,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_with_wal_inner(
        config: DynModeConfig,
        executor: Arc<E>,
        fs: Arc<dyn DynFs>,
        wal_cfg: RuntimeWalConfig,
        manifest: TonboManifest<FS, E>,
        manifest_table: TableId,
        table_meta: TableMeta,
    ) -> Result<Self, KeyExtractError> {
        let state_commit_hint = if let Some(store) = wal_cfg.state_store.as_ref() {
            WalStateHandle::load(Arc::clone(store), &wal_cfg.dir)
                .await?
                .state()
                .commit_ts()
        } else {
            None
        };
        let (schema, delete_schema, commit_ack_mode, mem) = config.build()?;
        let mut db = Self::from_components(
            schema,
            delete_schema,
            commit_ack_mode,
            mem,
            fs,
            manifest,
            manifest_table,
            table_meta,
            Some(wal_cfg.clone()),
            executor,
        );
        db.set_wal_config(Some(wal_cfg.clone()));

        let wal_floor = db.manifest_wal_floor().await;
        let replayer = Replayer::new(wal_cfg);
        let events = replayer
            .scan_with_floor(wal_floor.as_ref())
            .await
            .map_err(KeyExtractError::from)?;
        if events.is_empty() {
            db.set_mutable_wal_range(None);
        } else if let Some(ref floor_ref) = wal_floor {
            db.set_mutable_wal_range(Some(WalFrameRange {
                first: floor_ref.first_frame(),
                last: floor_ref.last_frame(),
            }));
        } else {
            db.set_mutable_wal_range(Some(WalFrameRange {
                first: INITIAL_FRAME_SEQ,
                last: INITIAL_FRAME_SEQ,
            }));
        }

        let last_commit_ts = db.replay_wal_events(events)?;
        let effective_commit = last_commit_ts.or(state_commit_hint);
        if let Some(ts) = effective_commit {
            db.commit_clock.advance_to_at_least(ts.saturating_add(1));
        }

        Ok(db)
    }

    /// Unified ingestion entry point for dynamic batches.
    pub async fn ingest(&self, batch: RecordBatch) -> Result<(), KeyExtractError> {
        if self.schema.as_ref() != batch.schema().as_ref() {
            return Err(KeyExtractError::SchemaMismatch {
                expected: self.schema.clone(),
                actual: batch.schema(),
            });
        }

        let commit_ts = self.next_commit_ts();
        let mut wal_spans: Vec<(u64, u64)> = Vec::new();
        if let Some(handle) = self.wal_handle().cloned() {
            let provisional_id = handle.next_provisional_id();
            let append_ticket = handle
                .txn_append(provisional_id, &batch, commit_ts)
                .await
                .map_err(KeyExtractError::from)?;
            let commit_ticket = handle
                .txn_commit(provisional_id, commit_ts)
                .await
                .map_err(KeyExtractError::from)?;
            for ticket in [append_ticket, commit_ticket] {
                let ack = ticket.durable().await.map_err(KeyExtractError::from)?;
                wal_spans.push((ack.first_seq, ack.last_seq));
            }
        }
        // Record WAL spans BEFORE insert that may trigger auto-seal.
        // If we record after, a sealed segment would miss the current batch's frames,
        // causing WAL GC to prematurely delete frames needed for recovery.
        for (first, last) in wal_spans {
            self.observe_mutable_wal_span(first, last);
        }
        self.insert_into_mutable(batch, commit_ts)?;
        self.maybe_seal_after_insert()?;
        self.maybe_run_minor_compaction().await.map_err(|err| {
            KeyExtractError::Arrow(ArrowError::ComputeError(format!(
                "minor compaction failed: {err}"
            )))
        })?;
        Ok(())
    }

    /// Access the executor powering async subsystems.
    pub(crate) fn executor(&self) -> &Arc<E> {
        &self.executor
    }

    /// Table ID registered in the manifest for this DB.
    #[cfg(test)]
    pub fn table_id(&self) -> TableId {
        self.manifest_table
    }

    /// Open a read-only snapshot pinned to the current manifest head.
    pub async fn begin_snapshot(&self) -> Result<TxSnapshot, SnapshotError> {
        let manifest_snapshot = self
            .manifest
            .snapshot_latest_with_fallback(self.manifest_table, &self.table_meta)
            .await?;
        let next_ts = self.commit_clock.peek();
        let read_ts = next_ts.saturating_sub(1);
        let read_view = ReadView::new(read_ts);
        Ok(TxSnapshot::from_table_snapshot(
            read_view,
            manifest_snapshot,
        ))
    }

    /// Open a read-only snapshot pinned to a specific historical timestamp.
    ///
    /// This enables "time travel" queries: you can read the database state as it
    /// existed at any previously committed version. Use [`list_versions`](Self::list_versions)
    /// to discover available timestamps.
    ///
    /// The snapshot will load the exact SST files that existed at that version,
    /// allowing queries to see historical data even if files were later compacted away.
    ///
    /// # Example
    /// ```ignore
    /// // Get available versions
    /// let versions = db.list_versions(10).await?;
    ///
    /// // Query at an older version
    /// if let Some(old_version) = versions.last() {
    ///     let snapshot = db.snapshot_at(old_version.timestamp).await?;
    ///     let old_data = snapshot.scan(&db).collect().await?;
    /// }
    /// ```
    pub async fn snapshot_at(&self, timestamp: Timestamp) -> Result<TxSnapshot, SnapshotError> {
        // Find the version that was active at the requested timestamp.
        // List versions to find one with commit_timestamp <= requested timestamp.
        let versions = self
            .manifest
            .list_versions(self.manifest_table, 0) // 0 = unlimited
            .await?;

        // Find the version at or before the requested timestamp (versions are newest-first)
        let target_version = versions.iter().find(|v| v.commit_timestamp <= timestamp);

        let manifest_snapshot = if let Some(version) = target_version {
            // Load the historical version from manifest
            self.manifest
                .snapshot_at_version(self.manifest_table, version.commit_timestamp)
                .await?
        } else {
            // No version at or before the timestamp - use latest with MVCC filtering
            self.manifest
                .snapshot_latest_with_fallback(self.manifest_table, &self.table_meta)
                .await?
        };

        let read_view = ReadView::new(timestamp);
        Ok(TxSnapshot::from_table_snapshot(
            read_view,
            manifest_snapshot,
        ))
    }

    /// List committed versions of the database, ordered newest-first.
    ///
    /// Each commit (via transaction or ingest) creates a new version with a unique
    /// timestamp. This method returns metadata about up to `limit` recent versions.
    ///
    /// Use the returned timestamps with [`snapshot_at`](Self::snapshot_at) to query
    /// historical data.
    ///
    /// # Example
    /// ```ignore
    /// // List the 5 most recent versions
    /// let versions = db.list_versions(5).await?;
    /// for v in &versions {
    ///     println!("Version {} has {} SSTs", v.timestamp.get(), v.sst_count);
    /// }
    /// ```
    pub async fn list_versions(&self, limit: usize) -> Result<Vec<Version>, ManifestError> {
        let states = self
            .manifest
            .list_versions(self.manifest_table, limit)
            .await?;

        Ok(states
            .into_iter()
            .map(|s| Version {
                timestamp: s.commit_timestamp,
                sst_count: s.ssts.iter().map(|level| level.len()).sum(),
                level_count: s.ssts.iter().filter(|level| !level.is_empty()).count(),
            })
            .collect())
    }

    /// Allocate the next commit timestamp for WAL/autocommit flows.
    pub(crate) fn next_commit_ts(&self) -> Timestamp {
        self.commit_clock.alloc()
    }

    /// Number of immutable segments attached to this DB (oldest..newest).
    pub(crate) fn num_immutable_segments(&self) -> usize {
        self.seal_state_lock().immutables.len()
    }

    /// Best-effort minor compaction trigger based on configured policy.
    pub(crate) async fn maybe_run_minor_compaction(&self) -> Result<Option<SsTable>, SsTableError> {
        if let Some(compaction) = &self.minor_compaction {
            compaction.maybe_compact(self).await
        } else {
            Ok(None)
        }
    }

    /// Plan and flush immutable segments into a Parquet-backed SSTable.
    pub(crate) async fn flush_immutables_with_descriptor(
        &self,
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> Result<SsTable, SsTableError> {
        let _guard = self.flush_lock.lock().await;
        let (immutables_snapshot, flush_count) = {
            let seal_read = self.seal_state_lock();
            if seal_read.immutables.is_empty() {
                return Err(SsTableError::NoImmutableSegments);
            }
            (seal_read.immutables.clone(), seal_read.immutables.len())
        };
        let mut builder = SsTableBuilder::new(config, descriptor);
        for seg in &immutables_snapshot {
            builder.add_immutable(seg)?;
        }
        let existing_floor = self.manifest_wal_floor().await;
        let live_floor = self.mutable_wal_range_snapshot().map(|range| range.first);
        let (wal_ids, wal_refs) = if let Some(cfg) = &self.wal_config {
            match manifest_ext::collect_wal_segment_refs(cfg, existing_floor.as_ref(), live_floor)
                .await
            {
                Ok(refs) => {
                    let wal_ids = if refs.is_empty() {
                        builder.set_wal_ids(None);
                        None
                    } else {
                        let ids: Vec<FileId> = refs.iter().map(|ref_| *ref_.file_id()).collect();
                        builder.set_wal_ids(Some(ids.clone()));
                        Some(ids)
                    };
                    (wal_ids, Some(refs))
                }
                Err(_err) => {
                    return Err(SsTableError::Manifest(ManifestError::Invariant(
                        "failed to enumerate wal segments",
                    )));
                }
            }
        } else {
            builder.set_wal_ids(None);
            (None, None)
        };

        let executor: E = (*self.executor).clone();
        match builder.finish(executor).await {
            Ok(table) => {
                let descriptor_ref = table.descriptor();
                let data_path = descriptor_ref.data_path().cloned().ok_or_else(|| {
                    SsTableError::Manifest(ManifestError::Invariant(
                        "sst descriptor missing data path",
                    ))
                })?;
                let delete_path = descriptor_ref.delete_path().cloned();
                let stats = descriptor_ref.stats().cloned();
                let sst_entry = SstEntry::new(
                    descriptor_ref.id().clone(),
                    stats,
                    wal_ids.clone(),
                    data_path,
                    delete_path,
                );
                let mut edits = vec![VersionEdit::AddSsts {
                    level: descriptor_ref.level() as u32,
                    entries: vec![sst_entry],
                }];

                if let Some(stats) = descriptor_ref.stats()
                    && let Some(max_commit) = stats.max_commit_ts
                {
                    edits.push(VersionEdit::SetTombstoneWatermark {
                        watermark: max_commit.get(),
                    });
                }

                if let Some(refs) = wal_refs {
                    edits.push(VersionEdit::SetWalSegments { segments: refs });
                }
                self.manifest
                    .apply_version_edits(self.manifest_table, &edits)
                    .await?;

                self.prune_wal_segments_below_floor().await;

                // Only remove the segments we actually flushed (the first `flush_count`).
                // New segments may have been appended concurrently; those must be preserved.
                let mut seal = self.seal_state_lock();
                let actual_len = seal.immutables.len();
                if actual_len >= flush_count {
                    seal.immutables.drain(0..flush_count);
                } else {
                    // Defensive: should not happen, but clear if state is inconsistent.
                    seal.immutables.clear();
                }
                let wal_ranges_len = seal.immutable_wal_ranges.len();
                if wal_ranges_len >= flush_count {
                    seal.immutable_wal_ranges.drain(0..flush_count);
                } else {
                    seal.immutable_wal_ranges.clear();
                }
                seal.last_seal_at = Some(self.executor.now());
                self.kick_compaction_worker();
                Ok(table)
            }
            Err(err) => Err(err),
        }
    }

    async fn manifest_wal_floor(&self) -> Option<WalSegmentRef> {
        self.manifest
            .wal_floor(self.manifest_table)
            .await
            .ok()
            .flatten()
    }

    /// Set or replace the sealing policy used by this DB.
    #[cfg(test)]
    pub fn set_seal_policy(&mut self, policy: Arc<dyn SealPolicy + Send + Sync>) {
        self.policy = policy;
    }

    /// Access the per-key transactional lock map.
    pub(crate) fn key_locks(&self) -> &LockMap<KeyOwned> {
        &self._key_locks
    }
}

// In-memory convenience constructors.
#[cfg(test)]
impl<E> DbInner<InMemoryFs, E>
where
    E: Executor + Timer + Clone + 'static,
{
    /// Construct a new in-memory DbInner using the dynamic configuration.
    pub(crate) async fn new(
        config: DynModeConfig,
        executor: Arc<E>,
    ) -> Result<Self, KeyExtractError> {
        use crate::{
            id::FileIdGenerator, manifest::init_in_memory_manifest, mode::table_definition,
        };

        let table_definition = table_definition(&config, builder::DEFAULT_TABLE_NAME);
        let (schema, delete_schema, commit_ack_mode, mem) = config.build()?;
        let file_ids = FileIdGenerator::default();
        let manifest = init_in_memory_manifest((*executor).clone())
            .await
            .map_err(manifest_error_as_key_extract)?;
        let table_meta = manifest
            .register_table(&file_ids, &table_definition)
            .await
            .map_err(manifest_error_as_key_extract)?;
        let manifest_table = table_meta.table_id;
        let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
        Ok(Self::from_components(
            schema,
            delete_schema,
            commit_ack_mode,
            mem,
            fs,
            manifest,
            manifest_table,
            table_meta,
            None,
            executor,
        ))
    }
}
