//! Generic DB parametrized by a `Mode` implementation.
//!
//! At the moment Tonbo only ships with the dynamic runtime-schema mode. The
//! trait-driven structure remains so that compile-time typed dispatch can be
//! reintroduced without reshaping the API.

use std::{
    future::Future,
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard, RwLock as StdRwLock, RwLockReadGuard, RwLockWriteGuard},
};

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use compaction::CompactionLoopHandle;
use fusio::{
    DynFs,
    executor::{Executor, Timer},
};
use lockable::LockableHashMap;
use wal::SealState;
use web_time::Instant;
mod builder;
mod compaction;
mod error;
mod scan;
#[cfg(all(test, feature = "tokio-runtime"))]
mod tests;
mod wal;

pub use builder::{
    AwsCreds, AwsCredsError, DbBuildError, DbBuilder, ObjectSpec, S3Spec, WalConfig,
};
pub use error::DBError;
pub use scan::DEFAULT_SCAN_BATCH_ROWS;
pub(crate) use wal::{TxnWalPublishContext, WalFrameRange};

pub use crate::mode::{DynMode, DynModeConfig, Mode};
use crate::{
    extractor::KeyExtractError,
    id::{FileId, FileIdGenerator},
    inmem::{mutable::MutableLayout, policy::SealPolicy},
    key::KeyOwned,
    manifest::{
        ManifestError, SstEntry, TableId, TonboManifest, VersionEdit, WalSegmentRef,
        init_in_memory_manifest,
    },
    mode::CatalogDescribe,
    mvcc::{CommitClock, ReadView, Timestamp},
    ondisk::sstable::{SsTable, SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableError},
    transaction::{
        Snapshot as TxSnapshot, SnapshotError, Transaction, TransactionDurability, TransactionError,
    },
    wal::{
        WalConfig as RuntimeWalConfig, WalHandle, frame::INITIAL_FRAME_SEQ, manifest_ext,
        replay::Replayer, state::WalStateHandle,
    },
};

/// Shared handle for the dynamic mode database backed by an `Arc`.
pub type DynDbHandle<E> = Arc<DB<DynMode, E>>;
/// Extension methods on dynamic DB handles.
pub trait DynDbHandleExt<E>
where
    E: Executor + Timer,
{
    /// Clone the underlying `Arc`.
    fn clone_handle(&self) -> DynDbHandle<E>;

    /// Begin a transaction using the shared handle.
    fn begin_transaction(&self) -> impl Future<Output = Result<Transaction<E>, TransactionError>>;
}

type LockMap<K> = Arc<LockableHashMap<K, ()>>;

impl<E> DynDbHandleExt<E> for DynDbHandle<E>
where
    E: Executor + Timer,
{
    fn clone_handle(&self) -> DynDbHandle<E> {
        Arc::clone(self)
    }

    fn begin_transaction(&self) -> impl Future<Output = Result<Transaction<E>, TransactionError>> {
        let handle = Arc::clone(self);
        async move {
            let snapshot = handle.begin_snapshot().await?;
            let durability = if handle.wal_handle().is_some() {
                TransactionDurability::Durable
            } else {
                TransactionDurability::Volatile
            };
            let schema = handle.mode.schema.clone();
            let delete_schema = handle.mode.delete_schema.clone();
            let extractor = Arc::clone(&handle.mode.extractor);
            let commit_ack_mode = handle.mode.commit_ack_mode;
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
    }
}

fn manifest_error_as_key_extract(err: ManifestError) -> KeyExtractError {
    KeyExtractError::Arrow(ArrowError::ComputeError(format!("manifest error: {err}")))
}

/// A DB parametrized by a mode `M` that defines key, payload and insert interface.
pub struct DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    mode: M,
    mem: Arc<StdRwLock<M::Mutable>>,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest) plus metadata.
    seal_state: Mutex<SealState<M>>,
    // Sealing policy (pure/lock-free) and last seal timestamp (held inside seal_state)
    policy: Arc<dyn SealPolicy + Send + Sync>,
    // Executor powering async subsystems such as the WAL.
    executor: Arc<E>,
    /// Unified filesystem access for SSTable reads, WAL, and other I/O operations.
    fs: Arc<dyn DynFs>,
    // Optional WAL handle when durability is enabled.
    wal: Option<WalHandle<E>>,
    /// Pending WAL configuration captured before the writer is installed.
    wal_config: Option<RuntimeWalConfig>,
    /// Monotonic commit timestamp assigned to ingests (autocommit path for now).
    commit_clock: CommitClock,
    /// Manifest handle used by the dev branch manifest integration (in-memory for now).
    manifest: TonboManifest,
    manifest_table: TableId,
    /// WAL frame bounds covering the current mutable memtable, if any.
    mutable_wal_range: Arc<Mutex<Option<WalFrameRange>>>,
    /// Per-key transactional locks (wired once transactional writes arrive).
    _key_locks: LockMap<M::Key>,
    /// Optional background compaction driver/handle pair for local loops.
    compaction_worker: Option<CompactionLoopHandle<M, E>>,
}

// SAFETY: DB shares internal state behind explicit synchronization. The mode key and executor
// bounds ensure the constituent types are safe to send/share across threads when guarded.
unsafe impl<M, E> Send for DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone + Send + Sync,
    M::Mutable: Send + Sync,
    E: Executor + Timer + Send + Sync,
{
}

// SAFETY: See rationale above for `Send`; read access is synchronized via external locks.
unsafe impl<M, E> Sync for DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone + Send + Sync,
    M::Mutable: Send + Sync,
    E: Executor + Timer + Send + Sync,
{
}

impl<M, E> DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    #[inline]
    fn mem_read(&self) -> RwLockReadGuard<'_, M::Mutable> {
        self.mem.read().expect("mutable mem rwlock poisoned")
    }

    #[inline]
    fn mem_write(&self) -> RwLockWriteGuard<'_, M::Mutable> {
        self.mem.write().expect("mutable mem rwlock poisoned")
    }

    #[inline]
    fn seal_state_lock(&self) -> MutexGuard<'_, SealState<M>> {
        self.seal_state.lock().expect("seal_state mutex poisoned")
    }

    /// Begin constructing a DB in mode `M` through the fluent builder API.
    pub fn builder(config: M::Config) -> DbBuilder<M>
    where
        M: Sized + CatalogDescribe,
    {
        DbBuilder::new(config)
    }
}

// Methods common to all modes
impl<M, E> DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    #[allow(clippy::too_many_arguments)]
    fn from_components(
        mode: M,
        mem: M::Mutable,
        fs: Arc<dyn DynFs>,
        manifest: TonboManifest,
        manifest_table: TableId,
        wal_config: Option<RuntimeWalConfig>,
        executor: Arc<E>,
    ) -> Self
    where
        M: Sized,
    {
        Self {
            mode,
            mem: Arc::new(StdRwLock::new(mem)),
            seal_state: Mutex::new(SealState::default()),
            policy: crate::inmem::policy::default_policy(),
            executor,
            fs,
            wal: None,
            wal_config,
            commit_clock: CommitClock::default(),
            manifest,
            manifest_table,
            mutable_wal_range: Arc::new(Mutex::new(None)),
            _key_locks: Arc::new(LockableHashMap::new()),
            compaction_worker: None,
        }
    }

    /// Construct a new DB in mode `M` using its configuration.
    pub async fn new(config: M::Config, executor: Arc<E>) -> Result<Self, KeyExtractError>
    where
        M: Sized + CatalogDescribe,
    {
        let table_definition = M::table_definition(&config, builder::DEFAULT_TABLE_NAME);
        let (mode, mem) = M::build(config)?;
        let file_ids = FileIdGenerator::default();
        let manifest = init_in_memory_manifest()
            .await
            .map_err(manifest_error_as_key_extract)?;
        let table_meta = manifest
            .register_table(&file_ids, &table_definition)
            .await
            .map_err(manifest_error_as_key_extract)?;
        let manifest_table = table_meta.table_id;
        let fs: Arc<dyn DynFs> = Arc::new(fusio::disk::LocalFs {});
        Ok(Self::from_components(
            mode,
            mem,
            fs,
            manifest,
            manifest_table,
            None,
            executor,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn recover_with_wal_with_manifest(
        config: M::Config,
        executor: Arc<E>,
        fs: Arc<dyn DynFs>,
        wal_cfg: RuntimeWalConfig,
        manifest: TonboManifest,
        manifest_table: TableId,
    ) -> Result<Self, KeyExtractError>
    where
        M: Sized + CatalogDescribe,
    {
        Self::recover_with_wal_inner(config, executor, fs, wal_cfg, manifest, manifest_table).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_with_wal_inner(
        config: M::Config,
        executor: Arc<E>,
        fs: Arc<dyn DynFs>,
        wal_cfg: RuntimeWalConfig,
        manifest: TonboManifest,
        manifest_table: TableId,
    ) -> Result<Self, KeyExtractError>
    where
        M: Sized + CatalogDescribe,
    {
        let state_commit_hint = if let Some(store) = wal_cfg.state_store.as_ref() {
            WalStateHandle::load(Arc::clone(store), &wal_cfg.dir)
                .await?
                .state()
                .commit_ts()
        } else {
            None
        };
        let (mode, mem) = M::build(config)?;
        let mut db = Self::from_components(
            mode,
            mem,
            fs,
            manifest,
            manifest_table,
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

        let last_commit_ts = M::replay_wal(&mut db, events)?;
        let effective_commit = last_commit_ts.or(state_commit_hint);
        if let Some(ts) = effective_commit {
            db.commit_clock.advance_to_at_least(ts.saturating_add(1));
        }

        Ok(db)
    }

    /// Unified ingestion entry point using the mode's insertion contract.
    pub async fn ingest(&self, input: M::InsertInput) -> Result<(), KeyExtractError>
    where
        M: Sized,
    {
        M::insert(self, input).await
    }

    /// Ingest many inputs sequentially.
    pub async fn ingest_many<I>(&self, inputs: I) -> Result<(), KeyExtractError>
    where
        I: IntoIterator<Item = M::InsertInput>,
        M: Sized,
    {
        for item in inputs.into_iter() {
            M::insert(self, item).await?;
        }
        Ok(())
    }

    /// Approximate bytes used by keys in the mutable memtable.
    pub fn approx_mutable_bytes(&self) -> usize {
        <M::Mutable as MutableLayout<M::Key>>::approx_bytes(&self.mem_read())
    }

    /// Access the executor powering async subsystems.
    pub(crate) fn executor(&self) -> &Arc<E> {
        &self.executor
    }

    #[cfg(test)]
    pub(crate) fn manifest_table_id(&self) -> TableId {
        self.manifest_table
    }

    /// Open a read-only snapshot pinned to the current manifest head.
    pub async fn begin_snapshot(&self) -> Result<TxSnapshot, SnapshotError> {
        let manifest_snapshot = self.manifest.snapshot_latest(self.manifest_table).await?;
        let next_ts = self.commit_clock.peek();
        let read_ts = next_ts.saturating_sub(1);
        let read_view = ReadView::new(read_ts);
        Ok(TxSnapshot::from_table_snapshot(
            read_view,
            manifest_snapshot,
        ))
    }

    #[cfg(test)]
    pub(crate) async fn begin_snapshot_at(
        &self,
        read_ts: Timestamp,
    ) -> Result<TxSnapshot, SnapshotError> {
        let manifest_snapshot = self.manifest.snapshot_latest(self.manifest_table).await?;
        let read_view = ReadView::new(read_ts);
        Ok(TxSnapshot::from_table_snapshot(
            read_view,
            manifest_snapshot,
        ))
    }

    /// Test-only helper to capture a snapshot at an explicit timestamp.
    #[cfg(test)]
    pub async fn begin_snapshot_at_for_tests(
        &self,
        read_ts: Timestamp,
    ) -> Result<TxSnapshot, SnapshotError> {
        self.begin_snapshot_at(read_ts).await
    }

    /// Allocate the next commit timestamp for WAL/autocommit flows.
    pub(crate) fn next_commit_ts(&self) -> Timestamp {
        self.commit_clock.alloc()
    }

    /// Number of immutable segments attached to this DB (oldest..newest).
    pub fn num_immutable_segments(&self) -> usize {
        self.seal_state_lock().immutables.len()
    }

    /// Plan and flush immutable segments into a Parquet-backed SSTable.
    pub(crate) async fn flush_immutables_with_descriptor(
        &mut self,
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> Result<SsTable<M>, SsTableError>
    where
        M: Sized + Mode<ImmLayout = RecordBatch, Key = KeyOwned>,
    {
        let immutables_snapshot = {
            let seal_read = self.seal_state_lock();
            if seal_read.immutables.is_empty() {
                return Err(SsTableError::NoImmutableSegments);
            }
            seal_read.immutables.clone()
        };
        let mut builder = SsTableBuilder::<M>::new(config, descriptor);
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

        match builder.finish().await {
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

                let mut seal = self.seal_state_lock();
                seal.immutables = Vec::new();
                seal.immutable_wal_ranges.clear();
                seal.last_seal_at = Some(Instant::now());
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
    pub fn set_seal_policy(&mut self, policy: Arc<dyn SealPolicy + Send + Sync>) {
        self.policy = policy;
    }

    /// Access the per-key transactional lock map.
    pub(crate) fn key_locks(&self) -> &LockMap<M::Key> {
        &self._key_locks
    }
}
