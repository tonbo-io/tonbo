//! Generic DB parametrized by a `Mode` implementation.
//!
//! At the moment Tonbo only ships with the dynamic runtime-schema mode. The
//! trait-driven structure remains so that compile-time typed dispatch can be
//! reintroduced without reshaping the API.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, RwLock as StdRwLock, RwLockReadGuard, RwLockWriteGuard},
    time::{Duration, Instant},
};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::take::take;
#[allow(unused_imports)]
use fusio::executor::{Executor, RwLock, Timer};
use futures::{Stream, StreamExt, stream};
use lockable::LockableHashMap;
use typed_arrow_dyn::DynRow;
mod builder;
mod error;

pub use builder::{
    AwsCreds, AwsCredsError, DbBuildError, DbBuilder, ObjectSpec, S3Spec, WalConfig,
};
pub use error::DBError;
use predicate::Predicate;

pub use crate::mode::{DynMode, DynModeConfig, Mode};
use crate::{
    compaction::{
        CompactionHost,
        executor::{
            CompactionError, CompactionExecutor, CompactionJob, CompactionLease, CompactionOutcome,
        },
        planner::{CompactionInput, CompactionPlanner, CompactionSnapshot},
        scheduler::{CompactionScheduler, ScheduledCompaction},
    },
    extractor::KeyExtractError,
    id::{FileId, FileIdGenerator},
    inmem::{
        immutable::{
            self, Immutable,
            memtable::{ImmutableVisibleEntry, MVCC_COMMIT_COL},
        },
        mutable::{DynMem, MutableLayout, memtable::DynRowScanEntry},
        policy::{SealDecision, SealPolicy, StatsProvider},
    },
    key::{KeyOwned, KeyRow},
    manifest::{
        GcPlanState, GcSstRef, ManifestError, ManifestResult, SstEntry, TableId, TonboManifest,
        VersionEdit, VersionState, WalSegmentRef, init_in_memory_manifest,
    },
    mode::CatalogDescribe,
    mvcc::{CommitClock, ReadView, Timestamp},
    ondisk::sstable::{
        SsTable, SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableError, SsTableId,
    },
    query::{
        scan::{ScanPlan, projection_with_predicate},
        stream::{
            Order, OwnedImmutableScan, OwnedMutableScan, ScanStream, merge::MergeStream,
            package::PackageStream,
        },
    },
    transaction::{
        Snapshot as TxSnapshot, SnapshotError, Transaction, TransactionDurability,
        TransactionError, TransactionScan,
    },
    wal::{
        WalConfig as RuntimeWalConfig, WalError, WalHandle, WalResult,
        frame::{DynAppendEvent, INITIAL_FRAME_SEQ, WalEvent},
        manifest_ext,
        replay::Replayer,
        state::WalStateHandle,
    },
};

impl TxSnapshot {
    /// Plan a scan using this snapshot for MVCC visibility and manifest pinning.
    pub async fn plan_scan<E>(
        &self,
        db: &DB<DynMode, E>,
        predicate: &Predicate,
        projected_schema: Option<&SchemaRef>,
        limit: Option<usize>,
    ) -> Result<ScanPlan, DBError>
    where
        E: Executor + Timer,
    {
        let projected_schema = projected_schema.cloned();
        let residual_predicate = Some(predicate.clone());
        let scan_schema = if let Some(projection) = projected_schema.as_ref() {
            projection_with_predicate(&db.mode.schema, projection, residual_predicate.as_ref())?
        } else {
            Arc::clone(&db.mode.schema)
        };
        let seal = db.seal_state_lock();
        let prune_input: Vec<&Immutable<DynMode>> =
            seal.immutables.iter().map(|arc| arc.as_ref()).collect();
        let immutable_indexes = immutable::prune_segments::<DynMode>(&prune_input);
        let read_ts = self.read_view().read_ts();
        Ok(ScanPlan {
            predicate: predicate.clone(),
            immutable_indexes,
            residual_predicate,
            projected_schema,
            scan_schema,
            limit,
            read_ts,
            _snapshot: self.table_snapshot().clone(),
        })
    }
}

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
    fn begin_transaction(
        &self,
    ) -> impl Future<Output = Result<Transaction<E>, TransactionError>> + Send;
}

impl<E> DynDbHandleExt<E> for DynDbHandle<E>
where
    E: Executor + Timer,
{
    fn clone_handle(&self) -> DynDbHandle<E> {
        Arc::clone(self)
    }

    fn begin_transaction(
        &self,
    ) -> impl Future<Output = Result<Transaction<E>, TransactionError>> + Send {
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

type PendingWalTxns = HashMap<u64, PendingWalTxn>;
type LockMap<K> = Arc<LockableHashMap<K, ()>>;

struct CompactionLoopHandle<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    _driver: Arc<CompactionDriver<M, E>>,
    #[allow(dead_code)]
    handle: tokio::task::JoinHandle<()>,
}

pub(crate) struct CompactionDriver<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    manifest: TonboManifest,
    manifest_table: TableId,
    wal_config: Option<RuntimeWalConfig>,
    wal_handle: Option<WalHandle<E>>,
    _marker: PhantomData<M>,
}

impl<M, E> CompactionDriver<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    fn new(db: &DB<M, E>) -> Self {
        Self {
            manifest: db.manifest.clone(),
            manifest_table: db.manifest_table,
            wal_config: db.wal_config.clone(),
            wal_handle: db.wal_handle().cloned(),
            _marker: PhantomData,
        }
    }

    async fn prune_wal_segments_below_floor(&self) {
        let Some(cfg) = self.wal_config.as_ref() else {
            return;
        };
        let Ok(Some(floor)) = self.manifest.wal_floor(self.manifest_table).await else {
            return;
        };
        let wal_handle = self.wal_handle.clone();
        match manifest_ext::prune_wal_segments(cfg, &floor).await {
            Ok(removed) => {
                if let Some(handle) = wal_handle {
                    let metrics = handle.metrics();
                    let mut guard = metrics.write().await;
                    guard.record_wal_floor_advance();
                    if cfg.prune_dry_run {
                        guard.record_wal_prune_dry_run(removed as u64);
                    } else {
                        guard.record_wal_pruned(removed as u64);
                    }
                }
                if cfg.prune_dry_run {
                    eprintln!(
                        "wal prune dry-run: floor {} would remove {removed} segment(s)",
                        floor.seq()
                    );
                } else if removed > 0 {
                    eprintln!(
                        "wal prune removed {removed} segment(s) below floor {}",
                        floor.seq(),
                    );
                }
            }
            Err(err) => {
                if let Some(handle) = wal_handle {
                    let metrics = handle.metrics();
                    let mut guard = metrics.write().await;
                    guard.record_wal_prune_failure();
                }
                eprintln!(
                    "failed to prune wal segments below manifest floor {}: {}",
                    floor.seq(),
                    err
                );
            }
        }
    }

    async fn manifest_wal_floor(&self) -> Option<WalSegmentRef> {
        self.manifest
            .wal_floor(self.manifest_table)
            .await
            .ok()
            .flatten()
    }

    async fn wal_floor_seq(&self) -> Option<u64> {
        self.manifest_wal_floor().await.map(|ref_| ref_.seq())
    }

    async fn plan_compaction_task<P>(
        &self,
        planner: &P,
    ) -> ManifestResult<Option<crate::compaction::planner::CompactionTask>>
    where
        P: CompactionPlanner,
    {
        let snapshot = self.manifest.snapshot_latest(self.manifest_table).await?;
        let version = match snapshot.latest_version {
            Some(ref state) => state,
            None => return Ok(None),
        };
        DB::<M, E>::plan_compaction_from_version(planner, version)
    }

    async fn plan_compaction_task_with_head<P>(
        &self,
        planner: &P,
    ) -> ManifestResult<
        Option<(
            crate::compaction::planner::CompactionTask,
            Option<Timestamp>,
        )>,
    >
    where
        P: CompactionPlanner,
    {
        let snapshot = self.manifest.snapshot_latest(self.manifest_table).await?;
        let expected_head = snapshot.head.last_manifest_txn;
        let version = match snapshot.latest_version {
            Some(ref state) => state,
            None => return Ok(None),
        };
        Ok(DB::<M, E>::plan_compaction_from_version(planner, version)?
            .map(|task| (task, expected_head)))
    }

    async fn compact_once<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        self.run_compaction_task(planner, executor).await
    }

    async fn run_compaction_task<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        self.run_compaction_task_with_lease(planner, executor, None)
            .await
    }

    async fn run_compaction_task_with_lease<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
        lease: Option<CompactionLease>,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let snapshot = self
                .manifest
                .snapshot_latest(self.manifest_table)
                .await
                .map_err(CompactionError::Manifest)?;
            let version = match snapshot.latest_version {
                Some(ref state) => state,
                None => return Ok(None),
            };
            let expected_head = snapshot.head.last_manifest_txn;
            let existing_wal_segments: Vec<WalSegmentRef> = version.wal_segments().to_vec();

            let Some(task) = DB::<M, E>::plan_compaction_from_version(planner, version)? else {
                return Ok(None);
            };

            let inputs = DB::<M, E>::resolve_compaction_inputs(version, &task)?;
            let obsolete_ids = inputs.iter().map(|d| d.id().clone()).collect();
            let wal_floor = self.manifest_wal_floor().await;
            let job = CompactionJob {
                task,
                inputs,
                lease: lease.clone(),
            };

            let outcome = executor.execute(job).await?;
            let mut outcome = outcome;
            outcome.obsolete_sst_ids = obsolete_ids;
            DB::<M, E>::reconcile_wal_segments(
                version,
                &mut outcome,
                &existing_wal_segments,
                wal_floor,
            );
            let gc_plan = DB::<M, E>::gc_plan_from_outcome(&outcome)?;
            let edits = outcome.to_version_edits();
            if edits.is_empty() {
                return Ok(Some(outcome));
            }
            match self
                .manifest
                .apply_version_edits_cas(self.manifest_table, expected_head, &edits)
                .await
            {
                Ok(_) => {
                    self.prune_wal_segments_below_floor().await;
                    if let Some(plan) = gc_plan {
                        self.manifest
                            .record_gc_plan(self.manifest_table, plan)
                            .await
                            .map_err(CompactionError::Manifest)?;
                    }
                    return Ok(Some(outcome));
                }
                Err(ManifestError::CasConflict(_)) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    if attempts >= DB::<M, E>::MAX_COMPACTION_APPLY_RETRIES {
                        return Err(CompactionError::CasConflict);
                    }
                    continue;
                }
                Err(err) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    return Err(CompactionError::Manifest(err));
                }
            }
        }
    }

    async fn run_scheduled_compaction<CE>(
        &self,
        scheduled: ScheduledCompaction,
        executor: &CE,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
    {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let snapshot = self
                .manifest
                .snapshot_latest(self.manifest_table)
                .await
                .map_err(CompactionError::Manifest)?;
            let version = match snapshot.latest_version {
                Some(ref state) => state,
                None => return Ok(None),
            };

            if let Some(expected) = scheduled.manifest_head
                && snapshot.head.last_manifest_txn != Some(expected)
            {
                return Ok(None);
            }

            let existing_wal_segments: Vec<WalSegmentRef> = version.wal_segments().to_vec();
            let inputs = DB::<M, E>::resolve_compaction_inputs(version, &scheduled.task)?;
            let obsolete_ids = inputs.iter().map(|d| d.id().clone()).collect();
            let wal_floor = self.manifest_wal_floor().await;
            let job = CompactionJob {
                task: scheduled.task.clone(),
                inputs,
                lease: Some(scheduled.lease.clone()),
            };

            let outcome = executor.execute(job).await?;
            let mut outcome = outcome;
            outcome.obsolete_sst_ids = obsolete_ids;
            DB::<M, E>::reconcile_wal_segments(
                version,
                &mut outcome,
                &existing_wal_segments,
                wal_floor,
            );
            let gc_plan = DB::<M, E>::gc_plan_from_outcome(&outcome)?;
            let edits = outcome.to_version_edits();
            if edits.is_empty() {
                return Ok(Some(outcome));
            }
            let expected_head = scheduled.manifest_head.or(snapshot.head.last_manifest_txn);
            match self
                .manifest
                .apply_version_edits_cas(self.manifest_table, expected_head, &edits)
                .await
            {
                Ok(_) => {
                    self.prune_wal_segments_below_floor().await;
                    if let Some(plan) = gc_plan {
                        self.manifest
                            .record_gc_plan(self.manifest_table, plan)
                            .await
                            .map_err(CompactionError::Manifest)?;
                    }
                    return Ok(Some(outcome));
                }
                Err(ManifestError::CasConflict(_)) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    if attempts >= DB::<M, E>::MAX_COMPACTION_APPLY_RETRIES {
                        return Err(CompactionError::CasConflict);
                    }
                    continue;
                }
                Err(err) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    return Err(CompactionError::Manifest(err));
                }
            }
        }
    }

    /// Spawn a current-thread compaction worker that plans tasks, issues leases, and executes
    /// them via the scheduler loop.
    #[allow(dead_code)]
    pub(crate) fn spawn_compaction_worker_local<CE, P>(
        self: &Arc<Self>,
        planner: P,
        executor: CE,
        gc_sst_config: Option<Arc<SsTableConfig>>,
        interval: Duration,
        budget: usize,
    ) -> tokio::task::JoinHandle<()>
    where
        CE: CompactionExecutor + 'static,
        P: CompactionPlanner + 'static,
        M: 'static,
        E: 'static,
    {
        let (scheduler, mut rx) = CompactionScheduler::new(budget.max(1), budget.max(1));
        let driver = Arc::clone(self);
        let executor = Arc::new(executor);
        let _ = gc_sst_config;
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                for _ in 0..budget.max(1) {
                    match driver.plan_compaction_task_with_head(&planner).await {
                        Ok(Some((task, head))) => {
                            if let Err(err) = scheduler
                                .enqueue(
                                    task,
                                    head,
                                    "local-compaction",
                                    interval.as_millis() as u64,
                                )
                                .await
                            {
                                eprintln!("compaction scheduler closed: {err}");
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            eprintln!("compaction planner failed: {err}");
                            break;
                        }
                    }
                }
                // Avoid parking the worker if no jobs were enqueued this tick.
                if rx.is_empty() {
                    continue;
                }

                if let Err(err) = scheduler
                    .drain_with_budget(&mut rx, |job| {
                        let driver = Arc::clone(&driver);
                        let executor = Arc::clone(&executor);
                        async move {
                            if let Err(err) = driver
                                .run_scheduled_compaction(job, executor.as_ref())
                                .await
                            {
                                eprintln!("scheduled compaction failed: {err}");
                            }
                        }
                    })
                    .await
                {
                    eprintln!("compaction scheduler drain stopped: {err}");
                    return;
                }
            }
        })
    }
}

impl<M, E> CompactionHost<M, E> for CompactionDriver<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    fn compact_once<'a, CE, P>(
        &'a self,
        planner: &'a P,
        executor: &'a CE,
    ) -> Pin<Box<dyn Future<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>>
    where
        CE: CompactionExecutor + 'a,
        P: CompactionPlanner + 'a,
    {
        Box::pin(self.compact_once(planner, executor))
    }
}

impl<M, E> CompactionHost<M, E> for DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    fn compact_once<'a, CE, P>(
        &'a self,
        planner: &'a P,
        executor: &'a CE,
    ) -> Pin<Box<dyn Future<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>>
    where
        CE: CompactionExecutor + 'a,
        P: CompactionPlanner + 'a,
    {
        let driver = self.compaction_driver();
        Box::pin(async move { driver.compact_once(planner, executor).await })
    }
}

/// Default package size for a scan
pub const DEFAULT_SCAN_BATCH_ROWS: usize = 1024;

enum PendingWalPayload {
    Upsert {
        batch: RecordBatch,
        commit_ts_column: ArrayRef,
        commit_ts_hint: Option<Timestamp>,
    },
    Delete {
        batch: RecordBatch,
        commit_ts_hint: Option<Timestamp>,
    },
}

#[derive(Default)]
struct PendingWalTxn {
    payloads: Vec<PendingWalPayload>,
}

impl PendingWalTxn {
    fn push(&mut self, payload: PendingWalPayload) {
        self.payloads.push(payload);
    }

    fn into_payloads(self) -> Vec<PendingWalPayload> {
        self.payloads
    }

    fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalFrameRange {
    pub(crate) first: u64,
    pub(crate) last: u64,
}

impl WalFrameRange {
    fn include_seq(&mut self, seq: u64) {
        if seq < self.first {
            self.first = seq;
        }
        if seq > self.last {
            self.last = seq;
        }
    }

    pub(crate) fn extend(&mut self, other: &WalFrameRange) {
        self.include_seq(other.first);
        self.include_seq(other.last);
    }
}

#[derive(Default)]
struct WalRangeAccumulator {
    range: Option<WalFrameRange>,
}

impl WalRangeAccumulator {
    fn observe_range(&mut self, first: u64, last: u64) {
        debug_assert!(first <= last, "wal frame range inverted");
        match self.range.as_mut() {
            Some(range) => {
                range.include_seq(first);
                range.include_seq(last);
            }
            None => self.range = Some(WalFrameRange { first, last }),
        }
    }

    fn into_range(self) -> Option<WalFrameRange> {
        self.range
    }
}

#[derive(Clone)]
pub(crate) struct TxnWalPublishContext {
    pub(crate) manifest: TonboManifest,
    pub(crate) manifest_table: TableId,
    pub(crate) wal_config: Option<RuntimeWalConfig>,
    pub(crate) mutable_wal_range: Arc<Mutex<Option<WalFrameRange>>>,
    pub(crate) prev_live_floor: Option<u64>,
}

struct SealState<M: Mode> {
    immutables: Vec<Arc<Immutable<M>>>,
    immutable_wal_ranges: Vec<Option<WalFrameRange>>,
    last_seal_at: Option<Instant>,
}

impl<M: Mode> Default for SealState<M> {
    fn default() -> Self {
        Self {
            immutables: Vec::new(),
            immutable_wal_ranges: Vec::new(),
            last_seal_at: None,
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
    E: Executor + Timer + Send + Sync,
{
    mode: M,
    mem: Arc<StdRwLock<M::Mutable>>,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest) plus metadata.
    seal_state: Mutex<SealState<M>>,
    // Sealing policy (pure/lock-free) and last seal timestamp (held inside seal_state)
    policy: Arc<dyn SealPolicy + Send + Sync>,
    // Executor powering async subsystems such as the WAL.
    executor: Arc<E>,
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
    /// File identifier allocator scoped to this DB instance.
    #[allow(dead_code)]
    file_ids: FileIdGenerator,
    /// Per-key transactional locks (wired once transactional writes arrive).
    _key_locks: LockMap<M::Key>,
    /// Optional background compaction driver/handle pair for local loops.
    #[allow(dead_code)]
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

impl<E> DB<DynMode, E>
where
    E: Executor + Timer,
{
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.mode.schema
    }

    /// Wrap this database in an executor-provided read-write lock for shared transactional use.
    pub fn into_shared(self) -> DynDbHandle<E> {
        Arc::new(self)
    }

    pub(crate) fn insert_into_mutable(
        &self,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), KeyExtractError> {
        self.insert_with_seal_retry(|mem| {
            mem.insert_batch(self.mode.extractor.as_ref(), batch.clone(), commit_ts)
        })
    }

    pub(crate) fn replay_wal_events(
        &mut self,
        events: Vec<WalEvent>,
    ) -> Result<Option<Timestamp>, KeyExtractError> {
        let mut last_commit_ts: Option<Timestamp> = None;
        let mut pending: PendingWalTxns = HashMap::new();
        for event in events {
            match event {
                WalEvent::TxnBegin { provisional_id } => {
                    pending.entry(provisional_id).or_default();
                }
                WalEvent::DynAppend {
                    provisional_id,
                    payload,
                } => {
                    let DynAppendEvent {
                        batch,
                        commit_ts_hint,
                        commit_ts_column,
                    } = payload;
                    pending
                        .entry(provisional_id)
                        .or_default()
                        .push(PendingWalPayload::Upsert {
                            batch,
                            commit_ts_column,
                            commit_ts_hint,
                        });
                }
                WalEvent::DynDelete {
                    provisional_id,
                    payload,
                } => {
                    pending
                        .entry(provisional_id)
                        .or_default()
                        .push(PendingWalPayload::Delete {
                            batch: payload.batch,
                            commit_ts_hint: payload.commit_ts_hint,
                        });
                }
                WalEvent::TxnCommit {
                    provisional_id,
                    commit_ts,
                } => {
                    if let Some(txn) = pending.remove(&provisional_id) {
                        if txn.is_empty() {
                            continue;
                        }
                        for entry in txn.into_payloads() {
                            match entry {
                                PendingWalPayload::Upsert {
                                    batch,
                                    commit_ts_column,
                                    commit_ts_hint,
                                } => {
                                    if let Some(hint) = commit_ts_hint {
                                        debug_assert_eq!(
                                            hint, commit_ts,
                                            "commit timestamp derived from append payload diverged"
                                        );
                                    }
                                    apply_dyn_wal_batch(self, batch, commit_ts_column, commit_ts)?;
                                    self.maybe_seal_after_insert()?;
                                }
                                PendingWalPayload::Delete {
                                    batch,
                                    commit_ts_hint,
                                } => {
                                    if let Some(hint) = commit_ts_hint {
                                        debug_assert_eq!(
                                            hint, commit_ts,
                                            "commit timestamp derived from delete payload diverged"
                                        );
                                    }
                                    apply_dyn_delete_wal_batch(self, batch, commit_ts)?;
                                    self.maybe_seal_after_insert()?;
                                }
                            }
                        }
                        last_commit_ts = Some(match last_commit_ts {
                            Some(prev) => prev.max(commit_ts),
                            None => commit_ts,
                        });
                    }
                }
                WalEvent::TxnAbort { provisional_id } => {
                    pending.remove(&provisional_id);
                }
                WalEvent::SealMarker => {
                    // TODO: implement once transactional semantics are wired up.
                }
            }
        }

        Ok(last_commit_ts)
    }

    fn insert_with_seal_retry<F>(&self, mut op: F) -> Result<(), KeyExtractError>
    where
        F: FnMut(&mut DynMem) -> Result<(), KeyExtractError>,
    {
        loop {
            let mut mem = self.mem_write();
            match op(&mut mem) {
                Ok(()) => return Ok(()),
                Err(KeyExtractError::MemtableFull { .. }) => {
                    drop(mem);
                    self.seal_mutable_now()?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn seal_mutable_now(&self) -> Result<(), KeyExtractError> {
        let seg_opt = {
            let mut mem = self.mem_write();
            mem.seal_into_immutable(&self.mode.schema, self.mode.extractor.as_ref())?
        };
        if let Some(seg) = seg_opt {
            let wal_range = self.take_mutable_wal_range();
            self.add_immutable(seg, wal_range);
            let mut seal = self.seal_state_lock();
            seal.last_seal_at = Some(Instant::now());
        }
        Ok(())
    }

    /// Execute the scan plan with MVCC visibility
    pub async fn execute_scan<'a>(
        &'a self,
        plan: ScanPlan,
    ) -> Result<impl Stream<Item = Result<RecordBatch, DBError>> + 'a, DBError> {
        let result_projection = plan
            .projected_schema
            .clone()
            .unwrap_or_else(|| Arc::clone(&self.mode.schema));
        let scan_schema = Arc::clone(&plan.scan_schema);
        let streams = self.build_scan_streams(&plan, None)?;

        if streams.is_empty() {
            let stream = stream::empty::<Result<RecordBatch, DBError>>();
            return Ok(
                Box::pin(stream) as Pin<Box<dyn Stream<Item = Result<RecordBatch, DBError>> + 'a>>
            );
        }

        let ScanPlan {
            residual_predicate,
            limit,
            read_ts,
            ..
        } = plan;
        let merge = MergeStream::from_vec(streams, read_ts, limit, Some(Order::Asc))
            .await
            .map_err(DBError::from)?;
        let package = PackageStream::new(
            DEFAULT_SCAN_BATCH_ROWS,
            merge,
            Arc::clone(&scan_schema),
            Arc::clone(&result_projection),
            residual_predicate,
        )
        .map_err(DBError::from)?;

        let mapped = package.map(|result| result.map_err(DBError::from));
        Ok(Box::pin(mapped)
            as Pin<
                Box<dyn Stream<Item = Result<RecordBatch, DBError>> + 'a>,
            >)
    }

    pub(crate) fn build_scan_streams<'a>(
        &'a self,
        plan: &ScanPlan,
        txn_scan: Option<TransactionScan<'a>>,
    ) -> Result<Vec<ScanStream<'a, RecordBatch>>, DBError> {
        let mut streams = Vec::new();
        if let Some(txn_scan) = txn_scan {
            streams.push(ScanStream::from(txn_scan));
        }

        let projection_schema = Arc::clone(&plan.scan_schema);
        let mutable_scan = OwnedMutableScan::from_guard(
            self.mem_read(),
            Some(Arc::clone(&projection_schema)),
            plan.read_ts,
        )?;
        streams.push(ScanStream::from(mutable_scan));

        let seal = self.seal_state_lock();
        let immutables: Vec<Arc<Immutable<DynMode>>> = plan
            .immutable_indexes
            .iter()
            .filter_map(|idx| seal.immutables.get(*idx).cloned())
            .collect();
        drop(seal);
        for segment in immutables {
            let owned = OwnedImmutableScan::from_arc(
                Arc::clone(&segment),
                Some(Arc::clone(&projection_schema)),
                plan.read_ts,
            )?;
            streams.push(ScanStream::from(owned));
        }

        Ok(streams)
    }

    pub(crate) fn scan_immutable_rows_at(
        &self,
        read_ts: Timestamp,
    ) -> Result<Vec<(KeyRow, DynRow)>, KeyExtractError> {
        let mut rows = Vec::new();
        let segments = self.seal_state_lock().immutables.clone();
        for segment in segments {
            let scan = segment.scan_visible(None, read_ts)?;
            for result in scan {
                match result {
                    Ok(ImmutableVisibleEntry::Row(key_view, row_raw)) => {
                        let row = row_raw.into_owned().map_err(|err| {
                            KeyExtractError::Arrow(ArrowError::ComputeError(err.to_string()))
                        })?;
                        let (key_row, _) = key_view.into_parts();
                        rows.push((key_row, row));
                    }
                    Ok(ImmutableVisibleEntry::Tombstone(_)) => {}
                    Err(err) => return Err(KeyExtractError::from(err)),
                }
            }
        }
        Ok(rows)
    }

    pub(crate) fn scan_mutable_rows_at(
        &self,
        read_ts: Timestamp,
    ) -> Result<Vec<(KeyRow, DynRow)>, KeyExtractError> {
        let mut rows = Vec::new();
        let mem = self.mem_read();
        let scan = mem.scan_visible(None, read_ts)?;
        for entry in scan {
            match entry {
                Ok(DynRowScanEntry::Row(key_view, row_raw)) => {
                    let row = row_raw.into_owned().map_err(|err| {
                        KeyExtractError::Arrow(ArrowError::ComputeError(err.to_string()))
                    })?;
                    let (key_row, _) = key_view.into_parts();
                    rows.push((key_row, row));
                }
                Ok(DynRowScanEntry::Tombstone(_)) => {}
                Err(err) => return Err(KeyExtractError::from(err)),
            }
        }
        Ok(rows)
    }

    /// Apply a WAL-durable batch directly into the mutable table.
    pub(crate) fn apply_committed_batch(
        &self,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), KeyExtractError> {
        let commit_array: ArrayRef =
            Arc::new(UInt64Array::from(vec![commit_ts.get(); batch.num_rows()]));
        apply_dyn_wal_batch(self, batch, commit_array, commit_ts)
    }

    /// Apply a WAL-durable key-only delete batch directly into the mutable table.
    pub(crate) fn apply_committed_deletes(
        &self,
        batch: RecordBatch,
    ) -> Result<(), KeyExtractError> {
        let mut mem = self.mem_write();
        mem.insert_delete_batch(self.mode.delete_projection.as_ref(), batch)
    }

    /// Check whether a newer committed version exists for `key` relative to `snapshot_ts`.
    pub(crate) fn mutable_has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        self.mem_read().has_conflict(key, snapshot_ts)
    }

    pub(crate) fn immutable_has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        self.seal_state_lock()
            .immutables
            .iter()
            .any(|segment| segment.has_conflict(key, snapshot_ts))
    }

    pub(crate) fn maybe_seal_after_insert(&self) -> Result<(), KeyExtractError> {
        let last_seal = { self.seal_state_lock().last_seal_at };
        let since = last_seal.map(|t| t.elapsed());
        let stats = { self.mem_read().build_stats(since) };

        if let SealDecision::Seal(_reason) = self.policy.evaluate(&stats) {
            self.seal_mutable_now()?;
        }
        Ok(())
    }

    /// Ingest a batch along with its tombstone bitmap, routing through the WAL when enabled.
    pub async fn ingest_with_tombstones(
        &self,
        batch: RecordBatch,
        tombstones: Vec<bool>,
    ) -> Result<(), KeyExtractError> {
        insert_dyn_wal_batch(self, batch, tombstones).await
    }

    /// Ingest multiple batches, each paired with a tombstone bitmap.
    pub async fn ingest_many_with_tombstones(
        &self,
        batches: Vec<(RecordBatch, Vec<bool>)>,
    ) -> Result<(), KeyExtractError> {
        insert_dyn_wal_batches(self, batches).await
    }
}

// Methods common to all modes
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

    fn compaction_driver(&self) -> CompactionDriver<M, E> {
        CompactionDriver::new(self)
    }

    /// Remove WAL segments whose sequence is older than the manifest floor.
    pub async fn prune_wal_segments_below_floor(&self) {
        self.compaction_driver()
            .prune_wal_segments_below_floor()
            .await
    }

    /// Build a compaction plan based on the latest manifest snapshot.
    pub async fn plan_compaction_task<P>(
        &self,
        planner: &P,
    ) -> ManifestResult<Option<crate::compaction::planner::CompactionTask>>
    where
        P: CompactionPlanner,
    {
        self.compaction_driver().plan_compaction_task(planner).await
    }

    fn plan_compaction_from_version<P>(
        planner: &P,
        version: &VersionState,
    ) -> ManifestResult<Option<crate::compaction::planner::CompactionTask>>
    where
        P: CompactionPlanner,
    {
        let layout: CompactionSnapshot = version.into();
        if layout.is_empty() {
            return Ok(None);
        }
        Ok(planner.plan(&layout))
    }

    fn wal_ids_for_remaining_ssts(
        version: &VersionState,
        removed: &HashSet<SsTableId>,
        added: &[SsTableDescriptor],
    ) -> Option<HashSet<FileId>> {
        let mut wal_ids = HashSet::new();
        for bucket in version.ssts() {
            for entry in bucket {
                if removed.contains(entry.sst_id()) {
                    continue;
                }
                let Some(ids) = entry.wal_segments() else {
                    // If any remaining SST lacks WAL metadata, keep the existing manifest set.
                    return None;
                };
                wal_ids.extend(ids.iter().cloned());
            }
        }
        for desc in added {
            let Some(ids) = desc.wal_ids() else {
                // Missing WAL metadata on new outputs: preserve manifest set.
                return None;
            };
            wal_ids.extend(ids.iter().cloned());
        }
        Some(wal_ids)
    }

    fn reconcile_wal_segments(
        version: &VersionState,
        outcome: &mut CompactionOutcome,
        existing_wal_segments: &[WalSegmentRef],
        wal_floor: Option<WalSegmentRef>,
    ) {
        let wal_from_helper =
            Self::wal_segments_after_compaction(version, &outcome.remove_ssts, &outcome.outputs);
        let (final_wal_segments, obsolete_wal_segments) = match wal_from_helper {
            Some(filtered) => {
                outcome.wal_segments = Some(filtered.clone());
                let obsolete = existing_wal_segments
                    .iter()
                    .filter(|seg| !filtered.iter().any(|s| s == *seg))
                    .cloned()
                    .collect();
                (filtered, obsolete)
            }
            None => {
                outcome.wal_segments = Some(existing_wal_segments.to_vec());
                (existing_wal_segments.to_vec(), Vec::new())
            }
        };
        outcome.wal_floor = if final_wal_segments.is_empty() {
            wal_floor
        } else {
            wal_floor.or_else(|| final_wal_segments.first().cloned())
        };
        if outcome.wal_segments.is_none() && !final_wal_segments.is_empty() {
            outcome.wal_segments = Some(final_wal_segments.clone());
        }
        outcome.obsolete_wal_segments = obsolete_wal_segments;
    }

    fn wal_segments_after_compaction(
        version: &VersionState,
        removed_ssts: &[SsTableDescriptor],
        added_ssts: &[SsTableDescriptor],
    ) -> Option<Vec<WalSegmentRef>> {
        let removed_ids: HashSet<SsTableId> = removed_ssts.iter().map(|d| d.id().clone()).collect();
        let wal_ids = Self::wal_ids_for_remaining_ssts(version, &removed_ids, added_ssts)?;
        let existing = version.wal_segments();
        if existing.is_empty() {
            return None;
        }
        let mut filtered: Vec<WalSegmentRef> = existing
            .iter()
            .filter(|seg| wal_ids.contains(seg.file_id()))
            .cloned()
            .collect();
        if filtered == existing {
            return None;
        }
        if filtered.is_empty() {
            return Some(Vec::new());
        }
        let has_seq_gaps = existing
            .windows(2)
            .any(|pair| pair[1].seq() > pair[0].seq().saturating_add(1));
        if has_seq_gaps {
            if let Some(first_retained) = filtered.first()
                && let Some(first_idx) = existing
                    .iter()
                    .position(|seg| seg.file_id() == first_retained.file_id())
                && first_idx > 0
            {
                let mut with_gap = existing[..first_idx].to_vec();
                with_gap.append(&mut filtered);
                Some(with_gap)
            } else {
                Some(existing.to_vec())
            }
        } else {
            Some(filtered)
        }
    }

    /// Trigger a single compaction attempt using the provided planner and executor.
    /// Returns `Ok(None)` when no task is scheduled by the planner.
    #[allow(dead_code)]
    pub(crate) async fn compact_once<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        self.compaction_driver()
            .compact_once(planner, executor)
            .await
    }

    const MAX_COMPACTION_APPLY_RETRIES: usize = 2;

    /// End-to-end compaction orchestrator (plan -> resolve -> execute -> apply manifest).
    #[allow(dead_code)]
    pub(crate) async fn run_compaction_task<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        self.compaction_driver()
            .run_compaction_task(planner, executor)
            .await
    }
    /// End-to-end compaction orchestrator with an optional lease token.
    #[allow(dead_code)]
    pub(crate) async fn run_compaction_task_with_lease<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
        lease: Option<CompactionLease>,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        self.compaction_driver()
            .run_compaction_task_with_lease(planner, executor, lease)
            .await
    }

    /// Spawn a current-thread compaction worker that calls `compact_once` every `interval`.
    /// Caller owns cancellation of the returned handle. This is a stopgap until a Send-friendly
    /// scheduler/lease lands.
    fn gc_plan_from_outcome(
        outcome: &CompactionOutcome,
    ) -> Result<Option<GcPlanState>, CompactionError> {
        if outcome.remove_ssts.is_empty() && outcome.obsolete_wal_segments.is_empty() {
            return Ok(None);
        }
        let mut obsolete_ssts = Vec::new();
        for desc in &outcome.remove_ssts {
            let data_path = desc
                .data_path()
                .cloned()
                .ok_or(CompactionError::MissingPath("data"))?;
            obsolete_ssts.push(GcSstRef {
                id: desc.id().clone(),
                level: desc.level() as u32,
                data_path,
                delete_path: desc.delete_path().cloned(),
            });
        }
        Ok(Some(GcPlanState {
            obsolete_ssts,
            obsolete_wal_segments: outcome.obsolete_wal_segments.clone(),
        }))
    }

    pub(crate) fn resolve_compaction_inputs(
        version: &VersionState,
        task: &crate::compaction::planner::CompactionTask,
    ) -> Result<Vec<SsTableDescriptor>, CompactionError> {
        let mut descriptors = Vec::with_capacity(task.input.len());
        for CompactionInput { level, sst_id } in &task.input {
            let Some(bucket) = version.ssts().get(*level) else {
                return Err(CompactionError::Manifest(ManifestError::Invariant(
                    "planner selected level missing in manifest",
                )));
            };

            let Some(entry) = bucket.iter().find(|entry| entry.sst_id() == sst_id) else {
                return Err(CompactionError::Manifest(ManifestError::Invariant(
                    "planner selected SST missing in manifest",
                )));
            };
            let mut descriptor = SsTableDescriptor::new(entry.sst_id().clone(), *level);
            if let Some(stats) = entry.stats().cloned() {
                descriptor = descriptor.with_stats(stats);
            }
            descriptor = descriptor.with_wal_ids(entry.wal_segments().map(|ids| ids.to_vec()));
            descriptor = descriptor
                .with_storage_paths(entry.data_path().clone(), entry.delete_path().cloned());
            descriptors.push(descriptor);
        }
        Ok(descriptors)
    }

    /// Construct a database from pre-initialised components.
    fn from_components(
        mode: M,
        mem: M::Mutable,
        manifest: TonboManifest,
        manifest_table: TableId,
        wal_config: Option<RuntimeWalConfig>,
        file_ids: FileIdGenerator,
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
            wal: None,
            wal_config,
            commit_clock: CommitClock::default(),
            manifest,
            manifest_table,
            mutable_wal_range: Arc::new(Mutex::new(None)),
            file_ids,
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
        Ok(Self::from_components(
            mode,
            mem,
            manifest,
            manifest_table,
            None,
            file_ids,
            executor,
        ))
    }

    pub(crate) async fn recover_with_wal_with_manifest(
        config: M::Config,
        executor: Arc<E>,
        wal_cfg: RuntimeWalConfig,
        manifest: TonboManifest,
        manifest_table: TableId,
        file_ids: FileIdGenerator,
    ) -> Result<Self, KeyExtractError>
    where
        M: Sized + CatalogDescribe,
    {
        Self::recover_with_wal_inner(
            config,
            executor,
            wal_cfg,
            manifest,
            manifest_table,
            file_ids,
        )
        .await
    }

    async fn recover_with_wal_inner(
        config: M::Config,
        executor: Arc<E>,
        wal_cfg: RuntimeWalConfig,
        manifest: TonboManifest,
        manifest_table: TableId,
        file_ids: FileIdGenerator,
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
            manifest,
            manifest_table,
            Some(wal_cfg.clone()),
            file_ids,
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

    /// Attach WAL configuration prior to enabling durability.
    pub fn with_wal_config(mut self, cfg: RuntimeWalConfig) -> Self {
        self.wal_config = Some(cfg);
        self
    }

    /// Access the configured WAL settings, if any.
    pub fn wal_config(&self) -> Option<&RuntimeWalConfig> {
        self.wal_config.as_ref()
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
    #[allow(dead_code)]
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

    /// Open a read-only snapshot pinned to the current manifest head at an explicit read timestamp.
    #[allow(dead_code)]
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
    #[cfg(any(test, feature = "test-helpers"))]
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

    /// Allocate a new process-unique file identifier.
    #[allow(dead_code)]
    pub(crate) fn next_file_id(&self) -> FileId {
        self.file_ids.generate()
    }

    /// Access the active WAL handle, if any.
    pub(crate) fn wal_handle(&self) -> Option<&WalHandle<E>> {
        self.wal.as_ref()
    }

    /// Replace the WAL handle (used by WAL extension methods).
    pub(crate) fn set_wal_handle(&mut self, handle: Option<WalHandle<E>>) {
        self.wal = handle;
    }

    /// Replace the stored WAL configuration.
    pub(crate) fn set_wal_config(&mut self, cfg: Option<RuntimeWalConfig>) {
        self.wal_config = cfg;
    }

    pub(crate) async fn disable_wal_async(&mut self) -> WalResult<()> {
        if let Some(handle) = self.wal_handle().cloned() {
            self.set_wal_handle(None);
            handle.shutdown().await?;
        } else {
            self.set_wal_handle(None);
        }
        Ok(())
    }

    async fn manifest_wal_floor(&self) -> Option<WalSegmentRef> {
        self.manifest
            .wal_floor(self.manifest_table)
            .await
            .ok()
            .flatten()
    }

    /// Sequence number of the WAL floor currently recorded in the manifest.
    pub async fn wal_floor_seq(&self) -> Option<u64> {
        self.compaction_driver().wal_floor_seq().await
    }

    pub(crate) fn wal_live_frame_floor(&self) -> Option<u64> {
        let mutable_floor = self.mutable_wal_range_snapshot().map(|range| range.first);
        let seal = self.seal_state_lock();
        let immutable_floor = seal
            .immutable_wal_ranges
            .iter()
            .filter_map(|range| range.as_ref().map(|r| r.first))
            .min();

        match (mutable_floor, immutable_floor) {
            (Some(lhs), Some(rhs)) => Some(lhs.min(rhs)),
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        }
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
        let live_floor = self.wal_live_frame_floor();
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
                // Persist Sst change into manifest
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

                // Cleanup immutable memtable
                let mut seal = self.seal_state_lock();
                seal.immutables = Vec::new();
                seal.immutable_wal_ranges.clear();
                seal.last_seal_at = Some(Instant::now());
                Ok(table)
            }
            Err(err) => Err(err),
        }
    }

    // Key-only merged scans have been removed.
}

// Segment management (generic, zero-cost)
impl<M, E> DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    #[allow(dead_code)]
    fn add_immutable(&self, seg: Immutable<M>, wal_range: Option<WalFrameRange>) {
        let mut seal = self.seal_state_lock();
        seal.immutables.push(Arc::new(seg));
        seal.immutable_wal_ranges.push(wal_range);
    }

    fn record_mutable_wal_range(&self, range: WalFrameRange) {
        let mut guard = self
            .mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned");
        match &mut *guard {
            Some(existing) => existing.extend(&range),
            None => *guard = Some(range),
        }
    }

    fn take_mutable_wal_range(&self) -> Option<WalFrameRange> {
        self.mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned")
            .take()
    }

    fn mutable_wal_range_snapshot(&self) -> Option<WalFrameRange> {
        *self
            .mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned")
    }

    fn set_mutable_wal_range(&self, value: Option<WalFrameRange>) {
        *self
            .mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned") = value;
    }

    pub(crate) fn txn_publish_context(&self, prev_live_floor: Option<u64>) -> TxnWalPublishContext {
        TxnWalPublishContext {
            manifest: self.manifest.clone(),
            manifest_table: self.manifest_table,
            wal_config: self.wal_config.clone(),
            mutable_wal_range: Arc::clone(&self.mutable_wal_range),
            prev_live_floor,
        }
    }

    pub(crate) fn observe_mutable_wal_span(&self, first_seq: u64, last_seq: u64) {
        let (first, last) = if first_seq <= last_seq {
            (first_seq, last_seq)
        } else {
            (last_seq, first_seq)
        };
        self.record_mutable_wal_range(WalFrameRange { first, last });
    }

    /// Set or replace the sealing policy used by this DB.
    pub fn set_seal_policy(&mut self, policy: Arc<dyn SealPolicy + Send + Sync>) {
        self.policy = policy;
    }

    /// Access the per-key transactional lock map.
    #[allow(dead_code)]
    pub(crate) fn key_locks(&self) -> &LockMap<M::Key> {
        &self._key_locks
    }
}

async fn insert_dyn_wal_batch<E>(
    db: &DB<DynMode, E>,
    batch: RecordBatch,
    tombstones: Vec<bool>,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    validate_record_batch_schema(db, &batch)?;
    validate_vec_tombstone_bitmap(&batch, &tombstones)?;

    if batch.num_rows() == 0 {
        return Ok(());
    }

    let commit_ts = db.next_commit_ts();

    let (upsert_batch, delete_batch) =
        partition_batch_for_mutations(&db.mode, batch, &tombstones, commit_ts)?;

    if upsert_batch.is_none() && delete_batch.is_none() {
        return Ok(());
    }

    let mut wal_range: Option<WalFrameRange> = None;
    if let Some(handle) = db.wal_handle().cloned() {
        let provisional_id = handle.next_provisional_id();
        let mut append_tickets = Vec::new();
        if let Some(ref batch) = upsert_batch {
            let ticket = handle
                .txn_append(provisional_id, batch, commit_ts)
                .await
                .map_err(KeyExtractError::from)?;
            append_tickets.push(ticket);
        }
        if let Some(ref batch) = delete_batch {
            let ticket = handle
                .txn_append_delete(provisional_id, batch.clone())
                .await
                .map_err(KeyExtractError::from)?;
            append_tickets.push(ticket);
        }
        let mut tracker = WalRangeAccumulator::default();
        for ticket in append_tickets {
            let ack = ticket.durable().await.map_err(KeyExtractError::from)?;
            tracker.observe_range(ack.first_seq, ack.last_seq);
        }
        let commit_ticket = handle
            .txn_commit(provisional_id, commit_ts)
            .await
            .map_err(KeyExtractError::from)?;
        let commit_ack = commit_ticket
            .durable()
            .await
            .map_err(KeyExtractError::from)?;
        tracker.observe_range(commit_ack.first_seq, commit_ack.last_seq);
        wal_range = tracker.into_range();
    }

    let mutated = upsert_batch.is_some() || delete_batch.is_some();
    if mutated {
        db.insert_with_seal_retry(|mem| {
            if let Some(ref batch) = upsert_batch {
                mem.insert_batch(db.mode.extractor.as_ref(), batch.clone(), commit_ts)?;
            }
            if let Some(ref batch) = delete_batch {
                mem.insert_delete_batch(db.mode.delete_projection.as_ref(), batch.clone())?;
            }
            Ok(())
        })?;
        if let Some(range) = wal_range {
            db.record_mutable_wal_range(range);
        }
        db.maybe_seal_after_insert()?;
    }
    Ok(())
}

async fn insert_dyn_wal_batches<E>(
    db: &DB<DynMode, E>,
    batches: Vec<(RecordBatch, Vec<bool>)>,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    for (batch, tombstones) in batches {
        insert_dyn_wal_batch(db, batch, tombstones).await?;
    }
    Ok(())
}

fn apply_dyn_wal_batch<E>(
    db: &DB<DynMode, E>,
    batch: RecordBatch,
    commit_ts_column: ArrayRef,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    let schema_contains_tombstone = db
        .mode
        .schema
        .fields()
        .iter()
        .any(|field| field.name() == crate::inmem::immutable::memtable::MVCC_TOMBSTONE_COL);
    let batch_contains_tombstone = batch
        .schema()
        .fields()
        .iter()
        .any(|field| field.name() == crate::inmem::immutable::memtable::MVCC_TOMBSTONE_COL);
    if batch_contains_tombstone && !schema_contains_tombstone {
        return Err(KeyExtractError::from(WalError::Codec(
            "wal batch contained unexpected _tombstone column".into(),
        )));
    }
    validate_record_batch_schema(db, &batch)?;
    let commit_array = commit_ts_column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| KeyExtractError::from(WalError::Codec("_commit_ts column not u64".into())))?
        .clone();
    if commit_array.null_count() > 0 {
        return Err(KeyExtractError::from(WalError::Codec(
            "commit_ts column contained null".into(),
        )));
    }
    if commit_array
        .iter()
        .any(|value| value.map(|v| v != commit_ts.get()).unwrap_or(true))
    {
        return Err(KeyExtractError::from(WalError::Codec(
            "commit_ts column mismatch commit timestamp".into(),
        )));
    }

    db.insert_with_seal_retry(|mem| {
        mem.insert_batch_with_mvcc(
            db.mode.extractor.as_ref(),
            batch.clone(),
            commit_array.clone(),
        )
    })?;
    Ok(())
}

fn apply_dyn_delete_wal_batch<E>(
    db: &DB<DynMode, E>,
    batch: RecordBatch,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    validate_delete_batch_schema(db, &batch)?;
    validate_delete_commit_ts(&batch, commit_ts)?;
    {
        let mut mem = db.mem_write();
        mem.insert_delete_batch(db.mode.delete_projection.as_ref(), batch)?;
    }
    Ok(())
}

fn validate_record_batch_schema<E>(
    db: &DB<DynMode, E>,
    batch: &RecordBatch,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    if db.mode.schema.as_ref() != batch.schema().as_ref() {
        return Err(KeyExtractError::SchemaMismatch {
            expected: db.mode.schema.clone(),
            actual: batch.schema(),
        });
    }
    Ok(())
}

fn validate_vec_tombstone_bitmap(
    batch: &RecordBatch,
    tombstones: &[bool],
) -> Result<(), KeyExtractError> {
    if batch.num_rows() != tombstones.len() {
        return Err(KeyExtractError::TombstoneLengthMismatch {
            expected: batch.num_rows(),
            actual: tombstones.len(),
        });
    }
    Ok(())
}

fn validate_delete_batch_schema<E>(
    db: &DB<DynMode, E>,
    batch: &RecordBatch,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    if db.mode.delete_schema.as_ref() != batch.schema().as_ref() {
        return Err(KeyExtractError::SchemaMismatch {
            expected: db.mode.delete_schema.clone(),
            actual: batch.schema(),
        });
    }
    Ok(())
}

fn validate_delete_commit_ts(
    batch: &RecordBatch,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError> {
    let schema = batch.schema();
    let commit_idx = schema
        .fields()
        .iter()
        .position(|field| field.name() == MVCC_COMMIT_COL)
        .ok_or_else(|| {
            KeyExtractError::from(WalError::Codec("_commit_ts column missing".into()))
        })?;
    let commit_array = batch
        .column(commit_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            KeyExtractError::from(WalError::Codec("_commit_ts column not u64".into()))
        })?;
    if commit_array.null_count() > 0 {
        return Err(KeyExtractError::from(WalError::Codec(
            "delete payload commit_ts column contained null".into(),
        )));
    }
    if commit_array
        .iter()
        .any(|value| value.map(|v| v != commit_ts.get()).unwrap_or(true))
    {
        return Err(KeyExtractError::from(WalError::Codec(
            "delete payload commit_ts column mismatch commit timestamp".into(),
        )));
    }
    Ok(())
}

fn partition_batch_for_mutations(
    mode: &DynMode,
    batch: RecordBatch,
    tombstones: &[bool],
    commit_ts: Timestamp,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>), KeyExtractError> {
    if batch.num_rows() == 0 {
        return Ok((None, None));
    }
    let mut upsert_indices = Vec::new();
    let mut delete_indices = Vec::new();
    for (idx, tombstone) in tombstones.iter().enumerate() {
        if *tombstone {
            delete_indices.push(idx as u32);
        } else {
            upsert_indices.push(idx as u32);
        }
    }
    let delete_batch = if delete_indices.is_empty() {
        None
    } else {
        Some(build_delete_batch(
            mode,
            &batch,
            &delete_indices,
            commit_ts,
            None,
        )?)
    };
    let upsert_batch = if delete_indices.is_empty() {
        Some(batch)
    } else if upsert_indices.is_empty() {
        None
    } else {
        Some(take_full_batch(&batch, &upsert_indices).map_err(KeyExtractError::Arrow)?)
    };
    Ok((upsert_batch, delete_batch))
}

fn build_delete_batch(
    mode: &DynMode,
    batch: &RecordBatch,
    delete_indices: &[u32],
    commit_ts: Timestamp,
    commit_ts_column: Option<&UInt64Array>,
) -> Result<RecordBatch, KeyExtractError> {
    let idx_array = UInt32Array::from(delete_indices.to_vec());
    let mut columns = Vec::with_capacity(mode.extractor.key_indices().len() + 1);
    for &col_idx in mode.extractor.key_indices() {
        let column = batch.column(col_idx);
        let taken = take(column.as_ref(), &idx_array, None).map_err(KeyExtractError::Arrow)?;
        columns.push(taken);
    }
    let commit_values: ArrayRef = if let Some(column) = commit_ts_column {
        take(column, &idx_array, None).map_err(KeyExtractError::Arrow)?
    } else {
        Arc::new(UInt64Array::from(vec![
            commit_ts.get();
            delete_indices.len()
        ])) as ArrayRef
    };
    columns.push(commit_values);
    RecordBatch::try_new(mode.delete_schema.clone(), columns).map_err(KeyExtractError::Arrow)
}

fn take_full_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ArrowError> {
    let idx_array = UInt32Array::from(indices.to_vec());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), &idx_array, None)?);
    }
    RecordBatch::try_new(batch.schema(), columns)
}
#[cfg(test)]
mod tests {
    use std::{
        fs,
        future::Future,
        path::PathBuf,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{
        ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{
        disk::LocalFs,
        dynamic::DynFs,
        executor::{BlockingExecutor, tokio::TokioExecutor},
        fs::FsCas,
        mem::fs::InMemoryFs,
        path::{Path, PathPart},
    };
    use futures::{
        StreamExt, TryStreamExt,
        channel::{mpsc, oneshot as futures_oneshot},
        executor::block_on,
    };
    use predicate::{ColumnRef, Predicate, ScalarValue};
    use tokio::sync::{Mutex, oneshot};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        compaction::{
            executor::LocalCompactionExecutor,
            planner::{
                CompactionInput, CompactionTask, LeveledCompactionPlanner, LeveledPlannerConfig,
            },
        },
        extractor::{KeyProjection, projection_for_columns},
        inmem::{
            immutable::memtable::{MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL},
            mutable::memtable::DynMem,
            policy::{BatchesThreshold, NeverSeal},
        },
        manifest::{SstEntry, TableId, TonboManifest, VersionEdit, init_fs_manifest},
        mvcc::Timestamp,
        ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
        test_util::build_batch,
        wal::{
            DynBatchPayload, WalCommand, WalExt, WalResult, WalSyncPolicy,
            frame::{INITIAL_FRAME_SEQ, encode_command},
            metrics::WalMetrics,
            state::FsWalStateStore,
        },
    };

    fn build_dyn_components(config: DynModeConfig) -> Result<(DynMode, DynMem), KeyExtractError> {
        let DynModeConfig {
            schema,
            extractor,
            commit_ack_mode,
        } = config;
        extractor.validate_schema(&schema)?;

        let key_schema = extractor.key_schema();
        let mut delete_fields = key_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        delete_fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false));
        let delete_schema = Arc::new(Schema::new(delete_fields));

        let key_columns = key_schema.fields().len();
        let delete_projection =
            projection_for_columns(delete_schema.clone(), (0..key_columns).collect())?;
        let delete_projection: Arc<dyn KeyProjection> = delete_projection.into();

        let mutable = DynMem::new(schema.clone());
        let mode = DynMode {
            schema,
            delete_schema,
            extractor,
            delete_projection,
            commit_ack_mode,
        };
        Ok((mode, mutable))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingest_tombstone_length_mismatch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
        let executor = Arc::new(BlockingExecutor);
        let db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");

        let err = db
            .ingest_with_tombstones(batch, vec![])
            .await
            .expect_err("length mismatch");
        assert!(matches!(
            err,
            KeyExtractError::TombstoneLengthMismatch {
                expected: 1,
                actual: 0
            }
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingest_batch_with_tombstones_marks_versions_and_visibility() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let executor = Arc::new(BlockingExecutor);
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("k1".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("k2".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let result = db.ingest_with_tombstones(batch, vec![false, true]).await;
        result.expect("ingest");

        let chain_k1 = db
            .mem_read()
            .inspect_versions(&KeyOwned::from("k1"))
            .expect("chain k1");
        assert_eq!(chain_k1.len(), 1);
        assert!(!chain_k1[0].1);

        let chain_k2 = db
            .mem_read()
            .inspect_versions(&KeyOwned::from("k2"))
            .expect("chain k2");
        assert_eq!(chain_k2.len(), 1);
        assert!(chain_k2[0].1);

        let pred = Predicate::is_not_null(ColumnRef::new("id", None));
        let snapshot = block_on(db.begin_snapshot()).expect("snapshot");
        let plan = block_on(snapshot.plan_scan(&db, &pred, None, None)).expect("plan");
        let stream = block_on(db.execute_scan(plan)).expect("exec");
        let visible: Vec<String> = block_on(stream.try_collect::<Vec<_>>())
            .expect("collect")
            .into_iter()
            .flat_map(|batch| {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 col");
                col.iter()
                    .flatten()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(visible, vec!["k1".to_string()]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_dyn_wal_batch_inserts_live_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
        let executor = Arc::new(BlockingExecutor);
        let db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1", "k2"])) as _,
                Arc::new(Int32Array::from(vec![1, 2])) as _,
            ],
        )
        .expect("batch");
        let commit_ts = Timestamp::new(5);
        let commit_array: ArrayRef = Arc::new(UInt64Array::from(vec![commit_ts.get(); 2]));

        apply_dyn_wal_batch(&db, batch, commit_array, commit_ts).expect("apply");

        let live = db
            .mem_read()
            .inspect_versions(&KeyOwned::from("k1"))
            .expect("live key");
        assert_eq!(live, vec![(commit_ts, false)]);

        let deleted = db
            .mem_read()
            .inspect_versions(&KeyOwned::from("k2"))
            .expect("second key");
        assert_eq!(deleted, vec![(commit_ts, false)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_dyn_wal_batch_rejects_tombstone_payloads() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, true),
        ]));
        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
        let executor = Arc::new(BlockingExecutor);
        let db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");

        let wal_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, true),
            Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false),
        ]));
        let wal_batch = RecordBatch::try_new(
            wal_schema,
            vec![
                Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef,
            ],
        )
        .expect("wal batch");
        let commit_ts = Timestamp::new(22);
        let commit_array: ArrayRef = Arc::new(UInt64Array::from(vec![commit_ts.get()]));

        let err = apply_dyn_wal_batch(&db, wal_batch, commit_array, commit_ts)
            .expect_err("apply should fail");
        match err {
            KeyExtractError::SchemaMismatch { .. } | KeyExtractError::Wal(_) => {}
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_dyn_wal_batch_allows_user_tombstone_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("_tombstone", DataType::Utf8, false),
            Field::new("v", DataType::Int32, true),
        ]));
        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
        let executor = Arc::new(BlockingExecutor);
        let db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("flag")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef,
            ],
        )
        .expect("batch");
        let commit_ts = Timestamp::new(30);
        let commit_array: ArrayRef = Arc::new(UInt64Array::from(vec![commit_ts.get()]));

        apply_dyn_wal_batch(&db, batch, commit_array, commit_ts).expect("apply");

        let versions = db
            .mem_read()
            .inspect_versions(&KeyOwned::from("k"))
            .expect("versions");
        assert_eq!(versions, vec![(commit_ts, false)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn begin_snapshot_tracks_commit_clock() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let executor = Arc::new(BlockingExecutor);
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");

        let snapshot = db.begin_snapshot().await.expect("snapshot");
        assert_eq!(snapshot.read_view().read_ts(), Timestamp::MIN);
        assert!(snapshot.head().last_manifest_txn.is_none());
        assert!(snapshot.latest_version().is_none());

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k1".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        db.ingest_with_tombstones(batch, vec![false])
            .await
            .expect("ingest");

        let snapshot_after = db.begin_snapshot().await.expect("snapshot after ingest");
        assert_eq!(snapshot_after.read_view().read_ts(), Timestamp::new(0));
        assert!(snapshot_after.head().last_manifest_txn.is_none());
        assert!(snapshot_after.latest_version().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dynamic_seal_on_batches_threshold() {
        // Build a simple schema: id: Utf8 (key), v: Int32
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        // Build one batch with two rows
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");

        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name");
        let mut db: DB<DynMode, BlockingExecutor> = DB::new(config, Arc::new(BlockingExecutor))
            .await
            .expect("schema ok");
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
        assert_eq!(db.num_immutable_segments(), 0);
        db.ingest(batch).await.expect("insert batch");
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auto_seals_when_memtable_hits_capacity() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name");
        let mut db: DB<DynMode, BlockingExecutor> = DB::new(config, Arc::new(BlockingExecutor))
            .await
            .expect("schema ok");

        // Force minimal capacity and disable policy-based sealing to exercise MemtableFull
        // recovery.
        {
            let mut mem = db.mem_write();
            *mem = DynMem::with_capacity(db.mode.schema.clone(), 1);
        }
        db.set_seal_policy(Arc::new(NeverSeal));

        let make_batch = |val: i32| {
            build_batch(
                schema.clone(),
                vec![DynRow(vec![
                    Some(DynCell::Str("k".into())),
                    Some(DynCell::I32(val)),
                ])],
            )
            .expect("batch")
        };

        db.ingest(make_batch(1)).await.expect("ingest 1");
        db.ingest(make_batch(2)).await.expect("ingest 2");
        db.ingest(make_batch(3)).await.expect("ingest 3");

        // Each time capacity is hit we should seal and continue inserting.
        assert_eq!(db.num_immutable_segments(), 2);
        assert_eq!(db.mem_read().batch_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recover_with_manifest_preserves_table_id() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let build_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
        let executor = Arc::new(TokioExecutor::default());

        let fs = InMemoryFs::new();
        let manifest_root = Path::parse("durable-test").expect("path");
        let wal_dir = manifest_root.child(PathPart::parse("wal").expect("wal dir component"));

        let mut wal_cfg = RuntimeWalConfig::default();
        wal_cfg.dir = wal_dir;
        let dyn_fs: Arc<dyn DynFs> = Arc::new(fs.clone());
        wal_cfg.segment_backend = dyn_fs;
        let cas_backend: Arc<dyn FsCas> = Arc::new(fs.clone());
        wal_cfg.state_store = Some(Arc::new(FsWalStateStore::new(cas_backend)));

        let manifest = init_fs_manifest(fs.clone(), &manifest_root)
            .await
            .expect("init manifest");
        let file_ids = FileIdGenerator::default();
        let table_definition =
            DynMode::table_definition(&build_config, builder::DEFAULT_TABLE_NAME);
        let manifest_table = manifest
            .register_table(&file_ids, &table_definition)
            .await
            .expect("register table")
            .table_id;

        let (mode, mem) = build_dyn_components(build_config)?;
        let mut db = DB::from_components(
            mode,
            mem,
            manifest,
            manifest_table,
            Some(wal_cfg.clone()),
            file_ids,
            Arc::clone(&executor),
        );
        db.enable_wal(wal_cfg.clone()).await.expect("enable wal");

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["user-1"])) as _,
                Arc::new(Int32Array::from(vec![7])) as _,
            ],
        )?;
        db.ingest_with_tombstones(batch, vec![false])
            .await
            .expect("ingest");
        drop(db);

        let manifest = init_fs_manifest(fs.clone(), &manifest_root)
            .await
            .expect("reopen manifest");
        let recover_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
        let mut recovered = DB::recover_with_wal_with_manifest(
            recover_config,
            Arc::clone(&executor),
            wal_cfg.clone(),
            manifest,
            manifest_table,
            FileIdGenerator::default(),
        )
        .await
        .expect("recover");
        recovered.enable_wal(wal_cfg).await.expect("re-enable wal");

        assert_eq!(recovered.manifest_table_id(), manifest_table);

        let pred = Predicate::is_not_null(ColumnRef::new("id", None));
        let snapshot = block_on(recovered.begin_snapshot()).expect("snapshot");
        let plan = block_on(snapshot.plan_scan(&recovered, &pred, None, None)).expect("plan");
        let stream = block_on(recovered.execute_scan(plan)).expect("execute");
        let rows: Vec<(String, i32)> = block_on(stream.try_collect::<Vec<_>>())
            .expect("collect")
            .into_iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id col");
                let vals = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("v col");
                ids.iter()
                    .zip(vals.iter())
                    .filter_map(|(id, v)| Some((id?.to_string(), v?)))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows, vec![("user-1".into(), 7)]);

        Ok(())
    }

    #[test]
    fn plan_scan_filters_immutable_segments() {
        let db = db_with_immutable_keys(&["k1", "z1"]);
        let predicate = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k1"));
        let snapshot = block_on(db.begin_snapshot()).expect("snapshot");
        let plan = block_on(snapshot.plan_scan(&db, &predicate, None, None)).expect("plan");
        // Pruning is currently disabled; expect to scan all immutables and retain the predicate
        // for residual evaluation.
        assert_eq!(plan.immutable_indexes, vec![0, 1]);
        assert!(plan.residual_predicate.is_some());
    }

    #[test]
    fn plan_scan_preserves_residual_predicate() {
        let db = db_with_immutable_keys(&["k1"]);
        let key_pred = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k1"));
        let value_pred = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(5i64));
        let predicate = Predicate::and(vec![key_pred, value_pred]);
        let snapshot = block_on(db.begin_snapshot()).expect("snapshot");
        let plan = block_on(snapshot.plan_scan(&db, &predicate, None, None)).expect("plan");
        assert!(plan.residual_predicate.is_some());
    }

    #[test]
    fn plan_scan_marks_empty_range() {
        let db = db_with_immutable_keys(&["k1"]);
        let pred_a = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k1"));
        let pred_b = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k2"));
        let predicate = Predicate::and(vec![pred_a, pred_b]);
        let snapshot = block_on(db.begin_snapshot()).expect("snapshot");
        let plan = block_on(snapshot.plan_scan(&db, &predicate, None, None)).expect("plan");
        // Pruning is currently disabled; even contradictory predicates scan all immutables.
        assert_eq!(plan.immutable_indexes, vec![0]);
    }

    fn db_with_immutable_keys(keys: &[&str]) -> DB<DynMode, BlockingExecutor> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let executor = Arc::new(BlockingExecutor);
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let mut db: DB<DynMode, BlockingExecutor> =
            block_on(DB::new(config, Arc::clone(&executor))).expect("db");
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
        for (idx, key) in keys.iter().enumerate() {
            let rows = vec![DynRow(vec![
                Some(DynCell::Str((*key).into())),
                Some(DynCell::I32(idx as i32)),
            ])];
            let batch = build_batch(schema.clone(), rows).expect("batch");
            block_on(db.ingest_with_tombstones(batch, vec![false])).expect("ingest");
        }
        db
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_waits_for_wal_durable_ack() {
        use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

        let executor = Arc::new(TokioExecutor::default());
        let (sender, mut receiver) = mpsc::channel(1);
        let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ack_slot = Arc::new(Mutex::new(None));
        let (ack_ready_tx, ack_ready_rx) = oneshot::channel();
        let (release_ack_tx, release_ack_rx) = oneshot::channel();

        let ack_slot_clone = Arc::clone(&ack_slot);
        let join = executor.spawn(async move {
            let mut release_ack_rx = Some(release_ack_rx);
            while let Some(msg) = receiver.next().await {
                match msg {
                    writer::WriterMsg::Enqueue {
                        command, ack_tx, ..
                    } => match command {
                        WalCommand::TxnAppend { .. } => {
                            let ack = WalAck {
                                first_seq: frame::INITIAL_FRAME_SEQ,
                                last_seq: frame::INITIAL_FRAME_SEQ,
                                bytes_flushed: 0,
                                elapsed: Duration::from_millis(0),
                            };
                            let _ = ack_tx.send(Ok(ack));
                        }
                        WalCommand::TxnCommit { .. } => {
                            {
                                let mut slot = ack_slot_clone.lock().await;
                                *slot = Some(ack_tx);
                            }
                            let _ = ack_ready_tx.send(());
                            if let Some(rx) = release_ack_rx.take() {
                                let _ = rx.await;
                            }
                            let ack = WalAck {
                                first_seq: frame::INITIAL_FRAME_SEQ + 1,
                                last_seq: frame::INITIAL_FRAME_SEQ + 1,
                                bytes_flushed: 0,
                                elapsed: Duration::from_millis(0),
                            };
                            let mut slot = ack_slot_clone.lock().await;
                            if let Some(sender) = slot.take() {
                                let _ = sender.send(Ok(ack));
                            }
                            break;
                        }
                        _ => {
                            let ack = WalAck {
                                first_seq: frame::INITIAL_FRAME_SEQ,
                                last_seq: frame::INITIAL_FRAME_SEQ,
                                bytes_flushed: 0,
                                elapsed: Duration::from_millis(0),
                            };
                            let _ = ack_tx.send(Ok(ack));
                        }
                    },
                    writer::WriterMsg::Rotate { ack_tx } => {
                        let _ = ack_tx.send(Ok(()));
                    }
                    writer::WriterMsg::Snapshot { ack_tx } => {
                        let snapshot = WalSnapshot {
                            sealed_segments: Vec::new(),
                            active_segment: None,
                        };
                        let _ = ack_tx.send(Ok(snapshot));
                    }
                }
            }
            Ok(())
        });

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

        let mut db: DB<DynMode, TokioExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");
        let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
        let handle = WalHandle::test_from_parts(
            sender,
            queue_depth,
            join,
            frame::INITIAL_FRAME_SEQ,
            metrics,
        );
        db.set_wal_handle(Some(handle));

        let mut ingest_future = Box::pin(db.ingest(batch));
        tokio::select! {
            _ = ack_ready_rx => {}
            res = &mut ingest_future => panic!("ingest finished early: {:?}", res),
        }

        release_ack_tx.send(()).expect("release ack");
        ingest_future.await.expect("ingest after ack");

        let pred = Predicate::is_not_null(ColumnRef::new("id", None));
        let snapshot = db.begin_snapshot().await.expect("snapshot");
        let plan = snapshot
            .plan_scan(&db, &pred, None, None)
            .await
            .expect("plan");
        let stream = db.execute_scan(plan).await.expect("execute");
        let rows: Vec<_> = stream
            .try_collect::<Vec<_>>()
            .await
            .expect("collect")
            .into_iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id col")
                    .iter()
                    .flatten()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows, vec!["k".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_live_frame_floor_tracks_multi_frame_append() {
        use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

        let executor = Arc::new(TokioExecutor::default());
        let (sender, mut receiver) = mpsc::channel(4);
        let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let join = executor.spawn(async move {
            let mut next_seq = frame::INITIAL_FRAME_SEQ;
            while let Some(msg) = receiver.next().await {
                match msg {
                    writer::WriterMsg::Enqueue {
                        command, ack_tx, ..
                    } => {
                        let (first_seq, last_seq, advance) = match command {
                            WalCommand::TxnAppend { .. } => {
                                (next_seq, next_seq.saturating_add(1), 2)
                            }
                            WalCommand::TxnCommit { .. } => (next_seq, next_seq, 1),
                            _ => (next_seq, next_seq, 1),
                        };
                        next_seq = next_seq.saturating_add(advance);
                        let ack = WalAck {
                            first_seq,
                            last_seq,
                            bytes_flushed: 0,
                            elapsed: Duration::from_millis(0),
                        };
                        let _ = ack_tx.send(Ok(ack));
                    }
                    writer::WriterMsg::Rotate { ack_tx } => {
                        let _ = ack_tx.send(Ok(()));
                    }
                    writer::WriterMsg::Snapshot { ack_tx } => {
                        let snapshot = WalSnapshot {
                            sealed_segments: Vec::new(),
                            active_segment: None,
                        };
                        let _ = ack_tx.send(Ok(snapshot));
                    }
                }
            }
            Ok(())
        });

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

        let mut db: DB<DynMode, TokioExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");
        let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
        let handle = WalHandle::test_from_parts(
            sender,
            queue_depth,
            join,
            frame::INITIAL_FRAME_SEQ,
            metrics,
        );
        db.set_wal_handle(Some(handle));
        assert!(db.wal_handle().is_some(), "wal handle should be installed");

        db.ingest_with_tombstones(batch, vec![false])
            .await
            .expect("ingest");

        let observed_range = db
            .mutable_wal_range_snapshot()
            .or_else(|| {
                db.seal_state_lock()
                    .immutable_wal_ranges
                    .first()
                    .copied()
                    .flatten()
            })
            .expect("wal range populated after ingest");
        assert_eq!(observed_range.first, frame::INITIAL_FRAME_SEQ);
        assert_eq!(observed_range.last, frame::INITIAL_FRAME_SEQ + 2);
        assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

        if db.mutable_wal_range_snapshot().is_some() {
            let sealed = {
                let mut mem = db.mem_write();
                mem.seal_into_immutable(&db.mode.schema, db.mode.extractor.as_ref())
                    .expect("seal mutable")
                    .expect("mutable contained rows")
            };
            let wal_range = db.take_mutable_wal_range();
            db.add_immutable(sealed, wal_range);
        }

        assert!(db.mutable_wal_range_snapshot().is_none());
        assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

        {
            let mut seal = db.seal_state_lock();
            seal.immutables.clear();
            seal.immutable_wal_ranges.clear();
        }
        assert_eq!(db.wal_live_frame_floor(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_live_frame_floor_tracks_multi_frame_append_via_insert() {
        use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

        let executor = Arc::new(TokioExecutor::default());
        let (sender, mut receiver) = mpsc::channel(4);
        let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let join = executor.spawn(async move {
            let mut next_seq = frame::INITIAL_FRAME_SEQ;
            while let Some(msg) = receiver.next().await {
                match msg {
                    writer::WriterMsg::Enqueue {
                        command, ack_tx, ..
                    } => {
                        let (first_seq, last_seq, advance) = match command {
                            WalCommand::TxnAppend { .. } => {
                                (next_seq, next_seq.saturating_add(1), 2)
                            }
                            WalCommand::TxnCommit { .. } => (next_seq, next_seq, 1),
                            _ => (next_seq, next_seq, 1),
                        };
                        next_seq = next_seq.saturating_add(advance);
                        let ack = WalAck {
                            first_seq,
                            last_seq,
                            bytes_flushed: 0,
                            elapsed: Duration::from_millis(0),
                        };
                        let _ = ack_tx.send(Ok(ack));
                    }
                    writer::WriterMsg::Rotate { ack_tx } => {
                        let _ = ack_tx.send(Ok(()));
                    }
                    writer::WriterMsg::Snapshot { ack_tx } => {
                        let snapshot = WalSnapshot {
                            sealed_segments: Vec::new(),
                            active_segment: None,
                        };
                        let _ = ack_tx.send(Ok(snapshot));
                    }
                }
            }
            Ok(())
        });

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

        let mut db: DB<DynMode, TokioExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");
        let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
        let handle = WalHandle::test_from_parts(
            sender,
            queue_depth,
            join,
            frame::INITIAL_FRAME_SEQ,
            metrics,
        );
        db.set_wal_handle(Some(handle));
        assert!(db.wal_handle().is_some(), "wal handle should be installed");

        db.ingest(batch.clone()).await.expect("ingest");

        let observed_range = db
            .mutable_wal_range_snapshot()
            .or_else(|| {
                db.seal_state_lock()
                    .immutable_wal_ranges
                    .first()
                    .copied()
                    .flatten()
            })
            .expect("wal range populated after ingest");
        assert_eq!(observed_range.first, frame::INITIAL_FRAME_SEQ);
        assert_eq!(observed_range.last, frame::INITIAL_FRAME_SEQ + 2);
        assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

        if db.mutable_wal_range_snapshot().is_some() {
            let sealed = {
                let mut mem = db.mem_write();
                mem.seal_into_immutable(&db.mode.schema, db.mode.extractor.as_ref())
                    .expect("seal mutable")
                    .expect("mutable contained rows")
            };
            let wal_range = db.take_mutable_wal_range();
            db.add_immutable(sealed, wal_range);
        }

        assert!(db.mutable_wal_range_snapshot().is_none());
        assert_eq!(db.wal_live_frame_floor(), Some(frame::INITIAL_FRAME_SEQ));

        {
            let mut seal = db.seal_state_lock();
            seal.immutables.clear();
            seal.immutable_wal_ranges.clear();
        }
        assert_eq!(db.wal_live_frame_floor(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dyn_insert_enqueues_commit_before_append_ack() {
        use tokio::time;

        use crate::wal::{WalAck, WalHandle, WalSnapshot, frame, writer};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

        let executor = Arc::new(TokioExecutor::default());
        let (sender, mut receiver) = mpsc::channel(4);
        let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let (append_seen_tx, append_seen_rx) = oneshot::channel();
        let (commit_seen_tx, commit_seen_rx) = oneshot::channel();
        let (release_append_ack_tx, release_append_ack_rx) = oneshot::channel();
        let (release_commit_ack_tx, release_commit_ack_rx) = oneshot::channel();

        let join = executor.spawn(async move {
            let mut next_seq = frame::INITIAL_FRAME_SEQ;
            let mut pending_append_ack: Option<futures_oneshot::Sender<WalResult<WalAck>>> = None;
            let mut release_append_ack_rx = Some(release_append_ack_rx);
            let mut release_commit_ack_rx = Some(release_commit_ack_rx);
            let mut append_seen_tx = Some(append_seen_tx);
            let mut commit_seen_tx = Some(commit_seen_tx);
            while let Some(msg) = receiver.next().await {
                match msg {
                    writer::WriterMsg::Enqueue {
                        command, ack_tx, ..
                    } => match command {
                        WalCommand::TxnAppend { .. } => {
                            pending_append_ack = Some(ack_tx);
                            if let Some(tx) = append_seen_tx.take() {
                                let _ = tx.send(());
                            }
                        }
                        WalCommand::TxnCommit { .. } => {
                            if let Some(tx) = commit_seen_tx.take() {
                                let _ = tx.send(());
                            }
                            if let Some(rx) = release_append_ack_rx.take() {
                                let _ = rx.await;
                            }
                            if let Some(append_ack_tx) = pending_append_ack.take() {
                                let ack = WalAck {
                                    first_seq: next_seq,
                                    last_seq: next_seq,
                                    bytes_flushed: 0,
                                    elapsed: Duration::from_millis(0),
                                };
                                let _ = append_ack_tx.send(Ok(ack));
                                next_seq = next_seq.saturating_add(1);
                            }
                            if let Some(rx) = release_commit_ack_rx.take() {
                                let _ = rx.await;
                            }
                            let ack = WalAck {
                                first_seq: next_seq,
                                last_seq: next_seq,
                                bytes_flushed: 0,
                                elapsed: Duration::from_millis(0),
                            };
                            let _ = ack_tx.send(Ok(ack));
                            next_seq = next_seq.saturating_add(1);
                        }
                        _ => {}
                    },
                    writer::WriterMsg::Rotate { ack_tx } => {
                        let _ = ack_tx.send(Ok(()));
                    }
                    writer::WriterMsg::Snapshot { ack_tx } => {
                        let snapshot = WalSnapshot {
                            sealed_segments: Vec::new(),
                            active_segment: None,
                        };
                        let _ = ack_tx.send(Ok(snapshot));
                    }
                }
            }
            Ok(())
        });

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

        let mut db: DB<DynMode, TokioExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");
        let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
        let handle = WalHandle::test_from_parts(
            sender,
            queue_depth,
            join,
            frame::INITIAL_FRAME_SEQ,
            metrics,
        );
        db.set_wal_handle(Some(handle));

        let mut ingest_future = Box::pin(db.ingest(batch));
        tokio::select! {
            _ = append_seen_rx => {}
            res = &mut ingest_future => panic!("ingest finished early: {:?}", res),
        }
        tokio::select! {
            res = commit_seen_rx => {
                res.expect("commit notification");
            }
            _ = time::sleep(Duration::from_millis(50)) => {
                panic!("commit not enqueued before append ack release");
            }
            res = &mut ingest_future => panic!("ingest finished before commit ack gating: {:?}", res),
        }

        release_append_ack_tx.send(()).expect("release append ack");
        release_commit_ack_tx.send(()).expect("release commit ack");

        ingest_future.await.expect("ingest complete");
    }
    #[tokio::test(flavor = "current_thread")]
    async fn dynamic_new_from_metadata_field_marker() {
        use std::collections::HashMap;
        // Schema: mark id with field-level metadata tonbo.key = true
        let mut fm = HashMap::new();
        fm.insert("tonbo.key".to_string(), "true".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(fm);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata key config");
        let db: DB<DynMode, BlockingExecutor> = DB::new(config, Arc::new(BlockingExecutor))
            .await
            .expect("metadata key");

        // Build one batch and insert to ensure extractor wired
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        db.ingest(batch).await.expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dynamic_new_from_metadata_schema_level() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "id".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]).with_metadata(sm));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("schema metadata config");
        let db: DB<DynMode, BlockingExecutor> = DB::new(config, Arc::new(BlockingExecutor))
            .await
            .expect("schema metadata key");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("x".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("y".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        db.ingest(batch).await.expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn dynamic_new_from_metadata_conflicts_and_missing() {
        use std::collections::HashMap;
        // Conflict: two fields marked as key
        let mut fm1 = HashMap::new();
        fm1.insert("tonbo.key".to_string(), "true".to_string());
        let mut fm2 = HashMap::new();
        fm2.insert("tonbo.key".to_string(), "1".to_string());
        let f1 = Field::new("id1", DataType::Utf8, false).with_metadata(fm1);
        let f2 = Field::new("id2", DataType::Utf8, false).with_metadata(fm2);
        let schema_conflict = std::sync::Arc::new(Schema::new(vec![f1, f2]));
        assert!(DynModeConfig::from_metadata(schema_conflict).is_err());

        // Missing: no markers at field or schema level
        let schema_missing = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        assert!(DynModeConfig::from_metadata(schema_missing).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_without_immutables_errors() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name config");
        let executor = Arc::new(BlockingExecutor);
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, executor).await.expect("db init");

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sstable_cfg = Arc::new(SsTableConfig::new(
            schema.clone(),
            fs,
            Path::from("/tmp/tonbo-flush-test"),
        ));
        let descriptor = SsTableDescriptor::new(SsTableId::new(1), 0);

        let result = db
            .flush_immutables_with_descriptor(sstable_cfg, descriptor.clone())
            .await;
        assert!(matches!(result, Err(SsTableError::NoImmutableSegments)));
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_publishes_manifest_version() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let executor = Arc::new(BlockingExecutor);
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).await.expect("db");
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        db.ingest(batch).await.expect("ingest triggers seal");
        assert_eq!(db.num_immutable_segments(), 1);

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sstable_cfg = Arc::new(SsTableConfig::new(
            schema.clone(),
            fs,
            Path::from("/tmp/tonbo-flush-ok"),
        ));
        let descriptor = SsTableDescriptor::new(SsTableId::new(7), 0);

        let table = db
            .flush_immutables_with_descriptor(sstable_cfg, descriptor.clone())
            .await
            .expect("flush succeeds");
        assert_eq!(db.num_immutable_segments(), 0);

        let snapshot = db
            .manifest
            .snapshot_latest(db.manifest_table)
            .await
            .expect("manifest snapshot");
        assert_eq!(
            snapshot.head.last_manifest_txn,
            Some(Timestamp::new(1)),
            "first flush should publish manifest txn 1"
        );
        let latest = snapshot
            .latest_version
            .expect("latest version must exist after flush");
        assert_eq!(
            latest.commit_timestamp(),
            Timestamp::new(1),
            "latest version should reflect manifest txn 1"
        );
        assert_eq!(latest.ssts().len(), 1);
        assert_eq!(latest.ssts()[0].len(), 1);
        let recorded = &latest.ssts()[0][0];
        assert_eq!(recorded.sst_id(), descriptor.id());
        assert!(
            recorded.stats().is_some() || table.descriptor().stats().is_none(),
            "stats should propagate when available"
        );
        assert!(
            recorded.wal_segments().is_none(),
            "no WAL segments recorded since none were attached"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plan_compaction_returns_task() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let config = crate::schema::SchemaBuilder::from_schema(schema.clone())
            .primary_key("id")
            .with_metadata()
            .build()
            .expect("schema builder");
        let schema = Arc::clone(&config.schema);
        let executor = Arc::new(TokioExecutor::default());
        let mut db: DB<DynMode, TokioExecutor> = DB::new(config, Arc::clone(&executor)).await?;
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sst_cfg = Arc::new(SsTableConfig::new(
            Arc::clone(&schema),
            fs,
            Path::from("/tmp/plan-compaction"),
        ));

        for pass in 0..2 {
            let rows = vec![vec![
                Some(DynCell::Str(format!("comp-{pass}").into())),
                Some(DynCell::I32(pass as i32)),
            ]];
            let batch = build_batch(Arc::clone(&schema), rows).expect("batch");
            db.ingest(batch).await.expect("ingest");
            let descriptor = SsTableDescriptor::new(SsTableId::new(pass as u64 + 1), 0);
            db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor)
                .await
                .expect("flush");
        }

        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
            l0_trigger: 1,
            l0_max_inputs: 2,
            l0_max_bytes: None,
            level_thresholds: vec![usize::MAX],
            level_max_bytes: Vec::new(),
            max_inputs_per_task: 2,
            max_task_bytes: None,
        });
        let task = db
            .plan_compaction_task(&planner)
            .await?
            .expect("compaction task");
        assert_eq!(task.source_level, 0);
        assert_eq!(task.target_level, 1);
        assert_eq!(task.input.len(), 2);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plan_compaction_empty_manifest_is_none() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let config = crate::schema::SchemaBuilder::from_schema(schema.clone())
            .primary_key("id")
            .with_metadata()
            .build()
            .expect("schema builder");
        let executor = Arc::new(TokioExecutor::default());
        let db: DB<DynMode, TokioExecutor> = DB::new(config, Arc::clone(&executor)).await?;
        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig::default());
        let plan = db.plan_compaction_task(&planner).await?;
        assert!(plan.is_none());
        Ok(())
    }

    #[test]
    fn wal_segments_after_compaction_preserves_manifest_when_metadata_missing() {
        let generator = FileIdGenerator::default();
        let table_id = TableId::new(&generator);
        let mut version = VersionState::empty(table_id);

        let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
        let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

        let entry_missing_wal = SstEntry::new(SsTableId::new(1), None, None, Path::default(), None);
        let entry_with_wal = SstEntry::new(
            SsTableId::new(2),
            None,
            Some(vec![wal_b.file_id().clone()]),
            Path::default(),
            None,
        );

        version
            .apply_edits(&[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_missing_wal, entry_with_wal],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone()],
                },
            ])
            .expect("apply edits");

        let wal_ids = DB::<DynMode, BlockingExecutor>::wal_ids_for_remaining_ssts(
            &version,
            &HashSet::new(),
            &[],
        );
        assert!(
            wal_ids.is_none(),
            "missing wal metadata on a remaining SST should preserve the manifest wal set"
        );

        let filtered =
            DB::<DynMode, BlockingExecutor>::wal_segments_after_compaction(&version, &[], &[]);
        assert!(
            filtered.is_none(),
            "compaction should not rewrite wal segments when metadata is absent"
        );
    }

    #[test]
    fn wal_segments_after_compaction_filters_and_tracks_obsolete() {
        let generator = FileIdGenerator::default();
        let table_id = TableId::new(&generator);
        let mut version = VersionState::empty(table_id);

        let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
        let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);
        let wal_c = WalSegmentRef::new(3, generator.generate(), 0, 0);

        let entry_a = SstEntry::new(
            SsTableId::new(1),
            None,
            Some(vec![wal_a.file_id().clone(), wal_b.file_id().clone()]),
            Path::default(),
            None,
        );
        let entry_b = SstEntry::new(
            SsTableId::new(2),
            None,
            Some(vec![wal_c.file_id().clone()]),
            Path::default(),
            None,
        );

        version
            .apply_edits(&[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_a, entry_b],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone(), wal_c.clone()],
                },
            ])
            .expect("apply edits");

        let removed = vec![
            SsTableDescriptor::new(SsTableId::new(1), 0)
                .with_storage_paths(Path::from("L0/1.parquet"), None),
        ];
        let added = vec![SsTableDescriptor::new(SsTableId::new(3), 0)];

        assert_eq!(version.wal_segments().len(), 3);
        assert!(
            version.ssts()[0][0].wal_segments().is_some(),
            "entry should carry wal segments"
        );
        assert!(
            version.ssts()[0][1].wal_segments().is_some(),
            "second entry should carry wal segments"
        );
        let removed_ids: HashSet<SsTableId> = removed.iter().map(|d| d.id().clone()).collect();
        let wal_ids = DB::<DynMode, BlockingExecutor>::wal_ids_for_remaining_ssts(
            &version,
            &removed_ids,
            &added,
        );
        assert!(
            wal_ids.is_none(),
            "wal ids should be None when any SST lacks wal metadata"
        );
        let filtered = DB::<DynMode, BlockingExecutor>::wal_segments_after_compaction(
            &version, &removed, &added,
        );
        assert!(
            filtered.is_none(),
            "filtered wal segments should be None when any SST lacks wal metadata"
        );

        let outcome = CompactionOutcome {
            add_ssts: Vec::new(),
            remove_ssts: removed,
            target_level: 0,
            wal_segments: None,
            tombstone_watermark: None,
            outputs: added.clone(),
            obsolete_sst_ids: Vec::new(),
            wal_floor: None,
            obsolete_wal_segments: vec![wal_a.clone(), wal_b.clone()],
        };
        let plan = DB::<DynMode, BlockingExecutor>::gc_plan_from_outcome(&outcome)
            .expect("gc plan")
            .expect("plan present");
        assert_eq!(plan.obsolete_wal_segments.len(), 2);
        assert!(plan.obsolete_wal_segments.contains(&wal_a));
        assert!(plan.obsolete_wal_segments.contains(&wal_b));
    }

    #[derive(Clone)]
    struct StaticPlanner {
        task: CompactionTask,
    }

    impl CompactionPlanner for StaticPlanner {
        fn plan(&self, _snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
            Some(self.task.clone())
        }
    }

    #[derive(Clone)]
    struct StaticExecutor {
        outputs: Vec<SsTableDescriptor>,
        wal_segments: Vec<WalSegmentRef>,
        target_level: u32,
    }

    impl CompactionExecutor for StaticExecutor {
        fn execute(
            &self,
            job: CompactionJob,
        ) -> Pin<Box<dyn Future<Output = Result<CompactionOutcome, CompactionError>> + Send + '_>>
        {
            let outputs = self.outputs.clone();
            let wal_segments = self.wal_segments.clone();
            let target_level = self.target_level;
            Box::pin(async move {
                let mut outcome = CompactionOutcome::from_outputs(
                    outputs.clone(),
                    job.inputs.clone(),
                    target_level,
                    Some(wal_segments),
                )?;
                outcome.outputs = outputs;
                Ok(outcome)
            })
        }

        fn cleanup_outputs<'a>(
            &'a self,
            _outputs: &'a [SsTableDescriptor],
        ) -> Pin<Box<dyn Future<Output = Result<(), CompactionError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_updates_manifest_wal_and_records_gc_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let generator = FileIdGenerator::default();
        let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
        let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

        let entry_a = SstEntry::new(
            SsTableId::new(1),
            None,
            Some(vec![wal_a.file_id().clone(), wal_b.file_id().clone()]),
            Path::from("L0/1.parquet"),
            None,
        );
        let entry_b = SstEntry::new(
            SsTableId::new(2),
            None,
            Some(vec![wal_b.file_id().clone()]),
            Path::from("L0/2.parquet"),
            None,
        );

        let db: DB<DynMode, BlockingExecutor> = DB::new(
            crate::schema::SchemaBuilder::from_schema(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("v", DataType::Int32, false),
            ])))
            .primary_key("id")
            .build()
            .expect("schema builder"),
            Arc::new(BlockingExecutor),
        )
        .await
        .expect("db init");

        db.manifest
            .apply_version_edits(
                db.manifest_table,
                &[
                    VersionEdit::AddSsts {
                        level: 0,
                        entries: vec![entry_a, entry_b],
                    },
                    VersionEdit::SetWalSegments {
                        segments: vec![wal_a.clone(), wal_b.clone()],
                    },
                ],
            )
            .await
            .expect("apply edits");

        let task = CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![
                CompactionInput {
                    level: 0,
                    sst_id: SsTableId::new(1),
                },
                CompactionInput {
                    level: 0,
                    sst_id: SsTableId::new(2),
                },
            ],
            key_range: None,
        };
        let planner = StaticPlanner { task };

        let output_desc = SsTableDescriptor::new(SsTableId::new(3), 1)
            .with_wal_ids(Some(vec![wal_b.file_id().clone()]))
            .with_storage_paths(Path::from("L1/3.parquet"), None);
        let executor = StaticExecutor {
            outputs: vec![output_desc],
            wal_segments: vec![wal_b.clone()],
            target_level: 1,
        };

        let outcome = db
            .run_compaction_task(&planner, &executor)
            .await?
            .expect("compaction outcome");

        let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
        let latest = snapshot.latest_version.expect("latest version");
        assert_eq!(latest.wal_segments().len(), 1);
        assert_eq!(latest.wal_segments()[0].file_id(), wal_b.file_id());
        assert_eq!(
            outcome.obsolete_wal_segments,
            vec![wal_a.clone()],
            "should surface obsolete wal segments"
        );

        let plan = db
            .manifest
            .take_gc_plan(db.manifest_table)
            .await?
            .expect("gc plan recorded");
        assert_eq!(plan.obsolete_wal_segments.len(), 1);
        assert_eq!(plan.obsolete_wal_segments[0].file_id(), wal_a.file_id());
        assert_eq!(plan.obsolete_ssts.len(), 2);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_preserves_manifest_wal_when_metadata_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let generator = FileIdGenerator::default();
        let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
        let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

        let entry_missing_wal = SstEntry::new(
            SsTableId::new(1),
            None,
            None,
            Path::from("L0/1.parquet"),
            None,
        );
        let entry_with_wal = SstEntry::new(
            SsTableId::new(2),
            None,
            Some(vec![wal_b.file_id().clone()]),
            Path::from("L0/2.parquet"),
            None,
        );

        let db: DB<DynMode, BlockingExecutor> = DB::new(
            crate::schema::SchemaBuilder::from_schema(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("v", DataType::Int32, false),
            ])))
            .primary_key("id")
            .build()
            .expect("schema builder"),
            Arc::new(BlockingExecutor),
        )
        .await
        .expect("db init");

        db.manifest
            .apply_version_edits(
                db.manifest_table,
                &[
                    VersionEdit::AddSsts {
                        level: 0,
                        entries: vec![entry_missing_wal, entry_with_wal],
                    },
                    VersionEdit::SetWalSegments {
                        segments: vec![wal_a.clone(), wal_b.clone()],
                    },
                ],
            )
            .await
            .expect("apply edits");

        let task = CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![CompactionInput {
                level: 0,
                sst_id: SsTableId::new(2),
            }],
            key_range: None,
        };
        let planner = StaticPlanner { task };

        let output_desc = SsTableDescriptor::new(SsTableId::new(3), 1)
            .with_wal_ids(Some(vec![wal_b.file_id().clone()]))
            .with_storage_paths(Path::from("L1/3.parquet"), None);
        let executor = StaticExecutor {
            outputs: vec![output_desc],
            wal_segments: vec![wal_b.clone()],
            target_level: 1,
        };

        let outcome = db
            .run_compaction_task(&planner, &executor)
            .await?
            .expect("compaction outcome");

        let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
        let latest = snapshot.latest_version.expect("latest version");
        assert_eq!(
            latest.wal_segments(),
            &[wal_a.clone(), wal_b.clone()],
            "manifest wal set should remain unchanged when wal metadata is missing",
        );
        assert!(
            outcome.obsolete_wal_segments.is_empty(),
            "should not surface obsolete wal segments when manifest set is preserved"
        );

        let plan = db
            .manifest
            .take_gc_plan(db.manifest_table)
            .await?
            .expect("gc plan recorded");
        assert!(
            plan.obsolete_wal_segments.is_empty(),
            "gc plan should not mark wal segments obsolete"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_e2e_merges_and_advances_wal_floor() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_root = workspace_temp_dir("compaction-e2e-wal");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(Arc::clone(&schema), 0).expect("extractor");
        let mode_cfg = crate::schema::SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder");

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sst_root = temp_root.join("sst");
        fs::create_dir_all(&sst_root)?;
        let sst_root = Path::from_filesystem_path(&sst_root)?;
        let sst_cfg = Arc::new(
            SsTableConfig::new(Arc::clone(&schema), fs, sst_root)
                .with_key_extractor(extractor.into()),
        );

        // Build two SSTs with WAL ids.
        let wal_gen = FileIdGenerator::default();
        let wal_a = WalSegmentRef::new(10, wal_gen.generate(), 0, 0);
        let wal_b = WalSegmentRef::new(11, wal_gen.generate(), 0, 0);

        let batch_a = build_batch(
            Arc::clone(&schema),
            vec![DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I32(1)),
            ])],
        )?;
        let imm_a =
            crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_a, "id")?;
        let mut builder_a = SsTableBuilder::<DynMode>::new(
            Arc::clone(&sst_cfg),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder_a.add_immutable(&imm_a)?;
        let sst_a = builder_a.finish().await?;
        let desc_a = sst_a
            .descriptor()
            .clone()
            .with_wal_ids(Some(vec![wal_a.file_id().clone()]));

        let batch_b = build_batch(
            Arc::clone(&schema),
            vec![DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I32(2)),
            ])],
        )?;
        let imm_b =
            crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_b, "id")?;
        let mut builder_b = SsTableBuilder::<DynMode>::new(
            Arc::clone(&sst_cfg),
            SsTableDescriptor::new(SsTableId::new(2), 0),
        );
        builder_b.add_immutable(&imm_b)?;
        let sst_b = builder_b.finish().await?;
        let desc_b = sst_b
            .descriptor()
            .clone()
            .with_wal_ids(Some(vec![wal_b.file_id().clone()]));

        // Seed manifest with the two inputs and WAL set.
        let db: DB<DynMode, BlockingExecutor> =
            DB::new(mode_cfg, Arc::new(BlockingExecutor)).await?;
        let entry_a = SstEntry::new(
            desc_a.id().clone(),
            desc_a.stats().cloned(),
            desc_a.wal_ids().map(|ids| ids.to_vec()),
            desc_a
                .data_path()
                .expect("input descriptor missing data path")
                .clone(),
            desc_a.delete_path().cloned(),
        );
        let entry_b = SstEntry::new(
            desc_b.id().clone(),
            desc_b.stats().cloned(),
            desc_b.wal_ids().map(|ids| ids.to_vec()),
            desc_b
                .data_path()
                .expect("input descriptor missing data path")
                .clone(),
            desc_b.delete_path().cloned(),
        );
        db.manifest
            .apply_version_edits(
                db.manifest_table,
                &[
                    VersionEdit::AddSsts {
                        level: 0,
                        entries: vec![entry_a, entry_b],
                    },
                    VersionEdit::SetWalSegments {
                        segments: vec![wal_a.clone(), wal_b.clone()],
                    },
                ],
            )
            .await?;

        let task = CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![
                CompactionInput {
                    level: 0,
                    sst_id: desc_a.id().clone(),
                },
                CompactionInput {
                    level: 0,
                    sst_id: desc_b.id().clone(),
                },
            ],
            key_range: None,
        };
        let planner = StaticPlanner { task };
        let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100)
            .with_max_output_bytes(8 * 1024 * 1024);

        let outcome = db
            .run_compaction_task(&planner, &executor)
            .await?
            .expect("compaction outcome");
        assert_eq!(outcome.remove_ssts.len(), 2);
        assert_eq!(outcome.add_ssts.len(), 1);
        assert_eq!(
            outcome
                .add_ssts
                .first()
                .and_then(|e| e.wal_segments())
                .map(|ids| ids.len()),
            Some(2),
            "output should aggregate wal ids"
        );
        assert_eq!(
            outcome.wal_segments.as_ref().map(|segments| segments.len()),
            Some(2),
            "manifest wal set should retain gap when inputs have discontinuity"
        );
        assert_eq!(
            outcome.wal_floor.as_ref().map(|w| w.seq()),
            Some(wal_a.seq()),
            "wal floor should reflect first retained segment when gaps exist"
        );
        assert_eq!(
            outcome.obsolete_wal_segments.len(),
            0,
            "no wal segments should be obsolete when gaps are retained"
        );

        let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
        let latest = snapshot.latest_version.expect("latest version");
        assert_eq!(latest.wal_segments().len(), 2);
        let wal_ids: HashSet<_> = latest
            .wal_segments()
            .iter()
            .map(|seg| seg.file_id().clone())
            .collect();
        assert!(wal_ids.contains(wal_a.file_id()));
        assert!(wal_ids.contains(wal_b.file_id()));
        assert_eq!(
            latest.wal_floor().as_ref().map(|w| w.file_id()),
            Some(wal_a.file_id()),
            "wal floor should align to first retained segment"
        );

        let plan = db
            .manifest
            .take_gc_plan(db.manifest_table)
            .await?
            .expect("gc plan recorded");
        assert_eq!(plan.obsolete_wal_segments.len(), 0);
        assert!(
            plan.obsolete_ssts.len() >= 1,
            "gc plan should record obsolete ssts"
        );

        fs::remove_dir_all(&temp_root)?;
        Ok(())
    }

    #[derive(Clone)]
    struct ConflictingExecutor {
        inner: LocalCompactionExecutor,
        manifest: TonboManifest,
        table: TableId,
        outputs: Arc<Mutex<Vec<SsTableDescriptor>>>,
    }

    impl ConflictingExecutor {
        fn new(
            inner: LocalCompactionExecutor,
            manifest: TonboManifest,
            table: TableId,
            outputs: Arc<Mutex<Vec<SsTableDescriptor>>>,
        ) -> Self {
            Self {
                inner,
                manifest,
                table,
                outputs,
            }
        }
    }

    impl CompactionExecutor for ConflictingExecutor {
        fn execute(
            &self,
            job: CompactionJob,
        ) -> Pin<Box<dyn Future<Output = Result<CompactionOutcome, CompactionError>> + Send + '_>>
        {
            let inner = self.inner.clone();
            let manifest = self.manifest.clone();
            let table = self.table;
            let outputs = Arc::clone(&self.outputs);
            Box::pin(async move {
                let outcome = inner.execute(job).await?;
                {
                    let mut guard = outputs.lock().await;
                    guard.clear();
                    guard.extend(outcome.outputs.iter().cloned());
                }
                manifest
                    .apply_version_edits(
                        table,
                        &[VersionEdit::SetWalSegments {
                            segments: Vec::new(),
                        }],
                    )
                    .await
                    .map_err(CompactionError::Manifest)?;
                Ok(outcome)
            })
        }

        fn cleanup_outputs<'a>(
            &'a self,
            outputs: &'a [SsTableDescriptor],
        ) -> Pin<Box<dyn Future<Output = Result<(), CompactionError>> + Send + 'a>> {
            self.inner.cleanup_outputs(outputs)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_cas_conflict_cleans_outputs() -> Result<(), Box<dyn std::error::Error>> {
        let temp_root = workspace_temp_dir("compaction-cas-cleanup");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let mode_cfg = crate::schema::SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder");
        let extractor = Arc::clone(&mode_cfg.extractor);
        let schema = Arc::clone(&mode_cfg.schema);
        let executor = Arc::new(TokioExecutor::default());
        let mut db: DB<DynMode, TokioExecutor> = DB::new(mode_cfg, Arc::clone(&executor)).await?;
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

        let sst_root = temp_root.join("sst");
        fs::create_dir_all(&sst_root)?;
        let sst_root = Path::from_filesystem_path(&sst_root)?;
        let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sst_cfg = Arc::new(
            SsTableConfig::new(Arc::clone(&schema), sst_fs.clone(), sst_root)
                .with_key_extractor(extractor),
        );

        for idx in 0..2 {
            let rows = vec![vec![
                Some(DynCell::Str(format!("ck-{idx}").into())),
                Some(DynCell::I32(idx as i32)),
            ]];
            let batch = build_batch(Arc::clone(&schema), rows)?;
            db.ingest(batch).await?;
            let descriptor = SsTableDescriptor::new(SsTableId::new(idx as u64 + 1), 0);
            db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor)
                .await?;
        }

        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
            l0_trigger: 1,
            l0_max_inputs: 2,
            l0_max_bytes: None,
            level_thresholds: vec![usize::MAX],
            level_max_bytes: Vec::new(),
            max_inputs_per_task: 2,
            max_task_bytes: None,
        });
        let recorded_outputs = Arc::new(Mutex::new(Vec::new()));
        let executor = ConflictingExecutor::new(
            LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100),
            db.manifest.clone(),
            db.manifest_table,
            Arc::clone(&recorded_outputs),
        );

        let result = db.run_compaction_task(&planner, &executor).await;
        match result {
            Err(CompactionError::Manifest(ManifestError::CasConflict(_)))
            | Err(CompactionError::CasConflict) => {}
            other => panic!("expected CAS conflict, got {other:?}"),
        }

        let outputs = recorded_outputs.lock().await.clone();
        assert!(
            !outputs.is_empty(),
            "compaction should have produced outputs before CAS conflict"
        );
        for desc in outputs {
            if let Some(path) = desc.data_path() {
                assert!(
                    sst_fs.open(path).await.is_err(),
                    "data file should be cleaned up"
                );
            }
            if let Some(path) = desc.delete_path() {
                assert!(
                    sst_fs.open(path).await.is_err(),
                    "delete file should be cleaned up"
                );
            }
        }

        fs::remove_dir_all(&temp_root)?;
        Ok(())
    }

    #[test]
    fn resolve_compaction_inputs_keeps_levels() {
        let file_ids = FileIdGenerator::default();
        let table_id = TableId::new(&file_ids);
        let mut version = VersionState::empty(table_id);
        let l0_id = SsTableId::new(1);
        let l1_id = SsTableId::new(2);
        let edits = vec![
            VersionEdit::AddSsts {
                level: 0,
                entries: vec![SstEntry::new(
                    l0_id.clone(),
                    None,
                    None,
                    Path::from("L0/000.parquet"),
                    None,
                )],
            },
            VersionEdit::AddSsts {
                level: 1,
                entries: vec![SstEntry::new(
                    l1_id.clone(),
                    None,
                    None,
                    Path::from("L1/001.parquet"),
                    None,
                )],
            },
        ];
        version.apply_edits(&edits).expect("apply edits");

        let task = CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![
                CompactionInput {
                    level: 0,
                    sst_id: l0_id,
                },
                CompactionInput {
                    level: 1,
                    sst_id: l1_id,
                },
            ],
            key_range: None,
        };

        let resolved = DB::<DynMode, BlockingExecutor>::resolve_compaction_inputs(&version, &task)
            .expect("resolve");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].level(), 0);
        assert_eq!(resolved[1].level(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_gc_smoke_prunes_segments_after_flush() -> Result<(), Box<dyn std::error::Error>> {
        let temp_root = workspace_temp_dir("wal-gc-smoke");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let mode_config = DynModeConfig::from_key_name(schema.clone(), "id")?;
        let executor = Arc::new(TokioExecutor::default());
        let namespace = temp_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("wal-gc-smoke");
        let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
            .in_memory(namespace.to_string())
            .build_with_executor(Arc::clone(&executor))
            .await?;

        let wal_local_fs = Arc::new(LocalFs {});
        let wal_dyn_fs: Arc<dyn DynFs> = wal_local_fs.clone();
        let wal_cas: Arc<dyn FsCas> = wal_local_fs.clone();

        let wal_dir = temp_root.join("wal");
        fs::create_dir_all(&wal_dir)?;
        let wal_path = Path::from_filesystem_path(&wal_dir)?;

        let mut wal_cfg = RuntimeWalConfig::default();
        wal_cfg.dir = wal_path;
        wal_cfg.segment_backend = wal_dyn_fs;
        wal_cfg.state_store = Some(Arc::new(FsWalStateStore::new(wal_cas)));
        // Force extremely small segments so each batch append rotates the WAL; this
        // guarantees multiple sealed segments exist between the two flushes and
        // exercises the GC path we care about.
        wal_cfg.segment_max_bytes = 1;
        wal_cfg.flush_interval = Duration::from_millis(1);
        wal_cfg.sync = WalSyncPolicy::Disabled;

        db.enable_wal(wal_cfg.clone()).await?;
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

        let sst_dir = temp_root.join("sst");
        fs::create_dir_all(&sst_dir)?;
        let sst_root = Path::from_filesystem_path(&sst_dir)?;
        let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));

        // First ingest/flush establishes the initial WAL floor.
        for idx in 0..4 {
            let rows = vec![DynRow(vec![
                Some(DynCell::Str(format!("user-{idx}").into())),
                Some(DynCell::I32(idx as i32)),
            ])];
            let batch = build_batch(schema.clone(), rows)?;
            db.ingest(batch).await?;
        }
        assert!(
            db.num_immutable_segments() >= 1,
            "expected first seal to produce immutables"
        );
        let descriptor_a = SsTableDescriptor::new(SsTableId::new(99), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_a)
            .await?;
        let floor_after_first = db
            .manifest_wal_floor()
            .await
            .map(|ref_| ref_.seq())
            .expect("manifest floor after first flush");
        let first_prune_view = wal_segment_paths(&wal_dir);
        assert!(
            first_prune_view.len() >= 2,
            "expected multiple WAL segments present after first flush"
        );

        // Second ingest/flush should advance the floor and delete older segments.
        for idx in 4..8 {
            let rows = vec![DynRow(vec![
                Some(DynCell::Str(format!("user-{idx}").into())),
                Some(DynCell::I32(idx as i32)),
            ])];
            let batch = build_batch(schema.clone(), rows)?;
            db.ingest(batch).await?;
        }
        assert!(
            db.num_immutable_segments() >= 1,
            "expected second seal to produce immutables"
        );
        let descriptor_b = SsTableDescriptor::new(SsTableId::new(100), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_b)
            .await?;
        let floor_after_second = db
            .manifest_wal_floor()
            .await
            .map(|ref_| ref_.seq())
            .expect("manifest floor after second flush");
        assert!(
            floor_after_second > floor_after_first,
            "floor should advance after second flush"
        );
        let after = wal_segment_paths(&wal_dir);
        let removed: Vec<_> = first_prune_view
            .iter()
            .filter(|path| !after.contains(path))
            .collect();
        assert!(
            !removed.is_empty(),
            "second flush should prune at least one WAL segment"
        );

        if let Some(handle) = db.wal().cloned() {
            let metrics = handle.metrics();
            let guard = metrics.read().await;
            assert!(
                guard.wal_segments_pruned >= removed.len() as u64,
                "pruned segment count should be reflected in metrics"
            );
            assert!(
                guard.wal_floor_advancements >= 2,
                "floor should advance at least once per flush"
            );
            assert_eq!(
                guard.wal_prune_dry_runs, 0,
                "regular pruning should not increment dry-run counters"
            );
        }

        db.disable_wal().await?;
        fs::remove_dir_all(&temp_root)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_gc_dry_run_reports_only() -> Result<(), Box<dyn std::error::Error>> {
        let temp_root = workspace_temp_dir("wal-gc-dry-run");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let mode_config = crate::schema::SchemaBuilder::from_schema(schema)
            .primary_key("id")
            .with_metadata()
            .build()
            .expect("key field");
        let schema = Arc::clone(&mode_config.schema);
        let executor = Arc::new(TokioExecutor::default());
        let namespace = temp_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("wal-gc-dry-run");
        let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
            .in_memory(namespace.to_string())
            .build_with_executor(Arc::clone(&executor))
            .await?;

        let wal_local_fs = Arc::new(LocalFs {});
        let wal_dyn_fs: Arc<dyn DynFs> = wal_local_fs.clone();
        let wal_cas: Arc<dyn FsCas> = wal_local_fs.clone();
        let wal_dir = temp_root.join("wal");
        fs::create_dir_all(&wal_dir)?;
        let wal_path = Path::from_filesystem_path(&wal_dir)?;

        let mut wal_cfg = RuntimeWalConfig::default();
        wal_cfg.dir = wal_path;
        wal_cfg.segment_backend = wal_dyn_fs;
        wal_cfg.state_store = Some(Arc::new(FsWalStateStore::new(wal_cas)));
        wal_cfg.segment_max_bytes = 1;
        wal_cfg.prune_dry_run = true;

        db.enable_wal(wal_cfg.clone()).await?;
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

        let sst_dir = temp_root.join("sst");
        fs::create_dir_all(&sst_dir)?;
        let sst_root = Path::from_filesystem_path(&sst_dir)?;
        let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));

        for idx in 0..4 {
            let rows = vec![vec![
                Some(DynCell::Str(format!("dry-{idx}").into())),
                Some(DynCell::I32(idx as i32)),
            ]];
            let batch = build_batch(schema.clone(), rows)?;
            db.ingest(batch).await?;
        }
        let descriptor_a = SsTableDescriptor::new(SsTableId::new(991), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_a)
            .await?;
        let before = wal_segment_paths(&wal_dir);

        for idx in 4..8 {
            let rows = vec![vec![
                Some(DynCell::Str(format!("dry-{idx}").into())),
                Some(DynCell::I32(idx as i32)),
            ]];
            let batch = build_batch(schema.clone(), rows)?;
            db.ingest(batch).await?;
        }
        let descriptor_b = SsTableDescriptor::new(SsTableId::new(992), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor_b)
            .await?;
        let after = wal_segment_paths(&wal_dir);
        assert!(
            after.len() >= before.len(),
            "dry-run pruning should not delete WAL segments"
        );

        if let Some(handle) = db.wal().cloned() {
            let metrics = handle.metrics();
            let guard = metrics.read().await;
            assert_eq!(guard.wal_segments_pruned, 0);
            assert!(
                guard.wal_prune_dry_runs > 0,
                "dry-run pruning should report would-be deletions"
            );
        }

        db.disable_wal().await?;
        fs::remove_dir_all(&temp_root)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_records_manifest_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let temp_root = workspace_temp_dir("wal-manifest-metadata");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let mode_config = crate::schema::SchemaBuilder::from_schema(schema)
            .primary_key("id")
            .with_metadata()
            .build()
            .expect("key field");
        let schema = Arc::clone(&mode_config.schema);
        let executor = Arc::new(TokioExecutor::default());
        let namespace = temp_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("wal-manifest-metadata");
        let mut db: DB<DynMode, TokioExecutor> = DB::<DynMode, TokioExecutor>::builder(mode_config)
            .in_memory(namespace.to_string())
            .build_with_executor(Arc::clone(&executor))
            .await?;

        let wal_local_fs = Arc::new(LocalFs {});
        let wal_dyn_fs: Arc<dyn DynFs> = wal_local_fs.clone();
        let wal_cas: Arc<dyn FsCas> = wal_local_fs.clone();
        let wal_dir = temp_root.join("wal");
        fs::create_dir_all(&wal_dir)?;
        let wal_path = Path::from_filesystem_path(&wal_dir)?;

        let mut wal_cfg = RuntimeWalConfig::default();
        wal_cfg.dir = wal_path;
        wal_cfg.segment_backend = wal_dyn_fs;
        wal_cfg.state_store = Some(Arc::new(FsWalStateStore::new(wal_cas)));
        wal_cfg.segment_max_bytes = 1;
        wal_cfg.flush_interval = Duration::from_millis(1);
        wal_cfg.sync = WalSyncPolicy::Disabled;

        db.enable_wal(wal_cfg.clone()).await?;
        db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

        let rows = vec![
            vec![Some(DynCell::Str("alpha".into())), Some(DynCell::I32(7))],
            vec![Some(DynCell::Str("beta".into())), Some(DynCell::I32(9))],
        ];
        let batch = build_batch(schema.clone(), rows)?;
        db.ingest(batch).await?;
        assert!(db.num_immutable_segments() >= 1);

        let sst_dir = temp_root.join("sst");
        fs::create_dir_all(&sst_dir)?;
        let sst_root = Path::from_filesystem_path(&sst_dir)?;
        let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sst_cfg = Arc::new(SsTableConfig::new(schema.clone(), sst_fs, sst_root));
        let descriptor = SsTableDescriptor::new(SsTableId::new(555), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor.clone())
            .await?;

        let snapshot = db.manifest.snapshot_latest(db.manifest_table).await?;
        let latest = snapshot
            .latest_version
            .expect("latest version should exist after flush");
        assert!(
            !latest.wal_segments().is_empty(),
            "manifest should track wal segments for the version"
        );
        assert!(
            latest.wal_floor().is_some(),
            "wal floor should be derived from recorded segments"
        );
        let recorded = &latest.ssts()[0][0];
        let stats = recorded.stats().expect("sst stats should be recorded");
        assert_eq!(stats.rows, 2);
        assert!(stats.min_key.is_some() && stats.max_key.is_some());
        assert!(stats.min_commit_ts.is_some() && stats.max_commit_ts.is_some());
        let watermark = latest
            .tombstone_watermark()
            .expect("tombstone watermark should be populated");
        assert_eq!(
            watermark,
            stats
                .max_commit_ts
                .expect("max commit timestamp should be recorded")
                .get()
        );

        if let Some(handle) = db.wal().cloned() {
            let metrics = handle.metrics();
            let guard = metrics.read().await;
            assert!(guard.wal_floor_advancements >= 1);
        }

        db.disable_wal().await?;
        fs::remove_dir_all(&temp_root)?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dynamic_composite_from_field_ordinals_and_scan() {
        use std::collections::HashMap;
        // Fields: id (Utf8, ord 1), ts (Int64, ord 2), v (Int32)
        let mut m1 = HashMap::new();
        m1.insert("tonbo.key".to_string(), "1".to_string());
        let mut m2 = HashMap::new();
        m2.insert("tonbo.key".to_string(), "2".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
        let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
        let db: DB<DynMode, BlockingExecutor> = DB::new(config, Arc::new(BlockingExecutor))
            .await
            .expect("composite field metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        db.ingest(batch).await.expect("insert batch");

        let pred = Predicate::and(vec![
            Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("a")),
            Predicate::and(vec![
                Predicate::gte(ColumnRef::new("ts", None), ScalarValue::from(5i64)),
                Predicate::lte(ColumnRef::new("ts", None), ScalarValue::from(10i64)),
            ]),
        ]);
        let snapshot = db.begin_snapshot().await.expect("snapshot");
        let plan = snapshot
            .plan_scan(&db, &pred, None, None)
            .await
            .expect("plan");
        let batches = db
            .execute_scan(plan)
            .await
            .expect("exec")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect");
        let got: Vec<(String, i64)> = batches
            .into_iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id col");
                let ts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("ts col");
                ids.iter()
                    .zip(ts.iter())
                    .filter_map(|(id, t)| Some((id?.to_string(), t?)))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dynamic_composite_from_schema_list_and_scan() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_ts = Field::new("ts", DataType::Int64, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "[\"id\", \"ts\"]".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]).with_metadata(sm));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
        let db: DB<DynMode, BlockingExecutor> = DB::new(config, Arc::new(BlockingExecutor))
            .await
            .expect("composite schema metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        db.ingest(batch).await.expect("insert batch");

        let pred = Predicate::and(vec![
            Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("a")),
            Predicate::and(vec![
                Predicate::gte(ColumnRef::new("ts", None), ScalarValue::from(1i64)),
                Predicate::lte(ColumnRef::new("ts", None), ScalarValue::from(10i64)),
            ]),
        ]);
        let snapshot = db.begin_snapshot().await.expect("snapshot");
        let batches = db
            .execute_scan(
                snapshot
                    .plan_scan(&db, &pred, None, None)
                    .await
                    .expect("plan"),
            )
            .await
            .expect("exec")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect");
        let got: Vec<(String, i64)> = batches
            .into_iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id col");
                let ts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("ts col");
                ids.iter()
                    .zip(ts.iter())
                    .filter_map(|(id, t)| Some((id?.to_string(), t?)))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recover_replays_commit_timestamps_and_advances_clock() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let clock_nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(delta) => delta.as_nanos(),
            Err(err) => panic!("system clock before unix epoch: {err}"),
        };
        let wal_dir = std::env::temp_dir().join(format!("tonbo-replay-test-{clock_nanos}"));
        fs::create_dir_all(&wal_dir).expect("create wal dir");

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let delete_schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let delete_batch = RecordBatch::try_new(
            delete_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![42])) as ArrayRef,
            ],
        )
        .expect("delete batch");

        let mut frames = Vec::new();
        frames.extend(
            encode_command(WalCommand::TxnAppend {
                provisional_id: 7,
                payload: DynBatchPayload::Delete {
                    batch: delete_batch.clone(),
                },
            })
            .expect("encode delete"),
        );
        frames.extend(
            encode_command(WalCommand::TxnCommit {
                provisional_id: 7,
                commit_ts: Timestamp::new(42),
            })
            .expect("encode commit"),
        );

        let mut seq = INITIAL_FRAME_SEQ;
        let mut bytes = Vec::new();
        for frame in frames {
            bytes.extend_from_slice(&frame.into_bytes(seq));
            seq += 1;
        }
        fs::write(wal_dir.join("wal-00000000000000000001.tonwal"), bytes).expect("write wal");

        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let mut cfg = RuntimeWalConfig::default();
        cfg.dir = fusio::path::Path::from_filesystem_path(&wal_dir).expect("wal fusio path");
        let executor = Arc::new(BlockingExecutor);
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let table_definition = DynMode::table_definition(&config, builder::DEFAULT_TABLE_NAME);
        let manifest = init_in_memory_manifest().await.expect("init manifest");
        let file_ids = FileIdGenerator::default();
        let manifest_table = manifest
            .register_table(&file_ids, &table_definition)
            .await
            .expect("register table")
            .table_id;
        let db: DB<DynMode, BlockingExecutor> = DB::recover_with_wal_with_manifest(
            config,
            executor.clone(),
            cfg,
            manifest,
            manifest_table,
            file_ids,
        )
        .await
        .expect("recover");

        // Replayed version retains commit_ts 42 and tombstone state.
        let chain = db
            .mem_read()
            .inspect_versions(&KeyOwned::from("k"))
            .expect("chain");
        assert_eq!(chain, vec![(Timestamp::new(42), true)]);

        let pred = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("k"));
        let snapshot = db
            .begin_snapshot_at(Timestamp::new(50))
            .await
            .expect("snapshot");
        let plan = snapshot
            .plan_scan(&db, &pred, None, None)
            .await
            .expect("plan");
        let visible_rows: usize = db
            .execute_scan(plan)
            .await
            .expect("execute")
            .try_fold(
                0usize,
                |acc, batch| async move { Ok(acc + batch.num_rows()) },
            )
            .await
            .expect("fold");
        assert_eq!(visible_rows, 0);

        // New ingest should advance to > 42 (next clock tick).
        let new_batch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .expect("batch2");
        db.ingest(new_batch).await.expect("ingest new");

        let chain = db
            .mem_read()
            .inspect_versions(&KeyOwned::from("k"))
            .expect("chain");
        assert_eq!(
            chain,
            vec![(Timestamp::new(42), true), (Timestamp::new(43), false)]
        );

        fs::remove_dir_all(&wal_dir).expect("cleanup");
    }

    fn workspace_temp_dir(prefix: &str) -> PathBuf {
        let base = std::env::current_dir().expect("cwd");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = base
            .join("target")
            .join("tmp")
            .join(format!("{prefix}-{unique}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn wal_segment_paths(dir: &std::path::Path) -> Vec<PathBuf> {
        if !dir.exists() {
            return Vec::new();
        }
        let mut files = Vec::new();
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("tonwal") {
                    files.push(path);
                }
            }
        }
        files.sort();
        files
    }
}
