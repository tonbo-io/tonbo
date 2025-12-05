#[cfg(test)]
use std::pin::Pin;
use std::{collections::HashSet, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};

#[cfg(test)]
use fusio::dynamic::MaybeSendFuture;
use fusio::{
    dynamic::{MaybeSend, MaybeSync},
    executor::{Executor, RwLock, Timer},
};
use futures::future::AbortHandle;

#[cfg(test)]
use crate::compaction::CompactionHost;
use crate::{
    compaction::{
        executor::{CompactionError, CompactionExecutor, CompactionJob, CompactionOutcome},
        planner::{CompactionInput, CompactionPlanner, CompactionSnapshot},
        scheduler::{CompactionScheduler, ScheduledCompaction},
    },
    db::DB,
    id::FileId,
    manifest::{
        GcPlanState, GcSstRef, ManifestError, ManifestResult, TableId, TonboManifest, VersionState,
        WalSegmentRef,
    },
    mode::Mode,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableId},
    wal::{WalConfig as RuntimeWalConfig, WalHandle, manifest_ext},
};

pub(crate) struct CompactionLoopHandle<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    _driver: Arc<CompactionDriver<M, E>>,
    pub(crate) abort: AbortHandle,
    _handle: Option<<E as Executor>::JoinHandle<()>>,
}

impl<M, E> Drop for CompactionLoopHandle<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(crate) struct CompactionDriver<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    pub(crate) manifest: TonboManifest,
    pub(crate) manifest_table: TableId,
    pub(crate) wal_config: Option<RuntimeWalConfig>,
    pub(crate) wal_handle: Option<WalHandle<E>>,
    _marker: PhantomData<M>,
}

impl<M, E> CompactionDriver<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    pub(crate) fn new(db: &DB<M, E>) -> Self {
        Self {
            manifest: db.manifest.clone(),
            manifest_table: db.manifest_table,
            wal_config: db.wal_config.clone(),
            wal_handle: db.wal_handle().cloned(),
            _marker: PhantomData,
        }
    }

    pub(crate) async fn prune_wal_segments_below_floor(&self) {
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

    pub(crate) async fn manifest_wal_floor(&self) -> Option<WalSegmentRef> {
        self.manifest
            .wal_floor(self.manifest_table)
            .await
            .ok()
            .flatten()
    }

    pub(crate) async fn wal_floor_seq(&self) -> Option<u64> {
        self.manifest_wal_floor().await.map(|ref_| ref_.seq())
    }

    pub(crate) async fn plan_compaction_task<P>(
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

    #[cfg(test)]
    pub(crate) async fn run_compaction_task<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
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
                lease: None,
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

    pub(crate) async fn run_scheduled_compaction<CE>(
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
    pub(crate) fn spawn_compaction_worker_local<CE, P>(
        self: &Arc<Self>,
        runtime: Arc<E>,
        planner: P,
        executor: CE,
        gc_sst_config: Option<Arc<SsTableConfig>>,
        interval: Duration,
        budget: usize,
    ) -> CompactionLoopHandle<M, E>
    where
        CE: CompactionExecutor + MaybeSend + MaybeSync + 'static,
        P: CompactionPlanner + MaybeSend + MaybeSync + 'static,
        M: Mode + MaybeSend + MaybeSync + 'static,
        M::Key: Eq + Hash + Clone + MaybeSend + MaybeSync,
        E: Executor + Timer + 'static,
    {
        let (scheduler, mut rx) = CompactionScheduler::new(budget.max(1), budget.max(1));
        let driver = Arc::clone(self);
        let driver_for_handle = Arc::clone(&driver);
        let driver_for_loop = Arc::clone(&driver);
        let executor = Arc::new(executor);
        let _ = gc_sst_config;
        let runtime_for_loop = Arc::clone(&runtime);
        let (abort, reg) = futures::future::AbortHandle::new_pair();
        let loop_future = async move {
            let runtime = runtime_for_loop;
            loop {
                runtime.sleep(interval).await;
                for _ in 0..budget.max(1) {
                    match driver_for_loop.plan_compaction_task(&planner).await {
                        Ok(Some(task)) => {
                            let manifest_head = match driver_for_loop
                                .manifest
                                .snapshot_latest(driver_for_loop.manifest_table)
                                .await
                            {
                                Ok(snapshot) => snapshot.head.last_manifest_txn,
                                Err(err) => {
                                    eprintln!("compaction snapshot failed: {err}");
                                    continue;
                                }
                            };
                            if let Err(err) = scheduler
                                .enqueue(
                                    task,
                                    manifest_head,
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
        };
        let abortable = futures::future::Abortable::new(loop_future, reg);
        let handle = runtime.spawn(async move {
            let _ = abortable.await;
        });
        CompactionLoopHandle {
            _driver: driver_for_handle,
            abort,
            _handle: Some(handle),
        }
    }
}

#[cfg(test)]
impl<M, E> CompactionHost<M, E> for CompactionDriver<M, E>
where
    M: Mode + MaybeSend + MaybeSync,
    M::Key: Eq + Hash + Clone + MaybeSend + MaybeSync,
    E: Executor + Timer,
{
    fn compact_once<'a, CE, P>(
        &'a self,
        planner: &'a P,
        executor: &'a CE,
    ) -> Pin<
        Box<dyn MaybeSendFuture<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>,
    >
    where
        CE: CompactionExecutor + MaybeSend + MaybeSync + 'a,
        P: CompactionPlanner + MaybeSend + MaybeSync + 'a,
    {
        Box::pin(self.run_compaction_task(planner, executor))
    }
}

impl<M, E> DB<M, E>
where
    M: Mode,
    M::Key: Eq + Hash + Clone,
    E: Executor + Timer,
{
    pub(crate) fn compaction_driver(&self) -> CompactionDriver<M, E> {
        CompactionDriver::new(self)
    }

    /// Whether a background compaction worker was spawned for this DB.
    pub fn has_compaction_worker(&self) -> bool {
        self.compaction_worker.is_some()
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

    /// Sequence number of the WAL floor currently recorded in the manifest.
    pub async fn wal_floor_seq(&self) -> Option<u64> {
        self.compaction_driver().wal_floor_seq().await
    }

    pub(crate) const MAX_COMPACTION_APPLY_RETRIES: usize = 2;

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

    pub(crate) fn wal_ids_for_remaining_ssts(
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

    pub(crate) fn wal_segments_after_compaction(
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

    /// End-to-end compaction orchestrator (plan -> resolve -> execute -> apply manifest).
    #[cfg(test)]
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

    pub(crate) fn gc_plan_from_outcome(
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
}

#[cfg(test)]
impl<M, E> CompactionHost<M, E> for DB<M, E>
where
    M: Mode + MaybeSend + MaybeSync,
    M::Key: Eq + Hash + Clone + MaybeSend + MaybeSync,
    E: Executor + Timer,
{
    fn compact_once<'a, CE, P>(
        &'a self,
        planner: &'a P,
        executor: &'a CE,
    ) -> Pin<
        Box<dyn MaybeSendFuture<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>,
    >
    where
        CE: CompactionExecutor + MaybeSend + MaybeSync + 'a,
        P: CompactionPlanner + MaybeSend + MaybeSync + 'a,
    {
        let driver = self.compaction_driver();
        Box::pin(async move { driver.run_compaction_task(planner, executor).await })
    }
}
