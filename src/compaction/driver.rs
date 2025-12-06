//! Compaction driver for orchestrating compaction operations.
//!
//! The driver owns manifest access and WAL configuration, coordinates planning,
//! execution, and reconciliation without requiring the full DB type.

use std::{sync::Arc, time::Duration};

use fusio::{
    dynamic::{MaybeSend, MaybeSync},
    executor::{Executor, RwLock, Timer},
};
use futures::future::AbortHandle;

use crate::{
    compaction::{
        executor::{CompactionError, CompactionExecutor, CompactionJob, CompactionOutcome},
        handle::CompactionHandle,
        orchestrator,
        planner::{CompactionPlanner, CompactionTask},
        scheduler::{CompactionScheduler, ScheduledCompaction},
    },
    manifest::{ManifestError, ManifestFs, ManifestResult, TableId, TonboManifest, WalSegmentRef},
    ondisk::sstable::SsTableConfig,
    wal::{WalConfig as RuntimeWalConfig, WalHandle, manifest_ext},
};

/// Maximum retries for CAS conflicts when applying compaction edits.
const MAX_COMPACTION_APPLY_RETRIES: usize = 2;

/// Compaction driver that coordinates planning, execution, and manifest updates.
///
/// Unlike the previous design, this driver does not carry a phantom `M` generic.
/// It only needs filesystem and executor types to interact with manifest and WAL.
pub struct CompactionDriver<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    pub(crate) manifest: TonboManifest<FS, E>,
    pub(crate) table_id: TableId,
    pub(crate) wal_config: Option<RuntimeWalConfig>,
    pub(crate) wal_handle: Option<WalHandle<E>>,
}

impl<FS, E> CompactionDriver<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Create a new compaction driver with the given manifest and configuration.
    pub fn new(
        manifest: TonboManifest<FS, E>,
        table_id: TableId,
        wal_config: Option<RuntimeWalConfig>,
        wal_handle: Option<WalHandle<E>>,
    ) -> Self {
        Self {
            manifest,
            table_id,
            wal_config,
            wal_handle,
        }
    }

    /// Remove WAL segments whose sequence is older than the manifest floor.
    pub async fn prune_wal_below_floor(&self) {
        let Some(cfg) = self.wal_config.as_ref() else {
            return;
        };
        let Ok(Some(floor)) = self.manifest.wal_floor(self.table_id).await else {
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

    /// Get the WAL floor currently recorded in the manifest.
    pub async fn manifest_wal_floor(&self) -> Option<WalSegmentRef> {
        self.manifest.wal_floor(self.table_id).await.ok().flatten()
    }

    /// Sequence number of the WAL floor currently recorded in the manifest.
    #[cfg(test)]
    pub async fn wal_floor_seq(&self) -> Option<u64> {
        self.manifest_wal_floor().await.map(|ref_| ref_.seq())
    }

    /// Build a compaction plan based on the latest manifest snapshot.
    pub async fn plan_compaction_task<P>(
        &self,
        planner: &P,
    ) -> ManifestResult<Option<CompactionTask>>
    where
        P: CompactionPlanner,
    {
        let snapshot = self.manifest.snapshot_latest(self.table_id).await?;
        let version = match snapshot.latest_version {
            Some(ref state) => state,
            None => return Ok(None),
        };
        Ok(orchestrator::plan_from_version(planner, version))
    }

    /// End-to-end compaction orchestrator (plan -> resolve -> execute -> apply manifest).
    #[cfg(test)]
    pub async fn run_compaction<CE, P>(
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
                .snapshot_latest(self.table_id)
                .await
                .map_err(CompactionError::Manifest)?;
            let version = match snapshot.latest_version {
                Some(ref state) => state,
                None => return Ok(None),
            };
            let expected_head = snapshot.head.last_manifest_txn;
            let existing_wal_segments: Vec<WalSegmentRef> = version.wal_segments().to_vec();

            let Some(task) = orchestrator::plan_from_version(planner, version) else {
                return Ok(None);
            };

            let inputs = orchestrator::resolve_inputs(version, &task)?;
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
            orchestrator::reconcile_wal_segments(
                version,
                &mut outcome,
                &existing_wal_segments,
                wal_floor,
            );
            let gc_plan = orchestrator::gc_plan_from_outcome(&outcome)?;
            let edits = outcome.to_version_edits();
            if edits.is_empty() {
                return Ok(Some(outcome));
            }
            match self
                .manifest
                .apply_version_edits_cas(self.table_id, expected_head, &edits)
                .await
            {
                Ok(_) => {
                    self.prune_wal_below_floor().await;
                    if let Some(plan) = gc_plan {
                        self.manifest
                            .record_gc_plan(self.table_id, plan)
                            .await
                            .map_err(CompactionError::Manifest)?;
                    }
                    return Ok(Some(outcome));
                }
                Err(ManifestError::CasConflict(_)) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    if attempts >= MAX_COMPACTION_APPLY_RETRIES {
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

    /// Execute a pre-scheduled compaction task with lease validation.
    pub async fn run_scheduled_compaction<CE>(
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
                .snapshot_latest(self.table_id)
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
            let inputs = orchestrator::resolve_inputs(version, &scheduled.task)?;
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
            orchestrator::reconcile_wal_segments(
                version,
                &mut outcome,
                &existing_wal_segments,
                wal_floor,
            );
            let gc_plan = orchestrator::gc_plan_from_outcome(&outcome)?;
            let edits = outcome.to_version_edits();
            if edits.is_empty() {
                return Ok(Some(outcome));
            }
            let expected_head = scheduled.manifest_head.or(snapshot.head.last_manifest_txn);
            match self
                .manifest
                .apply_version_edits_cas(self.table_id, expected_head, &edits)
                .await
            {
                Ok(_) => {
                    self.prune_wal_below_floor().await;
                    if let Some(plan) = gc_plan {
                        self.manifest
                            .record_gc_plan(self.table_id, plan)
                            .await
                            .map_err(CompactionError::Manifest)?;
                    }
                    return Ok(Some(outcome));
                }
                Err(ManifestError::CasConflict(_)) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    if attempts >= MAX_COMPACTION_APPLY_RETRIES {
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

    /// Spawn a background compaction worker that plans and executes compactions.
    pub fn spawn_worker<CE, P>(
        self: &Arc<Self>,
        runtime: Arc<E>,
        planner: P,
        executor: CE,
        _gc_sst_config: Option<Arc<SsTableConfig>>,
        interval: Duration,
        budget: usize,
    ) -> CompactionHandle<E>
    where
        CE: CompactionExecutor + MaybeSend + MaybeSync + 'static,
        P: CompactionPlanner + MaybeSend + MaybeSync + 'static,
    {
        let (scheduler, mut rx) = CompactionScheduler::new(budget.max(1), budget.max(1));
        let driver = Arc::clone(self);
        let driver_for_loop = Arc::clone(&driver);
        let executor = Arc::new(executor);
        let runtime_for_loop = Arc::clone(&runtime);
        let (abort, reg) = AbortHandle::new_pair();
        let loop_future = async move {
            let runtime = runtime_for_loop;
            loop {
                runtime.sleep(interval).await;
                for _ in 0..budget.max(1) {
                    match driver_for_loop.plan_compaction_task(&planner).await {
                        Ok(Some(task)) => {
                            let manifest_head = match driver_for_loop
                                .manifest
                                .snapshot_latest(driver_for_loop.table_id)
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
        CompactionHandle::new(abort, Some(handle))
    }
}
