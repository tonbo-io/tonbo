//! Compaction driver for orchestrating compaction operations.
//!
//! The driver owns manifest access and WAL configuration, coordinates planning,
//! execution, and reconciliation without requiring the full DB type.

use std::{sync::Arc, time::Duration};

use fusio::{
    dynamic::{MaybeSend, MaybeSync},
    executor::{Executor, Instant, RwLock, Timer},
};
use futures::{FutureExt, StreamExt, channel::mpsc, future::AbortHandle, lock::Mutex};

use crate::{
    compaction::{
        executor::{CompactionError, CompactionExecutor, CompactionJob, CompactionOutcome},
        handle::{CompactionHandle, CompactionTrigger},
        metrics::{
            CompactionCascadeDecision, CompactionIoStats, CompactionJobSnapshot, CompactionMetrics,
            CompactionQueueDropContext, CompactionQueueDropReason, CompactionTriggerReason,
        },
        orchestrator,
        planner::{CompactionPlanner, CompactionTask},
        scheduler::{CompactionScheduleError, CompactionScheduler, ScheduledCompaction},
    },
    db::{CasBackoffConfig, CascadeConfig},
    manifest::{ManifestError, ManifestFs, ManifestResult, TableId, TonboManifest, WalSegmentRef},
    wal::{WalConfig as RuntimeWalConfig, WalHandle, manifest_ext},
};

/// Maximum retries for CAS conflicts when applying compaction edits.
const MAX_COMPACTION_APPLY_RETRIES: usize = 2;
const DEFAULT_COMPACTION_LEASE_TTL_MS: u64 = 30_000;

#[derive(Debug)]
struct CascadeControl {
    max_follow_ups: usize,
    cooldown: Duration,
    remaining: usize,
    last_cascade_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
enum CascadeDecision {
    Allowed,
    BudgetExhausted,
    CooldownActive,
}

impl CascadeControl {
    fn new(max_follow_ups: usize, cooldown: Duration) -> Self {
        Self {
            max_follow_ups,
            cooldown,
            remaining: max_follow_ups,
            last_cascade_at: None,
        }
    }

    fn reset_budget(&mut self) {
        self.remaining = self.max_follow_ups;
    }

    fn try_acquire(&mut self, now: Instant) -> CascadeDecision {
        if self.remaining == 0 {
            return CascadeDecision::BudgetExhausted;
        }
        if let Some(last) = self.last_cascade_at
            && now.duration_since(last) < self.cooldown
        {
            return CascadeDecision::CooldownActive;
        }
        self.remaining = self.remaining.saturating_sub(1);
        self.last_cascade_at = Some(now);
        CascadeDecision::Allowed
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fusio::executor::Instant;

    use super::{CascadeControl, CascadeDecision};

    #[test]
    fn cascade_control_enforces_budget_and_cooldown() {
        let mut control = CascadeControl::new(1, Duration::from_millis(5));
        let now = Instant::now();
        assert!(matches!(control.try_acquire(now), CascadeDecision::Allowed));
        assert!(matches!(
            control.try_acquire(now),
            CascadeDecision::BudgetExhausted
        ));

        control.reset_budget();
        assert!(matches!(
            control.try_acquire(now),
            CascadeDecision::CooldownActive
        ));

        let later = now + Duration::from_millis(10);
        assert!(matches!(
            control.try_acquire(later),
            CascadeDecision::Allowed
        ));
    }
}

/// Compaction driver that coordinates planning, execution, and manifest updates.
///
/// Unlike the previous design, this driver does not carry a phantom `M` generic.
/// It only needs filesystem and executor types to interact with manifest and WAL.
pub(crate) struct CompactionDriver<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    pub(crate) manifest: TonboManifest<FS, E>,
    pub(crate) table_id: TableId,
    pub(crate) wal_config: Option<RuntimeWalConfig>,
    pub(crate) wal_handle: Option<WalHandle<E>>,
    pub(crate) runtime: Arc<E>,
    pub(crate) cas_backoff: CasBackoffConfig,
    compaction_metrics: Option<Arc<CompactionMetrics>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompactionWorkerConfig {
    pub(crate) periodic_interval: Option<Duration>,
    pub(crate) queue_capacity: usize,
    pub(crate) max_concurrent_jobs: usize,
    pub(crate) cascade: CascadeConfig,
}

impl CompactionWorkerConfig {
    pub(crate) fn new(
        periodic_interval: Option<Duration>,
        queue_capacity: usize,
        max_concurrent_jobs: usize,
        cascade: CascadeConfig,
    ) -> Self {
        Self {
            periodic_interval,
            queue_capacity,
            max_concurrent_jobs,
            cascade,
        }
    }
}

impl<FS, E> CompactionDriver<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Create a new compaction driver with the given manifest and configuration.
    pub(crate) fn new(
        manifest: TonboManifest<FS, E>,
        table_id: TableId,
        wal_config: Option<RuntimeWalConfig>,
        wal_handle: Option<WalHandle<E>>,
        runtime: Arc<E>,
        cas_backoff: CasBackoffConfig,
        compaction_metrics: Option<Arc<CompactionMetrics>>,
    ) -> Self {
        Self {
            manifest,
            table_id,
            wal_config,
            wal_handle,
            runtime,
            cas_backoff,
            compaction_metrics,
        }
    }

    /// Remove WAL segments whose sequence is older than the manifest floor.
    pub(crate) async fn prune_wal_below_floor(&self) {
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
    async fn manifest_wal_floor(&self) -> Option<WalSegmentRef> {
        self.manifest.wal_floor(self.table_id).await.ok().flatten()
    }

    /// Sequence number of the WAL floor currently recorded in the manifest.
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) async fn wal_floor_seq(&self) -> Option<u64> {
        self.manifest_wal_floor().await.map(|ref_| ref_.seq())
    }

    /// Build a compaction plan based on the latest manifest snapshot.
    pub(crate) async fn plan_compaction_task<P>(
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

    /// Build a compaction plan starting from a minimum source level.
    pub(crate) async fn plan_compaction_task_from_level<P>(
        &self,
        planner: &P,
        min_level: usize,
    ) -> ManifestResult<Option<CompactionTask>>
    where
        P: CompactionPlanner,
    {
        let snapshot = self.manifest.snapshot_latest(self.table_id).await?;
        let version = match snapshot.latest_version {
            Some(ref state) => state,
            None => return Ok(None),
        };
        Ok(orchestrator::plan_from_version_with_min_level(
            planner, version, min_level,
        ))
    }

    fn cas_backoff_delay(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(0);
        }
        let mut delay = self.cas_backoff.base_delay();
        for _ in 1..attempt {
            delay = delay.saturating_mul(2);
        }
        let max_delay = self.cas_backoff.max_delay();
        if delay > max_delay { max_delay } else { delay }
    }

    async fn sleep_cas_backoff(&self, attempt: usize) {
        let delay = self.cas_backoff_delay(attempt);
        if !delay.is_zero() {
            self.runtime.sleep(delay).await;
        }
    }

    fn output_stats(outcome: &CompactionOutcome) -> (usize, CompactionIoStats) {
        if !outcome.outputs.is_empty() {
            (
                outcome.outputs.len(),
                CompactionIoStats::from_descriptors(&outcome.outputs),
            )
        } else if !outcome.add_ssts.is_empty() {
            (
                outcome.add_ssts.len(),
                CompactionIoStats::from_entries(&outcome.add_ssts),
            )
        } else {
            (
                0,
                CompactionIoStats {
                    bytes: 0,
                    rows: 0,
                    tombstones: 0,
                    complete: true,
                },
            )
        }
    }

    fn record_job_success(&self, job: CompactionJobSnapshot) {
        if let Some(metrics) = self.compaction_metrics.as_ref() {
            metrics.record_job_success(job);
        }
    }

    fn record_job_abort(&self, job: CompactionJobSnapshot) {
        if let Some(metrics) = self.compaction_metrics.as_ref() {
            metrics.record_job_abort(job);
        }
    }

    /// End-to-end compaction orchestrator (plan -> resolve -> execute -> apply manifest).
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) async fn run_compaction<CE, P>(
        &self,
        planner: &P,
        executor: &CE,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        let mut attempts = 0usize;
        let mut cas_retries = 0usize;
        let mut job_started_at: Option<Instant> = None;
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
            let input_stats = CompactionIoStats::from_descriptors(&inputs);
            let input_count = inputs.len();
            let source_level = task.source_level;
            let target_level = task.target_level;
            if job_started_at.is_none() {
                job_started_at = Some(self.runtime.now());
            }
            let obsolete_ids = inputs.iter().map(|d| d.id().clone()).collect();
            let wal_floor = self.manifest_wal_floor().await;
            let job = CompactionJob {
                task,
                inputs,
                lease: None,
            };

            let outcome = match executor.execute(job).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                    let duration = self.runtime.now().duration_since(started_at);
                    self.record_job_abort(CompactionJobSnapshot {
                        source_level,
                        target_level,
                        input_sst_count: input_count,
                        output_sst_count: 0,
                        input: input_stats,
                        output: CompactionIoStats::default(),
                        duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                        cas_retries: cas_retries as u64,
                        cas_aborted: false,
                    });
                    return Err(err);
                }
            };
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
                let (output_count, output_stats) = Self::output_stats(&outcome);
                let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                let duration = self.runtime.now().duration_since(started_at);
                self.record_job_success(CompactionJobSnapshot {
                    source_level,
                    target_level,
                    input_sst_count: input_count,
                    output_sst_count: output_count,
                    input: input_stats,
                    output: output_stats,
                    duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                    cas_retries: cas_retries as u64,
                    cas_aborted: false,
                });
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
                    let (output_count, output_stats) = Self::output_stats(&outcome);
                    let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                    let duration = self.runtime.now().duration_since(started_at);
                    self.record_job_success(CompactionJobSnapshot {
                        source_level,
                        target_level,
                        input_sst_count: input_count,
                        output_sst_count: output_count,
                        input: input_stats,
                        output: output_stats,
                        duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                        cas_retries: cas_retries as u64,
                        cas_aborted: false,
                    });
                    return Ok(Some(outcome));
                }
                Err(ManifestError::CasConflict(_)) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    cas_retries = cas_retries.saturating_add(1);
                    if attempts >= MAX_COMPACTION_APPLY_RETRIES {
                        let (output_count, output_stats) = Self::output_stats(&outcome);
                        let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                        let duration = self.runtime.now().duration_since(started_at);
                        self.record_job_abort(CompactionJobSnapshot {
                            source_level,
                            target_level,
                            input_sst_count: input_count,
                            output_sst_count: output_count,
                            input: input_stats,
                            output: output_stats,
                            duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                            cas_retries: cas_retries as u64,
                            cas_aborted: true,
                        });
                        return Err(CompactionError::CasConflict);
                    }
                    self.sleep_cas_backoff(attempts).await;
                    continue;
                }
                Err(err) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    let (output_count, output_stats) = Self::output_stats(&outcome);
                    let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                    let duration = self.runtime.now().duration_since(started_at);
                    self.record_job_abort(CompactionJobSnapshot {
                        source_level,
                        target_level,
                        input_sst_count: input_count,
                        output_sst_count: output_count,
                        input: input_stats,
                        output: output_stats,
                        duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                        cas_retries: cas_retries as u64,
                        cas_aborted: false,
                    });
                    return Err(CompactionError::Manifest(err));
                }
            }
        }
    }

    /// Execute a pre-scheduled compaction task with lease validation.
    async fn run_scheduled_compaction<CE>(
        &self,
        scheduled: ScheduledCompaction,
        executor: &CE,
    ) -> Result<Option<CompactionOutcome>, CompactionError>
    where
        CE: CompactionExecutor,
    {
        let mut attempts = 0usize;
        let mut cas_retries = 0usize;
        let mut job_started_at: Option<Instant> = None;
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
            let input_stats = CompactionIoStats::from_descriptors(&inputs);
            let input_count = inputs.len();
            let source_level = scheduled.task.source_level;
            let target_level = scheduled.task.target_level;
            if job_started_at.is_none() {
                job_started_at = Some(self.runtime.now());
            }
            let obsolete_ids = inputs.iter().map(|d| d.id().clone()).collect();
            let wal_floor = self.manifest_wal_floor().await;
            let job = CompactionJob {
                task: scheduled.task.clone(),
                inputs,
                lease: Some(scheduled.lease.clone()),
            };

            let outcome = match executor.execute(job).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                    let duration = self.runtime.now().duration_since(started_at);
                    self.record_job_abort(CompactionJobSnapshot {
                        source_level,
                        target_level,
                        input_sst_count: input_count,
                        output_sst_count: 0,
                        input: input_stats,
                        output: CompactionIoStats::default(),
                        duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                        cas_retries: cas_retries as u64,
                        cas_aborted: false,
                    });
                    return Err(err);
                }
            };
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
                let (output_count, output_stats) = Self::output_stats(&outcome);
                let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                let duration = self.runtime.now().duration_since(started_at);
                self.record_job_success(CompactionJobSnapshot {
                    source_level,
                    target_level,
                    input_sst_count: input_count,
                    output_sst_count: output_count,
                    input: input_stats,
                    output: output_stats,
                    duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                    cas_retries: cas_retries as u64,
                    cas_aborted: false,
                });
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
                    let (output_count, output_stats) = Self::output_stats(&outcome);
                    let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                    let duration = self.runtime.now().duration_since(started_at);
                    self.record_job_success(CompactionJobSnapshot {
                        source_level,
                        target_level,
                        input_sst_count: input_count,
                        output_sst_count: output_count,
                        input: input_stats,
                        output: output_stats,
                        duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                        cas_retries: cas_retries as u64,
                        cas_aborted: false,
                    });
                    return Ok(Some(outcome));
                }
                Err(ManifestError::CasConflict(_)) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    cas_retries = cas_retries.saturating_add(1);
                    if attempts >= MAX_COMPACTION_APPLY_RETRIES {
                        let (output_count, output_stats) = Self::output_stats(&outcome);
                        let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                        let duration = self.runtime.now().duration_since(started_at);
                        self.record_job_abort(CompactionJobSnapshot {
                            source_level,
                            target_level,
                            input_sst_count: input_count,
                            output_sst_count: output_count,
                            input: input_stats,
                            output: output_stats,
                            duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                            cas_retries: cas_retries as u64,
                            cas_aborted: true,
                        });
                        return Err(CompactionError::CasConflict);
                    }
                    self.sleep_cas_backoff(attempts).await;
                    continue;
                }
                Err(err) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    let (output_count, output_stats) = Self::output_stats(&outcome);
                    let started_at = job_started_at.unwrap_or_else(|| self.runtime.now());
                    let duration = self.runtime.now().duration_since(started_at);
                    self.record_job_abort(CompactionJobSnapshot {
                        source_level,
                        target_level,
                        input_sst_count: input_count,
                        output_sst_count: output_count,
                        input: input_stats,
                        output: output_stats,
                        duration_ms: duration.as_millis().try_into().unwrap_or(u64::MAX),
                        cas_retries: cas_retries as u64,
                        cas_aborted: false,
                    });
                    return Err(CompactionError::Manifest(err));
                }
            }
        }
    }

    /// Spawn a background compaction worker that plans and executes compactions.
    pub(crate) fn spawn_worker<CE, P>(
        self: &Arc<Self>,
        runtime: Arc<E>,
        planner: P,
        executor: CE,
        config: CompactionWorkerConfig,
    ) -> CompactionHandle<E>
    where
        CE: CompactionExecutor + MaybeSend + MaybeSync + 'static,
        P: CompactionPlanner + MaybeSend + MaybeSync + 'static,
    {
        let CompactionWorkerConfig {
            periodic_interval,
            queue_capacity,
            max_concurrent_jobs,
            cascade,
        } = config;
        let budget = max_concurrent_jobs.max(1);
        let queue_capacity = queue_capacity.max(budget).max(1);
        let (scheduler, mut rx) = CompactionScheduler::new(queue_capacity, budget);
        let (tick_tx, mut tick_rx) = mpsc::channel::<CompactionTrigger>(1);
        let planner = Arc::new(planner);
        let driver = Arc::clone(self);
        let driver_for_loop = Arc::clone(&driver);
        let executor = Arc::new(executor);
        let runtime_for_loop = Arc::clone(&runtime);
        let cascade_trigger = tick_tx.clone();
        let cascade_control = Arc::new(Mutex::new(CascadeControl::new(
            cascade.max_follow_ups(),
            cascade.cooldown(),
        )));
        let metrics = driver_for_loop.compaction_metrics.clone();
        let lease_ttl_ms = periodic_interval
            .map(|interval| interval.as_millis().max(1) as u64)
            .unwrap_or(DEFAULT_COMPACTION_LEASE_TTL_MS);
        let (abort, reg) = AbortHandle::new_pair();
        let loop_future = async move {
            let runtime = runtime_for_loop;
            let mut pending_tick = false;
            let mut pending_reason: Option<CompactionTriggerReason> = None;
            loop {
                if !pending_tick {
                    let interval = periodic_interval;
                    if let Some(interval) = interval {
                        let mut sleep = runtime.sleep(interval).fuse();
                        futures::select_biased! {
                            _ = sleep => {
                                pending_tick = true;
                                pending_reason = Some(CompactionTriggerReason::Periodic);
                            }
                            msg = tick_rx.next() => {
                                match msg {
                                    Some(_) => {
                                        pending_tick = true;
                                        pending_reason = Some(CompactionTriggerReason::Kick);
                                    }
                                    None => return,
                                }
                            }
                        }
                    } else {
                        match tick_rx.next().await {
                            Some(_) => {
                                pending_tick = true;
                                pending_reason = Some(CompactionTriggerReason::Kick);
                            }
                            None => return,
                        }
                    }
                }
                if !pending_tick {
                    continue;
                }
                pending_tick = false;
                if let Some(reason) = pending_reason.take()
                    && let Some(metrics) = metrics.as_ref()
                {
                    metrics.record_trigger(reason);
                }
                {
                    let mut guard = cascade_control.lock().await;
                    guard.reset_budget();
                }

                for _ in 0..budget {
                    match driver_for_loop.plan_compaction_task(planner.as_ref()).await {
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
                            match scheduler
                                .enqueue(task, manifest_head, "local-compaction", lease_ttl_ms)
                                .await
                            {
                                Ok(()) => {}
                                Err(CompactionScheduleError::Full) => {
                                    if let Some(metrics) = metrics.as_ref() {
                                        metrics.record_queue_drop(
                                            CompactionQueueDropContext::Planner,
                                            CompactionQueueDropReason::Full,
                                        );
                                    }
                                    eprintln!("compaction scheduler queue full; dropping task");
                                    break;
                                }
                                Err(CompactionScheduleError::Closed) => {
                                    if let Some(metrics) = metrics.as_ref() {
                                        metrics.record_queue_drop(
                                            CompactionQueueDropContext::Planner,
                                            CompactionQueueDropReason::Closed,
                                        );
                                    }
                                    eprintln!("compaction scheduler closed");
                                    return;
                                }
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
                        let planner = Arc::clone(&planner);
                        let scheduler = scheduler.clone();
                        let cascade_control = Arc::clone(&cascade_control);
                        let runtime = Arc::clone(&runtime);
                        let mut cascade_trigger = cascade_trigger.clone();
                        let metrics = metrics.clone();
                        async move {
                            let source_level = job.task.source_level;
                            let target_level = job.task.target_level;
                            let result = driver
                                .run_scheduled_compaction(job, executor.as_ref())
                                .await;
                            if let Err(err) = result {
                                eprintln!("scheduled compaction failed: {err}");
                                return;
                            }
                            let Ok(Some(_)) = result else {
                                return;
                            };
                            if source_level == 0 && target_level == 1 {
                                let decision = {
                                    let mut guard = cascade_control.lock().await;
                                    guard.try_acquire(runtime.now())
                                };
                                match decision {
                                    CascadeDecision::Allowed => {}
                                    CascadeDecision::BudgetExhausted => {
                                        if let Some(metrics) = metrics.as_ref() {
                                            metrics.record_cascade(
                                                CompactionCascadeDecision::BlockedBudget,
                                            );
                                        }
                                        return;
                                    }
                                    CascadeDecision::CooldownActive => {
                                        if let Some(metrics) = metrics.as_ref() {
                                            metrics.record_cascade(
                                                CompactionCascadeDecision::BlockedCooldown,
                                            );
                                        }
                                        return;
                                    }
                                }
                                let Ok(Some(task)) = driver
                                    .plan_compaction_task_from_level(planner.as_ref(), 1)
                                    .await
                                else {
                                    return;
                                };
                                let manifest_head =
                                    match driver.manifest.snapshot_latest(driver.table_id).await {
                                        Ok(snapshot) => snapshot.head.last_manifest_txn,
                                        Err(err) => {
                                            eprintln!("cascade snapshot failed: {err}");
                                            return;
                                        }
                                    };
                                match scheduler
                                    .enqueue(
                                        task,
                                        manifest_head,
                                        "cascade-compaction",
                                        lease_ttl_ms,
                                    )
                                    .await
                                {
                                    Ok(()) => {
                                        if let Some(metrics) = metrics.as_ref() {
                                            metrics.record_cascade(
                                                CompactionCascadeDecision::Scheduled,
                                            );
                                        }
                                        let _ = cascade_trigger.try_send(CompactionTrigger::Kick);
                                    }
                                    Err(CompactionScheduleError::Full) => {
                                        if let Some(metrics) = metrics.as_ref() {
                                            metrics.record_queue_drop(
                                                CompactionQueueDropContext::Cascade,
                                                CompactionQueueDropReason::Full,
                                            );
                                        }
                                        eprintln!(
                                            "compaction scheduler queue full; dropping cascade"
                                        );
                                    }
                                    Err(CompactionScheduleError::Closed) => {
                                        if let Some(metrics) = metrics.as_ref() {
                                            metrics.record_queue_drop(
                                                CompactionQueueDropContext::Cascade,
                                                CompactionQueueDropReason::Closed,
                                            );
                                        }
                                        eprintln!("compaction scheduler closed");
                                    }
                                }
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
        CompactionHandle::new(abort, Some(handle), Some(tick_tx))
    }
}
