//! Compaction driver for orchestrating compaction operations.
//!
//! The driver owns manifest access and WAL configuration, coordinates planning,
//! execution, and reconciliation without requiring the full DB type.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use fusio::{
    dynamic::{MaybeSend, MaybeSync},
    executor::{Executor, Instant, RwLock, Timer},
};
use futures::{FutureExt, StreamExt, channel::mpsc, future::AbortHandle, lock::Mutex};
use tracing::instrument;

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
    observability::{log_debug, log_info, log_warn},
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

struct DrainContext<'a, P, E> {
    planner: &'a Arc<P>,
    cascade_control: &'a Arc<Mutex<CascadeControl>>,
    runtime: &'a Arc<E>,
    cascade_trigger: &'a mpsc::Sender<CompactionTrigger>,
    lease_ttl_ms: u64,
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
                    log_info!(
                        component = "wal",
                        event = "wal_prune_dry_run",
                        floor_seq = floor.seq(),
                        removed_segments = removed,
                    );
                } else if removed > 0 {
                    log_info!(
                        component = "wal",
                        event = "wal_prune_completed",
                        floor_seq = floor.seq(),
                        removed_segments = removed,
                    );
                }
            }
            Err(err) => {
                if let Some(handle) = wal_handle {
                    let metrics = handle.metrics();
                    let mut guard = metrics.write().await;
                    guard.record_wal_prune_failure();
                }
                log_warn!(
                    component = "wal",
                    event = "wal_prune_failed",
                    floor_seq = floor.seq(),
                    error = ?err,
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

    fn estimated_bytes(stats: &CompactionIoStats) -> Option<u64> {
        stats.complete.then_some(stats.bytes)
    }

    fn log_plan_event(
        source_level: usize,
        target_level: usize,
        input_count: usize,
        stats: &CompactionIoStats,
    ) {
        let estimated_bytes = Self::estimated_bytes(stats);
        log_debug!(
            component = "compaction",
            event = "compaction_plan_built",
            source_level,
            target_level,
            input_count,
            estimated_bytes = ?estimated_bytes,
        );
    }

    fn log_execute_start(
        source_level: usize,
        target_level: usize,
        input_count: usize,
        stats: &CompactionIoStats,
    ) {
        log_info!(
            component = "compaction",
            event = "compaction_execute_start",
            source_level,
            target_level,
            input_count,
            input_bytes = stats.bytes,
            input_rows = stats.rows,
            input_tombstones = stats.tombstones,
            input_stats_complete = stats.complete,
        );
    }

    fn log_execute_complete(
        source_level: usize,
        target_level: usize,
        input_count: usize,
        output_count: usize,
        duration_ms: u64,
        output_stats: &CompactionIoStats,
    ) {
        log_info!(
            component = "compaction",
            event = "compaction_execute_complete",
            source_level,
            target_level,
            input_count,
            output_count,
            duration_ms,
            output_bytes = output_stats.bytes,
            output_rows = output_stats.rows,
            output_tombstones = output_stats.tombstones,
            output_stats_complete = output_stats.complete,
        );
    }

    fn log_execute_failed(
        source_level: usize,
        target_level: usize,
        input_count: usize,
        duration_ms: u64,
        err: &CompactionError,
    ) {
        log_warn!(
            component = "compaction",
            event = "compaction_execute_failed",
            source_level,
            target_level,
            input_count,
            duration_ms,
            error = ?err,
        );
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
    #[instrument(
        name = "compaction::run",
        skip(self, planner, executor),
        fields(component = "compaction", table_id = ?self.table_id)
    )]
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
            Self::log_plan_event(source_level, target_level, input_count, &input_stats);
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

            Self::log_execute_start(source_level, target_level, input_count, &input_stats);
            let exec_started_at = self.runtime.now();
            let outcome = match executor.execute(job).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    let exec_duration = self.runtime.now().duration_since(exec_started_at);
                    Self::log_execute_failed(
                        source_level,
                        target_level,
                        input_count,
                        exec_duration.as_millis().try_into().unwrap_or(u64::MAX),
                        &err,
                    );
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
            let exec_duration = self.runtime.now().duration_since(exec_started_at);
            let (output_count, output_stats) = Self::output_stats(&outcome);
            Self::log_execute_complete(
                source_level,
                target_level,
                input_count,
                output_count,
                exec_duration.as_millis().try_into().unwrap_or(u64::MAX),
                &output_stats,
            );
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
            log_debug!(
                component = "manifest",
                event = "manifest_cas_attempt",
                table_id = ?self.table_id,
                attempt = attempts,
                max_retries = MAX_COMPACTION_APPLY_RETRIES,
            );
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
                        log_warn!(
                            component = "manifest",
                            event = "manifest_cas_failed",
                            table_id = ?self.table_id,
                            attempt = attempts,
                            max_retries = MAX_COMPACTION_APPLY_RETRIES,
                        );
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
                    log_warn!(
                        component = "manifest",
                        event = "manifest_cas_retry",
                        table_id = ?self.table_id,
                        attempt = attempts,
                        max_retries = MAX_COMPACTION_APPLY_RETRIES,
                    );
                    self.sleep_cas_backoff(attempts).await;
                    continue;
                }
                Err(err) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    log_warn!(
                        component = "manifest",
                        event = "manifest_apply_failed",
                        table_id = ?self.table_id,
                        error = ?err,
                    );
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
    #[instrument(
        name = "compaction::run_scheduled",
        skip(self, scheduled, executor),
        fields(
            component = "compaction",
            table_id = ?self.table_id,
            source_level = scheduled.task.source_level,
            target_level = scheduled.task.target_level,
            input_count = scheduled.task.input.len()
        )
    )]
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
            Self::log_plan_event(source_level, target_level, input_count, &input_stats);
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

            Self::log_execute_start(source_level, target_level, input_count, &input_stats);
            let exec_started_at = self.runtime.now();
            let outcome = match executor.execute(job).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    let exec_duration = self.runtime.now().duration_since(exec_started_at);
                    Self::log_execute_failed(
                        source_level,
                        target_level,
                        input_count,
                        exec_duration.as_millis().try_into().unwrap_or(u64::MAX),
                        &err,
                    );
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
            let exec_duration = self.runtime.now().duration_since(exec_started_at);
            let (output_count, output_stats) = Self::output_stats(&outcome);
            Self::log_execute_complete(
                source_level,
                target_level,
                input_count,
                output_count,
                exec_duration.as_millis().try_into().unwrap_or(u64::MAX),
                &output_stats,
            );
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
            log_debug!(
                component = "manifest",
                event = "manifest_cas_attempt",
                table_id = ?self.table_id,
                attempt = attempts,
                max_retries = MAX_COMPACTION_APPLY_RETRIES,
            );
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
                        log_warn!(
                            component = "manifest",
                            event = "manifest_cas_failed",
                            table_id = ?self.table_id,
                            attempt = attempts,
                            max_retries = MAX_COMPACTION_APPLY_RETRIES,
                        );
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
                    log_warn!(
                        component = "manifest",
                        event = "manifest_cas_retry",
                        table_id = ?self.table_id,
                        attempt = attempts,
                        max_retries = MAX_COMPACTION_APPLY_RETRIES,
                    );
                    self.sleep_cas_backoff(attempts).await;
                    continue;
                }
                Err(err) => {
                    executor.cleanup_outputs(&outcome.outputs).await?;
                    log_warn!(
                        component = "manifest",
                        event = "manifest_apply_failed",
                        table_id = ?self.table_id,
                        error = ?err,
                    );
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

    async fn wait_for_trigger(
        runtime: &E,
        periodic_interval: Option<Duration>,
        tick_rx: &mut mpsc::Receiver<CompactionTrigger>,
    ) -> Option<CompactionTriggerReason> {
        if let Some(interval) = periodic_interval {
            let mut sleep = runtime.sleep(interval).fuse();
            futures::select_biased! {
                _ = sleep => Some(CompactionTriggerReason::Periodic),
                msg = tick_rx.next() => match msg {
                    Some(CompactionTrigger::Kick) => Some(CompactionTriggerReason::Kick),
                    Some(CompactionTrigger::Shutdown) | None => None,
                },
            }
        } else {
            match tick_rx.next().await {
                Some(CompactionTrigger::Kick) => Some(CompactionTriggerReason::Kick),
                Some(CompactionTrigger::Shutdown) | None => None,
            }
        }
    }

    async fn plan_and_enqueue<P>(
        &self,
        planner: &P,
        scheduler: &CompactionScheduler,
        budget: usize,
        lease_ttl_ms: u64,
    ) -> Result<(), CompactionScheduleError>
    where
        P: CompactionPlanner,
    {
        let metrics = self.compaction_metrics.as_ref();
        for _ in 0..budget {
            match self.plan_compaction_task(planner).await {
                Ok(Some(task)) => {
                    let source_level = task.source_level;
                    let target_level = task.target_level;
                    let input_count = task.input.len();
                    let manifest_head = match self.manifest.snapshot_latest(self.table_id).await {
                        Ok(snapshot) => snapshot.head.last_manifest_txn,
                        Err(err) => {
                            log_warn!(
                                component = "compaction",
                                event = "compaction_snapshot_failed",
                                table_id = ?self.table_id,
                                error = ?err,
                            );
                            continue;
                        }
                    };
                    match scheduler
                        .enqueue(task, manifest_head, "local-compaction", lease_ttl_ms)
                        .await
                    {
                        Ok(()) => {
                            log_debug!(
                                component = "compaction",
                                event = "compaction_scheduled",
                                source_level,
                                target_level,
                                input_count,
                            );
                        }
                        Err(CompactionScheduleError::Full) => {
                            if let Some(metrics) = metrics {
                                metrics.record_queue_drop(
                                    CompactionQueueDropContext::Planner,
                                    CompactionQueueDropReason::Full,
                                );
                            }
                            log_warn!(
                                component = "compaction",
                                event = "compaction_queue_drop",
                                context = CompactionQueueDropContext::Planner.as_str(),
                                reason = CompactionQueueDropReason::Full.as_str(),
                            );
                            break;
                        }
                        Err(CompactionScheduleError::Closed) => {
                            if let Some(metrics) = metrics {
                                metrics.record_queue_drop(
                                    CompactionQueueDropContext::Planner,
                                    CompactionQueueDropReason::Closed,
                                );
                            }
                            log_warn!(
                                component = "compaction",
                                event = "compaction_queue_drop",
                                context = CompactionQueueDropContext::Planner.as_str(),
                                reason = CompactionQueueDropReason::Closed.as_str(),
                            );
                            return Err(CompactionScheduleError::Closed);
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    log_warn!(
                        component = "compaction",
                        event = "compaction_plan_failed",
                        table_id = ?self.table_id,
                        error = ?err,
                    );
                    break;
                }
            }
        }
        Ok(())
    }

    async fn drain_and_execute<CE, P>(
        driver: &Arc<Self>,
        scheduler: &CompactionScheduler,
        rx: &mut mpsc::Receiver<ScheduledCompaction>,
        executor: &Arc<CE>,
        ctx: &DrainContext<'_, P, E>,
    ) -> Result<bool, CompactionScheduleError>
    where
        CE: CompactionExecutor,
        P: CompactionPlanner,
    {
        let applied_manifest = Arc::new(AtomicBool::new(false));
        let metrics = driver.compaction_metrics.clone();
        scheduler
            .drain_with_budget(rx, |job| {
                let driver = Arc::clone(driver);
                let executor = Arc::clone(executor);
                let applied_manifest = Arc::clone(&applied_manifest);
                let metrics = metrics.clone();
                let planner = Arc::clone(ctx.planner);
                let scheduler = scheduler.clone();
                let cascade_control = Arc::clone(ctx.cascade_control);
                let runtime = Arc::clone(ctx.runtime);
                let mut cascade_trigger = ctx.cascade_trigger.clone();
                let lease_ttl_ms = ctx.lease_ttl_ms;
                async move {
                    let source_level = job.task.source_level;
                    let target_level = job.task.target_level;
                    let outcome = match driver
                        .run_scheduled_compaction(job, executor.as_ref())
                        .await
                    {
                        Ok(Some(outcome)) => outcome,
                        Ok(None) => return,
                        Err(err) => {
                            log_warn!(
                                component = "compaction",
                                event = "compaction_job_failed",
                                error = ?err,
                            );
                            return;
                        }
                    };
                    if !outcome.to_version_edits().is_empty() {
                        applied_manifest.store(true, Ordering::Release);
                    }

                    if source_level == 0 && target_level == 1 {
                        let decision = {
                            let mut guard = cascade_control.lock().await;
                            guard.try_acquire(runtime.now())
                        };
                        match decision {
                            CascadeDecision::Allowed => {}
                            CascadeDecision::BudgetExhausted => {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics
                                        .record_cascade(CompactionCascadeDecision::BlockedBudget);
                                }
                                log_info!(
                                    component = "compaction",
                                    event = "compaction_cascade_decision",
                                    decision = CompactionCascadeDecision::BlockedBudget.as_str(),
                                );
                                return;
                            }
                            CascadeDecision::CooldownActive => {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics
                                        .record_cascade(CompactionCascadeDecision::BlockedCooldown);
                                }
                                log_info!(
                                    component = "compaction",
                                    event = "compaction_cascade_decision",
                                    decision = CompactionCascadeDecision::BlockedCooldown.as_str(),
                                );
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
                                    log_warn!(
                                        component = "compaction",
                                        event = "compaction_snapshot_failed",
                                        table_id = ?driver.table_id,
                                        error = ?err,
                                    );
                                    return;
                                }
                            };
                        match scheduler
                            .enqueue(task, manifest_head, "cascade-compaction", lease_ttl_ms)
                            .await
                        {
                            Ok(()) => {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics.record_cascade(CompactionCascadeDecision::Scheduled);
                                }
                                log_info!(
                                    component = "compaction",
                                    event = "compaction_cascade_decision",
                                    decision = CompactionCascadeDecision::Scheduled.as_str(),
                                );
                                let _ = cascade_trigger.try_send(CompactionTrigger::Kick);
                            }
                            Err(CompactionScheduleError::Full) => {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics.record_queue_drop(
                                        CompactionQueueDropContext::Cascade,
                                        CompactionQueueDropReason::Full,
                                    );
                                }
                                log_warn!(
                                    component = "compaction",
                                    event = "compaction_queue_drop",
                                    context = CompactionQueueDropContext::Cascade.as_str(),
                                    reason = CompactionQueueDropReason::Full.as_str(),
                                );
                            }
                            Err(CompactionScheduleError::Closed) => {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics.record_queue_drop(
                                        CompactionQueueDropContext::Cascade,
                                        CompactionQueueDropReason::Closed,
                                    );
                                }
                                log_warn!(
                                    component = "compaction",
                                    event = "compaction_queue_drop",
                                    context = CompactionQueueDropContext::Cascade.as_str(),
                                    reason = CompactionQueueDropReason::Closed.as_str(),
                                );
                            }
                        }
                    }
                }
            })
            .await?;
        Ok(applied_manifest.load(Ordering::Acquire))
    }

    async fn should_self_kick<P>(&self, planner: &P) -> bool
    where
        P: CompactionPlanner,
    {
        match self.plan_compaction_task(planner).await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(err) => {
                log_warn!(
                    component = "compaction",
                    event = "compaction_plan_failed",
                    table_id = ?self.table_id,
                    error = ?err,
                );
                false
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
            let mut pending_reason: Option<CompactionTriggerReason> = None;
            loop {
                if pending_reason.is_none() {
                    pending_reason =
                        Self::wait_for_trigger(runtime.as_ref(), periodic_interval, &mut tick_rx)
                            .await;
                    if pending_reason.is_none() {
                        return;
                    }
                }

                if let Some(reason) = pending_reason.take()
                    && let Some(metrics) = metrics.as_ref()
                {
                    metrics.record_trigger(reason);
                }
                {
                    let mut guard = cascade_control.lock().await;
                    guard.reset_budget();
                }

                if let Err(err) = driver_for_loop
                    .plan_and_enqueue(planner.as_ref(), &scheduler, budget, lease_ttl_ms)
                    .await
                {
                    log_warn!(
                        component = "compaction",
                        event = "compaction_scheduler_closed",
                        error = ?err,
                    );
                    return;
                }

                let drain_ctx = DrainContext {
                    planner: &planner,
                    cascade_control: &cascade_control,
                    runtime: &runtime,
                    cascade_trigger: &cascade_trigger,
                    lease_ttl_ms,
                };
                let applied_manifest = match Self::drain_and_execute(
                    &driver, &scheduler, &mut rx, &executor, &drain_ctx,
                )
                .await
                {
                    Ok(applied_manifest) => applied_manifest,
                    Err(err) => {
                        log_warn!(
                            component = "compaction",
                            event = "compaction_scheduler_drain_stopped",
                            error = ?err,
                        );
                        return;
                    }
                };

                // Self-kick after manifest edits so we can immediately pick up newly-eligible
                // compactions (or remaining work beyond the current budget) without waiting
                // for the next external or periodic trigger.
                if applied_manifest && driver_for_loop.should_self_kick(planner.as_ref()).await {
                    pending_reason = Some(CompactionTriggerReason::Kick);
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
