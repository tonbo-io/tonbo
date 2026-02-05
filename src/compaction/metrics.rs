//! Compaction observability counters and summaries.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::{manifest::SstEntry, ondisk::sstable::SsTableDescriptor};

/// Aggregate I/O statistics for compaction inputs or outputs.
#[derive(Debug, Clone, Copy)]
pub struct CompactionIoStats {
    /// Total bytes observed.
    pub bytes: u64,
    /// Total rows observed.
    pub rows: u64,
    /// Total tombstones observed.
    pub tombstones: u64,
    /// `true` if every input had stats; `false` if some were missing.
    pub complete: bool,
}

impl Default for CompactionIoStats {
    fn default() -> Self {
        Self {
            bytes: 0,
            rows: 0,
            tombstones: 0,
            complete: true,
        }
    }
}

impl CompactionIoStats {
    /// Aggregate stats from SST descriptors.
    pub(crate) fn from_descriptors(descriptors: &[SsTableDescriptor]) -> Self {
        let mut stats = Self {
            bytes: 0,
            rows: 0,
            tombstones: 0,
            complete: true,
        };
        for desc in descriptors {
            let Some(sst_stats) = desc.stats() else {
                stats.complete = false;
                continue;
            };
            stats.bytes = stats.bytes.saturating_add(sst_stats.bytes as u64);
            stats.rows = stats.rows.saturating_add(sst_stats.rows as u64);
            stats.tombstones = stats.tombstones.saturating_add(sst_stats.tombstones as u64);
        }
        stats
    }

    /// Aggregate stats from manifest SST entries.
    pub(crate) fn from_entries(entries: &[SstEntry]) -> Self {
        let mut stats = Self {
            bytes: 0,
            rows: 0,
            tombstones: 0,
            complete: true,
        };
        for entry in entries {
            let Some(sst_stats) = entry.stats() else {
                stats.complete = false;
                continue;
            };
            stats.bytes = stats.bytes.saturating_add(sst_stats.bytes as u64);
            stats.rows = stats.rows.saturating_add(sst_stats.rows as u64);
            stats.tombstones = stats.tombstones.saturating_add(sst_stats.tombstones as u64);
        }
        stats
    }
}

/// Summary of a completed or aborted compaction job.
#[derive(Debug, Clone, Copy)]
pub struct CompactionJobSnapshot {
    /// Source level for the compaction job.
    pub source_level: usize,
    /// Target level for the compaction job.
    pub target_level: usize,
    /// Input SST count.
    pub input_sst_count: usize,
    /// Output SST count.
    pub output_sst_count: usize,
    /// Aggregated input statistics.
    pub input: CompactionIoStats,
    /// Aggregated output statistics.
    pub output: CompactionIoStats,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// CAS retry count observed before completion.
    pub cas_retries: u64,
    /// `true` if the job aborted due to CAS exhaustion.
    pub cas_aborted: bool,
}

/// Snapshot of compaction observability counters.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct CompactionMetricsSnapshot {
    /// Completed compaction job count.
    pub job_count: u64,
    /// Failed/aborted compaction job count.
    pub job_failures: u64,
    /// Total CAS retries observed.
    pub cas_retries: u64,
    /// Total CAS aborts observed.
    pub cas_aborts: u64,
    /// Planner queue drops because it was full.
    pub queue_drops_planner_full: u64,
    /// Planner queue drops because it was closed.
    pub queue_drops_planner_closed: u64,
    /// Cascade queue drops because it was full.
    pub queue_drops_cascade_full: u64,
    /// Cascade queue drops because it was closed.
    pub queue_drops_cascade_closed: u64,
    /// Cascades successfully scheduled.
    pub cascades_scheduled: u64,
    /// Cascades blocked by cooldown.
    pub cascades_blocked_cooldown: u64,
    /// Cascades blocked by budget exhaustion.
    pub cascades_blocked_budget: u64,
    /// Backpressure slowdown signals observed.
    pub backpressure_slowdown: u64,
    /// Backpressure stall signals observed.
    pub backpressure_stall: u64,
    /// Manual trigger (kick) ticks observed.
    pub trigger_kick: u64,
    /// Periodic trigger ticks observed.
    pub trigger_periodic: u64,
    /// Total input bytes for completed jobs (best effort).
    pub bytes_in: u64,
    /// Total output bytes for completed jobs (best effort).
    pub bytes_out: u64,
    /// Total input rows for completed jobs (best effort).
    pub rows_in: u64,
    /// Total output rows for completed jobs (best effort).
    pub rows_out: u64,
    /// Total input tombstones for completed jobs (best effort).
    pub tombstones_in: u64,
    /// Total output tombstones for completed jobs (best effort).
    pub tombstones_out: u64,
    /// Total duration for completed jobs (milliseconds).
    pub duration_ms_total: u64,
    /// Last observed job summary, if any.
    pub last_job: Option<CompactionJobSnapshot>,
}

/// Source of a compaction tick.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CompactionTriggerReason {
    /// Tick triggered by an explicit kick.
    Kick,
    /// Tick triggered by periodic scheduling.
    Periodic,
}

/// Reason a compaction job was dropped from the scheduler queue.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CompactionQueueDropReason {
    /// Queue was at capacity.
    Full,
    /// Queue was closed.
    Closed,
}

/// Context in which a compaction job drop occurred.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CompactionQueueDropContext {
    /// Drop happened during initial planning enqueue.
    Planner,
    /// Drop happened when scheduling a cascade.
    Cascade,
}

/// Outcome for cascade scheduling decisions.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CompactionCascadeDecision {
    /// Cascade task was scheduled successfully.
    Scheduled,
    /// Cascade was blocked by cooldown.
    BlockedCooldown,
    /// Cascade was blocked by budget exhaustion.
    BlockedBudget,
}

/// Backpressure signals emitted by L0 thresholds.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CompactionBackpressureSignal {
    /// Slowdown signal with a delay.
    Slowdown,
    /// Stall signal with a delay.
    Stall,
}

/// Shared compaction metrics with optional stderr logging.
#[derive(Debug)]
pub struct CompactionMetrics {
    emit_logs: bool,
    job_count: AtomicU64,
    job_failures: AtomicU64,
    cas_retries: AtomicU64,
    cas_aborts: AtomicU64,
    queue_drops_planner_full: AtomicU64,
    queue_drops_planner_closed: AtomicU64,
    queue_drops_cascade_full: AtomicU64,
    queue_drops_cascade_closed: AtomicU64,
    cascades_scheduled: AtomicU64,
    cascades_blocked_cooldown: AtomicU64,
    cascades_blocked_budget: AtomicU64,
    backpressure_slowdown: AtomicU64,
    backpressure_stall: AtomicU64,
    trigger_kick: AtomicU64,
    trigger_periodic: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    rows_in: AtomicU64,
    rows_out: AtomicU64,
    tombstones_in: AtomicU64,
    tombstones_out: AtomicU64,
    duration_ms_total: AtomicU64,
    last_job_present: AtomicU64,
    last_job_source_level: AtomicU64,
    last_job_target_level: AtomicU64,
    last_job_input_sst_count: AtomicU64,
    last_job_output_sst_count: AtomicU64,
    last_job_input_bytes: AtomicU64,
    last_job_output_bytes: AtomicU64,
    last_job_input_rows: AtomicU64,
    last_job_output_rows: AtomicU64,
    last_job_input_tombstones: AtomicU64,
    last_job_output_tombstones: AtomicU64,
    last_job_input_complete: AtomicU64,
    last_job_output_complete: AtomicU64,
    last_job_duration_ms: AtomicU64,
    last_job_cas_retries: AtomicU64,
    last_job_cas_aborted: AtomicU64,
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionMetrics {
    /// Create metrics without emitting logs.
    pub fn new() -> Self {
        Self::with_logs(false)
    }

    /// Create metrics that also emit stderr logs per job and scheduler event.
    #[allow(dead_code)]
    pub fn with_job_logging() -> Self {
        Self::with_logs(true)
    }

    fn with_logs(emit_logs: bool) -> Self {
        Self {
            emit_logs,
            job_count: AtomicU64::new(0),
            job_failures: AtomicU64::new(0),
            cas_retries: AtomicU64::new(0),
            cas_aborts: AtomicU64::new(0),
            queue_drops_planner_full: AtomicU64::new(0),
            queue_drops_planner_closed: AtomicU64::new(0),
            queue_drops_cascade_full: AtomicU64::new(0),
            queue_drops_cascade_closed: AtomicU64::new(0),
            cascades_scheduled: AtomicU64::new(0),
            cascades_blocked_cooldown: AtomicU64::new(0),
            cascades_blocked_budget: AtomicU64::new(0),
            backpressure_slowdown: AtomicU64::new(0),
            backpressure_stall: AtomicU64::new(0),
            trigger_kick: AtomicU64::new(0),
            trigger_periodic: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            rows_in: AtomicU64::new(0),
            rows_out: AtomicU64::new(0),
            tombstones_in: AtomicU64::new(0),
            tombstones_out: AtomicU64::new(0),
            duration_ms_total: AtomicU64::new(0),
            last_job_present: AtomicU64::new(0),
            last_job_source_level: AtomicU64::new(0),
            last_job_target_level: AtomicU64::new(0),
            last_job_input_sst_count: AtomicU64::new(0),
            last_job_output_sst_count: AtomicU64::new(0),
            last_job_input_bytes: AtomicU64::new(0),
            last_job_output_bytes: AtomicU64::new(0),
            last_job_input_rows: AtomicU64::new(0),
            last_job_output_rows: AtomicU64::new(0),
            last_job_input_tombstones: AtomicU64::new(0),
            last_job_output_tombstones: AtomicU64::new(0),
            last_job_input_complete: AtomicU64::new(0),
            last_job_output_complete: AtomicU64::new(0),
            last_job_duration_ms: AtomicU64::new(0),
            last_job_cas_retries: AtomicU64::new(0),
            last_job_cas_aborted: AtomicU64::new(0),
        }
    }

    /// Snapshot all counters and the last job summary.
    #[allow(dead_code)]
    pub fn snapshot(&self) -> CompactionMetricsSnapshot {
        let present = self.last_job_present.load(Ordering::Relaxed) != 0;
        let last_job = if present {
            Some(CompactionJobSnapshot {
                source_level: self.last_job_source_level.load(Ordering::Relaxed) as usize,
                target_level: self.last_job_target_level.load(Ordering::Relaxed) as usize,
                input_sst_count: self.last_job_input_sst_count.load(Ordering::Relaxed) as usize,
                output_sst_count: self.last_job_output_sst_count.load(Ordering::Relaxed) as usize,
                input: CompactionIoStats {
                    bytes: self.last_job_input_bytes.load(Ordering::Relaxed),
                    rows: self.last_job_input_rows.load(Ordering::Relaxed),
                    tombstones: self.last_job_input_tombstones.load(Ordering::Relaxed),
                    complete: self.last_job_input_complete.load(Ordering::Relaxed) != 0,
                },
                output: CompactionIoStats {
                    bytes: self.last_job_output_bytes.load(Ordering::Relaxed),
                    rows: self.last_job_output_rows.load(Ordering::Relaxed),
                    tombstones: self.last_job_output_tombstones.load(Ordering::Relaxed),
                    complete: self.last_job_output_complete.load(Ordering::Relaxed) != 0,
                },
                duration_ms: self.last_job_duration_ms.load(Ordering::Relaxed),
                cas_retries: self.last_job_cas_retries.load(Ordering::Relaxed),
                cas_aborted: self.last_job_cas_aborted.load(Ordering::Relaxed) != 0,
            })
        } else {
            None
        };
        CompactionMetricsSnapshot {
            job_count: self.job_count.load(Ordering::Relaxed),
            job_failures: self.job_failures.load(Ordering::Relaxed),
            cas_retries: self.cas_retries.load(Ordering::Relaxed),
            cas_aborts: self.cas_aborts.load(Ordering::Relaxed),
            queue_drops_planner_full: self.queue_drops_planner_full.load(Ordering::Relaxed),
            queue_drops_planner_closed: self.queue_drops_planner_closed.load(Ordering::Relaxed),
            queue_drops_cascade_full: self.queue_drops_cascade_full.load(Ordering::Relaxed),
            queue_drops_cascade_closed: self.queue_drops_cascade_closed.load(Ordering::Relaxed),
            cascades_scheduled: self.cascades_scheduled.load(Ordering::Relaxed),
            cascades_blocked_cooldown: self.cascades_blocked_cooldown.load(Ordering::Relaxed),
            cascades_blocked_budget: self.cascades_blocked_budget.load(Ordering::Relaxed),
            backpressure_slowdown: self.backpressure_slowdown.load(Ordering::Relaxed),
            backpressure_stall: self.backpressure_stall.load(Ordering::Relaxed),
            trigger_kick: self.trigger_kick.load(Ordering::Relaxed),
            trigger_periodic: self.trigger_periodic.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            tombstones_in: self.tombstones_in.load(Ordering::Relaxed),
            tombstones_out: self.tombstones_out.load(Ordering::Relaxed),
            duration_ms_total: self.duration_ms_total.load(Ordering::Relaxed),
            last_job,
        }
    }

    /// Record a compaction trigger tick.
    pub(crate) fn record_trigger(&self, trigger: CompactionTriggerReason) {
        match trigger {
            CompactionTriggerReason::Kick => {
                add_saturating(&self.trigger_kick, 1);
                if self.emit_logs {
                    eprintln!("compaction trigger: kick");
                }
            }
            CompactionTriggerReason::Periodic => {
                add_saturating(&self.trigger_periodic, 1);
                if self.emit_logs {
                    eprintln!("compaction trigger: periodic");
                }
            }
        }
    }

    /// Record a scheduler queue drop event.
    pub(crate) fn record_queue_drop(
        &self,
        context: CompactionQueueDropContext,
        reason: CompactionQueueDropReason,
    ) {
        match (context, reason) {
            (CompactionQueueDropContext::Planner, CompactionQueueDropReason::Full) => {
                add_saturating(&self.queue_drops_planner_full, 1);
            }
            (CompactionQueueDropContext::Planner, CompactionQueueDropReason::Closed) => {
                add_saturating(&self.queue_drops_planner_closed, 1);
            }
            (CompactionQueueDropContext::Cascade, CompactionQueueDropReason::Full) => {
                add_saturating(&self.queue_drops_cascade_full, 1);
            }
            (CompactionQueueDropContext::Cascade, CompactionQueueDropReason::Closed) => {
                add_saturating(&self.queue_drops_cascade_closed, 1);
            }
        }
        if self.emit_logs {
            eprintln!(
                "compaction scheduler drop: context={} reason={}",
                match context {
                    CompactionQueueDropContext::Planner => "planner",
                    CompactionQueueDropContext::Cascade => "cascade",
                },
                match reason {
                    CompactionQueueDropReason::Full => "full",
                    CompactionQueueDropReason::Closed => "closed",
                }
            );
        }
    }

    /// Record a cascade scheduling decision.
    pub(crate) fn record_cascade(&self, decision: CompactionCascadeDecision) {
        match decision {
            CompactionCascadeDecision::Scheduled => {
                add_saturating(&self.cascades_scheduled, 1);
                if self.emit_logs {
                    eprintln!("compaction cascade: scheduled");
                }
            }
            CompactionCascadeDecision::BlockedCooldown => {
                add_saturating(&self.cascades_blocked_cooldown, 1);
                if self.emit_logs {
                    eprintln!("compaction cascade: blocked (cooldown)");
                }
            }
            CompactionCascadeDecision::BlockedBudget => {
                add_saturating(&self.cascades_blocked_budget, 1);
                if self.emit_logs {
                    eprintln!("compaction cascade: blocked (budget)");
                }
            }
        }
    }

    /// Record a backpressure signal.
    pub(crate) fn record_backpressure(
        &self,
        signal: CompactionBackpressureSignal,
        delay: Duration,
    ) {
        match signal {
            CompactionBackpressureSignal::Slowdown => {
                add_saturating(&self.backpressure_slowdown, 1);
                if self.emit_logs {
                    eprintln!(
                        "compaction backpressure: slowdown delay_ms={}",
                        duration_ms(delay)
                    );
                }
            }
            CompactionBackpressureSignal::Stall => {
                add_saturating(&self.backpressure_stall, 1);
                if self.emit_logs {
                    eprintln!(
                        "compaction backpressure: stall delay_ms={}",
                        duration_ms(delay)
                    );
                }
            }
        }
    }

    /// Record a successful compaction job.
    pub(crate) fn record_job_success(&self, job: CompactionJobSnapshot) {
        add_saturating(&self.job_count, 1);
        add_saturating(&self.cas_retries, job.cas_retries);
        add_saturating(&self.duration_ms_total, job.duration_ms);
        if job.input.complete {
            add_saturating(&self.bytes_in, job.input.bytes);
            add_saturating(&self.rows_in, job.input.rows);
            add_saturating(&self.tombstones_in, job.input.tombstones);
        }
        if job.output.complete {
            add_saturating(&self.bytes_out, job.output.bytes);
            add_saturating(&self.rows_out, job.output.rows);
            add_saturating(&self.tombstones_out, job.output.tombstones);
        }
        self.set_last_job(job);
        if self.emit_logs {
            self.log_job("completed", job);
        }
    }

    /// Record an aborted compaction job.
    pub(crate) fn record_job_abort(&self, job: CompactionJobSnapshot) {
        add_saturating(&self.job_failures, 1);
        add_saturating(&self.cas_retries, job.cas_retries);
        if job.cas_aborted {
            add_saturating(&self.cas_aborts, 1);
        }
        self.set_last_job(job);
        if self.emit_logs {
            self.log_job("aborted", job);
        }
    }

    fn set_last_job(&self, job: CompactionJobSnapshot) {
        self.last_job_present.store(1, Ordering::Relaxed);
        self.last_job_source_level
            .store(job.source_level as u64, Ordering::Relaxed);
        self.last_job_target_level
            .store(job.target_level as u64, Ordering::Relaxed);
        self.last_job_input_sst_count
            .store(job.input_sst_count as u64, Ordering::Relaxed);
        self.last_job_output_sst_count
            .store(job.output_sst_count as u64, Ordering::Relaxed);
        self.last_job_input_bytes
            .store(job.input.bytes, Ordering::Relaxed);
        self.last_job_output_bytes
            .store(job.output.bytes, Ordering::Relaxed);
        self.last_job_input_rows
            .store(job.input.rows, Ordering::Relaxed);
        self.last_job_output_rows
            .store(job.output.rows, Ordering::Relaxed);
        self.last_job_input_tombstones
            .store(job.input.tombstones, Ordering::Relaxed);
        self.last_job_output_tombstones
            .store(job.output.tombstones, Ordering::Relaxed);
        self.last_job_input_complete
            .store(u64::from(job.input.complete), Ordering::Relaxed);
        self.last_job_output_complete
            .store(u64::from(job.output.complete), Ordering::Relaxed);
        self.last_job_duration_ms
            .store(job.duration_ms, Ordering::Relaxed);
        self.last_job_cas_retries
            .store(job.cas_retries, Ordering::Relaxed);
        self.last_job_cas_aborted
            .store(u64::from(job.cas_aborted), Ordering::Relaxed);
    }

    fn log_job(&self, status: &str, job: CompactionJobSnapshot) {
        eprintln!(
            "compaction job {status}: source=L{} target=L{} inputs={} outputs={} bytes_in={} \
             bytes_out={} rows_in={} rows_out={} tombstones_in={} tombstones_out={} \
             duration_ms={} cas_retries={} cas_aborted={} stats_in_complete={} \
             stats_out_complete={}",
            job.source_level,
            job.target_level,
            job.input_sst_count,
            job.output_sst_count,
            job.input.bytes,
            job.output.bytes,
            job.input.rows,
            job.output.rows,
            job.input.tombstones,
            job.output.tombstones,
            job.duration_ms,
            job.cas_retries,
            job.cas_aborted,
            job.input.complete,
            job.output.complete
        );
    }
}

fn add_saturating(counter: &AtomicU64, delta: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(delta))
    });
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
