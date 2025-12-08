//! Send-friendly compaction scheduler primitives with lease issuance.

use std::future::Future;

use futures::{FutureExt, SinkExt, StreamExt, channel::mpsc};
use thiserror::Error;
use ulid::Ulid;

use crate::{
    compaction::{executor::CompactionLease, planner::CompactionTask},
    mvcc::Timestamp,
};

/// Single scheduled compaction task bundled with CAS context and a lease token.
#[derive(Debug, Clone)]
pub(super) struct ScheduledCompaction {
    pub(super) task: CompactionTask,
    pub(super) manifest_head: Option<Timestamp>,
    pub(super) lease: CompactionLease,
}

/// Errors that can surface while scheduling or draining compaction jobs.
#[derive(Debug, Error)]
pub(super) enum CompactionScheduleError {
    /// Scheduler channel closed.
    #[error("compaction scheduler closed")]
    Closed,
}

/// In-process scheduler that hands out leases and enqueues compaction tasks.
#[derive(Debug)]
pub(super) struct CompactionScheduler {
    tx: mpsc::Sender<ScheduledCompaction>,
    budget: usize,
}

impl CompactionScheduler {
    /// Create a scheduler with bounded capacity and a per-cycle drain budget.
    #[must_use]
    pub(super) fn new(
        capacity: usize,
        budget: usize,
    ) -> (Self, mpsc::Receiver<ScheduledCompaction>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (
            Self {
                tx,
                budget: budget.max(1),
            },
            rx,
        )
    }

    /// Enqueue a planned compaction task with an issued lease.
    pub(super) async fn enqueue(
        &self,
        task: CompactionTask,
        manifest_head: Option<Timestamp>,
        owner: impl Into<String>,
        ttl_ms: u64,
    ) -> Result<(), CompactionScheduleError> {
        let lease = CompactionLease {
            id: Ulid::new(),
            owner: owner.into(),
            ttl_ms,
        };
        let mut tx = self.tx.clone();
        tx.send(ScheduledCompaction {
            task,
            manifest_head,
            lease,
        })
        .await
        .map_err(|_| CompactionScheduleError::Closed)
    }

    /// Drain up to the configured budget of scheduled jobs, invoking `f` per job.
    pub(super) async fn drain_with_budget<F, Fut>(
        &self,
        rx: &mut mpsc::Receiver<ScheduledCompaction>,
        mut f: F,
    ) -> Result<(), CompactionScheduleError>
    where
        F: FnMut(ScheduledCompaction) -> Fut,
        Fut: Future<Output = ()>,
    {
        for _ in 0..self.budget {
            match rx.next().now_or_never() {
                Some(Some(job)) => f(job).await,
                Some(None) => return Err(CompactionScheduleError::Closed),
                None => break,
            }
        }
        Ok(())
    }
}
