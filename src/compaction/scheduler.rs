//! Send-friendly compaction scheduler primitives with lease issuance.

use std::future::Future;

use futures::{FutureExt, StreamExt, channel::mpsc, lock::Mutex, stream::FuturesUnordered};
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
    /// Scheduler queue full; admission drops the job.
    #[error("compaction scheduler queue full")]
    Full,
}

/// In-process scheduler that hands out leases and enqueues compaction tasks.
#[derive(Debug, Clone)]
pub(super) struct CompactionScheduler {
    tx: std::sync::Arc<Mutex<mpsc::Sender<ScheduledCompaction>>>,
    budget: usize,
}

impl CompactionScheduler {
    /// Create a scheduler with bounded capacity and a per-cycle drain budget.
    #[must_use]
    pub(super) fn new(
        capacity: usize,
        budget: usize,
    ) -> (Self, mpsc::Receiver<ScheduledCompaction>) {
        let capacity = capacity.max(1);
        // futures::channel::mpsc capacity is buffer + num_senders; reserve one
        // sender slot so total capacity matches the configured queue capacity.
        let channel_capacity = capacity.saturating_sub(1);
        let (tx, rx) = mpsc::channel(channel_capacity);
        (
            Self {
                tx: std::sync::Arc::new(Mutex::new(tx)),
                budget: budget.max(1),
            },
            rx,
        )
    }

    /// Enqueue a planned compaction task with an issued lease.
    ///
    /// Admission is non-blocking: if the queue is full, the task is dropped and
    /// [`CompactionScheduleError::Full`] is returned.
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
        let mut tx = self.tx.lock().await;
        tx.try_send(ScheduledCompaction {
            task,
            manifest_head,
            lease,
        })
        .map_err(|err| {
            if err.is_disconnected() {
                CompactionScheduleError::Closed
            } else {
                CompactionScheduleError::Full
            }
        })
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
        let mut running = FuturesUnordered::new();
        for _ in 0..self.budget {
            match rx.next().now_or_never() {
                Some(Some(job)) => {
                    running.push(f(job));
                }
                Some(None) => return Err(CompactionScheduleError::Closed),
                None => break,
            }
        }
        while running.next().await.is_some() {}
        Ok(())
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::{
        compaction::planner::{CompactionInput, CompactionTask},
        ondisk::sstable::SsTableId,
    };

    fn task(id: u64) -> CompactionTask {
        CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![CompactionInput {
                level: 0,
                sst_id: SsTableId::new(id),
            }],
            key_range: None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enqueue_drops_when_full() {
        let (scheduler, mut rx) = CompactionScheduler::new(1, 1);
        scheduler
            .enqueue(task(1), None, "test", 5)
            .await
            .expect("first enqueue");
        let err = scheduler
            .enqueue(task(2), None, "test", 5)
            .await
            .expect_err("second enqueue should drop");
        assert!(matches!(err, CompactionScheduleError::Full));

        let scheduled = rx.next().await.expect("scheduled task");
        assert_eq!(scheduled.task, task(1));
    }
}
