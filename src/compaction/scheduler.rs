//! Send-friendly compaction scheduler primitives with lease issuance.

use std::future::Future;

use thiserror::Error;
use tokio::sync::mpsc;
use ulid::Ulid;

use crate::{
    compaction::{executor::CompactionLease, planner::CompactionTask},
    mvcc::Timestamp,
};

/// Single scheduled compaction task bundled with CAS context and a lease token.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ScheduledCompaction {
    pub task: CompactionTask,
    pub manifest_head: Option<Timestamp>,
    pub lease: CompactionLease,
}

/// Errors that can surface while scheduling or draining compaction jobs.
#[derive(Debug, Error)]
pub enum CompactionScheduleError {
    /// Scheduler channel closed.
    #[error("compaction scheduler closed")]
    Closed,
}

/// In-process scheduler that hands out leases and enqueues compaction tasks.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct CompactionScheduler {
    tx: mpsc::Sender<ScheduledCompaction>,
    budget: usize,
}

impl CompactionScheduler {
    /// Create a scheduler with bounded capacity and a per-cycle drain budget.
    #[allow(dead_code)]
    #[must_use]
    pub fn new(capacity: usize, budget: usize) -> (Self, mpsc::Receiver<ScheduledCompaction>) {
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
    #[allow(dead_code)]
    pub async fn enqueue(
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
        self.tx
            .send(ScheduledCompaction {
                task,
                manifest_head,
                lease,
            })
            .await
            .map_err(|_| CompactionScheduleError::Closed)
    }

    /// Drain up to the configured budget of scheduled jobs, invoking `f` per job.
    #[allow(dead_code)]
    pub async fn drain_with_budget<F, Fut>(
        &self,
        rx: &mut mpsc::Receiver<ScheduledCompaction>,
        mut f: F,
    ) -> Result<(), CompactionScheduleError>
    where
        F: FnMut(ScheduledCompaction) -> Fut,
        Fut: Future<Output = ()>,
    {
        for _ in 0..self.budget {
            match rx.recv().await {
                Some(job) => f(job).await,
                None => return Err(CompactionScheduleError::Closed),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ondisk::sstable::SsTableId;

    #[tokio::test]
    async fn enqueue_and_drain_with_budget() {
        let (scheduler, mut rx) = CompactionScheduler::new(4, 2);
        let task = CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![SsTableId::new(1)],
            key_range: None,
        };
        scheduler
            .enqueue(task.clone(), None, "worker-a", 1_000)
            .await
            .expect("enqueue");
        scheduler
            .enqueue(task.clone(), None, "worker-b", 1_000)
            .await
            .expect("enqueue");

        let mut owners: Vec<String> = Vec::new();
        scheduler
            .drain_with_budget(&mut rx, |job| {
                owners.push(job.lease.owner.clone());
                async move {
                    assert_eq!(job.task.source_level, 0);
                }
            })
            .await
            .expect("drain");
        assert_eq!(owners, vec!["worker-a".to_string(), "worker-b".to_string()]);
    }
}
