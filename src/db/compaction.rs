//! DB compaction integration.
//!
//! This module provides the bridge between the DB type and the compaction subsystem.

use std::sync::Arc;

use fusio::executor::{Executor, Timer};

#[cfg(all(test, feature = "tokio"))]
use crate::compaction::{
    executor::{CompactionError, CompactionExecutor, CompactionOutcome},
    planner::CompactionPlanner,
};
use crate::{
    compaction::{
        CompactionDriver,
        metrics::{CompactionMetricsSnapshot, SstGcInspection, SstGcStatus, SstSweepSummary},
    },
    db::DbInner,
    manifest::{ManifestFs, ManifestResult},
};

impl<FS, E> DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Whether a background compaction worker was spawned for this DB.
    #[cfg(test)]
    pub fn has_compaction_worker(&self) -> bool {
        self.compaction_worker.is_some()
    }

    /// Best-effort signal to the background compaction worker.
    pub(crate) fn kick_compaction_worker(&self) {
        if let Some(handle) = &self.compaction_worker {
            handle.kick();
        }
    }

    /// Create a compaction driver from this DB's manifest and configuration.
    ///
    /// The driver is created on demand; callers can Arc-wrap it for background workers.
    pub(crate) fn compaction_driver(&self) -> CompactionDriver<FS, E> {
        CompactionDriver::new(
            self.manifest.clone(),
            self.manifest_table,
            self.wal_config.clone(),
            self.wal_handle().cloned(),
            Arc::clone(&self.executor),
            self.cas_backoff.clone(),
            Arc::clone(&self.fs),
            self.sst_root.clone(),
            Arc::clone(&self.snapshot_pins),
            self.compaction_metrics.clone(),
        )
    }

    /// Remove WAL segments whose sequence is older than the manifest floor.
    pub(crate) async fn prune_wal_segments_below_floor(&self) {
        self.compaction_driver().prune_wal_below_floor().await
    }

    /// Build a compaction plan based on the latest manifest snapshot.
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) async fn plan_compaction_task<P>(
        &self,
        planner: &P,
    ) -> ManifestResult<Option<crate::compaction::planner::CompactionTask>>
    where
        P: CompactionPlanner,
    {
        self.compaction_driver().plan_compaction_task(planner).await
    }

    /// Sequence number of the WAL floor currently recorded in the manifest.
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) async fn wal_floor_seq(&self) -> Option<u64> {
        self.compaction_driver().wal_floor_seq().await
    }

    /// End-to-end compaction orchestrator (plan -> resolve -> execute -> apply manifest).
    #[cfg(all(test, feature = "tokio"))]
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
            .run_compaction(planner, executor)
            .await
    }

    /// Sweep unreachable manifest-published SST objects for the current table.
    pub(crate) async fn sweep_manifest_ssts(&self) -> ManifestResult<SstSweepSummary> {
        self.compaction_driver().sweep_authorized_ssts().await
    }

    /// Snapshot staged SST GC authorization state for the current table.
    pub(crate) async fn sst_gc_status(&self) -> ManifestResult<Option<SstGcStatus>> {
        self.compaction_driver().sst_gc_status().await
    }

    /// Inspect the exact persisted SST GC plan for the current table.
    pub(crate) async fn inspect_sst_gc_plan(&self) -> ManifestResult<Option<SstGcInspection>> {
        self.compaction_driver().inspect_sst_gc_plan().await
    }

    /// Snapshot current compaction metrics if this DB was configured to collect them.
    pub(crate) fn compaction_metrics_snapshot(&self) -> Option<CompactionMetricsSnapshot> {
        self.compaction_metrics
            .as_ref()
            .map(|metrics| metrics.snapshot())
    }
}
