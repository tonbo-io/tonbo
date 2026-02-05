//! Test-only utilities (compiled under `cfg(test)` only).
//!
//! These helpers are not part of the public API surface for consumers. They exist solely
//! to support integration/e2e tests without requiring a "test-helpers" feature.

#![allow(dead_code)]

use std::{pin::Pin, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use fusio::executor::{Executor, Timer};
use futures::Future;
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

use crate::{
    compaction::{
        executor::{
            CompactionError, CompactionExecutor, CompactionOutcome, LocalCompactionExecutor,
        },
        planner::{CompactionInput, CompactionPlanner, CompactionSnapshot, CompactionTask},
    },
    db::{DBError, DbInner},
    manifest::ManifestFs,
    mode::DynModeConfig,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
    query::Expr,
    schema::SchemaBuilder,
    transaction::Snapshot as TxSnapshot,
};

/// Trait for types that can be converted into a `DynRow`.
pub(crate) trait IntoDynRow {
    /// Convert into a `DynRow`.
    fn into_dyn_row(self) -> DynRow;
}

impl IntoDynRow for DynRow {
    fn into_dyn_row(self) -> DynRow {
        self
    }
}

impl IntoDynRow for Vec<Option<DynCell>> {
    fn into_dyn_row(self) -> DynRow {
        DynRow(self)
    }
}

/// Build a `RecordBatch` from dynamic rows, validating nullability.
///
/// Accepts either `Vec<DynRow>` or `Vec<Vec<Option<DynCell>>>` for convenience.
pub(crate) fn build_batch<R: IntoDynRow>(
    schema: SchemaRef,
    rows: Vec<R>,
) -> Result<RecordBatch, DynError> {
    let mut builders = DynBuilders::new(schema.clone(), rows.len());
    for row in rows {
        builders.append_option_row(Some(row.into_dyn_row()))?;
    }
    builders.try_finish_into_batch()
}

/// Convenience helper that builds a DynMode configuration with embedded PK metadata.
pub fn config_with_pk(fields: Vec<Field>, primary_key: &[&str]) -> DynModeConfig {
    assert!(
        !primary_key.is_empty(),
        "schema builder requires at least one primary-key column",
    );

    let schema = SchemaRef::new(Schema::new(fields));
    let builder = SchemaBuilder::from_schema(schema);
    let builder = if primary_key.len() == 1 {
        builder.primary_key(primary_key[0].to_string())
    } else {
        builder.composite_key(primary_key.iter().copied().collect::<Vec<_>>())
    }
    .with_metadata();

    builder
        .build()
        .expect("schema builder configuration should succeed")
}

/// Test-only helper to flush immutables using a provided descriptor.
pub fn flush_immutables<'a, FS, E>(
    db: &'a DbInner<FS, E>,
    config: Arc<SsTableConfig>,
    descriptor: SsTableDescriptor,
) -> Pin<Box<dyn Future<Output = Result<(), SsTableError>> + 'a>>
where
    FS: ManifestFs<E> + 'a,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    Box::pin(async move {
        db.flush_immutables_with_descriptor(config, descriptor)
            .await
            .map(|_| ())
    })
}

/// Test-only helper to trigger WAL pruning below the manifest floor.
pub fn prune_wal_segments_below_floor<'a, FS, E>(
    db: &'a DbInner<FS, E>,
) -> Pin<Box<dyn Future<Output = ()> + 'a>>
where
    FS: ManifestFs<E> + 'a,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    Box::pin(async move { db.prune_wal_segments_below_floor().await })
}

/// Test-only wrapper to run a compaction task and surface the outcome.
#[allow(private_bounds, private_interfaces)]
pub fn run_compaction_task<'a, FS, E, P>(
    db: &'a DbInner<FS, E>,
    planner: &'a P,
    executor: &'a impl CompactionExecutor,
) -> Pin<Box<dyn Future<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>>
where
    FS: ManifestFs<E> + 'a,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    P: CompactionPlanner + Sync,
{
    Box::pin(async move { db.run_compaction_task(planner, executor).await })
}

/// Helper to merge specific L0 SST ids into a target level using the local executor.
pub(crate) fn compact_merge_l0<'a, FS, E>(
    db: &'a DbInner<FS, E>,
    sst_ids: Vec<u64>,
    target_level: u32,
    sst_cfg: Arc<SsTableConfig>,
    start_id: u64,
) -> Pin<Box<dyn Future<Output = Result<CompactionOutcome, Box<dyn std::error::Error>>> + 'a>>
where
    FS: ManifestFs<E> + 'a,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    Box::pin(async move {
        struct StaticPlanner {
            task: CompactionTask,
        }
        impl CompactionPlanner for StaticPlanner {
            fn plan(&self, _snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
                Some(self.task.clone())
            }
        }

        let task = CompactionTask {
            source_level: 0,
            target_level: target_level as usize,
            input: sst_ids
                .into_iter()
                .map(|id| CompactionInput {
                    level: 0,
                    sst_id: SsTableId::new(id),
                })
                .collect(),
            key_range: None,
        };
        let planner = StaticPlanner { task };
        let executor = LocalCompactionExecutor::new(sst_cfg, start_id);
        let outcome = db
            .run_compaction_task(&planner, &executor)
            .await?
            .ok_or("compaction returned no outcome")?;
        Ok(outcome)
    })
}

/// Plan a scan using a snapshot.
pub(crate) fn plan_scan_snapshot<'a, FS, E>(
    snapshot: &'a TxSnapshot,
    db: &'a DbInner<FS, E>,
    predicate: &'a Expr,
    projected_schema: Option<&'a SchemaRef>,
    limit: Option<usize>,
) -> Pin<Box<dyn Future<Output = Result<crate::query::scan::ScanPlan, DBError>> + 'a>>
where
    FS: ManifestFs<E> + 'a,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    Box::pin(async move {
        snapshot
            .plan_scan(db, predicate, projected_schema, limit)
            .await
    })
}

/// Execute a prepared scan plan and stream `RecordBatch` results.
pub(crate) fn execute_scan_plan<'a, FS, E>(
    db: &'a DbInner<FS, E>,
    plan: crate::query::scan::ScanPlan,
) -> Pin<
    Box<
        dyn Future<
                Output = Result<
                    impl futures::Stream<Item = Result<arrow_array::RecordBatch, DBError>> + 'a,
                    DBError,
                >,
            > + 'a,
    >,
>
where
    FS: ManifestFs<E> + 'a,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    Box::pin(async move { db.execute_scan(plan).await })
}

// Re-export test-only types to make internal tests more concise.
pub(crate) use crate::{
    ondisk::sstable::{
        SsTableConfig as TestSsTableConfig, SsTableDescriptor as TestSsTableDescriptor,
        SsTableId as TestSsTableId,
    },
    wal::{
        WalExt as TestWalExt, WalSyncPolicy as TestWalSyncPolicy,
        state::FsWalStateStore as TestFsWalStateStore,
    },
};
