use std::{collections::BTreeMap, pin::Pin, sync::Arc, time::Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use fusio::executor::{Executor, Timer};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use tonbo_predicate::Predicate;
use typed_arrow_dyn::DynRow;

use crate::{
    db::DbInner,
    extractor::KeyExtractError,
    inmem::{
        immutable::{self, ImmutableSegment, memtable::ImmutableVisibleEntry},
        mutable::memtable::DynRowScanEntry,
    },
    key::{KeyOwned, KeyRow},
    mutation::DynMutation,
    mvcc::Timestamp,
    ondisk::{
        scan::{DeleteStreamWithExtractor, SstableScan},
        sstable::open_parquet_stream,
    },
    query::{
        scan::{ScanPlan, projection_with_predicate},
        stream::{
            Order, OwnedImmutableScan, OwnedMutableScan, ScanStream, merge::MergeStream,
            package::PackageStream,
        },
    },
    transaction::{Snapshot as TxSnapshot, TransactionScan},
};

/// Default package size for a scan
pub const DEFAULT_SCAN_BATCH_ROWS: usize = 1024;

impl TxSnapshot {
    /// Plan a scan using this snapshot for MVCC visibility and manifest pinning.
    pub(crate) async fn plan_scan<FS, E>(
        &self,
        db: &DbInner<FS, E>,
        predicate: &Predicate,
        projected_schema: Option<&SchemaRef>,
        limit: Option<usize>,
    ) -> Result<ScanPlan, crate::db::DBError>
    where
        FS: crate::manifest::ManifestFs<E>,
        E: Executor + Timer + Clone,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        let started = Instant::now();
        let projected_schema = projected_schema.cloned();
        let residual_predicate = Some(predicate.clone());
        let scan_schema = if let Some(projection) = projected_schema.as_ref() {
            projection_with_predicate(&db.schema, projection, residual_predicate.as_ref())?
        } else {
            Arc::clone(&db.schema)
        };
        let seal = db.seal_state_lock();
        let prune_input: Vec<&ImmutableSegment> =
            seal.immutables.iter().map(|arc| arc.as_ref()).collect();
        let immutable_indexes = immutable::prune_segments(&prune_input);
        let read_ts = self.read_view().read_ts();
        let plan = ScanPlan {
            _predicate: predicate.clone(),
            immutable_indexes,
            residual_predicate,
            projected_schema,
            scan_schema,
            limit,
            read_ts,
            _snapshot: self.table_snapshot().clone(),
        };
        db.read_metrics().record_plan(started.elapsed());
        Ok(plan)
    }
}

impl<FS, E> DbInner<FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Execute the scan plan with MVCC visibility
    #[cfg(test)]
    pub(crate) async fn execute_scan<'a>(
        &'a self,
        plan: ScanPlan,
    ) -> Result<impl Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a, crate::db::DBError>
    {
        let result_projection = plan
            .projected_schema
            .clone()
            .unwrap_or_else(|| Arc::clone(&self.schema));
        let scan_schema = Arc::clone(&plan.scan_schema);
        let streams = self.build_scan_streams(&plan, None).await?;

        if streams.is_empty() {
            let stream = stream::empty::<Result<RecordBatch, crate::db::DBError>>();
            return Ok(Box::pin(stream)
                as Pin<
                    Box<dyn Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a>,
                >);
        }

        let ScanPlan {
            residual_predicate,
            limit,
            ..
        } = plan;
        // Don't pass limit to MergeStream - it should be applied after predicate
        // evaluation in PackageStream.
        let merge = MergeStream::from_vec(streams, None, Some(Order::Asc))
            .await
            .map_err(crate::db::DBError::from)?;
        let package = PackageStream::with_limit(
            DEFAULT_SCAN_BATCH_ROWS,
            merge,
            Arc::clone(&scan_schema),
            Arc::clone(&result_projection),
            residual_predicate,
            Some(self.read_metrics()),
            limit,
        )
        .map_err(crate::db::DBError::from)?;

        let mapped = package.map(|result| result.map_err(crate::db::DBError::from));
        Ok(Box::pin(mapped)
            as Pin<
                Box<dyn Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a>,
            >)
    }

    pub(crate) async fn build_scan_streams<'a>(
        &'a self,
        plan: &ScanPlan,
        txn_scan: Option<TransactionScan<'a>>,
    ) -> Result<Vec<ScanStream<'a, E>>, crate::db::DBError> {
        let mut streams = Vec::new();
        if let Some(txn_scan) = txn_scan {
            streams.push(ScanStream::from(txn_scan));
        }
        let projection_schema = Arc::clone(&plan.scan_schema);
        let mutable_scan = OwnedMutableScan::from_guard(
            self.mem.read(),
            Some(Arc::clone(&projection_schema)),
            plan.read_ts,
        )?;
        streams.push(ScanStream::from(mutable_scan));

        let immutables: Vec<Arc<ImmutableSegment>> = {
            let seal = self.seal_state_lock();
            plan.immutable_indexes
                .iter()
                .filter_map(|idx| seal.immutables.get(*idx).cloned())
                .collect()
        };
        for segment in immutables {
            let owned = OwnedImmutableScan::from_arc(
                Arc::clone(&segment),
                Some(Arc::clone(&projection_schema)),
                plan.read_ts,
            )?;
            streams.push(ScanStream::from(owned));
        }

        // Add SSTable scans for each SST entry in the plan
        for sst_entry in plan.sst_entries() {
            let data_path = sst_entry.data_path().clone();
            let executor: E = (**self.executor()).clone();
            let data_stream = open_parquet_stream(
                Arc::clone(&self.fs),
                data_path,
                None,
                executor.clone(),
                Some(self.object_store_metrics()),
            )
            .await
            .map_err(crate::db::DBError::SsTable)?;

            // Open delete sidecar stream if present (streaming merge, no eager loading)
            let delete_stream_with_extractor = if let Some(delete_path) = sst_entry.delete_path() {
                let stream = open_parquet_stream(
                    Arc::clone(&self.fs),
                    delete_path.clone(),
                    None,
                    executor.clone(),
                    Some(self.object_store_metrics()),
                )
                .await
                .map_err(crate::db::DBError::SsTable)?;
                // Delete sidecar uses key-only schema
                Some(DeleteStreamWithExtractor {
                    stream,
                    extractor: self.delete_extractor().as_ref(),
                })
            } else {
                None
            };

            // Calculate projection indices for user columns (exclude _commit_ts)
            let projection_indices: Vec<usize> = (0..projection_schema.fields().len()).collect();

            let sstable_scan = SstableScan::new(
                data_stream,
                delete_stream_with_extractor,
                self.extractor().as_ref(),
                projection_indices,
                Some(Order::Asc),
                plan.read_ts,
            );

            streams.push(ScanStream::from(sstable_scan));
        }

        Ok(streams)
    }

    pub(crate) fn scan_immutable_rows_at(
        &self,
        read_ts: Timestamp,
    ) -> Result<Vec<(KeyRow, DynRow)>, KeyExtractError> {
        let mut rows = Vec::new();
        let segments = self.seal_state_lock().immutables.clone();
        for segment in segments {
            let scan = segment.scan_visible(None, read_ts)?;
            for result in scan {
                match result {
                    Ok(ImmutableVisibleEntry::Row(key_view, row_raw)) => {
                        let row = row_raw.into_owned().map_err(|err| {
                            KeyExtractError::Arrow(arrow_schema::ArrowError::ComputeError(
                                err.to_string(),
                            ))
                        })?;
                        let (key_row, _) = key_view.into_parts();
                        rows.push((key_row, row));
                    }
                    Ok(ImmutableVisibleEntry::Tombstone(_)) => {}
                    Err(err) => return Err(KeyExtractError::from(err)),
                }
            }
        }
        Ok(rows)
    }

    pub(crate) fn scan_mutable_rows_at(
        &self,
        read_ts: Timestamp,
    ) -> Result<Vec<(KeyRow, DynRow)>, KeyExtractError> {
        let mut rows = Vec::new();
        let mem = self.mem.read();
        let scan = mem.scan_visible(None, read_ts)?;
        for entry in scan {
            match entry {
                Ok(DynRowScanEntry::Row(key_view, row_raw)) => {
                    let row = row_raw.into_owned().map_err(|err| {
                        KeyExtractError::Arrow(arrow_schema::ArrowError::ComputeError(
                            err.to_string(),
                        ))
                    })?;
                    let (key_row, _) = key_view.into_parts();
                    rows.push((key_row, row));
                }
                Ok(DynRowScanEntry::Tombstone(_)) => {}
                Err(err) => return Err(KeyExtractError::from(err)),
            }
        }
        Ok(rows)
    }

    /// Begin building a scan query with fluent API.
    #[cfg(test)]
    pub fn scan(&self) -> ScanBuilder<'_, FS, E> {
        ScanBuilder::new(self)
    }
}

/// Snapshot source for scan operations.
///
/// Controls whether a scan uses a pre-existing snapshot or creates one lazily.
pub(crate) enum SnapshotSource<'a> {
    /// Create a new snapshot when the scan executes (DB-level scan).
    Lazy,
    /// Use a pre-existing snapshot (Snapshot::scan or Transaction::scan).
    Preexisting(&'a TxSnapshot),
}

/// Staged mutations to overlay on scan results (for Transaction::scan).
pub(crate) struct StagedOverlay<'a> {
    /// Staged mutations (uncommitted writes).
    pub(crate) staging: &'a BTreeMap<KeyOwned, DynMutation<DynRow, ()>>,
    /// Schema for the staged rows.
    pub(crate) schema: &'a SchemaRef,
}

/// Fluent builder for constructing scan queries.
///
/// Use [`crate::db::DB::scan`] to create a new builder, then chain methods to
/// configure the scan before executing with [`stream`](ScanBuilder::stream) or
/// [`collect`](ScanBuilder::collect). Transaction and snapshot scan variants
/// wrap the same builder internally.
///
/// # Scan Hierarchy
///
/// All scans ultimately use `Snapshot` for MVCC visibility:
/// - `Snapshot::scan()` - Core implementation using a pre-existing snapshot
/// - `Transaction::scan()` - Wraps snapshot scan + overlays uncommitted writes
/// - `DB::scan()` - Creates an ephemeral snapshot internally
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
///
/// use arrow_array::{Int32Array, RecordBatch, StringArray};
/// use arrow_schema::{DataType, Field, Schema};
/// use fusio::{executor::tokio::TokioExecutor, mem::fs::InMemoryFs};
/// use tonbo::{
///     db::{ColumnRef, DB, DbBuilder, Predicate, ScalarValue},
///     schema::SchemaBuilder,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let schema = Arc::new(Schema::new(vec![
///         Field::new("id", DataType::Utf8, false),
///         Field::new("value", DataType::Int32, false),
///     ]));
///     let config = SchemaBuilder::from_schema(Arc::clone(&schema))
///         .primary_key("id")
///         .build()?;
///
///     // In-memory DB; use DbBuilder::on_disk/object_store for durable backends.
///     let db: DB<InMemoryFs, TokioExecutor> = DbBuilder::from_schema_key_name(schema, "id")?
///         .in_memory("scan-example")?
///         .open_with_executor(Arc::new(TokioExecutor::default()))
///         .await?;
///
///     // Seed data
///     let batch = RecordBatch::try_new(
///         Arc::clone(&config.schema()),
///         vec![
///             Arc::new(StringArray::from(vec!["a", "b"])) as _,
///             Arc::new(Int32Array::from(vec![1, 2])) as _,
///         ],
///     )?;
///     db.ingest(batch).await?;
///
///     // Scan with predicate + limit
///     let pred = Predicate::eq(ColumnRef::new("id"), ScalarValue::from("a"));
///     let batches = db.scan().filter(pred).limit(1).collect().await?;
///     assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
///     Ok(())
/// }
/// ```
pub struct ScanBuilder<'a, FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    db: &'a DbInner<FS, E>,
    /// Snapshot source: lazy (DB scan) or pre-existing (Snapshot/Transaction scan).
    snapshot_source: SnapshotSource<'a>,
    /// Optional staged mutations overlay (Transaction scan only).
    staged_overlay: Option<StagedOverlay<'a>>,
    predicate: Option<Predicate>,
    projection: Option<SchemaRef>,
    limit: Option<usize>,
}

impl<'a, FS, E> ScanBuilder<'a, FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Create a new scan builder for DB-level scans (lazy snapshot, no staging).
    pub(crate) fn new(db: &'a DbInner<FS, E>) -> Self {
        Self {
            db,
            snapshot_source: SnapshotSource::Lazy,
            staged_overlay: None,
            predicate: None,
            projection: None,
            limit: None,
        }
    }

    /// Create a scan builder from an existing snapshot (core scan implementation).
    ///
    /// This is used by `Snapshot::scan()` and forms the basis for all other scans.
    pub(crate) fn from_snapshot(db: &'a DbInner<FS, E>, snapshot: &'a TxSnapshot) -> Self {
        Self {
            db,
            snapshot_source: SnapshotSource::Preexisting(snapshot),
            staged_overlay: None,
            predicate: None,
            projection: None,
            limit: None,
        }
    }

    /// Create a scan builder from an existing snapshot using the public DB handle.
    ///
    /// This is the public entry point for `Snapshot::scan(&db)`.
    pub(crate) fn from_snapshot_with_db(
        db: &'a super::DB<FS, E>,
        snapshot: &'a TxSnapshot,
    ) -> Self {
        Self::from_snapshot(&db.inner, snapshot)
    }

    /// Create a new scan builder with transaction context (includes staging buffer).
    ///
    /// Used by `Transaction::scan()` to include uncommitted writes.
    pub(crate) fn with_transaction_overlay(
        db: &'a DbInner<FS, E>,
        snapshot: &'a TxSnapshot,
        staging: &'a BTreeMap<KeyOwned, DynMutation<DynRow, ()>>,
        schema: &'a SchemaRef,
    ) -> Self {
        Self {
            db,
            snapshot_source: SnapshotSource::Preexisting(snapshot),
            staged_overlay: Some(StagedOverlay { staging, schema }),
            predicate: None,
            projection: None,
            limit: None,
        }
    }

    /// Set the filter predicate for this scan.
    ///
    /// Only rows matching the predicate will be returned.
    /// If not called, all rows are returned.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set the projection schema for this scan.
    ///
    /// Only the specified columns will be returned in the result batches.
    /// If not called, all columns are returned.
    #[must_use]
    pub fn projection(mut self, schema: SchemaRef) -> Self {
        self.projection = Some(schema);
        self
    }

    /// Limit the number of rows returned by this scan.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the scan and return a stream of record batches.
    pub async fn stream(
        self,
    ) -> Result<impl Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a, crate::db::DBError>
    {
        // Take ownership of all fields upfront to avoid partial move issues
        let Self {
            db,
            snapshot_source,
            staged_overlay,
            predicate,
            projection,
            limit,
        } = self;

        let predicate = predicate.unwrap_or_else(Predicate::always);

        // Resolve snapshot: use pre-existing or create new one
        let snapshot = match snapshot_source {
            SnapshotSource::Preexisting(snap) => snap.clone(),
            SnapshotSource::Lazy => db.begin_snapshot().await?,
        };

        // Build transaction scan if we have staged mutations
        let txn_scan = match staged_overlay {
            Some(overlay) if !overlay.staging.is_empty() => {
                // We need to plan first to get the scan_schema
                let plan = snapshot
                    .plan_scan(db, &predicate, projection.as_ref(), limit)
                    .await?;
                Some((
                    TransactionScan::new(
                        overlay.staging,
                        overlay.schema,
                        plan.read_ts,
                        Some(&plan.scan_schema),
                    )
                    .map_err(crate::db::DBError::from)?,
                    plan,
                ))
            }
            _ => None,
        };

        // Build plan (reuse if already created for transaction scan)
        let plan = match txn_scan {
            Some((scan, plan)) => {
                // Execute with transaction scan overlay
                return execute_with_txn_scan(db, plan, Some(scan)).await;
            }
            None => {
                snapshot
                    .plan_scan(db, &predicate, projection.as_ref(), limit)
                    .await?
            }
        };

        execute_with_txn_scan(db, plan, None).await
    }

    /// Execute the scan and collect all batches into a vector.
    pub async fn collect(self) -> Result<Vec<RecordBatch>, crate::db::DBError> {
        self.stream().await?.try_collect().await
    }
}

async fn execute_with_txn_scan<'a, FS, E>(
    db: &'a DbInner<FS, E>,
    plan: ScanPlan,
    txn_scan: Option<TransactionScan<'a>>,
) -> Result<
    Pin<Box<dyn Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a>>,
    crate::db::DBError,
>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    let result_projection = plan
        .projected_schema
        .clone()
        .unwrap_or_else(|| Arc::clone(&db.schema));
    let scan_schema = Arc::clone(&plan.scan_schema);
    let streams = db.build_scan_streams(&plan, txn_scan).await?;

    if streams.is_empty() {
        let stream = stream::empty::<Result<RecordBatch, crate::db::DBError>>();
        return Ok(Box::pin(stream)
            as Pin<
                Box<dyn Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a>,
            >);
    }

    let ScanPlan {
        residual_predicate,
        limit,
        ..
    } = plan;
    // Don't pass limit to MergeStream - it should be applied after predicate
    // evaluation in PackageStream. Otherwise, limit counts rows before filtering.
    let merge = MergeStream::from_vec(streams, None, Some(Order::Asc))
        .await
        .map_err(crate::db::DBError::from)?;
    let package = PackageStream::with_limit(
        DEFAULT_SCAN_BATCH_ROWS,
        merge,
        Arc::clone(&scan_schema),
        Arc::clone(&result_projection),
        residual_predicate,
        Some(db.read_metrics()),
        limit,
    )
    .map_err(crate::db::DBError::from)?;

    let mapped = package.map(|result| result.map_err(crate::db::DBError::from));
    Ok(Box::pin(mapped)
        as Pin<
            Box<dyn Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a>,
        >)
}
