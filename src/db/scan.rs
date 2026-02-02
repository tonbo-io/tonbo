use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
};

use aisle::PruneRequest;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use fusio::{
    DynFs,
    executor::{Executor, Timer},
};
use fusio_parquet::reader::AsyncReader;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    },
    errors::ParquetError,
    file::metadata::{PageIndexPolicy, ParquetMetaDataReader},
};
use typed_arrow_dyn::DynRow;

use crate::{
    db::DbInner,
    extractor::{KeyExtractError, KeyProjection, projection_for_columns},
    inmem::{
        immutable::{self, ImmutableSegment, memtable::ImmutableVisibleEntry},
        mutable::memtable::DynRowScanEntry,
    },
    key::{KeyOwned, KeyRow},
    mutation::DynMutation,
    mvcc::{MVCC_COMMIT_COL, Timestamp},
    ondisk::{
        scan::{DeleteStreamWithExtractor, SstableScan, UnpinExec},
        sstable::{
            ParquetStreamOptions, SsTableError, open_parquet_stream_with_metadata,
            split_predicate_for_row_filter, validate_page_indexes,
        },
    },
    query::{
        Expr, ScalarValue,
        scan::{
            DeleteSelection, RowSet, ScanPlan, ScanSelection, SstScanSelection, SstSelection,
            key_bounds_for_predicate, projection_with_predicate,
        },
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
        predicate: &Expr,
        projected_schema: Option<&SchemaRef>,
        limit: Option<usize>,
    ) -> Result<ScanPlan, crate::db::DBError>
    where
        FS: crate::manifest::ManifestFs<E>,
        E: Executor + Timer + Clone,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        if let Some(column) = find_bloom_filter_column(predicate) {
            return Err(crate::db::DBError::UnsupportedPredicate {
                reason: format!(
                    "bloom filter predicates are not supported yet (column '{column}')"
                ),
            });
        }
        let projected_schema = projected_schema.cloned();
        let scan_schema = if let Some(projection) = projected_schema.as_ref() {
            projection_with_predicate(&db.schema, projection, Some(predicate))?
        } else {
            projection_with_predicate(&db.schema, &db.schema, Some(predicate))?
        };
        let split = split_predicate_for_row_filter(predicate, &scan_schema);
        let pushdown_predicate = split.pushdown;
        let mut residual_predicate = split.residual;
        let read_ts = self.read_view().read_ts();
        let key_schema = db.extractor().key_schema();
        let key_bounds = key_bounds_for_predicate(predicate, &key_schema);
        let (immutable_indexes, immutable_row_sets) = {
            let seal = db.seal_state_lock();
            let prune_input: Vec<&ImmutableSegment> =
                seal.immutables.iter().map(|arc| arc.as_ref()).collect();
            let indexes = immutable::prune_segments(&prune_input, key_bounds.as_ref(), read_ts);
            let row_sets = indexes
                .iter()
                .filter_map(|idx| seal.immutables.get(*idx))
                .map(|segment| RowSet::all(segment.entry_count()))
                .collect();
            (indexes, row_sets)
        };
        let mutable_row_set = {
            let row_count = db.mem.row_count();
            if row_count == 0 {
                RowSet::all(0)
            } else {
                let (min_key, max_key) = db.mem.key_bounds();
                let mut keep = true;
                if let (Some(min_key), Some(max_key)) = (min_key.as_ref(), max_key.as_ref())
                    && let Some(bounds) = key_bounds.as_ref()
                    && !bounds.overlaps(min_key, max_key)
                {
                    keep = false;
                }
                if keep
                    && let Some((min_commit_ts, _)) = db.mem.commit_ts_bounds()
                    && min_commit_ts > read_ts
                {
                    keep = false;
                }
                if keep {
                    RowSet::all(row_count)
                } else {
                    RowSet::none(row_count)
                }
            }
        };
        let fs = Arc::clone(&db.fs);
        let executor: E = (**db.executor()).clone();
        let mut sst_selections = Vec::new();
        let mut sst_requires_full_residual = false;
        for entry in self
            .table_snapshot()
            .latest_version
            .as_ref()
            .map(|v| v.ssts())
            .unwrap_or(&[])
            .iter()
            .flatten()
        {
            if let Some(bounds) = key_bounds.as_ref() {
                if bounds.is_empty() {
                    continue;
                }
                if let Some(stats) = entry.stats()
                    && let (Some(min_key), Some(max_key)) =
                        (stats.min_key.as_ref(), stats.max_key.as_ref())
                    && !bounds.overlaps(min_key, max_key)
                {
                    continue;
                }
            }
            if let Some(min_commit_ts) = entry.stats().and_then(|stats| stats.min_commit_ts)
                && min_commit_ts > read_ts
            {
                continue;
            }
            let (selection, requires_residual) = prune_sst_selection(
                Arc::clone(&fs),
                entry.data_path(),
                predicate,
                read_ts,
                &scan_schema,
                &key_schema,
                executor.clone(),
            )
            .await?;
            if requires_residual {
                sst_requires_full_residual = true;
            }
            let mut selection = selection;
            if let Some(delete_path) = entry.delete_path() {
                let delete_selection = plan_delete_sidecar_selection(
                    Arc::clone(&fs),
                    delete_path,
                    &key_schema,
                    executor.clone(),
                )
                .await?;
                selection.delete_selection = Some(delete_selection);
            }
            sst_selections.push(SstScanSelection {
                entry: entry.clone(),
                selection: ScanSelection::Sst(selection),
            });
        }
        if sst_requires_full_residual && !matches!(predicate, Expr::True) {
            // Keep the full predicate for post-merge filtering when any SST
            // schema cannot accept the pushdown portion.
            residual_predicate = Some(predicate.clone());
        }
        Ok(ScanPlan {
            pushdown_predicate,
            immutable_indexes,
            mutable_row_set,
            immutable_row_sets,
            mutable_selection: ScanSelection::AllRows,
            immutable_selection: ScanSelection::AllRows,
            sst_selections,
            residual_predicate,
            projected_schema,
            scan_schema,
            limit,
            read_ts,
            _snapshot: self.table_snapshot().clone(),
        })
    }
}

async fn prune_sst_selection<E>(
    fs: Arc<dyn DynFs>,
    data_path: &fusio::path::Path,
    predicate: &Expr,
    read_ts: Timestamp,
    scan_schema: &SchemaRef,
    key_schema: &SchemaRef,
    executor: E,
) -> Result<(SstSelection, bool), SsTableError>
where
    E: Executor + Clone + 'static,
{
    let file = fs.open(data_path).await.map_err(SsTableError::Fs)?;
    let size = file.size().await.map_err(SsTableError::Fs)?;
    let mut reader = AsyncReader::new(file, size, UnpinExec(executor))
        .await
        .map_err(SsTableError::Fs)?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .load_and_finish(&mut reader, size)
        .await
        .map_err(SsTableError::Parquet)?;
    let metadata = Arc::new(metadata);
    validate_page_indexes(data_path, metadata.as_ref())?;
    let options = ArrowReaderOptions::new().with_page_index(true);
    let arrow_metadata = ArrowReaderMetadata::try_new(Arc::clone(&metadata), options)
        .map_err(SsTableError::Parquet)?;
    let schema = arrow_metadata.schema();
    let requires_residual = split_predicate_for_row_filter(predicate, schema)
        .residual
        .is_some();
    let commit_predicate = Expr::lt_eq(MVCC_COMMIT_COL, ScalarValue::UInt64(Some(read_ts.get())));
    let prune_predicate = if matches!(predicate, Expr::True) {
        commit_predicate
    } else {
        Expr::and(vec![predicate.clone(), commit_predicate])
    };
    let prune_result = PruneRequest::new(metadata.as_ref(), schema.as_ref())
        .with_predicate(&prune_predicate)
        .enable_page_index(true)
        .prune();
    let mut row_groups = prune_result.row_groups().to_vec();
    // Preserve PK-ascending scan order by keeping row groups in file order.
    row_groups.sort_unstable();
    row_groups.dedup();
    let file_total_rows = usize::try_from(metadata.file_metadata().num_rows()).map_err(|_| {
        SsTableError::Parquet(ParquetError::General(
            "parquet row count exceeds usize::MAX".to_string(),
        ))
    })?;
    let selected_row_groups_rows = if row_groups.is_empty() {
        0usize
    } else {
        row_groups
            .iter()
            .map(|idx| {
                usize::try_from(metadata.row_group(*idx).num_rows()).map_err(|_| {
                    SsTableError::Parquet(ParquetError::General(
                        "parquet row group count exceeds usize::MAX".to_string(),
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<usize>()
    };
    let row_set = match prune_result.row_selection().cloned() {
        Some(selection) => {
            let total_rows = if row_groups.is_empty() {
                file_total_rows
            } else {
                selected_row_groups_rows
            };
            RowSet::from_row_selection(total_rows, selection).map_err(|err| {
                SsTableError::RowSelection {
                    reason: err.to_string(),
                }
            })?
        }
        None => {
            let total_rows = if row_groups.is_empty() {
                file_total_rows
            } else {
                selected_row_groups_rows
            };
            RowSet::all(total_rows)
        }
    };
    let total_row_groups = metadata.num_row_groups();
    let row_groups = if row_groups.len() == total_row_groups {
        None
    } else {
        Some(row_groups)
    };

    let mut required = BTreeSet::new();
    for field in scan_schema.fields() {
        required.insert(field.name().to_string());
    }
    for field in key_schema.fields() {
        required.insert(field.name().to_string());
    }
    required.insert(MVCC_COMMIT_COL.to_string());

    let mut remaining = required;
    let mut projected_fields = Vec::new();
    let mut root_indices = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if remaining.remove(field.name()) {
            projected_fields.push(field.clone());
            root_indices.push(idx);
        }
    }

    if let Some(missing) = remaining.iter().next() {
        return Err(KeyExtractError::NoSuchField {
            name: missing.to_string(),
        }
        .into());
    }

    let projected_schema = Arc::new(Schema::new(projected_fields));
    let projection = ProjectionMask::roots(arrow_metadata.parquet_schema(), root_indices);

    Ok((
        SstSelection {
            row_groups,
            row_set,
            metadata,
            projection,
            projected_schema,
            delete_selection: None,
        },
        requires_residual,
    ))
}

fn schema_projection_indices(
    base_schema: &SchemaRef,
    target_schema: &SchemaRef,
) -> Result<Vec<usize>, KeyExtractError> {
    let mut indices = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let Some((idx, _)) = base_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, candidate)| candidate.name() == field.name())
        else {
            return Err(KeyExtractError::NoSuchField {
                name: field.name().to_string(),
            });
        };
        indices.push(idx);
    }
    Ok(indices)
}

async fn plan_delete_sidecar_selection<E>(
    fs: Arc<dyn DynFs>,
    delete_path: &fusio::path::Path,
    key_schema: &SchemaRef,
    executor: E,
) -> Result<DeleteSelection, SsTableError>
where
    E: Executor + Clone + 'static,
{
    let file = fs.open(delete_path).await.map_err(SsTableError::Fs)?;
    let size = file.size().await.map_err(SsTableError::Fs)?;
    let mut reader = AsyncReader::new(file, size, UnpinExec(executor))
        .await
        .map_err(SsTableError::Fs)?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .load_and_finish(&mut reader, size)
        .await
        .map_err(SsTableError::Parquet)?;
    validate_page_indexes(delete_path, &metadata)?;
    let options = ArrowReaderOptions::new().with_page_index(true);
    let metadata = Arc::new(metadata);
    let arrow_metadata = ArrowReaderMetadata::try_new(Arc::clone(&metadata), options)
        .map_err(SsTableError::Parquet)?;
    let file_schema = arrow_metadata.schema();
    let parquet_schema = arrow_metadata.parquet_schema();

    let mut required = BTreeSet::new();
    for field in key_schema.fields() {
        required.insert(field.name().to_string());
    }
    required.insert(MVCC_COMMIT_COL.to_string());

    let mut remaining = required;
    let mut root_indices = Vec::new();
    for (idx, field) in file_schema.fields().iter().enumerate() {
        if remaining.remove(field.name()) {
            root_indices.push(idx);
        }
    }

    if let Some(missing) = remaining.iter().next() {
        return Err(KeyExtractError::NoSuchField {
            name: missing.to_string(),
        }
        .into());
    }

    let projection = ProjectionMask::roots(parquet_schema, root_indices);
    Ok(DeleteSelection {
        metadata,
        projection,
    })
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

        let has_non_sst = !plan.mutable_row_set.is_empty() || !plan.immutable_indexes.is_empty();
        let ScanPlan {
            pushdown_predicate,
            residual_predicate,
            limit,
            ..
        } = plan;
        let needs_post_filter =
            residual_predicate.is_some() || (pushdown_predicate.is_some() && has_non_sst);
        let limit_for_merge = if needs_post_filter { None } else { limit };
        let limit_for_package = if needs_post_filter { limit } else { None };
        let merge = MergeStream::from_vec(streams, limit_for_merge, Some(Order::Asc))
            .await
            .map_err(crate::db::DBError::from)?;
        let package = PackageStream::with_limit(
            DEFAULT_SCAN_BATCH_ROWS,
            merge,
            Arc::clone(&scan_schema),
            Arc::clone(&result_projection),
            pushdown_predicate,
            residual_predicate,
            limit_for_package,
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
        let scan_schema = Arc::clone(&plan.scan_schema);
        let key_schema = self.extractor().key_schema();
        if !plan.mutable_row_set.is_empty() {
            let mutable_scan = match &plan.mutable_selection {
                ScanSelection::AllRows | ScanSelection::KeyRange(_) | ScanSelection::Sst(_) => {
                    // TODO: apply key-range/memtable pruning once selection is wired.
                    OwnedMutableScan::from_guard(
                        self.mem.read(),
                        Some(Arc::clone(&scan_schema)),
                        plan.read_ts,
                    )?
                }
            };
            streams.push(ScanStream::from(mutable_scan));
        }

        let immutables: Vec<Arc<ImmutableSegment>> = {
            let seal = self.seal_state_lock();
            plan.immutable_indexes
                .iter()
                .filter_map(|idx| seal.immutables.get(*idx).cloned())
                .collect()
        };
        for segment in immutables {
            let owned = match &plan.immutable_selection {
                ScanSelection::AllRows | ScanSelection::KeyRange(_) | ScanSelection::Sst(_) => {
                    // TODO: apply key-range/immutable pruning once selection is wired.
                    OwnedImmutableScan::from_arc(
                        Arc::clone(&segment),
                        Some(Arc::clone(&scan_schema)),
                        plan.read_ts,
                    )?
                }
            };
            streams.push(ScanStream::from(owned));
        }

        // Add SSTable scans for each SST entry in the plan
        for sst in plan.sst_selections() {
            let selection = match &sst.selection {
                ScanSelection::Sst(selection) => selection,
                ScanSelection::AllRows => {
                    return Err(crate::db::DBError::SsTable(
                        SsTableError::InvalidScanSelection {
                            selection: "AllRows",
                        },
                    ));
                }
                ScanSelection::KeyRange(_) => {
                    return Err(crate::db::DBError::SsTable(
                        SsTableError::InvalidScanSelection {
                            selection: "KeyRange",
                        },
                    ));
                }
            };
            let data_path = sst.entry.data_path().clone();
            let executor: E = (**self.executor()).clone();

            let projected_schema = Arc::clone(&selection.projected_schema);
            let projection_indices = schema_projection_indices(&projected_schema, &scan_schema)?;
            let key_indices = schema_projection_indices(&projected_schema, &key_schema)?;
            let data_extractor: Arc<dyn KeyProjection> =
                projection_for_columns(projected_schema, key_indices)?.into();

            let options = ParquetStreamOptions {
                projection: Some(selection.projection.clone()),
                row_groups: selection.row_groups.clone(),
                row_selection: selection.row_set.to_row_selection(),
                row_filter_predicate: plan.pushdown_predicate.as_ref(),
            };
            let data_stream = open_parquet_stream_with_metadata(
                Arc::clone(&self.fs),
                data_path,
                Arc::clone(&selection.metadata),
                options,
                executor.clone(),
            )
            .await
            .map_err(crate::db::DBError::SsTable)?;

            // Open delete sidecar stream if present (streaming merge, no eager loading)
            let delete_stream_with_extractor = if let Some(delete_path) = sst.entry.delete_path() {
                let delete_selection = selection.delete_selection.as_ref().ok_or_else(|| {
                    crate::db::DBError::SsTable(SsTableError::InvalidScanSelection {
                        selection: "missing delete sidecar selection",
                    })
                })?;
                let delete_path = delete_path.clone();
                let options = ParquetStreamOptions {
                    projection: Some(delete_selection.projection.clone()),
                    row_groups: None,
                    row_selection: None,
                    row_filter_predicate: None,
                };
                let stream = open_parquet_stream_with_metadata(
                    Arc::clone(&self.fs),
                    delete_path,
                    Arc::clone(&delete_selection.metadata),
                    options,
                    executor.clone(),
                )
                .await
                .map_err(crate::db::DBError::SsTable)?;
                Some(DeleteStreamWithExtractor {
                    stream,
                    extractor: Arc::clone(self.delete_extractor()),
                })
            } else {
                if selection.delete_selection.is_some() {
                    return Err(crate::db::DBError::SsTable(
                        SsTableError::InvalidScanSelection {
                            selection: "unexpected delete sidecar selection",
                        },
                    ));
                }
                None
            };

            let sstable_scan = SstableScan::new(
                data_stream,
                delete_stream_with_extractor,
                data_extractor,
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
///     db::{DB, DbBuilder, Expr, ScalarValue},
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
///     let pred = Expr::eq("id", ScalarValue::Utf8(Some("a".to_string())));
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
    predicate: Option<Expr>,
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
    pub fn filter(mut self, predicate: Expr) -> Self {
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

        let predicate = predicate.unwrap_or(Expr::True);

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
    let has_txn_scan = txn_scan.is_some();
    let streams = db.build_scan_streams(&plan, txn_scan).await?;

    if streams.is_empty() {
        let stream = stream::empty::<Result<RecordBatch, crate::db::DBError>>();
        return Ok(Box::pin(stream)
            as Pin<
                Box<dyn Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a>,
            >);
    }

    let has_non_sst =
        has_txn_scan || !plan.mutable_row_set.is_empty() || !plan.immutable_indexes.is_empty();
    let ScanPlan {
        pushdown_predicate,
        residual_predicate,
        limit,
        ..
    } = plan;
    let needs_post_filter =
        residual_predicate.is_some() || (pushdown_predicate.is_some() && has_non_sst);
    let limit_for_merge = if needs_post_filter { None } else { limit };
    let limit_for_package = if needs_post_filter { limit } else { None };
    // Only apply limit early when no post-merge filtering is required.
    let merge = MergeStream::from_vec(streams, limit_for_merge, Some(Order::Asc))
        .await
        .map_err(crate::db::DBError::from)?;
    let package = PackageStream::with_limit(
        DEFAULT_SCAN_BATCH_ROWS,
        merge,
        Arc::clone(&scan_schema),
        Arc::clone(&result_projection),
        pushdown_predicate,
        residual_predicate,
        limit_for_package,
    )
    .map_err(crate::db::DBError::from)?;

    let mapped = package.map(|result| result.map_err(crate::db::DBError::from));
    Ok(Box::pin(mapped)
        as Pin<
            Box<dyn Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a>,
        >)
}

fn find_bloom_filter_column(predicate: &Expr) -> Option<&str> {
    match predicate {
        Expr::BloomFilterEq { column, .. } | Expr::BloomFilterInList { column, .. } => {
            Some(column.as_str())
        }
        Expr::And(children) | Expr::Or(children) => children
            .iter()
            .find_map(|child| find_bloom_filter_column(child)),
        Expr::Not(child) => find_bloom_filter_column(child.as_ref()),
        _ => None,
    }
}
