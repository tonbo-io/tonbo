use std::{pin::Pin, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use fusio::executor::{Executor, Timer};
use futures::{Stream, StreamExt, stream};
use predicate::Predicate;
use typed_arrow_dyn::DynRow;

use crate::{
    db::DB,
    extractor::KeyExtractError,
    inmem::{
        immutable::{self, Immutable, memtable::ImmutableVisibleEntry},
        mutable::memtable::DynRowScanEntry,
    },
    key::KeyRow,
    mode::DynMode,
    mvcc::Timestamp,
    ondisk::{
        scan::{SstableScan, load_delete_index},
        sstable::{SsTableError, open_parquet_stream},
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
    pub async fn plan_scan<FS, E>(
        &self,
        db: &DB<DynMode, FS, E>,
        predicate: &Predicate,
        projected_schema: Option<&SchemaRef>,
        limit: Option<usize>,
    ) -> Result<ScanPlan, crate::db::DBError>
    where
        FS: crate::manifest::ManifestFs<E>,
        E: Executor + Timer + Clone,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        let projected_schema = projected_schema.cloned();
        let residual_predicate = Some(predicate.clone());
        let scan_schema = if let Some(projection) = projected_schema.as_ref() {
            projection_with_predicate(&db.mode.schema, projection, residual_predicate.as_ref())?
        } else {
            Arc::clone(&db.mode.schema)
        };
        let seal = db.seal_state_lock();
        let prune_input: Vec<&Immutable<DynMode>> =
            seal.immutables.iter().map(|arc| arc.as_ref()).collect();
        let immutable_indexes = immutable::prune_segments::<DynMode>(&prune_input);
        let read_ts = self.read_view().read_ts();
        Ok(ScanPlan {
            _predicate: predicate.clone(),
            immutable_indexes,
            residual_predicate,
            projected_schema,
            scan_schema,
            limit,
            read_ts,
            _snapshot: self.table_snapshot().clone(),
        })
    }
}

impl<FS, E> DB<DynMode, FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Execute the scan plan with MVCC visibility
    pub async fn execute_scan<'a>(
        &'a self,
        plan: ScanPlan,
    ) -> Result<impl Stream<Item = Result<RecordBatch, crate::db::DBError>> + 'a, crate::db::DBError>
    {
        let result_projection = plan
            .projected_schema
            .clone()
            .unwrap_or_else(|| Arc::clone(&self.mode.schema));
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
            read_ts,
            ..
        } = plan;
        let merge = MergeStream::from_vec(streams, read_ts, limit, Some(Order::Asc))
            .await
            .map_err(crate::db::DBError::from)?;
        let package = PackageStream::new(
            DEFAULT_SCAN_BATCH_ROWS,
            merge,
            Arc::clone(&scan_schema),
            Arc::clone(&result_projection),
            residual_predicate,
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
    ) -> Result<Vec<ScanStream<'a, RecordBatch>>, crate::db::DBError> {
        let mut streams = Vec::new();
        if let Some(txn_scan) = txn_scan {
            streams.push(ScanStream::from(txn_scan));
        }
        let projection_schema = Arc::clone(&plan.scan_schema);
        let mutable_scan = OwnedMutableScan::from_guard(
            self.mem_read(),
            Some(Arc::clone(&projection_schema)),
            plan.read_ts,
        )?;
        streams.push(ScanStream::from(mutable_scan));

        let immutables: Vec<Arc<Immutable<DynMode>>> = {
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
            let data_stream = open_parquet_stream(Arc::clone(&self.fs), data_path, None)
                .await
                .map_err(crate::db::DBError::SsTable)?;

            // Load delete sidecar if present
            let delete_index = if let Some(delete_path) = sst_entry.delete_path() {
                let delete_stream =
                    open_parquet_stream(Arc::clone(&self.fs), delete_path.clone(), None)
                        .await
                        .map_err(crate::db::DBError::SsTable)?;
                // Collect all delete batches and build index
                use futures::StreamExt;
                let mut delete_stream = std::pin::pin!(delete_stream);
                let mut index = std::collections::BTreeMap::new();
                while let Some(batch_result) = delete_stream.next().await {
                    let batch = batch_result
                        .map_err(|e| crate::db::DBError::SsTable(SsTableError::Parquet(e)))?;
                    let batch_index = load_delete_index(&batch, self.mode.extractor.as_ref())
                        .map_err(|e| {
                            crate::db::DBError::Stream(crate::query::stream::StreamError::SsTable(
                                e,
                            ))
                        })?;
                    for (key, ts) in batch_index {
                        // Keep the latest (max) delete timestamp for each key across batches
                        index
                            .entry(key)
                            .and_modify(|cur: &mut crate::mvcc::Timestamp| *cur = (*cur).max(ts))
                            .or_insert(ts);
                    }
                }
                index
            } else {
                std::collections::BTreeMap::new()
            };

            // Calculate projection indices for user columns (exclude _commit_ts)
            let projection_indices: Vec<usize> = (0..projection_schema.fields().len()).collect();

            let sstable_scan = SstableScan::new(
                data_stream,
                self.mode.extractor.as_ref(),
                projection_indices,
                Some(Order::Asc),
                plan.read_ts,
                delete_index,
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
        let mem = self.mem_read();
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
}
