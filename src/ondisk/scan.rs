//! SSTable scan with MVCC visibility filtering.

use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::{Array, RecordBatch, UInt64Array};
use fusio::executor::NoopExecutor;
use fusio_parquet::reader::AsyncReader;
use futures::{Stream, ready};
use parquet::{arrow::async_reader::ParquetRecordBatchStream, errors::ParquetError};
use pin_project_lite::pin_project;
use thiserror::Error;
use typed_arrow_dyn::{DynProjection, DynRow, DynRowRaw, DynSchema, DynViewError};

use crate::{
    extractor::{KeyExtractError, KeyProjection},
    inmem::immutable::memtable::{MVCC_COMMIT_COL, MvccColumns},
    key::{KeyOwned, KeyTsOwned, KeyTsViewRaw},
    mvcc::Timestamp,
    query::stream::Order,
};

/// Errors surfaced while streaming rows out of SSTables.
#[derive(Debug, Error)]
pub enum SstableScanError {
    /// Parquet error
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    /// Failure to reconstruct the logical key for range filtering.
    #[error("key extraction failed: {0}")]
    Key(#[from] KeyExtractError),
    /// Failure to read row from record batch
    #[error("dynamic view of record batch failure: {0}")]
    DynView(#[from] DynViewError),
    /// Key ownership conversion failed.
    #[error("key owned conversion failed: {0}")]
    KeyOwned(#[from] crate::key::KeyOwnedError),
}

/// A reference to an SSTable row that keeps the underlying batch alive.
///
/// This struct bundles an `Arc<RecordBatch>` with borrowed views into that batch,
/// ensuring the batch remains valid as long as the row reference exists. This enables
/// zero-copy streaming through the merge pipeline until final materialization.
pub struct SstableRowRef {
    /// The batch backing the views. Prevents deallocation while views are in use.
    _batch: Arc<RecordBatch>,
    /// Key + timestamp view into the batch.
    key_ts: KeyTsViewRaw,
    /// Row data view into the batch.
    row: DynRowRaw,
}

impl SstableRowRef {
    /// Create a new row reference from an Arc'd batch and views into it.
    ///
    /// # Safety
    /// The caller must ensure `key_ts` and `row` point into `batch`.
    pub(crate) fn new(batch: Arc<RecordBatch>, key_ts: KeyTsViewRaw, row: DynRowRaw) -> Self {
        Self {
            _batch: batch,
            key_ts,
            row,
        }
    }

    /// Borrow the key + timestamp view.
    pub fn key_ts(&self) -> &KeyTsViewRaw {
        &self.key_ts
    }

    /// Borrow the row data view.
    pub fn row(&self) -> &DynRowRaw {
        &self.row
    }

    /// Convert the row to owned data, consuming the reference.
    pub fn into_row_owned(self) -> Result<DynRow, DynViewError> {
        self.row.into_owned()
    }
}

impl std::fmt::Debug for SstableRowRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstableRowRef")
            .field("key_ts", &self.key_ts)
            .finish()
    }
}

/// Entry yielded by SSTable visible scan - either a data row or a tombstone.
pub enum SstableVisibleEntry {
    /// A visible data row with Arc-backed views for zero-copy streaming.
    Row(SstableRowRef),
    /// A tombstone indicating deletion (owned since it comes from the delete sidecar).
    Tombstone(KeyTsOwned),
}

impl std::fmt::Debug for SstableVisibleEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Row(row_ref) => f.debug_tuple("Row").field(row_ref).finish(),
            Self::Tombstone(key_ts) => f.debug_tuple("Tombstone").field(key_ts).finish(),
        }
    }
}

// SSTable scan with MVCC visibility filtering.
//
// This scan:
// 1. Filters rows by `read_ts` - only rows with `commit_ts <= read_ts` are visible
// 2. Handles delete sidecar - tombstones override data rows with older timestamps
// 3. Deduplicates by key - emits only the latest visible version per key
pin_project! {
    pub(crate) struct SstableScan<'t> {
        #[pin]
        data_stream: ParquetRecordBatchStream<AsyncReader<NoopExecutor>>,
        iter: Option<RecordBatchIterator<'t>>,
        projection_indices: Vec<usize>,
        extractor: &'t dyn KeyProjection,
        order: Option<Order>,
        read_ts: Timestamp,
        // Delete index: key -> delete timestamp
        delete_index: BTreeMap<KeyOwned, Timestamp>,
        // Pending deletes to emit (keys with deletes but no data rows seen yet)
        pending_deletes: Vec<(KeyOwned, Timestamp)>,
        pending_delete_idx: usize,
        // Current key being processed (for dedup)
        current_key: Option<KeyOwned>,
        // Whether we've emitted an entry for the current key
        emitted_for_key: bool,
        // Data stream exhausted, now emitting remaining deletes
        data_exhausted: bool,
        // Deferred entry to re-process after emitting pending tombstones
        deferred_entry: Option<(Arc<RecordBatch>, KeyTsViewRaw, DynRowRaw)>,
    }
}

impl<'t> SstableScan<'t> {
    /// Create a new SSTable scan with MVCC visibility filtering.
    ///
    /// # Arguments
    /// * `data_stream` - Parquet stream for data rows
    /// * `extractor` - Key extractor for identifying primary keys
    /// * `projection_indices` - Column indices to project
    /// * `order` - Scan order (Asc/Desc)
    /// * `read_ts` - Snapshot timestamp for visibility filtering
    /// * `delete_sidecar` - Pre-loaded delete entries (key -> delete timestamp)
    pub fn new(
        data_stream: ParquetRecordBatchStream<AsyncReader<NoopExecutor>>,
        extractor: &'t dyn KeyProjection,
        projection_indices: Vec<usize>,
        order: Option<Order>,
        read_ts: Timestamp,
        delete_sidecar: BTreeMap<KeyOwned, Timestamp>,
    ) -> Self {
        // Collect visible deletes sorted by key for later emission
        let pending_deletes: Vec<_> = delete_sidecar
            .iter()
            .filter(|(_, ts)| **ts <= read_ts)
            .map(|(k, ts)| (k.clone(), *ts))
            .collect();

        Self {
            data_stream,
            iter: None,
            projection_indices,
            extractor,
            order,
            read_ts,
            delete_index: delete_sidecar,
            pending_deletes,
            pending_delete_idx: 0,
            current_key: None,
            emitted_for_key: false,
            data_exhausted: false,
            deferred_entry: None,
        }
    }
}

impl<'t> Stream for SstableScan<'t> {
    type Item = Result<SstableVisibleEntry, SstableScanError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        // If data is exhausted, emit remaining visible deletes as tombstones
        if *this.data_exhausted {
            while *this.pending_delete_idx < this.pending_deletes.len() {
                let (key, ts) = &this.pending_deletes[*this.pending_delete_idx];
                *this.pending_delete_idx += 1;

                // Skip if not visible
                if *ts > *this.read_ts {
                    continue;
                }

                // Create tombstone entry (owned since delete sidecar data is stable)
                let key_ts_owned = KeyTsOwned::new(key.clone(), *ts);
                return Poll::Ready(Some(Ok(SstableVisibleEntry::Tombstone(key_ts_owned))));
            }
            return Poll::Ready(None);
        }

        loop {
            // First check if we have a deferred entry from a previous tombstone emission
            let next_entry = if let Some(deferred) = this.deferred_entry.take() {
                Some(Ok(deferred))
            } else if let Some(iter) = this.iter.as_mut() {
                iter.next()
            } else {
                None
            };

            // Try to get next entry from current batch iterator or deferred
            match next_entry {
                Some(Ok((batch, key_ts, row))) => {
                    let commit_ts = key_ts.timestamp();

                    // Visibility check: skip if not visible at read_ts
                    if commit_ts > *this.read_ts {
                        continue;
                    }

                    // Extract owned key for comparison
                    let key_owned = match KeyOwned::from_key_row(key_ts.key()) {
                        Ok(k) => k,
                        Err(e) => return Poll::Ready(Some(Err(SstableScanError::KeyOwned(e)))),
                    };

                    // Check if this is a new key
                    let is_new_key = this
                        .current_key
                        .as_ref()
                        .map(|k| k != &key_owned)
                        .unwrap_or(true);

                    if is_new_key {
                        // Before processing new key, emit any pending deletes that come before
                        // it
                        while *this.pending_delete_idx < this.pending_deletes.len() {
                            let (pending_key, pending_ts) =
                                &this.pending_deletes[*this.pending_delete_idx];

                            if pending_key < &key_owned {
                                // This delete key comes before current data key
                                *this.pending_delete_idx += 1;

                                if *pending_ts <= *this.read_ts {
                                    let tombstone_key =
                                        KeyTsOwned::new(pending_key.clone(), *pending_ts);
                                    // Save current entry for next poll
                                    *this.deferred_entry = Some((batch, key_ts, row));
                                    *this.current_key = Some(key_owned);
                                    *this.emitted_for_key = false;
                                    return Poll::Ready(Some(Ok(SstableVisibleEntry::Tombstone(
                                        tombstone_key,
                                    ))));
                                }
                            } else if pending_key == &key_owned {
                                // Key matches - will be handled below
                                *this.pending_delete_idx += 1;
                                break;
                            } else {
                                // Delete key is after current data key
                                break;
                            }
                        }

                        *this.current_key = Some(key_owned.clone());
                        *this.emitted_for_key = false;
                    }

                    // Dedup: skip if already emitted for this key
                    if *this.emitted_for_key {
                        continue;
                    }

                    // Check if this row is superseded by a delete
                    // Use >= to handle ties: if delete_ts == commit_ts, delete wins
                    // (e.g., insert+delete in same transaction yields no visible row)
                    if let Some(delete_ts) = this.delete_index.get(&key_owned)
                        && *delete_ts <= *this.read_ts
                        && *delete_ts >= commit_ts
                    {
                        // Delete wins - emit tombstone
                        *this.emitted_for_key = true;
                        let tombstone_key = KeyTsOwned::new(key_owned, *delete_ts);
                        return Poll::Ready(Some(Ok(SstableVisibleEntry::Tombstone(
                            tombstone_key,
                        ))));
                    }

                    // Data wins - emit row with Arc-backed reference for zero-copy
                    *this.emitted_for_key = true;
                    let row_ref = SstableRowRef::new(batch, key_ts, row);
                    return Poll::Ready(Some(Ok(SstableVisibleEntry::Row(row_ref))));
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => {
                    // No deferred entry and iterator exhausted (or no iterator yet)
                    if this.iter.is_some() {
                        *this.iter = None;
                        continue;
                    }
                }
            }

            // Need to fetch next batch (no iterator exists)
            // Fetch next batch from data stream
            let data = ready!(this.data_stream.as_mut().poll_next(cx)).transpose()?;
            let data = match data {
                Some(d) => d,
                None => {
                    // Data stream exhausted, switch to emitting remaining deletes
                    *this.data_exhausted = true;
                    return self.poll_next(cx);
                }
            };
            let mvcc = decode_mvcc_from_data(&data)?;
            let projection_indices = this.projection_indices.clone();
            let extractor = *this.extractor;
            let order = *this.order;
            *this.iter = Some(RecordBatchIterator::new(
                data,
                projection_indices,
                extractor,
                mvcc,
                order,
            )?);
            continue;
        }
    }
}

struct RecordBatchIterator<'t> {
    /// Arc-wrapped batch to allow sharing with yielded entries.
    batch: Arc<RecordBatch>,
    extractor: &'t dyn KeyProjection,
    dyn_schema: DynSchema,
    projection: DynProjection,
    mvcc: MvccColumns,
    offset: usize,
    remaining: usize,
    step: isize,
}

impl<'t> RecordBatchIterator<'t> {
    pub(crate) fn new(
        record_batch: RecordBatch,
        projection_indices: Vec<usize>,
        extractor: &'t dyn KeyProjection,
        mvcc: MvccColumns,
        _order: Option<Order>,
    ) -> Result<Self, SstableScanError> {
        let num_rows = record_batch.num_rows();
        let mvcc_len = mvcc.commit_ts.len();
        if mvcc_len != num_rows {
            return Err(SstableScanError::Key(
                KeyExtractError::TombstoneLengthMismatch {
                    expected: num_rows,
                    actual: mvcc_len,
                },
            ));
        }
        let offset = 0;
        let step = 1;

        // Build dyn_schema and projection from the actual batch schema
        // to ensure they match exactly with the data we're reading
        let batch_schema = record_batch.schema();
        let dyn_schema = DynSchema::from_ref(batch_schema.clone());
        let projection = DynProjection::from_indices(batch_schema.as_ref(), projection_indices)
            .map_err(crate::extractor::map_view_err)?;

        Ok(Self {
            batch: Arc::new(record_batch),
            extractor,
            dyn_schema,
            projection,
            mvcc,
            offset,
            remaining: num_rows,
            step,
        })
    }
}

impl<'t> Iterator for RecordBatchIterator<'t> {
    /// Yields (Arc<RecordBatch>, KeyTsViewRaw, DynRowRaw) - the Arc keeps the batch alive
    /// while the views reference it.
    type Item = Result<(Arc<RecordBatch>, KeyTsViewRaw, DynRowRaw), SstableScanError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let row_idx = self.offset;
        self.remaining -= 1;
        if self.step < 0 {
            let magnitude = (-self.step) as usize;
            self.offset = self.offset.saturating_sub(magnitude);
        } else if self.step > 0 {
            self.offset = self.offset.saturating_add(self.step as usize);
        }

        // Note: tombstone check not needed here - data file doesn't contain tombstones,
        // they're in the delete sidecar which is handled by SstableScan

        let key_rows = match self.extractor.project_view(&self.batch, &[row_idx]) {
            Ok(rows) => rows,
            Err(err) => return Some(Err(SstableScanError::Key(err))),
        };
        let key_row = match key_rows.into_iter().next() {
            Some(row) => row,
            None => {
                return Some(Err(SstableScanError::Key(KeyExtractError::RowOutOfBounds(
                    row_idx,
                    self.batch.num_rows(),
                ))));
            }
        };

        let commit_ts = match self.mvcc.commit_ts.get(row_idx).copied() {
            Some(ts) => ts,
            None => {
                return Some(Err(SstableScanError::Key(KeyExtractError::RowOutOfBounds(
                    row_idx,
                    self.mvcc.commit_ts.len(),
                ))));
            }
        };

        let row = match self
            .projection
            .project_row_raw(&self.dyn_schema, &self.batch, row_idx)
        {
            Ok(row) => row,
            Err(err) => return Some(Err(SstableScanError::DynView(err))),
        };

        let key = KeyTsViewRaw::new(key_row, commit_ts);
        Some(Ok((Arc::clone(&self.batch), key, row)))
    }
}

fn decode_mvcc_from_data(batch: &RecordBatch) -> Result<MvccColumns, SstableScanError> {
    let commit_binding = batch.column_by_name(MVCC_COMMIT_COL);
    let commit = commit_binding
        .as_ref()
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            SstableScanError::Parquet(ParquetError::ArrowError(
                "commit_ts column missing or not UInt64".into(),
            ))
        })?;
    if commit.null_count() > 0 {
        return Err(SstableScanError::Parquet(ParquetError::ArrowError(
            "commit_ts column contains nulls".into(),
        )));
    }
    let mut commit_ts = Vec::with_capacity(commit.len());
    for i in 0..commit.len() {
        commit_ts.push(Timestamp::new(commit.value(i)));
    }
    // Tombstones are handled via delete sidecar, not in data file
    let tombstones = vec![false; commit.len()];
    Ok(MvccColumns::new(commit_ts, tombstones))
}

/// Load delete sidecar from a Parquet batch into a BTreeMap for efficient lookup.
pub(crate) fn load_delete_index(
    batch: &RecordBatch,
    extractor: &dyn KeyProjection,
) -> Result<BTreeMap<KeyOwned, Timestamp>, SstableScanError> {
    let mut index = BTreeMap::new();

    let commit_col = batch
        .column_by_name(MVCC_COMMIT_COL)
        .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            SstableScanError::Parquet(ParquetError::ArrowError(
                "delete sidecar missing commit_ts column".into(),
            ))
        })?;

    for row_idx in 0..batch.num_rows() {
        let key_rows = extractor.project_view(batch, &[row_idx])?;
        let key_row = key_rows.into_iter().next().ok_or(SstableScanError::Key(
            KeyExtractError::RowOutOfBounds(row_idx, batch.num_rows()),
        ))?;
        let key = KeyOwned::from_key_row(&key_row)?;
        let ts = Timestamp::new(commit_col.value(row_idx));

        // Keep the latest delete timestamp for each key
        index
            .entry(key)
            .and_modify(|existing: &mut Timestamp| {
                if ts > *existing {
                    *existing = ts;
                }
            })
            .or_insert(ts);
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::{Int32Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{disk::LocalFs, dynamic::DynFs, executor::NoopExecutor, path::Path};
    use fusio_parquet::writer::AsyncWriter;
    use futures::StreamExt;
    use parquet::arrow::AsyncArrowWriter;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        inmem::immutable::memtable::MVCC_COMMIT_COL, mvcc::Timestamp,
        ondisk::sstable::open_parquet_stream,
    };

    /// Helper to write a data Parquet file directly with custom commit timestamps
    async fn write_data_parquet(
        fs: Arc<dyn DynFs>,
        path: Path,
        rows: Vec<(&str, i32, u64)>, // (key, value, commit_ts)
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));

        let ids: Vec<&str> = rows.iter().map(|(k, _, _)| *k).collect();
        let values: Vec<i32> = rows.iter().map(|(_, v, _)| *v).collect();
        let timestamps: Vec<u64> = rows.iter().map(|(_, _, ts)| *ts).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int32Array::from(values)),
                Arc::new(UInt64Array::from(timestamps)),
            ],
        )
        .expect("batch");

        let file = fs
            .open_options(
                &path,
                fusio::fs::OpenOptions::default().create(true).write(true),
            )
            .await
            .expect("open file");
        let writer = AsyncWriter::new(file, NoopExecutor);
        let mut arrow_writer =
            AsyncArrowWriter::try_new(writer, Arc::clone(&schema), None).expect("arrow writer");
        arrow_writer.write(&batch).await.expect("write batch");
        arrow_writer.close().await.expect("close");
    }

    /// Helper to write a delete sidecar Parquet file directly
    async fn write_delete_parquet(
        fs: Arc<dyn DynFs>,
        path: Path,
        deletes: Vec<(&str, u64)>, // (key, delete_ts)
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));

        let ids: Vec<&str> = deletes.iter().map(|(k, _)| *k).collect();
        let timestamps: Vec<u64> = deletes.iter().map(|(_, ts)| *ts).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(UInt64Array::from(timestamps)),
            ],
        )
        .expect("batch");

        let file = fs
            .open_options(
                &path,
                fusio::fs::OpenOptions::default().create(true).write(true),
            )
            .await
            .expect("open file");
        let writer = AsyncWriter::new(file, NoopExecutor);
        let mut arrow_writer =
            AsyncArrowWriter::try_new(writer, Arc::clone(&schema), None).expect("arrow writer");
        arrow_writer.write(&batch).await.expect("write batch");
        arrow_writer.close().await.expect("close");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_ts_filters_future_data() {
        // Test: rows with commit_ts > read_ts should not be visible
        let tmpdir = tempdir().expect("tempdir");
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let root = Path::from(tmpdir.path().to_string_lossy().to_string());

        // User schema (without commit_ts column - that's in the stored file)
        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(Arc::clone(&user_schema), 0).expect("extractor");

        // Write data Parquet file with rows at different timestamps
        let data_path = root.child("data.parquet");
        write_data_parquet(
            Arc::clone(&fs),
            data_path.clone(),
            vec![
                ("a", 1, 10), // visible at read_ts=20
                ("b", 2, 20), // visible at read_ts=20
                ("c", 3, 30), // NOT visible at read_ts=20
            ],
        )
        .await;

        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None)
            .await
            .expect("open stream");

        let read_ts = Timestamp::new(20);
        let delete_index = BTreeMap::new();
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::new(
            data_stream,
            extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
            delete_index,
        );

        let mut keys = Vec::new();
        while let Some(entry) = scan.next().await {
            match entry.expect("entry") {
                SstableVisibleEntry::Row(row_ref) => {
                    let key = KeyOwned::from_key_row(row_ref.key_ts().key()).expect("key");
                    keys.push(key.as_utf8().expect("utf8").to_string());
                }
                SstableVisibleEntry::Tombstone(_) => {}
            }
        }

        assert_eq!(
            keys,
            vec!["a", "b"],
            "row 'c' at ts=30 should be invisible at read_ts=20"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_sidecar_hides_data() {
        // Test: delete_ts >= commit_ts should hide the row
        let tmpdir = tempdir().expect("tempdir");
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let root = Path::from(tmpdir.path().to_string_lossy().to_string());

        // User schema for data file extractor (matches data columns minus _commit_ts)
        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let data_extractor =
            crate::extractor::projection_for_field(Arc::clone(&user_schema), 0).expect("extractor");

        // Key-only schema for delete sidecar extractor
        let key_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let delete_extractor = crate::extractor::projection_for_field(Arc::clone(&key_schema), 0)
            .expect("delete extractor");

        // Write data Parquet file
        let data_path = root.child("data.parquet");
        write_data_parquet(
            Arc::clone(&fs),
            data_path.clone(),
            vec![
                ("a", 1, 10), // deleted at ts=15
                ("b", 2, 20), // NOT deleted
                ("c", 3, 30), // deleted at same ts (tie: delete wins)
            ],
        )
        .await;

        // Write delete sidecar file
        let delete_path = root.child("delete.parquet");
        write_delete_parquet(
            Arc::clone(&fs),
            delete_path.clone(),
            vec![
                ("a", 15), // delete after data
                ("c", 30), // delete at same timestamp as data
            ],
        )
        .await;

        // Load delete index with key-only extractor
        let mut delete_index = BTreeMap::new();
        let del_stream = open_parquet_stream(Arc::clone(&fs), delete_path, None)
            .await
            .expect("delete stream");
        let mut del_stream = std::pin::pin!(del_stream);
        while let Some(batch) = del_stream.next().await {
            let batch = batch.expect("batch");
            let batch_index = load_delete_index(&batch, delete_extractor.as_ref()).expect("index");
            for (k, ts) in batch_index {
                delete_index
                    .entry(k)
                    .and_modify(|cur: &mut Timestamp| *cur = (*cur).max(ts))
                    .or_insert(ts);
            }
        }

        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None)
            .await
            .expect("open stream");

        let read_ts = Timestamp::MAX;
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::new(
            data_stream,
            data_extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
            delete_index,
        );

        let mut keys = Vec::new();
        let mut tombstones = Vec::new();
        while let Some(entry) = scan.next().await {
            match entry.expect("entry") {
                SstableVisibleEntry::Row(row_ref) => {
                    let key = KeyOwned::from_key_row(row_ref.key_ts().key()).expect("key");
                    keys.push(key.as_utf8().expect("utf8").to_string());
                }
                SstableVisibleEntry::Tombstone(key_ts) => {
                    tombstones.push(key_ts.key().as_utf8().expect("utf8").to_string());
                }
            }
        }

        assert_eq!(keys, vec!["b"], "'a' and 'c' should be hidden by deletes");
        assert!(
            tombstones.contains(&"a".to_string()),
            "tombstone for 'a' should be emitted"
        );
        assert!(
            tombstones.contains(&"c".to_string()),
            "tombstone for 'c' should be emitted (same-ts delete wins)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn source_level_dedup_emits_one_version_per_key() {
        // Test: when SSTable has multiple versions of same key, only latest visible is emitted
        let tmpdir = tempdir().expect("tempdir");
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let root = Path::from(tmpdir.path().to_string_lossy().to_string());

        // User schema for extractor (matches data columns minus _commit_ts)
        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(Arc::clone(&user_schema), 0).expect("extractor");

        // Write data with multiple versions of same key (sorted by key, then ts desc)
        let data_path = root.child("data.parquet");
        write_data_parquet(
            Arc::clone(&fs),
            data_path.clone(),
            vec![
                ("a", 100, 30), // latest version of 'a'
                ("a", 10, 20),  // older version of 'a'
                ("a", 1, 10),   // oldest version of 'a'
                ("b", 2, 15),   // only version of 'b'
            ],
        )
        .await;

        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None)
            .await
            .expect("open stream");

        let read_ts = Timestamp::MAX;
        let delete_index = BTreeMap::new();
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::new(
            data_stream,
            extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
            delete_index,
        );

        let mut results: Vec<(String, i32)> = Vec::new();
        while let Some(entry) = scan.next().await {
            if let SstableVisibleEntry::Row(row_ref) = entry.expect("entry") {
                let key = KeyOwned::from_key_row(row_ref.key_ts().key()).expect("key");
                let key_str = key.as_utf8().expect("utf8").to_string();
                let row = row_ref.into_row_owned().expect("row");
                let value = match &row.0[1] {
                    Some(typed_arrow_dyn::DynCell::I32(v)) => *v,
                    _ => panic!("expected i32 value"),
                };
                results.push((key_str, value));
            }
        }

        assert_eq!(
            results,
            vec![("a".to_string(), 100), ("b".to_string(), 2)],
            "should emit only latest version (v=100) for 'a'"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tombstone_only_keys_emitted() {
        // Test: keys that exist only in delete sidecar (no data rows) should emit tombstones
        let tmpdir = tempdir().expect("tempdir");
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let root = Path::from(tmpdir.path().to_string_lossy().to_string());

        // User schema for data file extractor (matches data columns minus _commit_ts)
        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let data_extractor =
            crate::extractor::projection_for_field(Arc::clone(&user_schema), 0).expect("extractor");

        // Key-only schema for delete sidecar extractor
        let key_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let delete_extractor = crate::extractor::projection_for_field(Arc::clone(&key_schema), 0)
            .expect("delete extractor");

        // Write data with only 'b'
        let data_path = root.child("data.parquet");
        write_data_parquet(
            Arc::clone(&fs),
            data_path.clone(),
            vec![("b", 2, 20)], // only data row
        )
        .await;

        // Write delete sidecar with tombstones for 'a', 'c', 'd'
        let delete_path = root.child("delete.parquet");
        write_delete_parquet(
            Arc::clone(&fs),
            delete_path.clone(),
            vec![
                ("a", 10), // tombstone-only (comes before 'b')
                ("c", 30), // tombstone-only (comes after 'b')
                ("d", 40), // tombstone-only (comes after 'b')
            ],
        )
        .await;

        // Load delete index with key-only extractor
        let mut delete_index = BTreeMap::new();
        let del_stream = open_parquet_stream(Arc::clone(&fs), delete_path, None)
            .await
            .expect("delete stream");
        let mut del_stream = std::pin::pin!(del_stream);
        while let Some(batch) = del_stream.next().await {
            let batch = batch.expect("batch");
            let batch_index = load_delete_index(&batch, delete_extractor.as_ref()).expect("index");
            for (k, ts) in batch_index {
                delete_index
                    .entry(k)
                    .and_modify(|cur: &mut Timestamp| *cur = (*cur).max(ts))
                    .or_insert(ts);
            }
        }

        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None)
            .await
            .expect("open stream");

        let read_ts = Timestamp::MAX;
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::new(
            data_stream,
            data_extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
            delete_index,
        );

        let mut data_keys = Vec::new();
        let mut tombstone_keys = Vec::new();
        while let Some(entry) = scan.next().await {
            match entry.expect("entry") {
                SstableVisibleEntry::Row(row_ref) => {
                    let key = KeyOwned::from_key_row(row_ref.key_ts().key()).expect("key");
                    data_keys.push(key.as_utf8().expect("utf8").to_string());
                }
                SstableVisibleEntry::Tombstone(key_ts) => {
                    tombstone_keys.push(key_ts.key().as_utf8().expect("utf8").to_string());
                }
            }
        }

        assert_eq!(data_keys, vec!["b"], "only 'b' should have data");
        // Tombstones should include 'a' (before data), 'c', 'd' (after data exhausted)
        assert!(
            tombstone_keys.contains(&"a".to_string()),
            "tombstone 'a' should be emitted"
        );
        assert!(
            tombstone_keys.contains(&"c".to_string()),
            "tombstone 'c' should be emitted"
        );
        assert!(
            tombstone_keys.contains(&"d".to_string()),
            "tombstone 'd' should be emitted"
        );
    }
}
