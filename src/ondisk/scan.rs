#![allow(dead_code)]
//! SSTable scan with MVCC visibility filtering.

use std::{
    cmp::Ordering,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::{Array, RecordBatch, UInt64Array};
use fusio::executor::Executor;
use fusio_parquet::reader::AsyncReader;
use futures::{Stream, ready};
use parquet::{arrow::async_reader::ParquetRecordBatchStream, errors::ParquetError};
use pin_project_lite::pin_project;
use thiserror::Error;
use typed_arrow_dyn::{DynProjection, DynRow, DynRowRaw, DynSchema, DynViewError};

/// Wrapper around an executor that unconditionally implements `Unpin`.
///
/// # Safety
///
/// All practical executor implementations (tokio Handle, NoopExecutor, etc.) are `Unpin`.
/// Executors are lightweight handles to runtime state and do not contain self-referential
/// data. This wrapper allows `AsyncReader<UnpinExec<E>>` to be `Unpin` unconditionally,
/// which is required by `ParquetRecordBatchStream`'s `Stream` impl.
#[repr(transparent)]
pub(crate) struct UnpinExec<E>(pub E);

impl<E> Unpin for UnpinExec<E> {}

impl<E: Clone> Clone for UnpinExec<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<E: Executor> Executor for UnpinExec<E> {
    type JoinHandle<R>
        = E::JoinHandle<R>
    where
        R: fusio::MaybeSend;

    type Mutex<T>
        = E::Mutex<T>
    where
        T: fusio::MaybeSend + fusio::MaybeSync;

    type RwLock<T>
        = E::RwLock<T>
    where
        T: fusio::MaybeSend + fusio::MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + fusio::MaybeSend + 'static,
        F::Output: fusio::MaybeSend,
    {
        self.0.spawn(future)
    }

    fn mutex<T>(value: T) -> Self::Mutex<T>
    where
        T: fusio::MaybeSend + fusio::MaybeSync,
    {
        E::mutex(value)
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: fusio::MaybeSend + fusio::MaybeSync,
    {
        E::rw_lock(value)
    }
}

/// Type alias for a parquet stream using our Unpin executor wrapper.
pub(crate) type ParquetStream<E> = ParquetRecordBatchStream<AsyncReader<UnpinExec<E>>>;

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

/// Holds a peeked data entry for merge comparison.
struct DataEntry {
    batch: Arc<RecordBatch>,
    key_ts: KeyTsViewRaw,
    row: DynRowRaw,
    key_owned: KeyOwned,
}

/// Holds a peeked delete entry for merge comparison.
struct DeleteEntry {
    key: KeyOwned,
    ts: Timestamp,
}

/// Delete stream with its extractor (key-only schema).
pub(crate) struct DeleteStreamWithExtractor<'t, E: Executor> {
    pub stream: ParquetStream<E>,
    pub extractor: &'t dyn KeyProjection,
}

// SSTable scan with MVCC visibility filtering using streaming merge.
//
// This scan merges two sorted streams (data + optional delete sidecar) in O(1) memory:
// 1. Filters rows by `read_ts` - only rows with `commit_ts <= read_ts` are visible
// 2. Merges data and delete streams by key order
// 3. When keys match, higher timestamp wins; on tie, delete wins
// 4. Deduplicates by key - emits only the latest visible version per key
pin_project! {
    pub(crate) struct SstableScan<'t, E>
    where
        E: Executor,
    {
        #[pin]
        data_stream: ParquetStream<E>,
        #[pin]
        delete_stream: Option<ParquetStream<E>>,
        data_iter: Option<DataBatchIterator<'t>>,
        delete_iter: Option<DeleteBatchIterator<'t>>,
        // Peeked entries for merge comparison
        peeked_data: Option<DataEntry>,
        peeked_delete: Option<DeleteEntry>,
        projection_indices: Vec<usize>,
        data_extractor: &'t dyn KeyProjection,
        delete_extractor: Option<&'t dyn KeyProjection>,
        order: Option<Order>,
        read_ts: Timestamp,
        // Current key being processed (for dedup)
        current_key: Option<KeyOwned>,
        // Whether we've emitted an entry for the current key
        emitted_for_key: bool,
    }
}

impl<'t, E> SstableScan<'t, E>
where
    E: Executor + Clone + 'static,
{
    /// Create a new SSTable scan with MVCC visibility filtering.
    ///
    /// Uses streaming merge of data and delete streams - O(1) memory, no blocking I/O.
    ///
    /// # Arguments
    /// * `data_stream` - Parquet stream for data rows
    /// * `delete_stream` - Optional delete sidecar with its own extractor
    /// * `data_extractor` - Key extractor for data file (user schema)
    /// * `projection_indices` - Column indices to project
    /// * `order` - Scan order (Asc/Desc)
    /// * `read_ts` - Snapshot timestamp for visibility filtering
    pub fn new(
        data_stream: ParquetStream<E>,
        delete_stream: Option<DeleteStreamWithExtractor<'t, E>>,
        data_extractor: &'t dyn KeyProjection,
        projection_indices: Vec<usize>,
        order: Option<Order>,
        read_ts: Timestamp,
    ) -> Self {
        let (delete_stream, delete_extractor) = match delete_stream {
            Some(d) => (Some(d.stream), Some(d.extractor)),
            None => (None, None),
        };
        Self {
            data_stream,
            delete_stream,
            data_iter: None,
            delete_iter: None,
            peeked_data: None,
            peeked_delete: None,
            projection_indices,
            data_extractor,
            delete_extractor,
            order,
            read_ts,
            current_key: None,
            emitted_for_key: false,
        }
    }
}

impl<'t, E> Stream for SstableScan<'t, E>
where
    E: Executor + Clone + 'static,
{
    type Item = Result<SstableVisibleEntry, SstableScanError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // Step 1: Ensure we have peeked entries from both streams (if available)
            // Advance data stream if needed
            if this.peeked_data.is_none() {
                match poll_next_data_entry(
                    this.data_stream.as_mut(),
                    this.data_iter,
                    *this.data_extractor,
                    this.projection_indices,
                    *this.order,
                    *this.read_ts,
                    cx,
                ) {
                    Poll::Ready(Some(Ok(entry))) => *this.peeked_data = Some(entry),
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {} // Data exhausted
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Advance delete stream if needed
            if this.peeked_delete.is_none()
                && let Some(delete_stream_pin) = this.delete_stream.as_mut().as_pin_mut()
            {
                let delete_extractor = this
                    .delete_extractor
                    .expect("delete extractor must be set when delete stream exists");
                match poll_next_delete_entry(
                    delete_stream_pin,
                    this.delete_iter,
                    delete_extractor,
                    *this.read_ts,
                    cx,
                ) {
                    Poll::Ready(Some(Ok(entry))) => *this.peeked_delete = Some(entry),
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        // Delete stream exhausted, drop it
                        this.delete_stream.set(None);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Step 2: Merge logic - compare peeked entries and emit winner
            // Take ownership of peeked values to avoid borrow-then-unwrap pattern
            let peeked_data = this.peeked_data.take();
            let peeked_delete = this.peeked_delete.take();

            match (peeked_data, peeked_delete) {
                (None, None) => {
                    // Both streams exhausted
                    return Poll::Ready(None);
                }
                (Some(data), None) => {
                    // Only data remains
                    if let Some(entry) =
                        process_data_entry(data, this.current_key, this.emitted_for_key)
                    {
                        return Poll::Ready(Some(Ok(entry)));
                    }
                    // Skipped (dedup), continue loop
                }
                (None, Some(delete)) => {
                    // Only deletes remain
                    if let Some(entry) =
                        process_delete_entry(delete, this.current_key, this.emitted_for_key)
                    {
                        return Poll::Ready(Some(Ok(entry)));
                    }
                    // Skipped (dedup), continue loop
                }
                (Some(data), Some(delete)) => {
                    // Both have entries - compare keys
                    match data.key_owned.cmp(&delete.key) {
                        Ordering::Less => {
                            // Data key comes first, put delete back
                            *this.peeked_delete = Some(delete);
                            if let Some(entry) =
                                process_data_entry(data, this.current_key, this.emitted_for_key)
                            {
                                return Poll::Ready(Some(Ok(entry)));
                            }
                        }
                        Ordering::Greater => {
                            // Delete key comes first, put data back
                            *this.peeked_data = Some(data);
                            if let Some(entry) =
                                process_delete_entry(delete, this.current_key, this.emitted_for_key)
                            {
                                return Poll::Ready(Some(Ok(entry)));
                            }
                        }
                        Ordering::Equal => {
                            // Same key - compare timestamps, higher wins; on tie delete wins
                            let data_ts = data.key_ts.timestamp();
                            if delete.ts >= data_ts {
                                // Delete wins (or tie) - consume both
                                if let Some(entry) = process_delete_entry(
                                    delete,
                                    this.current_key,
                                    this.emitted_for_key,
                                ) {
                                    return Poll::Ready(Some(Ok(entry)));
                                }
                            } else {
                                // Data wins (newer timestamp) - consume both
                                if let Some(entry) =
                                    process_data_entry(data, this.current_key, this.emitted_for_key)
                                {
                                    return Poll::Ready(Some(Ok(entry)));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Process a data entry, handling deduplication.
/// Returns Some(entry) if should emit, None if should skip (dedup).
fn process_data_entry(
    data: DataEntry,
    current_key: &mut Option<KeyOwned>,
    emitted_for_key: &mut bool,
) -> Option<SstableVisibleEntry> {
    let is_new_key = current_key
        .as_ref()
        .map(|k| k != &data.key_owned)
        .unwrap_or(true);

    if is_new_key {
        *current_key = Some(data.key_owned.clone());
        *emitted_for_key = false;
    }

    if *emitted_for_key {
        return None; // Skip duplicate
    }

    *emitted_for_key = true;
    let row_ref = SstableRowRef::new(data.batch, data.key_ts, data.row);
    Some(SstableVisibleEntry::Row(row_ref))
}

/// Process a delete entry, handling deduplication.
/// Returns Some(entry) if should emit, None if should skip (dedup).
fn process_delete_entry(
    delete: DeleteEntry,
    current_key: &mut Option<KeyOwned>,
    emitted_for_key: &mut bool,
) -> Option<SstableVisibleEntry> {
    let is_new_key = current_key
        .as_ref()
        .map(|k| k != &delete.key)
        .unwrap_or(true);

    if is_new_key {
        *current_key = Some(delete.key.clone());
        *emitted_for_key = false;
    }

    if *emitted_for_key {
        return None; // Skip duplicate
    }

    *emitted_for_key = true;
    let key_ts_owned = KeyTsOwned::new(delete.key, delete.ts);
    Some(SstableVisibleEntry::Tombstone(key_ts_owned))
}

/// Poll for the next visible data entry from the data stream.
fn poll_next_data_entry<'t, E: Executor + Clone + 'static>(
    mut data_stream: Pin<&mut ParquetStream<E>>,
    data_iter: &mut Option<DataBatchIterator<'t>>,
    extractor: &'t dyn KeyProjection,
    projection_indices: &[usize],
    order: Option<Order>,
    read_ts: Timestamp,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<DataEntry, SstableScanError>>> {
    loop {
        // Try to get next entry from current batch iterator
        if let Some(iter) = data_iter.as_mut() {
            match iter.next() {
                Some(Ok((batch, key_ts, row, key_owned))) => {
                    // Visibility check
                    if key_ts.timestamp() > read_ts {
                        continue; // Skip invisible
                    }
                    return Poll::Ready(Some(Ok(DataEntry {
                        batch,
                        key_ts,
                        row,
                        key_owned,
                    })));
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    // Iterator exhausted
                    *data_iter = None;
                }
            }
        }

        // Need to fetch next batch
        let batch = match ready!(data_stream.as_mut().poll_next(cx)).transpose() {
            Ok(Some(b)) => b,
            Ok(None) => return Poll::Ready(None), // Stream exhausted
            Err(e) => return Poll::Ready(Some(Err(SstableScanError::Parquet(e)))),
        };

        let mvcc = match decode_mvcc_from_data(&batch) {
            Ok(m) => m,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        *data_iter = match DataBatchIterator::new(
            batch,
            projection_indices.to_vec(),
            extractor,
            mvcc,
            order,
        ) {
            Ok(iter) => Some(iter),
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
    }
}

/// Poll for the next visible delete entry from the delete stream.
fn poll_next_delete_entry<'t, E: Executor + Clone + 'static>(
    mut delete_stream: Pin<&mut ParquetStream<E>>,
    delete_iter: &mut Option<DeleteBatchIterator<'t>>,
    extractor: &'t dyn KeyProjection,
    read_ts: Timestamp,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<DeleteEntry, SstableScanError>>> {
    loop {
        // Try to get next entry from current batch iterator
        if let Some(iter) = delete_iter.as_mut() {
            match iter.next() {
                Some(Ok((key, ts))) => {
                    // Visibility check
                    if ts > read_ts {
                        continue; // Skip invisible
                    }
                    return Poll::Ready(Some(Ok(DeleteEntry { key, ts })));
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    // Iterator exhausted
                    *delete_iter = None;
                }
            }
        }

        // Need to fetch next batch
        let batch = match ready!(delete_stream.as_mut().poll_next(cx)).transpose() {
            Ok(Some(b)) => b,
            Ok(None) => return Poll::Ready(None), // Stream exhausted
            Err(e) => return Poll::Ready(Some(Err(SstableScanError::Parquet(e)))),
        };

        *delete_iter = match DeleteBatchIterator::new(batch, extractor) {
            Ok(iter) => Some(iter),
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
    }
}

/// Iterator over data rows in a RecordBatch.
struct DataBatchIterator<'t> {
    /// Arc-wrapped batch to allow sharing with yielded entries.
    batch: Arc<RecordBatch>,
    extractor: &'t dyn KeyProjection,
    dyn_schema: DynSchema,
    projection: DynProjection,
    mvcc: MvccColumns,
    offset: usize,
    remaining: usize,
}

impl<'t> DataBatchIterator<'t> {
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

        // Build dyn_schema and projection from the actual batch schema
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
            offset: 0,
            remaining: num_rows,
        })
    }
}

impl<'t> Iterator for DataBatchIterator<'t> {
    /// Yields (Arc<RecordBatch>, KeyTsViewRaw, DynRowRaw, KeyOwned).
    type Item = Result<(Arc<RecordBatch>, KeyTsViewRaw, DynRowRaw, KeyOwned), SstableScanError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let row_idx = self.offset;
        self.remaining -= 1;
        self.offset += 1;

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

        let key_owned = match KeyOwned::from_key_row(&key_row) {
            Ok(k) => k,
            Err(e) => return Some(Err(SstableScanError::KeyOwned(e))),
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

        let key_ts = KeyTsViewRaw::new(key_row, commit_ts);
        Some(Ok((Arc::clone(&self.batch), key_ts, row, key_owned)))
    }
}

/// Iterator over delete entries in a RecordBatch (delete sidecar).
struct DeleteBatchIterator<'t> {
    batch: RecordBatch,
    extractor: &'t dyn KeyProjection,
    commit_col: UInt64Array,
    offset: usize,
    remaining: usize,
}

impl<'t> DeleteBatchIterator<'t> {
    pub(crate) fn new(
        batch: RecordBatch,
        extractor: &'t dyn KeyProjection,
    ) -> Result<Self, SstableScanError> {
        let commit_col = batch
            .column_by_name(MVCC_COMMIT_COL)
            .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                SstableScanError::Parquet(ParquetError::ArrowError(
                    "delete sidecar missing commit_ts column".into(),
                ))
            })?
            .clone();

        let num_rows = batch.num_rows();
        Ok(Self {
            batch,
            extractor,
            commit_col,
            offset: 0,
            remaining: num_rows,
        })
    }
}

impl<'t> Iterator for DeleteBatchIterator<'t> {
    /// Yields (KeyOwned, Timestamp) for each delete entry.
    type Item = Result<(KeyOwned, Timestamp), SstableScanError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let row_idx = self.offset;
        self.remaining -= 1;
        self.offset += 1;

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

        let key = match KeyOwned::from_key_row(&key_row) {
            Ok(k) => k,
            Err(e) => return Some(Err(SstableScanError::KeyOwned(e))),
        };

        let ts = Timestamp::new(self.commit_col.value(row_idx));
        Some(Ok((key, ts)))
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

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{disk::LocalFs, dynamic::DynFs, executor::NoopExecutor, path::Path};
    use fusio_parquet::writer::AsyncWriter;
    use futures::StreamExt;
    use parquet::arrow::AsyncArrowWriter;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        inmem::immutable::memtable::MVCC_COMMIT_COL, ondisk::sstable::open_parquet_stream,
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

    #[cfg_attr(feature = "tokio", tokio::test(flavor = "multi_thread"))]
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

        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None, NoopExecutor, None)
            .await
            .expect("open stream");

        let read_ts = Timestamp::new(20);
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::<NoopExecutor>::new(
            data_stream,
            None, // no delete sidecar
            extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
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

    #[cfg_attr(feature = "tokio", tokio::test(flavor = "multi_thread"))]
    async fn delete_sidecar_hides_data() {
        // Test: delete_ts >= commit_ts should hide the row
        let tmpdir = tempdir().expect("tempdir");
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let root = Path::from(tmpdir.path().to_string_lossy().to_string());

        // User schema for data extractor
        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let data_extractor =
            crate::extractor::projection_for_field(Arc::clone(&user_schema), 0).expect("extractor");

        // Key-only schema for delete extractor
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

        // Open both streams for streaming merge
        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None, NoopExecutor, None)
            .await
            .expect("open stream");
        let delete_stream =
            open_parquet_stream(Arc::clone(&fs), delete_path, None, NoopExecutor, None)
                .await
                .expect("delete stream");

        let read_ts = Timestamp::MAX;
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::<NoopExecutor>::new(
            data_stream,
            Some(DeleteStreamWithExtractor {
                stream: delete_stream,
                extractor: delete_extractor.as_ref(),
            }),
            data_extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
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

    #[cfg_attr(feature = "tokio", tokio::test(flavor = "multi_thread"))]
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

        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None, NoopExecutor, None)
            .await
            .expect("open stream");

        let read_ts = Timestamp::MAX;
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::<NoopExecutor>::new(
            data_stream,
            None, // no delete sidecar
            extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
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

    #[cfg_attr(feature = "tokio", tokio::test(flavor = "multi_thread"))]
    async fn tombstone_only_keys_emitted() {
        // Test: keys that exist only in delete sidecar (no data rows) should emit tombstones
        let tmpdir = tempdir().expect("tempdir");
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let root = Path::from(tmpdir.path().to_string_lossy().to_string());

        // User schema for data extractor
        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let data_extractor =
            crate::extractor::projection_for_field(Arc::clone(&user_schema), 0).expect("extractor");

        // Key-only schema for delete extractor
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

        // Open both streams for streaming merge
        let data_stream = open_parquet_stream(Arc::clone(&fs), data_path, None, NoopExecutor, None)
            .await
            .expect("open stream");
        let delete_stream =
            open_parquet_stream(Arc::clone(&fs), delete_path, None, NoopExecutor, None)
                .await
                .expect("delete stream");

        let read_ts = Timestamp::MAX;
        let projection_indices = vec![0, 1];

        let mut scan = SstableScan::<NoopExecutor>::new(
            data_stream,
            Some(DeleteStreamWithExtractor {
                stream: delete_stream,
                extractor: delete_extractor.as_ref(),
            }),
            data_extractor.as_ref(),
            projection_indices,
            Some(Order::Asc),
            read_ts,
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
