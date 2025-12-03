//! Read path scaffolding (planner, plan, executor).

pub(crate) mod merge;
pub(crate) mod package;

use core::fmt;
use std::{
    mem,
    pin::Pin,
    sync::{Arc, RwLockReadGuard},
    task::{Context, Poll},
};

use arrow_schema::SchemaRef;
use futures::{Stream, ready, stream};
pub use package::ResidualError;
use pin_project_lite::pin_project;
use thiserror::Error;
use typed_arrow_dyn::{DynError, DynRowRaw, DynViewError};

use crate::{
    extractor::KeyExtractError,
    inmem::{
        immutable::memtable::{
            ImmutableMemTable, ImmutableVisibleEntry, ImmutableVisibleScan, RecordBatchStorage,
        },
        mutable::memtable::{DynMem, DynRowScan, DynRowScanEntry},
    },
    key::KeyTsViewRaw,
    mvcc::Timestamp,
    ondisk::scan::{SstableScan, SstableScanError},
    transaction::{TransactionScan, TransactionScanEntry},
};

/// Direction for ordered scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Order {
    /// Ascending (lowest to highest primary key).
    #[default]
    Asc,
    #[allow(dead_code)]
    /// Descending (highest to lowest primary key).
    Desc,
}

/// Source priority applied while reconciling overlapping entries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum SourcePriority {
    /// In-flight transaction stream (highest priority).
    Txn,
    /// Mutable memtable scan.
    Mutable,
    /// Frozen memtable scan.
    Immutable,
    /// SSTable scan (lowest priority).
    Sstable,
}

/// Iterator wrapper that keeps a mutable memtable read guard alive while streaming it.
pub(crate) struct OwnedMutableScan<'t> {
    inner: DynRowScan<'t>,
    _guard: Option<RwLockReadGuard<'t, DynMem>>,
}

impl<'t> OwnedMutableScan<'t> {
    pub(crate) fn from_guard(
        guard: RwLockReadGuard<'t, DynMem>,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<Self, KeyExtractError> {
        let inner = {
            let scan = guard.scan_visible(projection_schema, read_ts)?;
            // SAFETY: `scan` borrows from `guard`, which we keep alive inside `_guard`.
            unsafe { mem::transmute::<DynRowScan<'_>, DynRowScan<'t>>(scan) }
        };
        Ok(Self {
            inner,
            _guard: Some(guard),
        })
    }

    pub(crate) fn from_scan(inner: DynRowScan<'t>) -> Self {
        Self {
            inner,
            _guard: None,
        }
    }
}

impl<'t> Iterator for OwnedMutableScan<'t> {
    type Item = Result<DynRowScanEntry, DynViewError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl fmt::Debug for OwnedMutableScan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedMutableScan").finish()
    }
}

/// Iterator wrapper that keeps an immutable segment alive while streaming it.
pub(crate) struct OwnedImmutableScan<'t, S>
where
    S: RecordBatchStorage,
{
    _keep: Arc<ImmutableMemTable<S>>,
    inner: ImmutableVisibleScan<'t, S>,
}

impl<'t, S> OwnedImmutableScan<'t, S>
where
    S: RecordBatchStorage,
{
    #[allow(dead_code)]
    pub(crate) fn new(keep: Arc<ImmutableMemTable<S>>, inner: ImmutableVisibleScan<'t, S>) -> Self {
        Self { _keep: keep, inner }
    }

    pub(crate) fn from_arc(
        segment: Arc<ImmutableMemTable<S>>,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<Self, KeyExtractError> {
        let inner = {
            let scan = segment.scan_visible(projection_schema, read_ts)?;
            // SAFETY: `scan` borrows from `segment`, which is kept alive by `_keep`.
            unsafe {
                mem::transmute::<ImmutableVisibleScan<'_, S>, ImmutableVisibleScan<'t, S>>(scan)
            }
        };
        Ok(Self {
            _keep: Arc::clone(&segment),
            inner,
        })
    }
}

impl<'t, S> Iterator for OwnedImmutableScan<'t, S>
where
    S: RecordBatchStorage,
{
    type Item = Result<ImmutableVisibleEntry, DynViewError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<S> fmt::Debug for OwnedImmutableScan<'_, S>
where
    S: RecordBatchStorage,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedImmutableScan").finish()
    }
}

/// Errors surfaced while composing read streams.
#[derive(Debug, Error)]
pub enum StreamError {
    /// Failure originating while scanning SSTables.
    #[error(transparent)]
    SsTable(#[from] SstableScanError),
    /// Dynamic row conversion failed while materializing a batch.
    #[error("dynamic row conversion failed: {0}")]
    DynRow(#[from] DynViewError),
    /// Building a projected batch failed.
    #[error("dynamic batch construction failed: {0}")]
    DynBuilder(#[from] DynError),
    /// Residual predicate evaluation failed.
    #[error(transparent)]
    Predicate(#[from] ResidualError),
}

/// Unified entry yielded by the read stream for both in-memory and on-disk sources.
#[derive(Debug)]
pub enum StreamEntry {
    /// Entry sourced from the staging buffer of a transaction.
    Txn((KeyTsViewRaw, DynRowRaw)),
    /// Tombstone emitted by the current transaction.
    TxnTombstone(KeyTsViewRaw),
    /// Entry sourced from the mutable layer.
    MemTable((KeyTsViewRaw, DynRowRaw)),
    /// Tombstone sourced from a mutable layer.
    MemTableTombstone(KeyTsViewRaw),
    /// Entry produced by the SSTable scan pipeline.
    Sstable((KeyTsViewRaw, DynRowRaw)),
}

impl StreamEntry {
    pub(crate) fn key(&self) -> &KeyTsViewRaw {
        match self {
            StreamEntry::Txn((key_ts, _)) => key_ts,
            StreamEntry::TxnTombstone(key_ts) => key_ts,
            StreamEntry::MemTable((key_ts, _)) => key_ts,
            StreamEntry::MemTableTombstone(key_ts) => key_ts,
            StreamEntry::Sstable((key_ts, _)) => key_ts,
        }
    }

    pub(crate) fn ts(&self) -> Timestamp {
        match self {
            StreamEntry::Txn((key_ts, _)) => key_ts.timestamp(),
            StreamEntry::TxnTombstone(key_ts) => key_ts.timestamp(),
            StreamEntry::MemTable((key_ts, _)) => key_ts.timestamp(),
            StreamEntry::MemTableTombstone(key_ts) => key_ts.timestamp(),
            StreamEntry::Sstable((key_ts, _)) => key_ts.timestamp(),
        }
    }

    pub(crate) fn into_row(self) -> Option<DynRowRaw> {
        match self {
            StreamEntry::Txn((_, row)) => Some(row),
            StreamEntry::MemTable((_, row)) => Some(row),
            StreamEntry::Sstable((_, row)) => Some(row),
            StreamEntry::TxnTombstone(_) => None,
            StreamEntry::MemTableTombstone(_) => None,
        }
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        matches!(
            self,
            StreamEntry::TxnTombstone(_) | StreamEntry::MemTableTombstone(_)
        )
    }
}

pin_project! {
    #[project = ScanStreamProject]
    pub(crate) enum ScanStream<'t, S>
    where
        S: RecordBatchStorage,
    {
        Txn {
            #[pin]
            inner: stream::Iter<TransactionScan<'t>>,
        },
        Mutable {
            #[pin]
            inner: stream::Iter<OwnedMutableScan<'t>>,
        },
        Immutable {
            #[pin]
            inner: stream::Iter<OwnedImmutableScan<'t, S>>,
        },
        SsTable {
            #[pin]
            inner: SstableScan<'t>,
        },
    }
}

impl<'t, S: RecordBatchStorage> From<OwnedImmutableScan<'t, S>> for ScanStream<'t, S> {
    fn from(inner: OwnedImmutableScan<'t, S>) -> Self {
        ScanStream::Immutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, S: RecordBatchStorage> From<OwnedMutableScan<'t>> for ScanStream<'t, S> {
    fn from(inner: OwnedMutableScan<'t>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, S: RecordBatchStorage> From<DynRowScan<'t>> for ScanStream<'t, S> {
    fn from(inner: DynRowScan<'t>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(OwnedMutableScan::from_scan(inner)),
        }
    }
}

impl<'t, S: RecordBatchStorage> From<SstableScan<'t>> for ScanStream<'t, S> {
    fn from(inner: SstableScan<'t>) -> Self {
        ScanStream::SsTable { inner }
    }
}

impl<'t, S: RecordBatchStorage> From<TransactionScan<'t>> for ScanStream<'t, S> {
    fn from(inner: TransactionScan<'t>) -> Self {
        ScanStream::Txn {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, S: std::fmt::Debug + RecordBatchStorage> fmt::Debug for ScanStream<'t, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Txn { inner } => f
                .debug_struct("ScanStream::Txn")
                .field("inner", inner)
                .finish(),
            Self::Mutable { inner } => f
                .debug_struct("ScanStream::Mutable")
                .field("inner", inner)
                .finish(),
            Self::Immutable { inner } => f
                .debug_struct("ScanStream::Immutable")
                .field("inner", inner)
                .finish(),
            Self::SsTable { .. } => f.debug_struct("ScanStream::SsTable").finish(),
        }
    }
}

impl<'t, S: RecordBatchStorage> ScanStream<'t, S> {
    pub(crate) fn priority(&self) -> SourcePriority {
        match self {
            ScanStream::Txn { .. } => SourcePriority::Txn,
            ScanStream::Mutable { .. } => SourcePriority::Mutable,
            ScanStream::Immutable { .. } => SourcePriority::Immutable,
            ScanStream::SsTable { .. } => SourcePriority::Sstable,
        }
    }
}

impl<'t, S> Stream for ScanStream<'t, S>
where
    S: RecordBatchStorage,
{
    type Item = Result<StreamEntry, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            ScanStreamProject::Txn { inner } => match ready!(inner.poll_next(cx)) {
                Some(Ok(TransactionScanEntry::Row(entry))) => {
                    Poll::Ready(Some(Ok(StreamEntry::Txn(entry))))
                }
                Some(Ok(TransactionScanEntry::Tombstone(key))) => {
                    Poll::Ready(Some(Ok(StreamEntry::TxnTombstone(key))))
                }
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::DynRow(err)))),
                None => Poll::Ready(None),
            },
            ScanStreamProject::Mutable { inner } => match ready!(inner.poll_next(cx)) {
                Some(Ok(DynRowScanEntry::Row(key, row))) => {
                    Poll::Ready(Some(Ok(StreamEntry::MemTable((key, row)))))
                }
                Some(Ok(DynRowScanEntry::Tombstone(key))) => {
                    Poll::Ready(Some(Ok(StreamEntry::MemTableTombstone(key))))
                }
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::DynRow(err)))),
                None => Poll::Ready(None),
            },
            ScanStreamProject::Immutable { inner } => match ready!(inner.poll_next(cx)) {
                Some(Ok(ImmutableVisibleEntry::Row(key, row))) => {
                    Poll::Ready(Some(Ok(StreamEntry::MemTable((key, row)))))
                }
                Some(Ok(ImmutableVisibleEntry::Tombstone(key))) => {
                    Poll::Ready(Some(Ok(StreamEntry::MemTableTombstone(key))))
                }
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::DynRow(err)))),
                None => Poll::Ready(None),
            },
            ScanStreamProject::SsTable { inner } => match ready!(inner.poll_next(cx)) {
                Some(Ok(entry)) => Poll::Ready(Some(Ok(StreamEntry::Sstable(entry)))),
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::from(err)))),
                None => Poll::Ready(None),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use futures::StreamExt;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::{KeyProjection, projection_for_field},
        inmem::{immutable::memtable::ImmutableMemTable, mutable::memtable::DynMem},
        key::KeyOwned,
        mutation::DynMutation,
        mvcc::Timestamp,
        test_util::build_batch,
        transaction::TransactionScan,
    };

    struct ScanStreamFixture {
        schema: SchemaRef,
        mutable: DynMem,
        immutable: ImmutableMemTable<RecordBatch>,
        staged: BTreeMap<KeyOwned, DynMutation<DynRow, ()>>,
    }

    impl ScanStreamFixture {
        fn new() -> Self {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("v", DataType::Int64, true),
            ]));
            let extractor = Arc::new(projection_for_field(schema.clone(), 0).expect("extractor"));
            let mut mutable = DynMem::new(schema.clone());
            let mut immutable_builder = DynMem::new(schema.clone());

            fn insert_row(
                table: &mut DynMem,
                schema: &SchemaRef,
                extractor: &Arc<Box<dyn KeyProjection>>,
                key: &str,
                value: i64,
                ts: u64,
            ) {
                let rows = vec![DynRow(vec![
                    Some(DynCell::Str(key.into())),
                    Some(DynCell::I64(value)),
                ])];
                let batch = build_batch(schema.clone(), rows).expect("batch");
                table
                    .insert_batch(extractor.as_ref().as_ref(), batch, Timestamp::new(ts))
                    .expect("insert row");
            }

            insert_row(&mut mutable, &schema, &extractor, "m1", 10, 5);
            insert_row(&mut mutable, &schema, &extractor, "m2", 20, 7);
            insert_row(&mut immutable_builder, &schema, &extractor, "i1", 1, 2);
            insert_row(&mut immutable_builder, &schema, &extractor, "i2", 2, 3);

            let immutable = immutable_builder
                .seal_into_immutable(&schema, extractor.as_ref().as_ref())
                .expect("seal immutable")
                .expect("segment");

            let mut staged = BTreeMap::new();
            staged.insert(
                KeyOwned::from("txn-a"),
                DynMutation::Upsert(DynRow(vec![
                    Some(DynCell::Str("txn-a".into())),
                    Some(DynCell::I64(100)),
                ])),
            );
            staged.insert(
                KeyOwned::from("txn-b"),
                DynMutation::Upsert(DynRow(vec![
                    Some(DynCell::Str("txn-b".into())),
                    Some(DynCell::I64(200)),
                ])),
            );

            Self {
                schema,
                mutable,
                immutable,
                staged,
            }
        }
    }

    #[tokio::test]
    async fn scan_stream_mutable_variant_yields_entries() {
        let setup = ScanStreamFixture::new();
        let scan = setup
            .mutable
            .scan_visible(None, Timestamp::MAX)
            .expect("scan rows produces iterator");
        let mut stream = ScanStream::<RecordBatch>::from(scan);
        let next = stream.next().await.expect("entry present");
        assert!(matches!(next, Ok(StreamEntry::MemTable(_))));
    }

    #[tokio::test]
    async fn scan_stream_immutable_variant_yields_entries() {
        let setup = ScanStreamFixture::new();
        let immutable = Arc::new(setup.immutable);
        let scan = immutable
            .scan_visible(None, Timestamp::MAX)
            .expect("scan immutables");
        let mut stream =
            ScanStream::<RecordBatch>::from(OwnedImmutableScan::new(Arc::clone(&immutable), scan));
        let next = stream.next().await.expect("entry present");
        assert!(matches!(next, Ok(StreamEntry::MemTable(_))));
    }

    #[tokio::test]
    async fn scan_stream_txn_variant_yields_entries() {
        let setup = ScanStreamFixture::new();
        let txn_scan = TransactionScan::new(&setup.staged, &setup.schema, Timestamp::new(5), None)
            .expect("txn scan");
        let mut stream = Box::pin(ScanStream::<RecordBatch>::from(txn_scan));
        let entry = stream.next().await.expect("entry present");
        assert!(matches!(entry, Ok(StreamEntry::Txn(_))));
    }
}
