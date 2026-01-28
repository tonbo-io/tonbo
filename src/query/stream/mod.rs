//! Read path scaffolding (planner, plan, executor).

pub(crate) mod merge;
pub(crate) mod package;

use core::fmt;
use std::{
    mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_schema::SchemaRef;
use fusio::executor::Executor;
use futures::{Stream, ready, stream};
pub use package::ResidualError;
use pin_project_lite::pin_project;
use thiserror::Error;
use typed_arrow_dyn::{DynError, DynRow, DynRowRaw, DynViewError};

use crate::{
    extractor::KeyExtractError,
    inmem::{
        immutable::memtable::{ImmutableMemTable, ImmutableVisibleEntry, ImmutableVisibleScan},
        mutable::memtable::{DynMemReadGuard, DynRowScan, DynRowScanEntry},
    },
    key::{KeyRow, KeyTsOwned, KeyTsViewRaw},
    mvcc::Timestamp,
    ondisk::{
        scan::{SstableRowRef, SstableScan, SstableScanError},
        sstable::SsTableError,
    },
    transaction::{TransactionScan, TransactionScanEntry},
};

/// Direction for ordered scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Order {
    /// Ascending (lowest to highest primary key).
    #[default]
    Asc,
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
    _guard: Option<DynMemReadGuard<'t>>,
}

impl<'t> OwnedMutableScan<'t> {
    pub(crate) fn from_guard(
        guard: DynMemReadGuard<'t>,
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
pub(crate) struct OwnedImmutableScan<'t> {
    _keep: Arc<ImmutableMemTable>,
    inner: ImmutableVisibleScan<'t>,
}

impl<'t> OwnedImmutableScan<'t> {
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn new(keep: Arc<ImmutableMemTable>, inner: ImmutableVisibleScan<'t>) -> Self {
        Self { _keep: keep, inner }
    }

    pub(crate) fn from_arc(
        segment: Arc<ImmutableMemTable>,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<Self, KeyExtractError> {
        let inner = {
            let scan = segment.scan_visible(projection_schema, read_ts)?;
            // SAFETY: `scan` borrows from `segment`, which is kept alive by `_keep`.
            unsafe { mem::transmute::<ImmutableVisibleScan<'_>, ImmutableVisibleScan<'t>>(scan) }
        };
        Ok(Self {
            _keep: Arc::clone(&segment),
            inner,
        })
    }
}

impl<'t> Iterator for OwnedImmutableScan<'t> {
    type Item = Result<ImmutableVisibleEntry, DynViewError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl fmt::Debug for OwnedImmutableScan<'_> {
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
    /// SSTable I/O error (opening files, reading metadata, etc.).
    #[error("sstable I/O error: {0}")]
    SsTableIo(#[from] SsTableError),
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
pub(crate) enum StreamEntry {
    /// Entry sourced from the staging buffer of a transaction.
    Txn((KeyTsViewRaw, DynRowRaw)),
    /// Tombstone emitted by the current transaction.
    TxnTombstone(KeyTsViewRaw),
    /// Entry sourced from the mutable layer.
    MemTable((KeyTsViewRaw, DynRowRaw)),
    /// Tombstone sourced from a mutable layer.
    MemTableTombstone(KeyTsViewRaw),
    /// Entry produced by the SSTable scan pipeline with zero-copy Arc-backed reference.
    Sstable(SstableRowRef),
    /// Tombstone sourced from an SSTable delete sidecar (owned).
    SstableTombstone(KeyTsOwned),
}

impl std::fmt::Debug for StreamEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamEntry::Txn((key, _)) => f.debug_tuple("Txn").field(key).finish(),
            StreamEntry::TxnTombstone(key) => f.debug_tuple("TxnTombstone").field(key).finish(),
            StreamEntry::MemTable((key, _)) => f.debug_tuple("MemTable").field(key).finish(),
            StreamEntry::MemTableTombstone(key) => {
                f.debug_tuple("MemTableTombstone").field(key).finish()
            }
            StreamEntry::Sstable(row_ref) => {
                f.debug_tuple("Sstable").field(row_ref.key_ts()).finish()
            }
            StreamEntry::SstableTombstone(key) => {
                f.debug_tuple("SstableTombstone").field(key).finish()
            }
        }
    }
}

impl StreamEntry {
    /// Compare keys without allocation for the common case.
    ///
    /// Most variants hold `KeyTsViewRaw` which contains `KeyRow`, allowing zero-copy
    /// comparison via `as_dyn()`. Only `SstableTombstone` holds `KeyTsOwned` which
    /// requires `as_raw()` conversion (rare in practice since tombstones are uncommon).
    pub(crate) fn key_cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        use StreamEntry::*;

        // Helper to get &KeyRow from KeyTsViewRaw variants
        fn key_row(entry: &StreamEntry) -> Option<&KeyRow> {
            match entry {
                Txn((key_ts, _)) => Some(key_ts.key()),
                TxnTombstone(key_ts) => Some(key_ts.key()),
                MemTable((key_ts, _)) => Some(key_ts.key()),
                MemTableTombstone(key_ts) => Some(key_ts.key()),
                Sstable(row_ref) => Some(row_ref.key_ts().key()),
                SstableTombstone(_) => None,
            }
        }

        // Fast path: both have KeyRow (zero-alloc comparison)
        if let (Some(left), Some(right)) = (key_row(self), key_row(other)) {
            return left.cmp(right);
        }

        // Slow path: at least one is SstableTombstone (rare)
        match (self, other) {
            (SstableTombstone(left), SstableTombstone(right)) => left.key().cmp(right.key()),
            (SstableTombstone(left), _) => {
                let right = key_row(other).expect("covered by fast path");
                // KeyOwned compared with KeyRow uses cross-type PartialOrd
                left.key().partial_cmp(right).unwrap_or(Ordering::Equal)
            }
            (_, SstableTombstone(right)) => {
                let left = key_row(self).expect("covered by fast path");
                // KeyRow compared with KeyOwned
                left.partial_cmp(right.key()).unwrap_or(Ordering::Equal)
            }
            _ => unreachable!("all non-tombstone cases handled by fast path"),
        }
    }

    /// Check if two entries have the same key without allocation.
    pub(crate) fn key_eq(&self, other: &Self) -> bool {
        self.key_cmp(other) == std::cmp::Ordering::Equal
    }

    pub(crate) fn ts(&self) -> Timestamp {
        match self {
            StreamEntry::Txn((key_ts, _)) => key_ts.timestamp(),
            StreamEntry::TxnTombstone(key_ts) => key_ts.timestamp(),
            StreamEntry::MemTable((key_ts, _)) => key_ts.timestamp(),
            StreamEntry::MemTableTombstone(key_ts) => key_ts.timestamp(),
            StreamEntry::Sstable(row_ref) => row_ref.key_ts().timestamp(),
            StreamEntry::SstableTombstone(key_ts) => key_ts.timestamp(),
        }
    }

    /// Convert entry to owned row data. For SSTable entries, this is where
    /// the actual clone happens (deferred from scan time for zero-copy streaming).
    pub(crate) fn into_row(self) -> Option<DynRow> {
        match self {
            StreamEntry::Txn((_, row)) => Some(row.into_owned().expect("row conversion")),
            StreamEntry::MemTable((_, row)) => Some(row.into_owned().expect("row conversion")),
            StreamEntry::Sstable(row_ref) => {
                Some(row_ref.into_row_owned().expect("row conversion"))
            }
            StreamEntry::TxnTombstone(_) => None,
            StreamEntry::MemTableTombstone(_) => None,
            StreamEntry::SstableTombstone(_) => None,
        }
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        matches!(
            self,
            StreamEntry::TxnTombstone(_)
                | StreamEntry::MemTableTombstone(_)
                | StreamEntry::SstableTombstone(_)
        )
    }
}

pin_project! {
    #[project = ScanStreamProject]
    pub(crate) enum ScanStream<'t, E>
    where
        E: Executor,
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
            inner: stream::Iter<OwnedImmutableScan<'t>>,
        },
        SsTable {
            #[pin]
            inner: SstableScan<E>,
        },
    }
}

impl<'t, E: Executor> From<OwnedImmutableScan<'t>> for ScanStream<'t, E> {
    fn from(inner: OwnedImmutableScan<'t>) -> Self {
        ScanStream::Immutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, E: Executor> From<OwnedMutableScan<'t>> for ScanStream<'t, E> {
    fn from(inner: OwnedMutableScan<'t>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, E: Executor> From<DynRowScan<'t>> for ScanStream<'t, E> {
    fn from(inner: DynRowScan<'t>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(OwnedMutableScan::from_scan(inner)),
        }
    }
}

impl<'t, E: Executor> From<SstableScan<E>> for ScanStream<'t, E> {
    fn from(inner: SstableScan<E>) -> Self {
        ScanStream::SsTable { inner }
    }
}

impl<'t, E: Executor> From<TransactionScan<'t>> for ScanStream<'t, E> {
    fn from(inner: TransactionScan<'t>) -> Self {
        ScanStream::Txn {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, E: Executor> fmt::Debug for ScanStream<'t, E> {
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

impl<'t, E: Executor> ScanStream<'t, E> {
    pub(crate) fn priority(&self) -> SourcePriority {
        match self {
            ScanStream::Txn { .. } => SourcePriority::Txn,
            ScanStream::Mutable { .. } => SourcePriority::Mutable,
            ScanStream::Immutable { .. } => SourcePriority::Immutable,
            ScanStream::SsTable { .. } => SourcePriority::Sstable,
        }
    }
}

impl<'t, E> Stream for ScanStream<'t, E>
where
    E: Executor + Clone + 'static,
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
                Some(Ok(crate::ondisk::scan::SstableVisibleEntry::Row(row_ref))) => {
                    // Zero-copy: pass through the Arc-backed reference directly
                    Poll::Ready(Some(Ok(StreamEntry::Sstable(row_ref))))
                }
                Some(Ok(crate::ondisk::scan::SstableVisibleEntry::Tombstone(key_owned))) => {
                    Poll::Ready(Some(Ok(StreamEntry::SstableTombstone(key_owned))))
                }
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::from(err)))),
                None => Poll::Ready(None),
            },
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use futures::StreamExt;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::{KeyProjection, projection_for_columns, projection_for_field},
        inmem::{immutable::memtable::ImmutableMemTable, mutable::memtable::DynMem},
        key::KeyOwned,
        mutation::DynMutation,
        mvcc::Timestamp,
        test::build_batch,
        transaction::TransactionScan,
    };

    fn make_test_mem(schema: SchemaRef) -> DynMem {
        let extractor: Arc<dyn KeyProjection> = projection_for_field(schema.clone(), 0)
            .expect("extractor")
            .into();
        let delete_projection: Arc<dyn KeyProjection> = projection_for_columns(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![0],
        )
        .expect("delete projection")
        .into();
        DynMem::new(schema, extractor, delete_projection)
    }

    struct ScanStreamFixture {
        schema: SchemaRef,
        mutable: DynMem,
        immutable: ImmutableMemTable,
        staged: BTreeMap<KeyOwned, DynMutation<DynRow, ()>>,
    }

    impl ScanStreamFixture {
        fn new() -> Self {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("v", DataType::Int64, true),
            ]));
            let mutable = make_test_mem(schema.clone());
            let immutable_builder = make_test_mem(schema.clone());

            let insert_row = |table: &DynMem, key: &str, value: i64, ts: u64| {
                let rows = vec![DynRow(vec![
                    Some(DynCell::Str(key.into())),
                    Some(DynCell::I64(value)),
                ])];
                let batch = build_batch(schema.clone(), rows).expect("batch");
                table
                    .insert_batch(batch, Timestamp::new(ts))
                    .expect("insert row");
            };

            insert_row(&mutable, "m1", 10, 5);
            insert_row(&mutable, "m2", 20, 7);
            insert_row(&immutable_builder, "i1", 1, 2);
            insert_row(&immutable_builder, "i2", 2, 3);

            let immutable = immutable_builder
                .seal_now()
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
        let mutable_guard = setup.mutable.read();
        let scan = OwnedMutableScan::from_guard(mutable_guard, None, Timestamp::MAX)
            .expect("scan rows produces iterator");
        let mut stream = ScanStream::<fusio::executor::NoopExecutor>::from(scan);
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
        let mut stream = ScanStream::<fusio::executor::NoopExecutor>::from(
            OwnedImmutableScan::new(Arc::clone(&immutable), scan),
        );
        let next = stream.next().await.expect("entry present");
        assert!(matches!(next, Ok(StreamEntry::MemTable(_))));
    }

    #[tokio::test]
    async fn scan_stream_txn_variant_yields_entries() {
        let setup = ScanStreamFixture::new();
        let txn_scan = TransactionScan::new(&setup.staged, &setup.schema, Timestamp::new(5), None)
            .expect("txn scan");
        let mut stream = Box::pin(ScanStream::<fusio::executor::NoopExecutor>::from(txn_scan));
        let entry = stream.next().await.expect("entry present");
        assert!(matches!(entry, Ok(StreamEntry::Txn(_))));
    }
}
