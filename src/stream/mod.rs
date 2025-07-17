pub(crate) mod level;
pub(crate) mod mem_projection;
pub(crate) mod merge;
pub(crate) mod package;
pub(crate) mod record_batch;

use std::{
    fmt::{self, Debug, Formatter},
    mem::transmute,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common::{Key, KeyRef, PrimaryKey, PrimaryKeyRef};
use futures_core::Stream;
use futures_util::{ready, stream};
use parquet::arrow::ProjectionMask;
use pin_project_lite::pin_project;
use record_batch::RecordBatchEntry;

use crate::{
    inmem::{immutable::ImmutableScan, mutable::MutableScan},
    ondisk::scan::SsTableScan,
    record::{Record, RecordRef},
    stream::{level::LevelStream, mem_projection::MemProjectionStream},
    timestamp::Ts,
    transaction::TransactionScan,
};

pub enum Entry<'entry, R>
where
    R: Record,
{
    Transaction((Ts<PrimaryKeyRef<'entry>>, &'entry Option<R>)),
    Mutable(crossbeam_skiplist::map::Entry<'entry, Ts<PrimaryKey>, Option<R>>),
    Projection((Box<Entry<'entry, R>>, Arc<ProjectionMask>)),
    RecordBatch(RecordBatchEntry<R>),
}

impl<R> Entry<'_, R>
where
    R: Record,
{
    pub(crate) fn key(&self) -> Ts<PrimaryKey> {
        match self {
            Entry::Transaction((key, _)) => {
                // Safety: shorter lifetime must be safe
                // unsafe {
                //     transmute::<Ts<<R::Key as Key>::Ref<'_>>, Ts<<R::Key as Key>::Ref<'_>>>(
                //         key.clone(),
                //     )
                // }
                // key.clone()
                Ts::new(key.value().clone().to_key(), key.ts())
            }
            Entry::Mutable(entry) => entry.key().map(|key| {
                // Safety: shorter lifetime must be safe
                unsafe { transmute(key.clone()) }
            }),
            Entry::RecordBatch(entry) => entry.internal_key(),
            Entry::Projection((entry, _)) => entry.key(),
        }
    }

    pub fn value(&self) -> Option<R::Ref<'_>> {
        match self {
            Entry::Transaction((_, value)) => value.as_ref().map(R::as_record_ref),
            Entry::Mutable(entry) => entry.value().as_ref().map(R::as_record_ref),
            Entry::RecordBatch(entry) => entry.get(),
            Entry::Projection((entry, projection_mask)) => entry.value().map(|mut val_ref| {
                val_ref.projection(projection_mask);
                val_ref
            }),
        }
    }
}

impl<R> fmt::Debug for Entry<'_, R>
where
    R: Record + Debug,
    // R::Key: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Entry::Transaction((key, value)) => {
                write!(f, "Entry::Transaction({:?} -> {:?})", key, value)
            }
            Entry::Mutable(mutable) => write!(
                f,
                "Entry::Mutable({:?} -> {:?})",
                mutable.key(),
                mutable.value()
            ),
            Entry::RecordBatch(sstable) => write!(f, "Entry::SsTable({:?})", sstable),
            Entry::Projection((entry, projection_mask)) => {
                write!(f, "Entry::Projection({:?} -> {:?})", entry, projection_mask)
            }
        }
    }
}

pin_project! {
    #[project = ScanStreamProject]
    pub enum ScanStream<'scan, R>
    where
        R: Record,
    {
        Transaction {
            #[pin]
            inner: stream::Iter<TransactionScan<'scan, R>>,
        },
        Mutable {
            #[pin]
            inner: stream::Iter<MutableScan<'scan, R>>,
        },
        Immutable {
            #[pin]
            inner: stream::Iter<ImmutableScan<'scan, R>>,
        },
        SsTable {
            #[pin]
            inner: SsTableScan<'scan, R>,
        },
        Level {
            #[pin]
            inner: LevelStream<'scan, R>,
        },
        MemProjection {
            #[pin]
            inner: MemProjectionStream<'scan, R>,
        }
    }
}

impl<'scan, R> From<TransactionScan<'scan, R>> for ScanStream<'scan, R>
where
    R: Record,
{
    fn from(inner: TransactionScan<'scan, R>) -> Self {
        ScanStream::Transaction {
            inner: stream::iter(inner),
        }
    }
}

impl<'scan, R> From<MutableScan<'scan, R>> for ScanStream<'scan, R>
where
    R: Record,
{
    fn from(inner: MutableScan<'scan, R>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'scan, R> From<ImmutableScan<'scan, R>> for ScanStream<'scan, R>
where
    R: Record,
{
    fn from(inner: ImmutableScan<'scan, R>) -> Self {
        ScanStream::Immutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'scan, R> From<SsTableScan<'scan, R>> for ScanStream<'scan, R>
where
    R: Record,
{
    fn from(inner: SsTableScan<'scan, R>) -> Self {
        ScanStream::SsTable { inner }
    }
}

impl<'scan, R> From<MemProjectionStream<'scan, R>> for ScanStream<'scan, R>
where
    R: Record,
{
    fn from(inner: MemProjectionStream<'scan, R>) -> Self {
        ScanStream::MemProjection { inner }
    }
}

impl<R> fmt::Debug for ScanStream<'_, R>
where
    R: Record,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScanStream::Transaction { .. } => write!(f, "ScanStream::Transaction"),
            ScanStream::Mutable { .. } => write!(f, "ScanStream::Mutable"),
            ScanStream::SsTable { .. } => write!(f, "ScanStream::SsTable"),
            ScanStream::Immutable { .. } => write!(f, "ScanStream::Immutable"),
            ScanStream::Level { .. } => write!(f, "ScanStream::Level"),
            ScanStream::MemProjection { .. } => write!(f, "ScanStream::MemProjection"),
        }
    }
}

impl<'scan, R> Stream for ScanStream<'scan, R>
where
    R: Record,
{
    type Item = Result<Entry<'scan, R>, parquet::errors::ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            ScanStreamProject::Transaction { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(Entry::Transaction).map(Ok))
            }
            ScanStreamProject::Mutable { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(Entry::Mutable).map(Ok))
            }
            ScanStreamProject::SsTable { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(|entry| entry.map(Entry::RecordBatch)))
            }
            ScanStreamProject::Immutable { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(|entry| Ok(Entry::RecordBatch(entry))))
            }
            ScanStreamProject::Level { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(|entry| entry.map(Entry::RecordBatch)))
            }
            ScanStreamProject::MemProjection { inner } => Poll::Ready(ready!(inner.poll_next(cx))),
        }
    }
}
