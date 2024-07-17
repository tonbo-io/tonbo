pub(crate) mod level;
pub(crate) mod merge;
pub(crate) mod record_batch;

use std::{
    fmt::{self, Debug, Formatter},
    mem::transmute,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_util::{ready, stream};
use pin_project_lite::pin_project;
use record_batch::RecordBatchEntry;

use crate::{
    executor::Executor,
    inmem::{immutable::ImmutableScan, mutable::MutableScan},
    ondisk::scan::SsTableScan,
    oracle::timestamp::Timestamped,
    record::{Key, Record},
};

pub enum Entry<'entry, R>
where
    R: Record,
{
    Mutable(crossbeam_skiplist::map::Entry<'entry, Timestamped<R::Key>, Option<R>>),
    Immutable(RecordBatchEntry<R>),
    SsTable(RecordBatchEntry<R>),
}

impl<R> Entry<'_, R>
where
    R: Record,
{
    pub fn key(&self) -> Timestamped<<R::Key as Key>::Ref<'_>> {
        match self {
            Entry::Mutable(entry) => entry
                .key()
                .map(|key| unsafe { transmute(key.as_key_ref()) }),
            Entry::SsTable(entry) => entry.internal_key(),
            Entry::Immutable(entry) => entry.internal_key(),
        }
    }
}

impl<R> fmt::Debug for Entry<'_, R>
where
    R: Record + Debug,
    R::Key: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Entry::Mutable(mutable) => write!(
                f,
                "Entry::Mutable({:?} -> {:?})",
                mutable.key(),
                mutable.value()
            ),
            Entry::SsTable(sstable) => write!(f, "Entry::SsTable({:?})", sstable),
            Entry::Immutable(immutable) => write!(f, "Entry::Immutable({:?})", immutable),
        }
    }
}

pin_project! {
    #[project = ScanStreamProject]
    pub enum ScanStream<'scan, R, E>
    where
        R: Record,
        E: Executor,
    {
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
            inner: SsTableScan<R, E>,
        },
    }
}

impl<'scan, R, E> From<MutableScan<'scan, R>> for ScanStream<'scan, R, E>
where
    R: Record,
    E: Executor,
{
    fn from(inner: MutableScan<'scan, R>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'scan, R, E> From<ImmutableScan<'scan, R>> for ScanStream<'scan, R, E>
where
    R: Record,
    E: Executor,
{
    fn from(inner: ImmutableScan<'scan, R>) -> Self {
        ScanStream::Immutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'scan, R, E> From<SsTableScan<R, E>> for ScanStream<'scan, R, E>
where
    R: Record,
    E: Executor,
{
    fn from(inner: SsTableScan<R, E>) -> Self {
        ScanStream::SsTable { inner }
    }
}

impl<R, E> fmt::Debug for ScanStream<'_, R, E>
where
    R: Record,
    E: Executor,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScanStream::Mutable { .. } => write!(f, "ScanStream::Mutable"),
            ScanStream::SsTable { .. } => write!(f, "ScanStream::SsTable"),
            ScanStream::Immutable { .. } => write!(f, "ScanStream::Immutable"),
        }
    }
}

impl<'scan, R, E> Stream for ScanStream<'scan, R, E>
where
    R: Record,
    E: Executor,
{
    type Item = Result<Entry<'scan, R>, parquet::errors::ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            ScanStreamProject::Mutable { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(Entry::Mutable).map(Ok))
            }
            ScanStreamProject::SsTable { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(|entry| entry.map(Entry::SsTable)))
            }
            ScanStreamProject::Immutable { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(|entry| Ok(Entry::Immutable(entry))))
            }
        }
    }
}
