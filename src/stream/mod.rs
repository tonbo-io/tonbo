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
    fs::FileProvider,
    inmem::{immutable::ImmutableScan, mutable::MutableScan},
    ondisk::scan::SsTableScan,
    oracle::timestamp::Timestamped,
    record::{Key, Record},
    stream::level::LevelStream,
};

pub enum Entry<'entry, R>
where
    R: Record,
{
    Mutable(crossbeam_skiplist::map::Entry<'entry, Timestamped<R::Key>, Option<R>>),
    Immutable(RecordBatchEntry<R>),
    SsTable(RecordBatchEntry<R>),
    Level(RecordBatchEntry<R>),
}

impl<R> Entry<'_, R>
where
    R: Record,
{
    pub(crate) fn key(&self) -> Timestamped<<R::Key as Key>::Ref<'_>> {
        match self {
            Entry::Mutable(entry) => entry
                .key()
                .map(|key| unsafe { transmute(key.as_key_ref()) }),
            Entry::SsTable(entry) => entry.internal_key(),
            Entry::Immutable(entry) => entry.internal_key(),
            Entry::Level(entry) => entry.internal_key(),
        }
    }

    pub(crate) fn value(&self) -> R::Ref<'_> {
        match self {
            Entry::Mutable(entry) => entry.value().as_ref().map(R::as_record_ref).unwrap(),
            Entry::SsTable(entry) => entry.get(),
            Entry::Immutable(entry) => entry.get(),
            Entry::Level(entry) => entry.get(),
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
            Entry::Level(level) => write!(f, "Entry::Level({:?})", level),
        }
    }
}

pin_project! {
    #[project = ScanStreamProject]
    pub enum ScanStream<'scan, R, FP>
    where
        R: Record,
        FP: FileProvider,
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
            inner: SsTableScan<R, FP>,
        },
        Level {
            #[pin]
            inner: LevelStream<'scan, R, FP>,
        }
    }
}

impl<'scan, R, FP> From<MutableScan<'scan, R>> for ScanStream<'scan, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn from(inner: MutableScan<'scan, R>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'scan, R, FP> From<ImmutableScan<'scan, R>> for ScanStream<'scan, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn from(inner: ImmutableScan<'scan, R>) -> Self {
        ScanStream::Immutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'scan, R, FP> From<SsTableScan<R, FP>> for ScanStream<'scan, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn from(inner: SsTableScan<R, FP>) -> Self {
        ScanStream::SsTable { inner }
    }
}

impl<R, FP> fmt::Debug for ScanStream<'_, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScanStream::Mutable { .. } => write!(f, "ScanStream::Mutable"),
            ScanStream::SsTable { .. } => write!(f, "ScanStream::SsTable"),
            ScanStream::Immutable { .. } => write!(f, "ScanStream::Immutable"),
            ScanStream::Level { .. } => write!(f, "ScanStream::Level"),
        }
    }
}

impl<'scan, R, FP> Stream for ScanStream<'scan, R, FP>
where
    R: Record,
    FP: FileProvider + 'scan,
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
            ScanStreamProject::Level { inner } => {
                Poll::Ready(ready!(inner.poll_next(cx)).map(|entry| entry.map(Entry::Level)))
            }
        }
    }
}
