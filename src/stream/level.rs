use std::{
    collections::{Bound, VecDeque},
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;
use pin_project_lite::pin_project;

use crate::{
    executor::Executor,
    fs::FileId,
    ondisk::{scan::SsTableScan, sstable::SsTable},
    oracle::Timestamp,
    record::Record,
    stream::record_batch::RecordBatchEntry,
    DbOption,
};

enum FutureStatus<'level, R, E>
where
    R: Record,
    E: Executor,
{
    Ready(Pin<Box<SsTableScan<R, E>>>),
    OpenFile(Pin<Box<dyn Future<Output = io::Result<E::File>> + 'level>>),
    LoadStream(
        Pin<
            Box<
                dyn Future<Output = Result<SsTableScan<R, E>, parquet::errors::ParquetError>>
                    + 'level,
            >,
        >,
    ),
}

pin_project! {
    pub(crate) struct LevelStream<'level, R, E>
    where
        R: Record,
        E: Executor,
    {
        lower: Bound<&'level R::Key>,
        upper: Bound<&'level R::Key>,
        ts: Timestamp,
        option: Arc<DbOption>,
        gens: VecDeque<FileId>,
        statue: FutureStatus<'level, R, E>,
    }
}

impl<'level, R, E> Stream for LevelStream<'level, R, E>
where
    R: Record,
    E: Executor + 'level,
{
    type Item = Result<RecordBatchEntry<R>, parquet::errors::ParquetError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.statue {
            FutureStatus::Ready(stream) => match Pin::new(stream).poll_next(cx) {
                Poll::Ready(None) => match self.gens.pop_front() {
                    None => Poll::Ready(None),
                    Some(gen) => {
                        self.statue =
                            FutureStatus::OpenFile(Box::pin(E::open(self.option.table_path(&gen))));
                        self.poll_next(cx)
                    }
                },
                poll => poll,
            },
            FutureStatus::OpenFile(file_future) => match Pin::new(file_future).poll(cx) {
                Poll::Ready(Ok(file)) => {
                    self.statue = FutureStatus::LoadStream(Box::pin(
                        SsTable::open(file).scan((self.lower, self.upper), self.ts),
                    ));
                    self.poll_next(cx)
                }
                Poll::Ready(Err(err)) => {
                    Poll::Ready(Some(Err(parquet::errors::ParquetError::from(err))))
                }
                Poll::Pending => Poll::Pending,
            },
            FutureStatus::LoadStream(stream_future) => match Pin::new(stream_future).poll(cx) {
                Poll::Ready(Ok(scan)) => {
                    self.statue = FutureStatus::Ready(Box::pin(scan));
                    self.poll_next(cx)
                }
                Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

// TODO: Test Case after `Compaction`
