use std::{
    collections::{Bound, VecDeque},
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;
use parquet::errors::ParquetError;

use crate::{
    executor::Executor,
    fs::FileId,
    ondisk::{scan::SsTableScan, sstable::SsTable},
    oracle::Timestamp,
    record::Record,
    scope::Scope,
    stream::record_batch::RecordBatchEntry,
    version::Version,
    DbOption,
};

enum FutureStatus<'level, R, E>
where
    R: Record,
    E: Executor,
{
    Init(FileId),
    Ready(SsTableScan<R, E>),
    OpenFile(Pin<Box<dyn Future<Output = io::Result<E::File>> + 'level>>),
    LoadStream(Pin<Box<dyn Future<Output = Result<SsTableScan<R, E>, ParquetError>> + 'level>>),
}

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
    status: FutureStatus<'level, R, E>,
}

impl<'level, R, E> LevelStream<'level, R, E>
where
    R: Record,
    E: Executor,
{
    // Kould: only used by Compaction now, and the start and end of the sstables range are known
    pub(crate) fn new(
        version: &Version<R, E>,
        level: usize,
        start: usize,
        end: usize,
        range: (Bound<&'level R::Key>, Bound<&'level R::Key>),
        ts: Timestamp,
    ) -> Option<Self> {
        let (lower, upper) = range;
        let mut gens: VecDeque<FileId> = version.level_slice[level][start..end + 1]
            .iter()
            .map(Scope::gen)
            .collect();
        let first_gen = gens.pop_front()?;
        let status = FutureStatus::Init(first_gen);

        Some(LevelStream {
            lower,
            upper,
            ts,
            option: version.option().clone(),
            gens,
            status,
        })
    }
}

impl<'level, R, E> Stream for LevelStream<'level, R, E>
where
    R: Record,
    E: Executor + 'level,
{
    type Item = Result<RecordBatchEntry<R>, ParquetError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match &mut self.status {
                FutureStatus::Init(gen) => {
                    let gen = *gen;
                    self.status =
                        FutureStatus::OpenFile(Box::pin(E::open(self.option.table_path(&gen))));
                    continue;
                }
                FutureStatus::Ready(stream) => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(None) => match self.gens.pop_front() {
                        None => Poll::Ready(None),
                        Some(gen) => {
                            self.status = FutureStatus::OpenFile(Box::pin(E::open(
                                self.option.table_path(&gen),
                            )));
                            continue;
                        }
                    },
                    poll => poll,
                },
                FutureStatus::OpenFile(file_future) => match Pin::new(file_future).poll(cx) {
                    Poll::Ready(Ok(file)) => {
                        self.status = FutureStatus::LoadStream(Box::pin(
                            SsTable::open(file).scan((self.lower, self.upper), self.ts),
                        ));
                        continue;
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(ParquetError::from(err)))),
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::LoadStream(stream_future) => match Pin::new(stream_future).poll(cx) {
                    Poll::Ready(Ok(scan)) => {
                        self.status = FutureStatus::Ready(scan);
                        continue;
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                    Poll::Pending => Poll::Pending,
                },
            };
        }
    }
}

// TODO: Test Case after `Compaction`
