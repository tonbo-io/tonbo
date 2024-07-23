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
    fs::{FileId, FileProvider},
    ondisk::{scan::SsTableScan, sstable::SsTable},
    record::Record,
    scope::Scope,
    stream::record_batch::RecordBatchEntry,
    timestamp::Timestamp,
    version::Version,
    DbOption,
};

enum FutureStatus<'level, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    Init(FileId),
    Ready(SsTableScan<R, FP>),
    OpenFile(Pin<Box<dyn Future<Output = io::Result<FP::File>> + 'level>>),
    LoadStream(Pin<Box<dyn Future<Output = Result<SsTableScan<R, FP>, ParquetError>> + 'level>>),
}

pub(crate) struct LevelStream<'level, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    lower: Bound<&'level R::Key>,
    upper: Bound<&'level R::Key>,
    ts: Timestamp,
    option: Arc<DbOption>,
    gens: VecDeque<FileId>,
    limit: Option<usize>,
    status: FutureStatus<'level, R, FP>,
}

impl<'level, R, FP> LevelStream<'level, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    // Kould: only used by Compaction now, and the start and end of the sstables range are known
    pub(crate) fn new(
        version: &Version<R, FP>,
        level: usize,
        start: usize,
        end: usize,
        range: (Bound<&'level R::Key>, Bound<&'level R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
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
            limit,
            status,
        })
    }
}

impl<'level, R, FP> Stream for LevelStream<'level, R, FP>
where
    R: Record,
    FP: FileProvider + 'level,
{
    type Item = Result<RecordBatchEntry<R>, ParquetError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match &mut self.status {
                FutureStatus::Init(gen) => {
                    let gen = *gen;
                    self.status =
                        FutureStatus::OpenFile(Box::pin(FP::open(self.option.table_path(&gen))));
                    continue;
                }
                FutureStatus::Ready(stream) => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(None) => match self.gens.pop_front() {
                        None => Poll::Ready(None),
                        Some(gen) => {
                            self.status = FutureStatus::OpenFile(Box::pin(FP::open(
                                self.option.table_path(&gen),
                            )));
                            continue;
                        }
                    },
                    Poll::Ready(Some(result)) => {
                        if let Some(limit) = &mut self.limit {
                            *limit -= 1;
                        }
                        Poll::Ready(Some(result))
                    }
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::OpenFile(file_future) => match Pin::new(file_future).poll(cx) {
                    Poll::Ready(Ok(file)) => {
                        self.status = FutureStatus::LoadStream(Box::pin(SsTable::open(file).scan(
                            (self.lower, self.upper),
                            self.ts,
                            self.limit,
                        )));
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
