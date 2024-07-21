use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{ready, Stream};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use pin_project_lite::pin_project;
use tokio_util::compat::Compat;

use crate::{
    fs::FileProvider,
    record::Record,
    stream::record_batch::{RecordBatchEntry, RecordBatchIterator},
};

pin_project! {
    #[derive(Debug)]
    pub struct SsTableScan<R, FP>
    where
        FP: FileProvider,
    {
        #[pin]
        stream: ParquetRecordBatchStream<Compat<FP::File>>,
        iter: Option<RecordBatchIterator<R>>,
    }
}

impl<R, FP> SsTableScan<R, FP>
where
    FP: FileProvider,
{
    pub fn new(stream: ParquetRecordBatchStream<Compat<FP::File>>) -> Self {
        SsTableScan { stream, iter: None }
    }
}

impl<R, FP> Stream for SsTableScan<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    type Item = Result<RecordBatchEntry<R>, parquet::errors::ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.iter {
                Some(iter) => {
                    if let Some(entry) = iter.next() {
                        return Poll::Ready(Some(Ok(entry)));
                    }
                    *this.iter = None;
                }
                None => {
                    let record_batch = ready!(this.stream.as_mut().poll_next(cx)).transpose()?;
                    let record_batch = match record_batch {
                        Some(record_batch) => record_batch,
                        None => return Poll::Ready(None),
                    };
                    *this.iter = Some(RecordBatchIterator::new(record_batch));
                }
            }
        }
    }
}
