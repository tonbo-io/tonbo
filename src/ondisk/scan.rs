use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{ready, Stream};
use parquet::arrow::{
    async_reader::{ParquetObjectReader, ParquetRecordBatchStream},
    ProjectionMask,
};
use pin_project_lite::pin_project;

use crate::{
    record::Record,
    stream::record_batch::{RecordBatchEntry, RecordBatchIterator},
};

pin_project! {
    #[derive(Debug)]
    pub struct SsTableScan<'scan, R> {
        #[pin]
        stream: ParquetRecordBatchStream<ParquetObjectReader>,
        iter: Option<RecordBatchIterator<R>>,
        projection_mask: ProjectionMask,
        _marker: PhantomData<&'scan ()>
    }
}

impl<R> SsTableScan<'_, R> {
    pub fn new(
        stream: ParquetRecordBatchStream<ParquetObjectReader>,
        projection_mask: ProjectionMask,
    ) -> Self {
        SsTableScan {
            stream,
            iter: None,
            projection_mask,
            _marker: PhantomData,
        }
    }
}

impl<'scan, R> Stream for SsTableScan<'scan, R>
where
    R: Record,
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
                    *this.iter = Some(RecordBatchIterator::new(
                        record_batch,
                        this.projection_mask.clone(),
                    ));
                }
            }
        }
    }
}
