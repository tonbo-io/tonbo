use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::datatypes::Schema;
use futures_core::{ready, Stream};
use parquet::arrow::{
    async_reader::{AsyncFileReader, ParquetRecordBatchStream},
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
        stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
        iter: Option<RecordBatchIterator<R>>,
        projection_mask: ProjectionMask,
        full_schema: Arc<Schema>,
        _marker: PhantomData<&'scan ()>
    }
}

impl<R> SsTableScan<'_, R> {
    pub fn new(
        stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
        projection_mask: ProjectionMask,
        full_schema: Arc<Schema>,
    ) -> Self {
        SsTableScan {
            stream,
            iter: None,
            projection_mask,
            full_schema,
            _marker: PhantomData,
        }
    }
}

impl<R> Stream for SsTableScan<'_, R>
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
                        this.full_schema.clone(),
                    ));
                }
            }
        }
    }
}
