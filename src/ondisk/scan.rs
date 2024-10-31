use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::datatypes::Schema;
use futures_core::{ready, Stream};
use parquet::arrow::{async_reader::ParquetRecordBatchStream, ProjectionMask};
use pin_project_lite::pin_project;
use tonbo_ext_reader::CacheReader;

use crate::{
    record::Record,
    stream::record_batch::{RecordBatchEntry, RecordBatchIterator},
};

pin_project! {
    #[derive(Debug)]
    pub struct SsTableScan<'scan, R, C>{
        #[pin]
        stream: ParquetRecordBatchStream<C>,
        iter: Option<RecordBatchIterator<R>>,
        projection_mask: ProjectionMask,
        full_schema: Arc<Schema>,
        _marker: PhantomData<&'scan ()>
    }
}

impl<R, C> SsTableScan<'_, R, C> {
    pub(crate) fn new(
        stream: ParquetRecordBatchStream<C>,
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

impl<'scan, R, C> Stream for SsTableScan<'scan, R, C>
where
    R: Record,
    C: CacheReader + 'static,
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
