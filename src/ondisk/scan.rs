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
    record::{Record, RecordRef},
    stream::record_batch::{RecordBatchEntry, RecordBatchIterator},
};

pin_project! {
    #[derive(Debug)]
    pub struct SsTableScan<'scan, R>
    where
        R: Record,
    {
        #[pin]
        stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
        iter: Option<RecordBatchIterator<R>>,
        projection_mask: ProjectionMask,
        full_schema: Arc<Schema>,
        reverse: bool,
        batches: Option<Vec<arrow::array::RecordBatch>>,
        current_batch_index: Option<usize>,
        current_row_index: Option<usize>,
        collection_complete: bool,
        _marker: PhantomData<&'scan ()>
    }
}

impl<R> SsTableScan<'_, R>
where
    R: Record,
{
    pub fn new(
        stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
        projection_mask: ProjectionMask,
        full_schema: Arc<Schema>,
        reverse: bool,
    ) -> Self {
        SsTableScan {
            stream,
            iter: None,
            projection_mask,
            full_schema,
            reverse,
            batches: None,
            current_batch_index: None,
            current_row_index: None,
            collection_complete: false,
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
        
        if *this.reverse {
            if !*this.collection_complete {
                if this.batches.is_none() {
                    *this.batches = Some(Vec::new());
                }
                loop {
                    match this.stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            this.batches.as_mut().unwrap().push(batch);
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            let all_batches = this.batches.as_ref().unwrap();
                            
                            if all_batches.is_empty() {
                                *this.current_batch_index = None;
                                *this.current_row_index = None;
                            } else {
                                let mut batch_idx = all_batches.len() - 1;
                                while batch_idx > 0 && all_batches[batch_idx].num_rows() == 0 {
                                    batch_idx -= 1;
                                }
                                
                                if all_batches[batch_idx].num_rows() > 0 {
                                    let row_idx = all_batches[batch_idx].num_rows() - 1;
                                    *this.current_batch_index = Some(batch_idx);
                                    *this.current_row_index = Some(row_idx);
                                } else {
                                    *this.current_batch_index = None;
                                    *this.current_row_index = None;
                                }
                            }
                            *this.collection_complete = true;
                            break;
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
            }
            // Return in reverse order
            if let (Some(batches), Some(batch_idx), Some(row_idx)) = 
                (this.batches.as_ref(), *this.current_batch_index, *this.current_row_index) 
            {
                if batch_idx < batches.len() {
                    let batch = &batches[batch_idx];
                    if row_idx < batch.num_rows() {
                        let record = R::Ref::from_record_batch(
                            batch,
                            row_idx,
                            this.projection_mask,
                            this.full_schema,
                        );
                        let entry = RecordBatchEntry::new(batch.clone(), unsafe {
                            std::mem::transmute::<crate::record::option::OptionRecordRef<'_, R::Ref<'_>>, crate::record::option::OptionRecordRef<'static, R::Ref<'static>>>(
                                record,
                            )
                        });
                        
                        if row_idx == 0 {
                            if batch_idx == 0 {
                                *this.current_batch_index = None;
                                *this.current_row_index = None;
                            } else {
                                let prev_batch_idx = batch_idx - 1;
                                let prev_row_idx = if batches[prev_batch_idx].num_rows() > 0 {
                                    batches[prev_batch_idx].num_rows() - 1
                                } else {
                                    0
                                };
                                *this.current_batch_index = Some(prev_batch_idx);
                                *this.current_row_index = Some(prev_row_idx);
                            }
                        } else {
                            *this.current_row_index = Some(row_idx - 1);
                        }
                        
                        return Poll::Ready(Some(Ok(entry)));
                    }
                }
                return Poll::Ready(None);
            } else {
                return Poll::Ready(None);
            }
        } else {
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
}
