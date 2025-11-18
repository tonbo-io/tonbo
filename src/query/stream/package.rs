//! Package merged rows into Arrow record batches.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;
use pin_project_lite::pin_project;
use typed_arrow_dyn::{DynBuilders, DynRowRaw};

use crate::{
    inmem::immutable::memtable::RecordBatchStorage,
    query::stream::{StreamError, merge::MergeStream},
};

pin_project! {
    /// Stream adapter that batches merged rows into `RecordBatch` chunks.
    pub struct PackageStream<'t, S>
    {
        row_count: usize,
        batch_size: usize,
        #[pin]
        inner: MergeStream<'t, S>,
        builder: DynRecordBatchBuilder,
    }
}

#[allow(dead_code)]
impl<'t, S> PackageStream<'t, S>
where
    S: RecordBatchStorage,
{
    pub(crate) fn new(batch_size: usize, merge: MergeStream<'t, S>, schema: SchemaRef) -> Self {
        assert!(batch_size > 0, "batch size must be greater than zero");
        Self {
            row_count: 0,
            batch_size,
            inner: merge,
            builder: DynRecordBatchBuilder::new(schema, batch_size),
        }
    }
}

impl<'t, S> Stream for PackageStream<'t, S>
where
    S: RecordBatchStorage,
{
    type Item = Result<RecordBatch, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut upstream_done = false;

        while *this.row_count < *this.batch_size {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(entry))) => {
                    if let Some(row) = entry.into_row() {
                        if let Err(err) = this.builder.append_row(row) {
                            return Poll::Ready(Some(Err(err)));
                        }
                        *this.row_count += 1;
                        continue;
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => {
                    upstream_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        if *this.row_count == 0 {
            return if upstream_done {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        }

        let batch = match this.builder.finish_batch() {
            Ok(batch) => batch,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };
        *this.row_count = 0;

        Poll::Ready(Some(Ok(batch)))
    }
}

struct DynRecordBatchBuilder {
    schema: SchemaRef,
    batch_size: usize,
    builders: DynBuilders,
}

impl DynRecordBatchBuilder {
    #[allow(dead_code)]
    fn new(schema: SchemaRef, batch_size: usize) -> Self {
        let builders = DynBuilders::new(schema.clone(), batch_size);
        Self {
            schema,
            batch_size,
            builders,
        }
    }

    fn append_row(&mut self, row: DynRowRaw) -> Result<(), StreamError> {
        let owned = row.into_owned()?;
        self.builders.append_option_row(Some(owned))?;
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, StreamError> {
        let builders = std::mem::replace(
            &mut self.builders,
            DynBuilders::new(self.schema.clone(), self.batch_size),
        );
        Ok(builders.try_finish_into_batch()?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::projection_for_field,
        inmem::mutable::memtable::DynMem,
        mvcc::Timestamp,
        query::stream::{Order, ScanStream, merge::MergeStream},
        scan::RangeSet,
        test_util::build_batch,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn package_stream_emits_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let extractor = projection_for_field(schema.clone(), 0).expect("extractor");
        let mut mutable = DynMem::new(schema.clone());

        let rows = (0..5)
            .map(|idx| {
                DynRow(vec![
                    Some(DynCell::Str(format!("k{idx}").into())),
                    Some(DynCell::I64(idx as i64)),
                ])
            })
            .collect();
        let batch = build_batch(schema.clone(), rows).expect("batch");
        mutable
            .insert_batch(extractor.as_ref(), batch, Timestamp::new(1))
            .expect("insert batch");

        let ranges = RangeSet::all();
        let mutable_scan = mutable.scan_rows(&ranges, None).expect("scan rows");
        let merge = MergeStream::from_vec(
            vec![ScanStream::<'_, RecordBatch>::from(mutable_scan)],
            Timestamp::MAX,
            None,
            Some(Order::Asc),
        )
        .await
        .expect("merge stream");

        let mut stream = Box::pin(PackageStream::new(2, merge, Arc::clone(&schema)));
        let batches = {
            let mut out = Vec::new();
            while let Some(batch) = stream.next().await {
                out.push(batch.expect("batch ok"));
            }
            out
        };

        assert_eq!(batches.len(), 3, "expected three batches");
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
        assert_eq!(batches[2].num_rows(), 1);

        let mut collected = Vec::new();
        for batch in batches {
            let array = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column");
            for i in 0..array.len() {
                collected.push(array.value(i));
            }
        }
        assert_eq!(collected, vec![0, 1, 2, 3, 4]);
    }
}
