use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema, SchemaRef};
use futures::{Stream, ready};
use parquet::{
    arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream},
    errors::ParquetError,
};
use pin_project_lite::pin_project;
use thiserror::Error;
use typed_arrow_dyn::{DynProjection, DynRowRaw, DynSchema, DynViewError};

use crate::{
    extractor::{KeyExtractError, KeyProjection},
    inmem::immutable::memtable::{MVCC_COMMIT_COL, MvccColumns},
    key::KeyTsViewRaw,
    mvcc::Timestamp,
    query::stream::Order,
};

/// Errors surfaced while streaming rows out of SSTables.
#[derive(Debug, Error)]
pub enum SstableScanError {
    /// Parquet error
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    /// Failure to reconstruct the logical key for range filtering.
    #[error("key extraction failed: {0}")]
    Key(#[from] KeyExtractError),
    /// Failure to read row from record batch
    #[error("dynamic view of record batch failure: {0}")]
    DynView(#[from] DynViewError),
}

pin_project! {
pub(crate) struct SstableScan<'t> {
    #[pin]
    data_stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
    iter: Option<RecordBatchIterator<'t>>,
    dyn_schema: DynSchema,
    projection: DynProjection,
    extractor: &'t dyn KeyProjection,
    order: Option<Order>,
    }
}

impl<'t> SstableScan<'t> {
    #[allow(dead_code)]
    pub fn new(
        data_stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
        schema: SchemaRef,
        extractor: &'t dyn KeyProjection,
        projection_indices: Vec<usize>,
        order: Option<Order>,
    ) -> Result<Self, SstableScanError> {
        let has_commit = schema.fields().iter().any(|f| f.name() == MVCC_COMMIT_COL);
        let physical_schema = if has_commit {
            schema
        } else {
            let mut fields = schema.fields().to_vec();
            fields.push(Field::new(MVCC_COMMIT_COL, arrow_schema::DataType::UInt64, false).into());
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        };

        let dyn_schema = DynSchema::from_ref(physical_schema.clone());
        let projection = DynProjection::from_indices(physical_schema.as_ref(), projection_indices)
            .map_err(crate::extractor::map_view_err)?;

        Ok(Self {
            data_stream,
            iter: None,
            dyn_schema,
            projection,
            extractor,
            order,
        })
    }
}

impl<'t> Stream for SstableScan<'t> {
    type Item = Result<(KeyTsViewRaw, DynRowRaw), SstableScanError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        loop {
            if let Some(iter) = this.iter.as_mut() {
                match iter.next() {
                    Some(Ok(entry)) => return Poll::Ready(Some(Ok(entry))),
                    Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                    None => {
                        *this.iter = None;
                        continue;
                    }
                }
            } else {
                let data = ready!(this.data_stream.as_mut().poll_next(cx)).transpose()?;
                let data = match data {
                    Some(d) => d,
                    None => return Poll::Ready(None),
                };
                let mvcc = decode_mvcc_from_data(&data)?;
                let dyn_schema = this.dyn_schema.clone();
                let projection = this.projection.clone();
                let extractor = *this.extractor;
                let order = *this.order;
                *this.iter = Some(RecordBatchIterator::new(
                    data, dyn_schema, projection, extractor, mvcc, order,
                )?);
                continue;
            }
        }
    }
}

struct RecordBatchIterator<'t> {
    batch: RecordBatch,
    extractor: &'t dyn KeyProjection,
    dyn_schema: DynSchema,
    projection: DynProjection,
    mvcc: MvccColumns,
    offset: usize,
    remaining: usize,
    step: isize,
}

impl<'t> RecordBatchIterator<'t> {
    pub(crate) fn new(
        record_batch: RecordBatch,
        dyn_schema: DynSchema,
        projection: DynProjection,
        extractor: &'t dyn KeyProjection,
        mvcc: MvccColumns,
        order: Option<Order>,
    ) -> Result<Self, SstableScanError> {
        let num_rows = record_batch.num_rows();
        let mvcc_len = mvcc.commit_ts.len();
        if mvcc_len != num_rows {
            return Err(SstableScanError::Key(
                KeyExtractError::TombstoneLengthMismatch {
                    expected: num_rows,
                    actual: mvcc_len,
                },
            ));
        }
        let (offset, step) = if matches!(order, Some(Order::Desc)) {
            // Start from the last row for descending order
            (num_rows.saturating_sub(1), -1)
        } else {
            (0, 1)
        };

        Ok(Self {
            batch: record_batch,
            extractor,
            dyn_schema,
            projection,
            mvcc,
            offset,
            remaining: num_rows,
            step,
        })
    }
}

impl<'t> Iterator for RecordBatchIterator<'t> {
    type Item = Result<(KeyTsViewRaw, DynRowRaw), SstableScanError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.remaining > 0 {
            let row_idx = self.offset;
            self.remaining -= 1;
            if self.step < 0 {
                let magnitude = (-self.step) as usize;
                self.offset = self.offset.saturating_sub(magnitude);
            } else if self.step > 0 {
                self.offset = self.offset.saturating_add(self.step as usize);
            }

            if self.mvcc.tombstone.get(row_idx).copied().unwrap_or(false) {
                continue;
            }

            let key_rows = match self.extractor.project_view(&self.batch, &[row_idx]) {
                Ok(rows) => rows,
                Err(err) => return Some(Err(SstableScanError::Key(err))),
            };
            let key_row = match key_rows.into_iter().next() {
                Some(row) => row,
                None => {
                    return Some(Err(SstableScanError::Key(KeyExtractError::RowOutOfBounds(
                        row_idx,
                        self.batch.num_rows(),
                    ))));
                }
            };

            let commit_ts = match self.mvcc.commit_ts.get(row_idx).copied() {
                Some(ts) => ts,
                None => {
                    return Some(Err(SstableScanError::Key(KeyExtractError::RowOutOfBounds(
                        row_idx,
                        self.mvcc.commit_ts.len(),
                    ))));
                }
            };

            let row = match self
                .projection
                .project_row_raw(&self.dyn_schema, &self.batch, row_idx)
            {
                Ok(row) => row,
                Err(err) => return Some(Err(SstableScanError::DynView(err))),
            };

            let key = KeyTsViewRaw::new(key_row, commit_ts);
            return Some(Ok((key, row)));
        }

        None
    }
}

fn decode_mvcc_from_data(batch: &RecordBatch) -> Result<MvccColumns, SstableScanError> {
    let commit_binding = batch.column_by_name(MVCC_COMMIT_COL);
    let commit = commit_binding
        .as_ref()
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            SstableScanError::Parquet(ParquetError::ArrowError(
                "commit_ts column missing or not UInt64".into(),
            ))
        })?;
    if commit.null_count() > 0 {
        return Err(SstableScanError::Parquet(ParquetError::ArrowError(
            "commit_ts column contains nulls".into(),
        )));
    }
    let mut commit_ts = Vec::with_capacity(commit.len());
    for i in 0..commit.len() {
        commit_ts.push(Timestamp::new(commit.value(i)));
    }
    let tombstones = vec![false; commit.len()];
    Ok(MvccColumns::new(commit_ts, tombstones))
}
