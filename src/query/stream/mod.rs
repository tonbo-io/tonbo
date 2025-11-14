//! Read path scaffolding (planner, plan, executor).

pub(crate) mod merge;
pub(crate) mod package;

use core::fmt;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, ready, stream};
use pin_project_lite::pin_project;
use thiserror::Error;
use typed_arrow_dyn::{DynError, DynRowRaw, DynViewError};

use crate::{
    inmem::{
        immutable::memtable::{ImmutableVisibleScan, RecordBatchStorage},
        mutable::memtable::DynRowScan,
    },
    key::KeyTsViewRaw,
    mvcc::Timestamp,
    ondisk::scan::{SstableScan, SstableScanError},
};

/// Direction for ordered scans.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Order {
    /// Ascending (lowest to highest primary key).
    #[default]
    Asc,
    /// Descending (highest to lowest primary key).
    Desc,
}

/// Source priority applied while reconciling overlapping entries.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum SourcePriority {
    /// In-flight transaction stream (highest priority).
    Txn,
    /// Mutable memtable scan.
    Mutable,
    /// Frozen memtable scan.
    Immutable,
    /// SSTable scan (lowest priority).
    Sstable,
}

/// Errors surfaced while composing read streams.
#[derive(Debug, Error)]
pub enum StreamError {
    /// Failure originating while scanning SSTables.
    #[error(transparent)]
    SsTable(#[from] SstableScanError),
    /// Dynamic row conversion failed while materializing a batch.
    #[error("dynamic row conversion failed: {0}")]
    DynRow(#[from] DynViewError),
    /// Building a projected batch failed.
    #[error("dynamic batch construction failed: {0}")]
    DynBuilder(#[from] DynError),
}

/// Unified entry yielded by the read stream for both in-memory and on-disk sources.
#[allow(dead_code)]
#[derive(Debug)]
pub enum StreamEntry<'t> {
    /// Entry sourced from the mutable layer.
    MemTable((&'t KeyTsViewRaw, DynRowRaw)),
    /// Entry produced by the SSTable scan pipeline.
    Sstable((KeyTsViewRaw, DynRowRaw)),
}

#[allow(dead_code)]
impl<'t> StreamEntry<'t> {
    pub(crate) fn key(&self) -> &KeyTsViewRaw {
        match self {
            StreamEntry::MemTable((key_ts, _)) => key_ts,
            StreamEntry::Sstable((key_ts, _)) => key_ts,
        }
    }

    pub(crate) fn ts(&self) -> Timestamp {
        match self {
            StreamEntry::MemTable((key_ts, _)) => key_ts.timestamp(),
            StreamEntry::Sstable((key_ts, _)) => key_ts.timestamp(),
        }
    }

    pub(crate) fn into_row(self) -> DynRowRaw {
        match self {
            StreamEntry::MemTable((_, row)) => row,
            StreamEntry::Sstable((_, row)) => row,
        }
    }
}

pin_project! {
    #[project = ScanStreamProject]
    pub(crate) enum ScanStream<'t, S>
    {
        Mutable {
            #[pin]
            inner: stream::Iter<DynRowScan<'t>>,
        },
        Immutable {
            #[pin]
            inner: stream::Iter<ImmutableVisibleScan<'t, S>>,
        },
        SsTable {
            #[pin]
            inner: SstableScan<'t>,
        },
    }
}

impl<'t, S: RecordBatchStorage> From<ImmutableVisibleScan<'t, S>> for ScanStream<'t, S> {
    fn from(inner: ImmutableVisibleScan<'t, S>) -> Self {
        ScanStream::Immutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, S> From<DynRowScan<'t>> for ScanStream<'t, S> {
    fn from(inner: DynRowScan<'t>) -> Self {
        ScanStream::Mutable {
            inner: stream::iter(inner),
        }
    }
}

impl<'t, S> From<SstableScan<'t>> for ScanStream<'t, S> {
    fn from(inner: SstableScan<'t>) -> Self {
        ScanStream::SsTable { inner }
    }
}

impl<'t, S: std::fmt::Debug> fmt::Debug for ScanStream<'t, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutable { inner } => f
                .debug_struct("ScanStream::Mutable")
                .field("inner", inner)
                .finish(),
            Self::Immutable { inner } => f
                .debug_struct("ScanStream::Immutable")
                .field("inner", inner)
                .finish(),
            Self::SsTable { .. } => f.debug_struct("ScanStream::SsTable").finish(),
        }
    }
}

impl<'t, S> ScanStream<'t, S> {
    pub(crate) fn priority(&self) -> SourcePriority {
        match self {
            ScanStream::Mutable { .. } => SourcePriority::Mutable,
            ScanStream::Immutable { .. } => SourcePriority::Immutable,
            ScanStream::SsTable { .. } => SourcePriority::Sstable,
        }
    }
}

impl<'t, S> Stream for ScanStream<'t, S>
where
    S: RecordBatchStorage,
{
    type Item = Result<StreamEntry<'t>, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            ScanStreamProject::Mutable { inner } => match ready!(inner.poll_next(cx)) {
                Some(Ok(entry)) => Poll::Ready(Some(Ok(StreamEntry::MemTable(entry)))),
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::DynRow(err)))),
                None => Poll::Ready(None),
            },
            ScanStreamProject::Immutable { inner } => match ready!(inner.poll_next(cx)) {
                Some(Ok(entry)) => Poll::Ready(Some(Ok(StreamEntry::MemTable(entry)))),
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::DynRow(err)))),
                None => Poll::Ready(None),
            },
            ScanStreamProject::SsTable { inner } => match ready!(inner.poll_next(cx)) {
                Some(Ok(entry)) => Poll::Ready(Some(Ok(StreamEntry::Sstable(entry)))),
                Some(Err(err)) => Poll::Ready(Some(Err(StreamError::from(err)))),
                None => Poll::Ready(None),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::projection_for_field, inmem::mutable::memtable::DynMem, mvcc::Timestamp,
        scan::RangeSet, test_util::build_batch,
    };

    #[tokio::test]
    async fn scan_stream_mutable_variant_yields_entries() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let extractor = projection_for_field(schema.clone(), 0).expect("extractor");
        let mut mem = DynMem::new(schema.clone());
        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k0".into())),
            Some(DynCell::I64(42)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        mem.insert_batch(extractor.as_ref(), batch, Timestamp::new(1))
            .expect("insert");

        let ranges = RangeSet::all();
        let scan = mem
            .scan_rows(&ranges, None)
            .expect("scan rows produces iterator");
        let mut stream = ScanStream::<RecordBatch>::from(scan);
        let next = stream.next().await.expect("entry present");
        assert!(next.is_ok(), "entry ok");
    }
}
