//! Build read plans from caller options and database state.

use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt, ready};
use pin_project_lite::pin_project;

use super::Order;
use crate::{
    inmem::immutable::memtable::RecordBatchStorage,
    mvcc::Timestamp,
    query::stream::{ScanStream, SourcePriority, StreamEntry, StreamError},
};

pin_project! {
    /// Stream that merges multiple ordered sources respecting MVCC ordering semantics.
    pub struct MergeStream<'t, S>
    {
        streams: Vec<ScanStream<'t, S>>,
        peeked: BinaryHeap<Reverse<HeapEntry<'t>>>,
        buf: Option<StreamEntry<'t>>,
        ts: Timestamp,
        limit: Option<usize>,
        order: Option<Order>,
        stream_priority: Vec<SourcePriority>,
    }
}

impl<'t, S> MergeStream<'t, S>
where
    S: RecordBatchStorage,
{
    pub(crate) async fn from_vec(
        mut streams: Vec<ScanStream<'t, S>>,
        ts: Timestamp,
        limit: Option<usize>,
        order: Option<Order>,
    ) -> Result<Self, StreamError> {
        let mut peeked = BinaryHeap::with_capacity(streams.len());
        let mut stream_priority = Vec::with_capacity(streams.len());

        for (offset, stream) in streams.iter_mut().enumerate() {
            let priority = stream.priority();
            stream_priority.push(priority);
            if let Some(entry) = stream.next().await {
                peeked.push(Reverse(HeapEntry::new(offset, priority, entry?, order)));
            }
        }

        let merge_stream = Self {
            streams,
            peeked,
            buf: None,
            ts,
            limit,
            order,
            stream_priority,
        };
        Ok(merge_stream)
    }
}

impl<'t, S> Stream for MergeStream<'t, S>
where
    S: RecordBatchStorage,
{
    type Item = Result<StreamEntry<'t>, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if this.limit.as_ref().is_some_and(|limit| *limit == 0) {
            return Poll::Ready(None);
        }
        while let Some(Reverse(heap_entry)) = this.peeked.pop() {
            let stream_idx = heap_entry.stream_idx;
            let entry = heap_entry.entry;
            let next = ready!(Pin::new(&mut this.streams[stream_idx]).poll_next(cx)).transpose()?;
            if let Some(next) = next {
                let priority = this.stream_priority[stream_idx];
                this.peeked.push(Reverse(HeapEntry::new(
                    stream_idx,
                    priority,
                    next,
                    *this.order,
                )));
            }

            // De-duplicate keys from different streams (timestamps may differ).
            let duplicate_key = {
                let entry_key = entry.key().key();
                this.buf
                    .as_ref()
                    .is_some_and(|buf| buf.key().key() == entry_key)
            };
            if duplicate_key {
                continue;
            }
            if let Some(prev) = this.buf.replace(entry) {
                if prev.is_tombstone() {
                    continue;
                }
                decrement_limit(this.limit);
                if prev.is_tombstone() {
                    continue;
                }
                return Poll::Ready(Some(Ok(prev)));
            }
        }
        loop {
            match this.buf.take() {
                Some(entry) if entry.is_tombstone() => continue,
                Some(entry) => {
                    decrement_limit(this.limit);
                    return Poll::Ready(Some(Ok(entry)));
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

struct HeapEntry<'t> {
    stream_idx: usize,
    source_priority: SourcePriority,
    entry: StreamEntry<'t>,
    order: Option<Order>,
}

impl<'t> HeapEntry<'t> {
    pub(crate) fn new(
        stream_idx: usize,
        priority: SourcePriority,
        entry: StreamEntry<'t>,
        order: Option<Order>,
    ) -> Self {
        Self {
            stream_idx,
            source_priority: priority,
            entry,
            order,
        }
    }
}

impl<'t> Ord for HeapEntry<'t> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Entries are ordered by commit timestamp, then key ordering, followed by source priority
        // (txn > mutable > immutable > SST).
        let ordering = self.order.unwrap_or(Order::Asc);
        let key_cmp = match ordering {
            Order::Asc => self.entry.key().cmp(other.entry.key()),
            Order::Desc => other.entry.key().cmp(self.entry.key()),
        };
        // Timestamps are always ordered descending (latest first) regardless of key order.
        let ts_cmp = other.entry.ts().cmp(&self.entry.ts());
        key_cmp.then(ts_cmp.then(self.source_priority.cmp(&other.source_priority)))
    }
}

impl<'t> PartialOrd for HeapEntry<'t> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'t> PartialEq for HeapEntry<'t> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'t> Eq for HeapEntry<'t> {}

fn decrement_limit(limit: &mut Option<usize>) {
    if let Some(value) = limit {
        *value = value.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::{projection_for_columns, projection_for_field},
        inmem::{immutable::memtable::ImmutableMemTable, mutable::memtable::DynMem},
        key::{KeyOwned, RangeSet},
        mutation::DynMutation,
        mvcc::{MVCC_COMMIT_COL, Timestamp},
        query::stream::ScanStream,
        test_util::build_batch,
        transaction::TransactionScan,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_stream_prefers_higher_priority_for_same_key() {
        async fn run_merge(order: Order) -> Vec<(String, i64, u64)> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("v", DataType::Int64, true),
            ]));
            let extractor = projection_for_field(schema.clone(), 0).expect("extractor");

            let mut mutable = DynMem::new(schema.clone());
            let mut immutable_builder = DynMem::new(schema.clone());
            let insert_row = |table: &mut DynMem, key: &str, value: i64, ts: u64| {
                let batch = build_batch(
                    schema.clone(),
                    vec![DynRow(vec![
                        Some(DynCell::Str(key.into())),
                        Some(DynCell::I64(value)),
                    ])],
                )
                .expect("batch");
                table
                    .insert_batch(extractor.as_ref(), batch, Timestamp::new(ts))
                    .expect("insert");
            };

            insert_row(&mut mutable, "a", 2, 50);
            insert_row(&mut mutable, "b", 5, 20);
            insert_row(&mut mutable, "c", 10, 10);
            // Mutable + immutable both expose ("d", ts=60); mutable should win via source priority.
            insert_row(&mut mutable, "d", 100, 60);

            insert_row(&mut immutable_builder, "a", 1, 50);
            insert_row(&mut immutable_builder, "c", 40, 40);
            insert_row(&mut immutable_builder, "d", 25, 60);

            let immutable_segment: ImmutableMemTable<RecordBatch> = immutable_builder
                .seal_into_immutable(&schema, extractor.as_ref())
                .expect("seal ok")
                .expect("segment");

            let ranges = RangeSet::all();
            let mutable_scan = mutable.scan_rows(&ranges, None).expect("mutable scan");
            let immutable_scan = immutable_segment
                .scan_visible(&ranges, None, Timestamp::MAX)
                .expect("immutable scan");
            let mut streams = vec![
                ScanStream::<'_, RecordBatch>::from(immutable_scan),
                ScanStream::<'_, RecordBatch>::from(mutable_scan),
            ];

            let mut staged = BTreeMap::new();
            staged.insert(
                KeyOwned::from("d"),
                DynMutation::Upsert(DynRow(vec![
                    Some(DynCell::Str("d".into())),
                    Some(DynCell::I64(500)),
                ])),
            );
            let txn_scan =
                TransactionScan::new(&staged, RangeSet::all(), &schema, Timestamp::new(60), None)
                    .expect("txn scan");
            streams.push(ScanStream::<'_, RecordBatch>::from(txn_scan));

            let mut staged = BTreeMap::new();
            staged.insert(
                KeyOwned::from("d"),
                DynMutation::Upsert(DynRow(vec![
                    Some(DynCell::Str("d".into())),
                    Some(DynCell::I64(500)),
                ])),
            );
            let txn_scan =
                TransactionScan::new(&staged, RangeSet::all(), &schema, Timestamp::new(60), None)
                    .expect("txn scan");
            streams.push(ScanStream::<'_, RecordBatch>::from(txn_scan));

            let mut merge = MergeStream::from_vec(streams, Timestamp::MAX, None, Some(order))
                .await
                .expect("merge built");

            let mut rows = Vec::new();
            while let Some(entry) = merge.next().await {
                let entry = entry.expect("entry ok");
                let (key, ts, row) = match entry {
                    StreamEntry::Txn((key_ts, row)) => {
                        (key_ts.key().to_owned(), key_ts.timestamp(), row)
                    }
                    StreamEntry::MemTable((key_ts, row)) => {
                        (key_ts.key().to_owned(), key_ts.timestamp(), row)
                    }
                    StreamEntry::MemTableTombstone(_) => continue,
                    StreamEntry::Sstable((key_ts, row)) => {
                        (key_ts.key().to_owned(), key_ts.timestamp(), row)
                    }
                    StreamEntry::TxnTombstone(_) => continue,
                };
                let key_str = key.as_utf8().expect("utf8 key").to_string();
                let value = row.into_owned().expect("row owned").0[1]
                    .as_ref()
                    .and_then(|cell| match cell {
                        DynCell::I64(v) => Some(*v),
                        _ => None,
                    })
                    .expect("int value");
                rows.push((key_str, value, ts.get()));
            }
            rows
        }

        let asc = run_merge(Order::Asc).await;
        assert_eq!(
            asc,
            vec![
                ("a".to_string(), 2, 50),
                ("b".to_string(), 5, 20),
                ("c".to_string(), 40, 40),
                ("d".to_string(), 500, 60),
            ],
            "ascending order should emit keys from smallest to largest, preferring newer \
             timestamps and higher priority sources",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_stream_hides_rows_for_tombstoned_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let extractor = projection_for_field(schema.clone(), 0).expect("extractor");

        let mut mutable = DynMem::new(schema.clone());
        let mut immutable_builder = DynMem::new(schema.clone());
        let insert_row = |table: &mut DynMem, key: &str, value: i64, ts: u64| {
            let batch = build_batch(
                schema.clone(),
                vec![DynRow(vec![
                    Some(DynCell::Str(key.into())),
                    Some(DynCell::I64(value)),
                ])],
            )
            .expect("batch");
            table
                .insert_batch(extractor.as_ref(), batch, Timestamp::new(ts))
                .expect("insert");
        };

        insert_row(&mut immutable_builder, "ghost", 10, 10);
        insert_row(&mut immutable_builder, "keep-immutable", 25, 12);
        let immutable_segment: ImmutableMemTable<RecordBatch> = immutable_builder
            .seal_into_immutable(&schema, extractor.as_ref())
            .expect("seal ok")
            .expect("segment");

        insert_row(&mut mutable, "mutable-only", 99, 40);
        let delete_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let delete_projection =
            projection_for_columns(delete_schema.clone(), vec![0]).expect("delete projection");
        let delete_rows = vec![DynRow(vec![
            Some(DynCell::Str("ghost".into())),
            Some(DynCell::U64(30)),
        ])];
        let delete_batch = build_batch(delete_schema, delete_rows).expect("delete batch");
        mutable
            .insert_delete_batch(delete_projection.as_ref(), delete_batch)
            .expect("delete row");

        let ranges = RangeSet::all();
        let immutable_scan = immutable_segment
            .scan_visible(&ranges, None, Timestamp::MAX)
            .expect("immutable scan");
        let mutable_scan = mutable.scan_rows(&ranges, None).expect("mutable scan");
        let streams = vec![
            ScanStream::from(immutable_scan),
            ScanStream::from(mutable_scan),
        ];
        let mut merge = MergeStream::from_vec(streams, Timestamp::MAX, None, Some(Order::Asc))
            .await
            .expect("merge built");

        let mut rows = Vec::new();
        while let Some(entry) = merge.next().await {
            let entry = entry.expect("entry ok");
            let (key, row) = match entry {
                StreamEntry::Txn((key_ts, row)) => (key_ts.key().to_owned(), row),
                StreamEntry::MemTable((key_ts, row)) => (key_ts.key().to_owned(), row),
                StreamEntry::Sstable((key_ts, row)) => (key_ts.key().to_owned(), row),
                StreamEntry::TxnTombstone(_) | StreamEntry::MemTableTombstone(_) => continue,
            };
            let key_str = key.as_utf8().expect("utf8 key").to_string();
            let value = row.into_owned().expect("row").0[1]
                .as_ref()
                .and_then(|cell| match cell {
                    DynCell::I64(v) => Some(*v),
                    _ => None,
                })
                .expect("i64 value");
            rows.push((key_str, value));
        }

        assert_eq!(
            rows,
            vec![
                ("keep-immutable".to_string(), 25),
                ("mutable-only".to_string(), 99)
            ],
            "ghost should be suppressed because the mutable tombstone is newer"
        );
    }
}
