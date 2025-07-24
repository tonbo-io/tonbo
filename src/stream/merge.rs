use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{ready, Stream};
use futures_util::stream::StreamExt;
use pin_project_lite::pin_project;

use super::{Entry, ScanStream};
use crate::{option::Order, record::Record, timestamp::Timestamp};

pin_project! {
    pub struct MergeStream<'merge, R>
    where
        R: Record,
    {
        streams: Vec<ScanStream<'merge, R>>,
        peeked: BinaryHeap<CmpEntry<'merge, R>>,
        buf: Option<Entry<'merge, R>>,
        ts: Timestamp,
        limit: Option<usize>,
        order: Option<Order>,
    }
}

impl<'merge, R> MergeStream<'merge, R>
where
    R: Record,
{
    pub(crate) async fn from_vec(
        mut streams: Vec<ScanStream<'merge, R>>,
        ts: Timestamp,
        order: Option<Order>,
    ) -> Result<Self, parquet::errors::ParquetError> {
        let mut peeked = BinaryHeap::with_capacity(streams.len());

        for (offset, stream) in streams.iter_mut().enumerate() {
            if let Some(entry) = stream.next().await {
                peeked.push(CmpEntry::new(offset, entry?, order));
            }
        }

        let mut merge_stream = Self {
            streams,
            peeked,
            buf: None,
            ts,
            limit: None,
            order,
        };
        merge_stream.next().await;

        Ok(merge_stream)
    }

    /// limit for the stream
    pub(crate) fn limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }
}

impl<'merge, R> Stream for MergeStream<'merge, R>
where
    R: Record,
{
    type Item = Result<Entry<'merge, R>, parquet::errors::ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let ts = this.ts;
        if let Some(limit) = this.limit.as_ref() {
            if *limit == 0 {
                return Poll::Ready(None);
            }
        }
        while let Some(offset) = this.peeked.peek().map(|entry| entry.offset) {
            let next = ready!(Pin::new(&mut this.streams[offset]).poll_next(cx)).transpose()?;
            let peeked = match this.peeked.pop() {
                Some(peeked) => peeked,
                None => return Poll::Ready(None),
            };
            if let Some(next) = next {
                this.peeked.push(CmpEntry::new(offset, next, *this.order));
            }
            if peeked.entry.key().ts > *ts {
                continue;
            }
            if let Some(buf) = this.buf {
                if buf.key().value == peeked.entry.key().value {
                    continue;
                }
            }
            if let Some(limit) = this.limit.as_ref() {
                this.limit.replace(*limit - 1);
            }

            return Poll::Ready(this.buf.replace(peeked.entry).map(Ok));
        }
        Poll::Ready(this.buf.take().map(Ok))
    }
}

#[derive(Debug)]
struct CmpEntry<'stream, R>
where
    R: Record,
{
    offset: usize,
    entry: Entry<'stream, R>,
    order: Option<Order>,
}

impl<'stream, R> CmpEntry<'stream, R>
where
    R: Record,
{
    fn new(offset: usize, entry: Entry<'stream, R>, order: Option<Order>) -> Self {
        Self {
            offset,
            entry,
            order,
        }
    }
}

impl<R> PartialEq for CmpEntry<'_, R>
where
    R: Record,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<R> Eq for CmpEntry<'_, R> where R: Record {}

impl<R> PartialOrd for CmpEntry<'_, R>
where
    R: Record,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<R> Ord for CmpEntry<'_, R>
where
    R: Record,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let natural_ordering = self
            .entry
            .key()
            .cmp(&other.entry.key())
            .then(self.offset.cmp(&other.offset));

        // BinaryHeap is a max-heap, so we need to reverse the ordering for ascending order
        // For ascending order (normal scan), reverse the natural ordering
        // For descending order (reverse scan), use the natural ordering
        if self.order == Some(Order::Desc) {
            natural_ordering
        } else {
            natural_ordering.reverse()
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use fusio::{disk::TokioFs, path::Path, DynFs};
    use futures_util::StreamExt;

    use super::MergeStream;
    use crate::{
        inmem::mutable::MutableMemTable, option::Order, record::test::StringSchema, stream::Entry,
        trigger::TriggerFactory, wal::log::LogType, DbOption,
    };

    #[tokio::test]
    async fn merge_mutable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m1 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();

        m1.remove(LogType::Full, "b".into(), 3.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "c".into(), 4.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "d".into(), 5.into())
            .await
            .unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m2 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();
        m2.insert(LogType::Full, "a".into(), 1.into())
            .await
            .unwrap();
        m2.insert(LogType::Full, "b".into(), 2.into())
            .await
            .unwrap();
        m2.insert(LogType::Full, "c".into(), 3.into())
            .await
            .unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m3 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();
        m3.insert(LogType::Full, "e".into(), 4.into())
            .await
            .unwrap();

        let lower = "a".to_string();
        let upper = "e".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));
        let mut merge = MergeStream::<String>::from_vec(
            vec![
                m1.scan(bound, 6.into(), None).into(),
                m2.scan(bound, 6.into(), None).into(),
                m3.scan(bound, 6.into(), None).into(),
            ],
            6.into(),
            None,
        )
        .await
        .unwrap();

        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "a");
            assert_eq!(entry.key().ts, 1.into());
            assert_eq!(entry.value().as_deref(), Some("a"));
        } else {
            unreachable!()
        }
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "b");
            assert_eq!(entry.key().ts, 3.into());
            assert!(entry.value().is_none());
        } else {
            unreachable!()
        }
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "c");
            assert_eq!(entry.key().ts, 4.into());
            assert_eq!(entry.value().as_deref(), Some("c"));
        } else {
            unreachable!()
        }
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "d");
            assert_eq!(entry.key().ts, 5.into());
            assert_eq!(entry.value().as_deref(), Some("d"));
        } else {
            unreachable!()
        }
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "e");
            assert_eq!(entry.key().ts, 4.into());
            assert_eq!(entry.value().as_deref(), Some("e"));
        } else {
            unreachable!()
        }
        assert!(merge.next().await.is_none());
    }

    #[tokio::test]
    async fn merge_mutable_remove_duplicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m1 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();
        m1.insert(LogType::Full, "1".into(), 0_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "2".into(), 0_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "2".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "3".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "4".into(), 0_u32.into())
            .await
            .unwrap();

        let lower = "1".to_string();
        let upper = "4".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));
        let mut merge = MergeStream::<String>::from_vec(
            vec![m1.scan(bound, 0.into(), None).into()],
            0.into(),
            None,
        )
        .await
        .unwrap();

        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "1");
            assert_eq!(entry.key().ts, 0.into());
            assert_eq!(entry.value().as_deref(), Some("1"));
        };
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "2");
            assert_eq!(entry.key().ts, 0.into());
            assert_eq!(entry.value().as_deref(), Some("2"));
        } else {
            unreachable!()
        }
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "4");
            assert_eq!(entry.key().ts, 0.into());
            assert_eq!(entry.value().as_deref(), Some("4"));
        } else {
            unreachable!()
        }

        let lower = "1".to_string();
        let upper = "4".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));
        let mut merge = MergeStream::<String>::from_vec(
            vec![m1.scan(bound, 1.into(), None).into()],
            1.into(),
            None,
        )
        .await
        .unwrap();

        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "1");
            assert_eq!(entry.key().ts, 0.into());
            assert_eq!(entry.value().as_deref(), Some("1"));
        } else {
            unreachable!()
        }
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "2");
            assert_eq!(entry.key().ts, 1.into());
            assert_eq!(entry.value().as_deref(), Some("2"));
        } else {
            unreachable!()
        }
        if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
            assert_eq!(entry.key().value, "3");
            assert_eq!(entry.key().ts, 1.into());
            assert_eq!(entry.value().as_deref(), Some("3"));
        } else {
            unreachable!()
        };
    }

    #[tokio::test]
    async fn merge_mutable_limit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m1 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();
        m1.insert(LogType::Full, "1".into(), 0_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "2".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "3".into(), 1_u32.into())
            .await
            .unwrap();

        let lower = "1".to_string();
        let upper = "3".to_string();
        {
            let mut merge = MergeStream::<String>::from_vec(
                vec![m1
                    .scan(
                        (Bound::Included(&lower), Bound::Included(&upper)),
                        0.into(),
                        None,
                    )
                    .into()],
                0.into(),
                None,
            )
            .await
            .unwrap()
            .limit(2);

            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "1");
                assert_eq!(entry.key().ts, 0.into());
            } else {
                unreachable!()
            };
            // can not read data from future
            assert!(merge.next().await.is_none());
        }
        {
            let mut merge = MergeStream::<String>::from_vec(
                vec![m1
                    .scan(
                        (Bound::Included(&lower), Bound::Included(&upper)),
                        0.into(),
                        None,
                    )
                    .into()],
                1.into(),
                None,
            )
            .await
            .unwrap()
            .limit(2);

            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "1");
                assert_eq!(entry.key().ts, 0.into());
            } else {
                unreachable!()
            };
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "2");
                assert_eq!(entry.key().ts, 1.into());
            } else {
                unreachable!()
            };
            assert!(merge.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn merge_mutable_reverse() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m1 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();

        // Insert test data
        m1.insert(LogType::Full, "1".into(), 0_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "2".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "3".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "4".into(), 0_u32.into())
            .await
            .unwrap();

        let lower = "1".to_string();
        let upper = "4".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));

        // Test ascending order (default)
        {
            let mut merge = MergeStream::<String>::from_vec(
                vec![m1.scan(bound.clone(), 1.into(), None).into()],
                1.into(),
                None, // Default ascending
            )
            .await
            .unwrap();

            // Check entries come in ascending order: 1, 2, 3, 4
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "1");
            } else {
                unreachable!()
            }
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "2");
            } else {
                unreachable!()
            }
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "3");
            } else {
                unreachable!()
            }
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "4");
            } else {
                unreachable!()
            }
            assert!(merge.next().await.is_none());
        }

        // Test descending order
        {
            let mut merge = MergeStream::<String>::from_vec(
                vec![m1.scan(bound.clone(), 1.into(), Some(Order::Desc)).into()],
                1.into(),
                Some(Order::Desc), // Descending order
            )
            .await
            .unwrap();

            // Check entries come in descending order: 4, 3, 2, 1
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "4");
            } else {
                unreachable!()
            }
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "3");
            } else {
                unreachable!()
            }
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "2");
            } else {
                unreachable!()
            }
            if let Some(Ok(Entry::Mutable(entry))) = merge.next().await {
                assert_eq!(entry.key().value, "1");
            } else {
                unreachable!()
            }
            assert!(merge.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn test_mutable_scan_directly() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m1 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();

        // Insert minimal test data: just 3 simple keys
        m1.insert(LogType::Full, "a".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "b".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "c".into(), 1_u32.into())
            .await
            .unwrap();

        let lower = "a".to_string();
        let upper = "c".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));

        // Test ascending order (default)
        {
            let mut scan = m1.scan(bound.clone(), 1.into(), None);

            let mut results = Vec::new();
            while let Some(entry) = scan.next() {
                let key = entry.key().value.to_string();
                results.push(key);
            }
            assert_eq!(results, vec!["a", "b", "c"]);
        }

        // Test descending order
        {
            let mut scan = m1.scan(bound.clone(), 1.into(), Some(Order::Desc));

            let mut results = Vec::new();
            while let Some(entry) = scan.next() {
                let key = entry.key().value.to_string();
                results.push(key);
            }
            assert_eq!(results, vec!["c", "b", "a"]);
        }
    }

    #[tokio::test]
    async fn merge_controlled_test() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let m1 =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();

        // Insert minimal test data: just 3 simple keys
        m1.insert(LogType::Full, "a".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "b".into(), 1_u32.into())
            .await
            .unwrap();
        m1.insert(LogType::Full, "c".into(), 1_u32.into())
            .await
            .unwrap();

        let lower = "a".to_string();
        let upper = "c".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));

        // Test ascending order (default)
        {
            let mut merge = MergeStream::<String>::from_vec(
                vec![m1.scan(bound.clone(), 1.into(), None).into()],
                1.into(),
                None, // Default ascending
            )
            .await
            .unwrap();

            let mut results = Vec::new();
            while let Some(entry_result) = merge.next().await {
                let entry = entry_result.unwrap();
                let key = entry.key().value.to_string();
                results.push(key);
            }
            // Should be: ["a", "b", "c"]
            assert_eq!(results, vec!["a", "b", "c"]);
        }

        // Test descending order
        {
            let mut merge = MergeStream::<String>::from_vec(
                vec![m1.scan(bound.clone(), 1.into(), Some(Order::Desc)).into()],
                1.into(),
                Some(Order::Desc), // Descending order
            )
            .await
            .unwrap();

            let mut results = Vec::new();
            while let Some(entry_result) = merge.next().await {
                let entry = entry_result.unwrap();
                let key = entry.key().value.to_string();
                results.push(key);
            }
            // Should be: ["c", "b", "a"]
            assert_eq!(results, vec!["c", "b", "a"]);
        }
    }
}
