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
use crate::{record::Record, timestamp::Timestamp};

pin_project! {
    pub struct MergeStream<'merge, R>
    where
        R: Record,
    {
        streams: Vec<ScanStream<'merge, R>>,
        peeked: BinaryHeap<CmpEntry<'merge, R>>,
        buf: Option<Entry<'merge, R>>,
        ts: Timestamp,
    }
}

impl<'merge, R> MergeStream<'merge, R>
where
    R: Record,
{
    pub(crate) async fn from_vec(
        mut streams: Vec<ScanStream<'merge, R>>,
        ts: Timestamp,
    ) -> Result<Self, parquet::errors::ParquetError> {
        let mut peeked = BinaryHeap::with_capacity(streams.len());

        for (offset, stream) in streams.iter_mut().enumerate() {
            if let Some(entry) = stream.next().await {
                peeked.push(CmpEntry::new(offset, entry?));
            }
        }

        let mut merge_stream = Self {
            streams,
            peeked,
            buf: None,
            ts,
        };
        merge_stream.next().await;

        Ok(merge_stream)
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
        while let Some(offset) = this.peeked.peek().map(|entry| entry.offset) {
            let next = ready!(Pin::new(&mut this.streams[offset]).poll_next(cx)).transpose()?;
            let peeked = match this.peeked.pop() {
                Some(peeked) => peeked,
                None => return Poll::Ready(None),
            };
            if let Some(next) = next {
                this.peeked.push(CmpEntry::new(offset, next));
            }
            if let Some(buf) = this.buf {
                if peeked.entry.key().ts > *ts || buf.key().value == peeked.entry.key().value {
                    continue;
                }
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
}

impl<'stream, R> CmpEntry<'stream, R>
where
    R: Record,
{
    fn new(offset: usize, entry: Entry<'stream, R>) -> Self {
        Self { offset, entry }
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
        self.entry
            .key()
            .cmp(&other.entry.key())
            .then(self.offset.cmp(&other.offset))
            .reverse()
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Bound, str::FromStr, sync::Arc};

    use futures_util::StreamExt;
    use url::Url;

    use super::MergeStream;
    use crate::{
        executor::tokio::TokioExecutor, fs::FileProvider, inmem::mutable::Mutable, stream::Entry,
        trigger::TriggerFactory, wal::log::LogType, DbOption,
    };

    #[tokio::test]
    async fn merge_mutable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_root_url = Url::from_str("memory:").unwrap();
        let option = DbOption::build(&temp_dir.path(), table_root_url);
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let m1 = Mutable::<String, TokioExecutor>::new(&option, trigger)
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

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let m2 = Mutable::<String, TokioExecutor>::new(&option, trigger)
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

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let m3 = Mutable::<String, TokioExecutor>::new(&option, trigger)
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
                m1.scan(bound, 6.into()).into(),
                m2.scan(bound, 6.into()).into(),
                m3.scan(bound, 6.into()).into(),
            ],
            6.into(),
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
        let table_root_url = Url::from_str("memory:").unwrap();
        let option = DbOption::build(&temp_dir.path(), table_root_url);
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let m1 = Mutable::<String, TokioExecutor>::new(&option, trigger)
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
        let mut merge =
            MergeStream::<String>::from_vec(vec![m1.scan(bound, 0.into()).into()], 0.into())
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
        let mut merge =
            MergeStream::<String>::from_vec(vec![m1.scan(bound, 1.into()).into()], 1.into())
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
}
