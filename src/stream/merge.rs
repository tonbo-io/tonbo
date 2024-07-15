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
use crate::record::Record;

pin_project! {
    pub(crate) struct MergeStream<'merge, R>
    where
        R: Record,
    {
        streams: Vec<ScanStream<'merge, R>>,
        peeked: BinaryHeap<CmpEntry<'merge, R>>,
        buf: Option<Entry<'merge, R>>,
    }
}

impl<'merge, R> MergeStream<'merge, R>
where
    R: Record,
{
    async fn from_iter<T: IntoIterator<Item = ScanStream<'merge, R>>>(
        iter: T,
    ) -> Result<Self, parquet::errors::ParquetError> {
        let mut streams = iter.into_iter().collect::<Vec<_>>();
        let mut peeked = BinaryHeap::with_capacity(streams.len());

        for stream in &mut streams {
            if let Some(entry) = stream.next().await {
                peeked.push(CmpEntry::new(peeked.len(), entry?));
            }
        }

        let mut merge_stream = Self {
            streams,
            peeked,
            buf: None,
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
                if buf.key().value == peeked.entry.key().value {
                    continue;
                }
            }

            return Poll::Ready(this.buf.replace(peeked.entry).map(Ok));
        }
        Poll::Ready(this.buf.take().map(Ok))
    }
}

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
    use std::ops::Bound;
    use futures_util::StreamExt;

    use super::MergeStream;
    use crate::{inmem::mutable::Mutable, oracle::timestamp::Timestamped};

    #[tokio::test]
    async fn merge_mutable() {
        let m1 = Mutable::<String>::new();
        m1.remove(Timestamped::new("b".into(), 3.into()));
        m1.insert(Timestamped::new("c".into(), 4.into()));
        m1.insert(Timestamped::new("d".into(), 5.into()));

        let m2 = Mutable::<String>::new();
        m2.insert(Timestamped::new("a".into(), 1.into()));
        m2.insert(Timestamped::new("b".into(), 2.into()));
        m2.insert(Timestamped::new("c".into(), 3.into()));

        let m3 = Mutable::<String>::new();
        m3.insert(Timestamped::new("e".into(), 4.into()));

        let lower = "a".to_string();
        let upper = "e".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));
        let mut merge = MergeStream::from_iter(vec![
            m1.scan(bound, 6.into()).into(),
            m2.scan(bound, 6.into()).into(),
            m3.scan(bound, 6.into()).into(),
        ])
        .await
        .unwrap();

        dbg!(merge.next().await);
        dbg!(merge.next().await);
        dbg!(merge.next().await);
        dbg!(merge.next().await);
        dbg!(merge.next().await);
        dbg!(merge.next().await);
    }
}
