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
use crate::{fs::FileProvider, record::Record};

pin_project! {
    pub(crate) struct MergeStream<'merge, R, FP>
    where
        R: Record,
        FP: FileProvider,
    {
        streams: Vec<ScanStream<'merge, R, FP>>,
        peeked: BinaryHeap<CmpEntry<'merge, R>>,
        buf: Option<Entry<'merge, R>>,
    }
}

impl<'merge, R, FP> MergeStream<'merge, R, FP>
where
    R: Record,
    FP: FileProvider + 'merge,
{
    pub(crate) async fn from_vec(
        mut streams: Vec<ScanStream<'merge, R, FP>>,
    ) -> Result<Self, parquet::errors::ParquetError> {
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

impl<'merge, R, FP> Stream for MergeStream<'merge, R, FP>
where
    R: Record,
    FP: FileProvider + 'merge,
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
    use crate::{executor::tokio::TokioExecutor, inmem::mutable::Mutable};

    #[tokio::test]
    async fn merge_mutable() {
        let m1 = Mutable::<String>::new();
        m1.remove("b".into(), 3.into());
        m1.insert("c".into(), 4.into());
        m1.insert("d".into(), 5.into());

        let m2 = Mutable::<String>::new();
        m2.insert("a".into(), 1.into());
        m2.insert("b".into(), 2.into());
        m2.insert("c".into(), 3.into());

        let m3 = Mutable::<String>::new();
        m3.insert("e".into(), 4.into());

        let lower = "a".to_string();
        let upper = "e".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));
        let mut merge = MergeStream::<String, TokioExecutor>::from_vec(vec![
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

    #[tokio::test]
    async fn merge_mutable_remove_duplicates() {
        let m1 = Mutable::<String>::new();
        m1.insert("1".into(), 0_u32.into());
        m1.insert("2".into(), 0_u32.into());
        m1.insert("2".into(), 1_u32.into());
        m1.insert("3".into(), 1_u32.into());
        m1.insert("4".into(), 0_u32.into());

        let lower = "1".to_string();
        let upper = "4".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));
        let mut merge =
            MergeStream::<String, TokioExecutor>::from_vec(vec![m1.scan(bound, 0.into()).into()])
                .await
                .unwrap();

        dbg!(merge.next().await);
        dbg!(merge.next().await);
        dbg!(merge.next().await);

        let lower = "1".to_string();
        let upper = "4".to_string();
        let bound = (Bound::Included(&lower), Bound::Included(&upper));
        let mut merge =
            MergeStream::<String, TokioExecutor>::from_vec(vec![m1.scan(bound, 1.into()).into()])
                .await
                .unwrap();

        dbg!(merge.next().await);
        dbg!(merge.next().await);
        dbg!(merge.next().await);
    }
}
