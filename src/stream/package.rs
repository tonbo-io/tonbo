use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use pin_project_lite::pin_project;

use crate::{
    fs::FileProvider,
    inmem::immutable::{ArrowArrays, Builder},
    record::Record,
    stream::merge::MergeStream,
};

pin_project! {
    pub struct PackageStream<'package, R, FP>
    where
        R: Record,
        FP: FileProvider,
    {
        row_count: usize,
        batch_size: usize,
        inner: MergeStream<'package, R, FP>,
        builder: <R::Columns as ArrowArrays>::Builder,
        projection_indices: Option<Vec<usize>>,
    }
}

impl<'package, R, FP> PackageStream<'package, R, FP>
where
    R: Record,
    FP: FileProvider + 'package,
{
    pub(crate) fn new(
        batch_size: usize,
        merge: MergeStream<'package, R, FP>,
        projection_indices: Option<Vec<usize>>,
    ) -> Self {
        Self {
            row_count: 0,
            batch_size,
            inner: merge,
            builder: R::Columns::builder(batch_size),
            projection_indices,
        }
    }
}

impl<'package, R, FP> Stream for PackageStream<'package, R, FP>
where
    R: Record,
    FP: FileProvider + 'package,
{
    type Item = Result<R::Columns, parquet::errors::ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut project = self.project();

        while project.row_count <= project.batch_size {
            match Pin::new(&mut project.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(entry))) => {
                    if let Some(record) = entry.value() {
                        // filter null
                        project.builder.push(entry.key(), Some(record));
                        *project.row_count += 1;
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => break,
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready((*project.row_count != 0).then(|| {
            *project.row_count = 0;
            Ok(project
                .builder
                .finish(project.projection_indices.as_ref().map(Vec::as_slice)))
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;

    use futures_util::StreamExt;

    use crate::{
        executor::tokio::TokioExecutor,
        inmem::{
            immutable::{tests::TestImmutableArrays, ArrowArrays},
            mutable::Mutable,
        },
        stream::{merge::MergeStream, package::PackageStream},
        tests::Test,
    };

    #[tokio::test]
    async fn iter() {
        let m1 = Mutable::<Test>::new();
        m1.insert(
            Test {
                vstring: "a".into(),
                vu32: 0,
                vbool: Some(true),
            },
            0.into(),
        );
        m1.insert(
            Test {
                vstring: "b".into(),
                vu32: 1,
                vbool: Some(true),
            },
            1.into(),
        );
        m1.insert(
            Test {
                vstring: "c".into(),
                vu32: 2,
                vbool: Some(true),
            },
            2.into(),
        );
        m1.insert(
            Test {
                vstring: "d".into(),
                vu32: 3,
                vbool: Some(true),
            },
            3.into(),
        );
        m1.insert(
            Test {
                vstring: "e".into(),
                vu32: 4,
                vbool: Some(true),
            },
            4.into(),
        );
        m1.insert(
            Test {
                vstring: "f".into(),
                vu32: 5,
                vbool: Some(true),
            },
            5.into(),
        );

        let merge = MergeStream::<Test, TokioExecutor>::from_vec(vec![m1
            .scan((Bound::Unbounded, Bound::Unbounded), 6.into())
            .into()])
        .await
        .unwrap();
        let projection_indices = vec![0, 1, 2, 3];

        let mut package = PackageStream {
            row_count: 0,
            batch_size: 8192,
            inner: merge,
            builder: TestImmutableArrays::builder(8192),
            projection_indices: Some(projection_indices),
        };

        dbg!(package.next().await);
    }
}
