use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use pin_project_lite::pin_project;

use crate::{
    inmem::immutable::{ArrowArrays, Builder},
    record::{Record, RecordInstance},
    stream::merge::MergeStream,
};

pin_project! {
    pub struct PackageStream<'package, R>
    where
        R: Record,
    {
        row_count: usize,
        batch_size: usize,
        inner: MergeStream<'package, R>,
        builder: <R::Columns as ArrowArrays>::Builder,
        projection_indices: Option<Vec<usize>>,
    }
}

impl<'package, R> PackageStream<'package, R>
where
    R: Record,
{
    pub(crate) fn new(
        batch_size: usize,
        merge: MergeStream<'package, R>,
        projection_indices: Option<Vec<usize>>,
        instance: &RecordInstance,
    ) -> Self {
        Self {
            row_count: 0,
            batch_size,
            inner: merge,
            builder: R::Columns::builder(&instance.arrow_schema::<R>(), batch_size),
            projection_indices,
        }
    }
}

impl<'package, R> Stream for PackageStream<'package, R>
where
    R: Record,
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

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use arrow::array::{BooleanArray, RecordBatch, StringArray, UInt32Array};
    use fusio::{disk::TokioFs, path::Path, DynFs};
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        inmem::{
            immutable::{tests::TestImmutableArrays, ArrowArrays},
            mutable::Mutable,
        },
        record::Record,
        stream::{merge::MergeStream, package::PackageStream},
        tests::Test,
        trigger::TriggerFactory,
        wal::log::LogType,
        DbOption,
    };

    #[tokio::test]
    async fn iter() {
        let temp_dir = TempDir::new().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let m1 = Mutable::<Test>::new(&option, trigger, &fs).await.unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "a".into(),
                vu32: 0,
                vbool: Some(true),
            },
            0.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "b".into(),
                vu32: 1,
                vbool: Some(true),
            },
            1.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "c".into(),
                vu32: 2,
                vbool: Some(true),
            },
            2.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "d".into(),
                vu32: 3,
                vbool: Some(true),
            },
            3.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "e".into(),
                vu32: 4,
                vbool: Some(true),
            },
            4.into(),
        )
        .await
        .unwrap();
        m1.insert(
            LogType::Full,
            Test {
                vstring: "f".into(),
                vu32: 5,
                vbool: Some(true),
            },
            5.into(),
        )
        .await
        .unwrap();

        let merge = MergeStream::<Test>::from_vec(
            vec![m1
                .scan((Bound::Unbounded, Bound::Unbounded), 6.into())
                .into()],
            6.into(),
        )
        .await
        .unwrap();
        let projection_indices = vec![0, 1, 2, 3];

        let mut package = PackageStream {
            row_count: 0,
            batch_size: 8192,
            inner: merge,
            builder: TestImmutableArrays::builder(Test::arrow_schema(), 8192),
            projection_indices: Some(projection_indices.clone()),
        };

        let arrays = package.next().await.unwrap().unwrap();
        assert_eq!(
            arrays.as_record_batch(),
            &RecordBatch::try_new(
                Arc::new(Test::arrow_schema().project(&projection_indices).unwrap(),),
                vec![
                    Arc::new(BooleanArray::from(vec![
                        false, false, false, false, false, false
                    ])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2, 3, 4, 5])),
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2, 3, 4, 5])),
                ],
            )
            .unwrap()
        )
    }
}
