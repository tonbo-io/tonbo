use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;
use parquet::{arrow::ProjectionMask, errors::ParquetError};
use pin_project_lite::pin_project;

use crate::{
    record::Record,
    stream::{Entry, ScanStream},
};

pin_project! {
    pub struct MemProjectionStream<'projection, R>
    where
        R: Record,
    {
        stream: Box<ScanStream<'projection, R>>,
        projection_mask: Arc<ProjectionMask>,
    }
}

impl<'projection, R> MemProjectionStream<'projection, R>
where
    R: Record,
{
    pub(crate) fn new(stream: ScanStream<'projection, R>, projection_mask: ProjectionMask) -> Self {
        Self {
            stream: Box::new(stream),
            projection_mask: Arc::new(projection_mask),
        }
    }
}

impl<'projection, R> Stream for MemProjectionStream<'projection, R>
where
    R: Record,
{
    type Item = Result<Entry<'projection, R>, ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut project = self.project();

        return match Pin::new(&mut project.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(entry))) => Poll::Ready(Some(Ok(Entry::Projection((
                Box::new(entry),
                project.projection_mask.clone(),
            ))))),
            poll => poll,
        };
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use fusio::{disk::TokioFs, path::Path, DynFs};
    use futures_util::StreamExt;
    use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};

    use crate::{
        inmem::mutable::MutableMemTable, stream::mem_projection::MemProjectionStream, tests::Test,
        trigger::TriggerFactory, wal::log::LogType, DbOption,
    };

    #[tokio::test]
    async fn merge_mutable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());

        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let schema = Arc::new(Test::schema());

        let mutable = MutableMemTable::<Test>::new(&option, trigger, fs.clone(), schema.clone())
            .await
            .unwrap();

        mutable
            .insert(
                LogType::Full,
                Test {
                    vstring: "0".to_string(),
                    vu32: 0,
                    vbool: Some(true),
                },
                0.into(),
            )
            .await
            .unwrap();
        mutable
            .insert(
                LogType::Full,
                Test {
                    vstring: "1".to_string(),
                    vu32: 1,
                    vbool: Some(true),
                },
                0.into(),
            )
            .await
            .unwrap();
        mutable
            .insert(
                LogType::Full,
                Test {
                    vstring: "2".to_string(),
                    vu32: 2,
                    vbool: Some(true),
                },
                0.into(),
            )
            .await
            .unwrap();

        let mask = ProjectionMask::roots(
            &ArrowSchemaConverter::new()
                .convert(schema.arrow_schema())
                .unwrap(),
            vec![0, 1, 2, 4],
        );

        let mut stream = MemProjectionStream::<Test>::new(
            mutable
                .scan((Bound::Unbounded, Bound::Unbounded), 6.into())
                .into(),
            mask,
        );

        let entry_0 = stream.next().await.unwrap().unwrap();
        assert!(entry_0.value().unwrap().vu32.is_none());
        assert_eq!(entry_0.value().unwrap().vstring, "0");
        assert_eq!(entry_0.value().unwrap().vbool, Some(true));

        let entry_1 = stream.next().await.unwrap().unwrap();
        assert!(entry_1.value().unwrap().vu32.is_none());
        assert_eq!(entry_1.value().unwrap().vstring, "1");
        assert_eq!(entry_1.value().unwrap().vbool, Some(true));

        let entry_2 = stream.next().await.unwrap().unwrap();
        assert!(entry_2.value().unwrap().vu32.is_none());
        assert_eq!(entry_2.value().unwrap().vstring, "2");
        assert_eq!(entry_2.value().unwrap().vbool, Some(true));

        assert!(stream.next().await.is_none())
    }
}
