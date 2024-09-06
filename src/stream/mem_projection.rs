use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;
use parquet::{arrow::ProjectionMask, errors::ParquetError};
use pin_project_lite::pin_project;

use crate::{
    fs::FileProvider,
    record::Record,
    stream::{Entry, ScanStream},
};

pin_project! {
    pub struct MemProjectionStream<'projection, R, FP>
    where
        R: Record,
        FP: FileProvider,
    {
        stream: Box<ScanStream<'projection, R, FP>>,
        projection_mask: Arc<ProjectionMask>,
    }
}

impl<'projection, R, FP> MemProjectionStream<'projection, R, FP>
where
    R: Record,
    FP: FileProvider + 'projection,
{
    pub(crate) fn new(
        stream: ScanStream<'projection, R, FP>,
        projection_mask: ProjectionMask,
    ) -> Self {
        Self {
            stream: Box::new(stream),
            projection_mask: Arc::new(projection_mask),
        }
    }
}

impl<'projection, R, FP> Stream for MemProjectionStream<'projection, R, FP>
where
    R: Record,
    FP: FileProvider + 'projection,
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

#[cfg(test)]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use futures_util::StreamExt;
    use parquet::arrow::{arrow_to_parquet_schema, ProjectionMask};

    use crate::{
        executor::tokio::TokioExecutor, fs::FileProvider, inmem::mutable::Mutable, record::Record,
        stream::mem_projection::MemProjectionStream, tests::Test, trigger::TriggerFactory,
        wal::log::LogType, DbOption,
    };

    #[tokio::test]
    async fn merge_mutable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let option = DbOption::from(temp_dir.path());
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let mutable = Mutable::<Test, TokioExecutor>::new(&option, trigger)
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
            &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
            vec![0, 1, 2, 4],
        );

        let mut stream = MemProjectionStream::<Test, TokioExecutor>::new(
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
