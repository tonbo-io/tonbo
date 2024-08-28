use std::{marker::PhantomData, ops::Bound};

use futures_util::StreamExt;
use parquet::arrow::{
    arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions},
    async_reader::{AsyncReader, ParquetObjectReader},
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};

use super::{arrows::get_range_filter, scan::SsTableScan};
use crate::{
    record::Record,
    stream::record_batch::RecordBatchEntry,
    timestamp::{Timestamp, TimestampedRef},
};

pub(crate) struct SsTable<R>
where
    R: Record,
{
    reader: ParquetObjectReader,
    _marker: PhantomData<R>,
}

impl<R> SsTable<R>
where
    R: Record,
{
    pub(crate) fn open(reader: ParquetObjectReader) -> Self {
        SsTable {
            reader,
            _marker: PhantomData,
        }
    }

    async fn into_parquet_builder(
        self,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> parquet::errors::Result<ArrowReaderBuilder<AsyncReader<ParquetObjectReader>>> {
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
            self.reader,
            ArrowReaderOptions::default().with_page_index(true),
        )
        .await?;
        if let Some(limit) = limit {
            builder = builder.with_limit(limit);
        }
        Ok(builder.with_projection(projection_mask))
    }

    pub(crate) async fn get(
        self,
        key: &TimestampedRef<R::Key>,
        projection_mask: ProjectionMask,
    ) -> parquet::errors::Result<Option<RecordBatchEntry<R>>> {
        self.scan(
            (Bound::Included(key.value()), Bound::Unbounded),
            key.ts(),
            Some(1),
            projection_mask,
        )
        .await?
        .next()
        .await
        .transpose()
    }

    pub(crate) async fn scan<'scan>(
        self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<SsTableScan<R>, parquet::errors::ParquetError> {
        let builder = self
            .into_parquet_builder(limit, projection_mask.clone())
            .await?;

        let schema_descriptor = builder.metadata().file_metadata().schema_descr();

        // Safety: filter's lifetime relies on range's lifetime, sstable must not live longer than
        // it
        let filter = unsafe { get_range_filter::<R>(schema_descriptor, range, ts) };

        Ok(SsTableScan::new(
            builder.with_row_filter(filter).build()?,
            projection_mask,
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{borrow::Borrow, ops::Bound, str::FromStr, sync::Arc};

    use arrow::array::RecordBatch;
    use futures_util::StreamExt;
    use object_store::{buffered::BufWriter, memory::InMemory, path::Path, ObjectStore};
    use parquet::{
        arrow::{arrow_to_parquet_schema, AsyncArrowWriter, ProjectionMask},
        errors::ParquetError,
    };
    use url::Url;

    use super::SsTable;
    use crate::{
        executor::tokio::TokioExecutor,
        fs::build_reader,
        record::Record,
        tests::{get_test_record_batch, Test, TestRef},
        timestamp::Timestamped,
        DbOption,
    };

    async fn write_parquet(
        record_batch: &RecordBatch,
        store: &Arc<dyn ObjectStore>,
        path: Path,
    ) -> Result<(), ParquetError> {
        let mut writer = AsyncArrowWriter::try_new(
            BufWriter::new(store.clone(), path),
            Test::arrow_schema().clone(),
            None,
        )?;
        writer.write(&record_batch).await?;
        writer.close().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_sstable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_root_url = Url::from_str("memory:").unwrap();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::try_from(temp_dir.into_path())
                .unwrap()
                .all_level_url(table_root_url),
            TokioExecutor::new(),
        )
        .await;
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let table_path = Path::from("write_test.parquet");
        write_parquet(&record_batch, &store, table_path.clone())
            .await
            .unwrap();

        let key = Timestamped::new("hello".to_owned(), 1.into());

        assert_eq!(
            SsTable::<Test>::open(build_reader(store.clone(), table_path).await.unwrap())
                .get(key.borrow(), ProjectionMask::all())
                .await
                .unwrap()
                .unwrap()
                .get(),
            Some(TestRef {
                vstring: "hello",
                vu32: Some(12),
                vbool: Some(true),
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_root_url = Url::from_str("memory:").unwrap();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::try_from(temp_dir.into_path())
                .unwrap()
                .all_level_url(table_root_url),
            TokioExecutor::new(),
        )
        .await;
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let table_path = Path::from("projection_query_test.parquet");
        write_parquet(&record_batch, &store, table_path.clone())
            .await
            .unwrap();

        let key = Timestamped::new("hello".to_owned(), 1.into());

        {
            let test_ref_1 = SsTable::<Test>::open(
                build_reader(store.clone(), table_path.clone())
                    .await
                    .unwrap(),
            )
            .get(
                key.borrow(),
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 3],
                ),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(test_ref_1.get().unwrap().vstring, "hello");
            assert_eq!(test_ref_1.get().unwrap().vu32, Some(12));
            assert_eq!(test_ref_1.get().unwrap().vbool, None);
        }
        {
            let test_ref_2 = SsTable::<Test>::open(
                build_reader(store.clone(), table_path.clone())
                    .await
                    .unwrap(),
            )
            .get(
                key.borrow(),
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 4],
                ),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(test_ref_2.get().unwrap().vstring, "hello");
            assert_eq!(test_ref_2.get().unwrap().vu32, None);
            assert_eq!(test_ref_2.get().unwrap().vbool, Some(true));
        }
        {
            let test_ref_3 =
                SsTable::<Test>::open(build_reader(store.clone(), table_path).await.unwrap())
                    .get(
                        key.borrow(),
                        ProjectionMask::roots(
                            &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                            [0, 1, 2],
                        ),
                    )
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(test_ref_3.get().unwrap().vstring, "hello");
            assert_eq!(test_ref_3.get().unwrap().vu32, None);
            assert_eq!(test_ref_3.get().unwrap().vbool, None);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_scan() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_root_url = Url::from_str("memory:").unwrap();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::try_from(temp_dir.into_path())
                .unwrap()
                .all_level_url(table_root_url),
            TokioExecutor::new(),
        )
        .await;
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let table_path = Path::from("projection_scan_test.parquet");

        write_parquet(&record_batch, &store, table_path.clone())
            .await
            .unwrap();

        {
            let mut test_ref_1 = SsTable::<Test>::open(
                build_reader(store.clone(), table_path.clone())
                    .await
                    .unwrap(),
            )
            .scan(
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 3],
                ),
            )
            .await
            .unwrap();

            let entry_0 = test_ref_1.next().await.unwrap().unwrap();
            assert_eq!(entry_0.get().unwrap().vstring, "hello");
            assert_eq!(entry_0.get().unwrap().vu32, Some(12));
            assert_eq!(entry_0.get().unwrap().vbool, None);

            let entry_1 = test_ref_1.next().await.unwrap().unwrap();
            assert_eq!(entry_1.get().unwrap().vstring, "world");
            assert_eq!(entry_1.get().unwrap().vu32, Some(12));
            assert_eq!(entry_1.get().unwrap().vbool, None);
        }
        {
            let mut test_ref_2 = SsTable::<Test>::open(
                build_reader(store.clone(), table_path.clone())
                    .await
                    .unwrap(),
            )
            .scan(
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 4],
                ),
            )
            .await
            .unwrap();

            let entry_0 = test_ref_2.next().await.unwrap().unwrap();
            assert_eq!(entry_0.get().unwrap().vstring, "hello");
            assert_eq!(entry_0.get().unwrap().vu32, None);
            assert_eq!(entry_0.get().unwrap().vbool, Some(true));

            let entry_1 = test_ref_2.next().await.unwrap().unwrap();
            assert_eq!(entry_1.get().unwrap().vstring, "world");
            assert_eq!(entry_1.get().unwrap().vu32, None);
            assert_eq!(entry_1.get().unwrap().vbool, None);
        }
        {
            let mut test_ref_3 =
                SsTable::<Test>::open(build_reader(store.clone(), table_path).await.unwrap())
                    .scan(
                        (Bound::Unbounded, Bound::Unbounded),
                        1_u32.into(),
                        None,
                        ProjectionMask::roots(
                            &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                            [0, 1, 2],
                        ),
                    )
                    .await
                    .unwrap();

            let entry_0 = test_ref_3.next().await.unwrap().unwrap();
            assert_eq!(entry_0.get().unwrap().vstring, "hello");
            assert_eq!(entry_0.get().unwrap().vu32, None);
            assert_eq!(entry_0.get().unwrap().vbool, None);

            let entry_1 = test_ref_3.next().await.unwrap().unwrap();
            assert_eq!(entry_1.get().unwrap().vstring, "world");
            assert_eq!(entry_1.get().unwrap().vu32, None);
            assert_eq!(entry_1.get().unwrap().vbool, None);
        }
    }
}
