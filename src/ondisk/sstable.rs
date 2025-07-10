use std::{marker::PhantomData, ops::Bound, sync::Arc};

use common::Value;
use fusio::{dynamic::DynFile, DynRead};
use fusio_parquet::reader::AsyncReader;
use futures_util::StreamExt;
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions},
        async_reader::{AsyncFileReader, AsyncReader as ParquetAsyncReader},
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    },
    errors::Result as ParquetResult,
};
use parquet_lru::{BoxedFileReader, DynLruCache};
use ulid::Ulid;

use super::{arrows::get_range_filter, scan::SsTableScan};
use crate::{
    record::{Record, Schema},
    stream::record_batch::RecordBatchEntry,
    timestamp::{Timestamp, TsRef},
};

pub(crate) struct SsTable<R>
where
    R: Record,
{
    reader: BoxedFileReader,
    _marker: PhantomData<R>,
}

impl<R> SsTable<R>
where
    R: Record,
{
    pub(crate) async fn open(
        lru_cache: Arc<dyn DynLruCache<Ulid> + Send + Sync>,
        id: Ulid,
        file: Box<dyn DynFile>,
    ) -> Result<Self, fusio::Error> {
        let size = file.size().await?;

        Ok(SsTable {
            reader: lru_cache
                .get_reader(
                    id,
                    BoxedFileReader::new(AsyncReader::new(file, size).await?),
                )
                .await,
            _marker: PhantomData,
        })
    }

    async fn into_parquet_builder(
        self,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> ParquetResult<ArrowReaderBuilder<ParquetAsyncReader<Box<dyn AsyncFileReader + 'static>>>>
    {
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
            Box::new(self.reader) as Box<dyn AsyncFileReader + 'static>,
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
        key: &TsRef<<R::Schema as Schema>::Key>,
        projection_mask: ProjectionMask,
    ) -> ParquetResult<Option<RecordBatchEntry<R>>> {
        self.scan(
            (Bound::Included(key.value()), Bound::Included(key.value())),
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
        range: (Bound<&'scan dyn Value>, Bound<&'scan dyn Value>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<SsTableScan<'scan, R>, parquet::errors::ParquetError> {
        let builder = self
            .into_parquet_builder(limit, projection_mask.clone())
            .await?;

        let schema_descriptor = builder.metadata().file_metadata().schema_descr();
        let full_schema = builder.schema().clone();

        // Safety: filter's lifetime relies on range's lifetime, sstable must not live longer than
        // it
        let filter = unsafe { get_range_filter(schema_descriptor, range, ts) };

        Ok(SsTableScan::new(
            builder.with_row_filter(filter).build()?,
            projection_mask,
            full_schema,
        ))
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::{borrow::Borrow, fs::File, ops::Bound, sync::Arc};

    use arrow::array::RecordBatch;
    use fusio::{dynamic::DynFile, path::Path, DynFs};
    use fusio_dispatch::FsOptions;
    use fusio_parquet::writer::AsyncWriter;
    use futures_util::StreamExt;
    use parquet::{
        arrow::{
            arrow_writer::ArrowWriterOptions, ArrowSchemaConverter, AsyncArrowWriter,
            ProjectionMask,
        },
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    };
    use parquet_lru::NoCache;

    use super::SsTable;
    use crate::{
        executor::tokio::TokioExecutor,
        fs::{manager::StoreManager, FileType},
        inmem::immutable::tests::TestSchema,
        record::{Record, Schema},
        tests::{get_test_record_batch, Test},
        timestamp::Ts,
        DbOption,
    };

    async fn write_record_batch(
        file: Box<dyn DynFile>,
        record_batch: &RecordBatch,
    ) -> Result<(), parquet::errors::ParquetError> {
        // TODO: expose writer options
        let options = ArrowWriterOptions::new().with_properties(
            WriterProperties::builder()
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build(),
        );
        let mut writer = AsyncArrowWriter::try_new_with_options(
            AsyncWriter::new(file),
            TestSchema {}.arrow_schema().clone(),
            options,
        )
        .expect("Failed to create writer");
        writer.write(record_batch).await?;

        if writer.in_progress_size() > (1 << 20) - 1 {
            writer.flush().await?;
        }

        writer.close().await?;
        Ok(())
    }

    pub(crate) async fn open_sstable<R>(store: &Arc<dyn DynFs>, path: &Path) -> SsTable<R>
    where
        R: Record,
    {
        SsTable::open(
            Arc::new(NoCache::default()),
            Default::default(),
            store
                .open_options(path, FileType::Parquet.open_options(true))
                .await
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let base_fs = manager.base_fs();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::new(
                Path::from_filesystem_path(temp_dir.path()).unwrap(),
                &TestSchema,
            ),
            TokioExecutor::current(),
        )
        .await;
        let table_path = temp_dir.path().join("projection_query_test.parquet");
        let _ = File::create(&table_path).unwrap();
        let table_path = Path::from_filesystem_path(table_path).unwrap();

        let file = base_fs
            .open_options(&table_path, FileType::Parquet.open_options(false))
            .await
            .unwrap();
        write_record_batch(file, &record_batch).await.unwrap();

        let key = Ts::new("hello".to_owned(), 1.into());

        {
            let test_ref_1 = open_sstable::<Test>(base_fs, &table_path)
                .await
                .get(
                    key.borrow(),
                    ProjectionMask::roots(
                        &ArrowSchemaConverter::new()
                            .convert(TestSchema {}.arrow_schema())
                            .unwrap(),
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
            let test_ref_2 = open_sstable::<Test>(base_fs, &table_path)
                .await
                .get(
                    key.borrow(),
                    ProjectionMask::roots(
                        &ArrowSchemaConverter::new()
                            .convert(TestSchema {}.arrow_schema())
                            .unwrap(),
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
            let test_ref_3 = open_sstable::<Test>(base_fs, &table_path)
                .await
                .get(
                    key.borrow(),
                    ProjectionMask::roots(
                        &ArrowSchemaConverter::new()
                            .convert(TestSchema {}.arrow_schema())
                            .unwrap(),
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
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let base_fs = manager.base_fs();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::new(
                Path::from_filesystem_path(temp_dir.path()).unwrap(),
                &TestSchema,
            ),
            TokioExecutor::current(),
        )
        .await;
        let table_path = temp_dir.path().join("projection_scan_test.parquet");
        let _ = File::create(&table_path).unwrap();
        let table_path = Path::from_filesystem_path(table_path).unwrap();

        let file = base_fs
            .open_options(&table_path, FileType::Parquet.open_options(false))
            .await
            .unwrap();
        write_record_batch(file, &record_batch).await.unwrap();

        {
            let mut test_ref_1 = open_sstable::<Test>(base_fs, &table_path)
                .await
                .scan(
                    (Bound::Unbounded, Bound::Unbounded),
                    1_u32.into(),
                    None,
                    ProjectionMask::roots(
                        &ArrowSchemaConverter::new()
                            .convert(TestSchema {}.arrow_schema())
                            .unwrap(),
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
            let mut test_ref_2 = open_sstable::<Test>(base_fs, &table_path)
                .await
                .scan(
                    (Bound::Unbounded, Bound::Unbounded),
                    1_u32.into(),
                    None,
                    ProjectionMask::roots(
                        &ArrowSchemaConverter::new()
                            .convert(TestSchema {}.arrow_schema())
                            .unwrap(),
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
            let mut test_ref_3 = open_sstable::<Test>(base_fs, &table_path)
                .await
                .scan(
                    (Bound::Unbounded, Bound::Unbounded),
                    1_u32.into(),
                    None,
                    ProjectionMask::roots(
                        &ArrowSchemaConverter::new()
                            .convert(TestSchema {}.arrow_schema())
                            .unwrap(),
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
