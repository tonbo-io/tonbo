use std::{marker::PhantomData, ops::Bound};

use fusio::{dynamic::DynFile, path::Path, DynRead};
use fusio_parquet::reader::AsyncReader;
use futures_util::StreamExt;
use parquet::arrow::{
    arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions},
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};

use super::{arrows::get_range_filter, scan::SsTableScan};
use crate::{
    fs::{cache_reader::CacheReader, CacheError},
    record::Record,
    stream::record_batch::RecordBatchEntry,
    timestamp::{Timestamp, TimestampedRef},
    DbOption,
};

pub(crate) struct SsTable<R>
where
    R: Record,
{
    reader: CacheReader<AsyncReader>,
    _marker: PhantomData<R>,
}

impl<R> SsTable<R>
where
    R: Record,
{
    pub(crate) async fn open(
        option: &DbOption<R>,
        file: Box<dyn DynFile>,
        path: Path,
    ) -> Result<Self, CacheError> {
        let size = file.size().await?;

        Ok(SsTable {
            reader: CacheReader::new(
                &option.cache_option,
                option.meta_cache.clone(),
                AsyncReader::new(file, size).await?,
                path,
            )
            .await?,
            _marker: PhantomData,
        })
    }

    #[cfg(test)]
    pub(crate) async fn open_for_test(
        option: &crate::fs::CacheOption,
        meta_cache: crate::fs::cache_reader::MetaCache,
        file: Box<dyn DynFile>,
        path: Path,
    ) -> Result<Self, CacheError> {
        let size = file.size().await?;

        Ok(SsTable {
            reader: CacheReader::new(
                option,
                meta_cache,
                AsyncReader::new(file, size).await?,
                path,
            )
            .await?,
            _marker: PhantomData,
        })
    }

    async fn into_parquet_builder(
        self,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> parquet::errors::Result<
        ArrowReaderBuilder<parquet::arrow::async_reader::AsyncReader<CacheReader<AsyncReader>>>,
    > {
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
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<SsTableScan<R>, parquet::errors::ParquetError> {
        let builder = self
            .into_parquet_builder(limit, projection_mask.clone())
            .await?;

        let schema_descriptor = builder.metadata().file_metadata().schema_descr();
        let full_schema = builder.schema().clone();

        // Safety: filter's lifetime relies on range's lifetime, sstable must not live longer than
        // it
        let filter = unsafe { get_range_filter::<R>(schema_descriptor, range, ts) };

        Ok(SsTableScan::new(
            builder.with_row_filter(filter).build()?,
            projection_mask,
            full_schema,
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{borrow::Borrow, fs::File, ops::Bound, sync::Arc};

    use arrow::array::RecordBatch;
    use foyer::{CacheBuilder, LruConfig};
    use fusio::{dynamic::DynFile, path::Path, DynFs};
    use fusio_dispatch::FsOptions;
    use fusio_parquet::writer::AsyncWriter;
    use futures_util::StreamExt;
    use parquet::{
        arrow::{
            arrow_to_parquet_schema, arrow_writer::ArrowWriterOptions, AsyncArrowWriter,
            ProjectionMask,
        },
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    };

    use super::SsTable;
    use crate::{
        executor::tokio::TokioExecutor,
        fs::{cache_reader::MetaCache, manager::StoreManager, CacheOption, FileType},
        record::Record,
        tests::{get_test_record_batch, Test},
        timestamp::Timestamped,
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
            Test::arrow_schema().clone(),
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

    pub(crate) async fn open_sstable<R>(
        option: &CacheOption,
        meta_cache: MetaCache,
        store: &Arc<dyn DynFs>,
        path: Path,
    ) -> SsTable<R>
    where
        R: Record,
    {
        SsTable::open_for_test(
            option,
            meta_cache,
            store
                .open_options(&path, FileType::Parquet.open_options(true))
                .await
                .unwrap(),
            path,
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let option = CacheOption {
            path: Path::from_filesystem_path(temp_dir.path()).unwrap(),
            memory: 64 * 1024,
            local: 256 * 1024,
        };
        let meta_cache = Arc::new(
            CacheBuilder::new(32)
                .with_shards(4)
                .with_eviction_config(LruConfig {
                    high_priority_pool_ratio: 0.1,
                })
                .build(),
        );

        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let base_fs = manager.base_fs();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap()),
            TokioExecutor::new(),
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

        let key = Timestamped::new("hello".to_owned(), 1.into());

        {
            let test_ref_1 =
                open_sstable::<Test>(&option, meta_cache.clone(), base_fs, table_path.clone())
                    .await
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
            let test_ref_2 =
                open_sstable::<Test>(&option, meta_cache.clone(), base_fs, table_path.clone())
                    .await
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
                open_sstable::<Test>(&option, meta_cache.clone(), base_fs, table_path.clone())
                    .await
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
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let base_fs = manager.base_fs();
        let option = CacheOption {
            path: Path::from_filesystem_path(temp_dir.path()).unwrap(),
            memory: 64 * 1024 * 1024,
            local: 256 * 1024 * 1024,
        };
        let meta_cache = Arc::new(
            CacheBuilder::new(32)
                .with_shards(4)
                .with_eviction_config(LruConfig {
                    high_priority_pool_ratio: 0.1,
                })
                .build(),
        );

        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap()),
            TokioExecutor::new(),
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
            let mut test_ref_1 =
                open_sstable::<Test>(&option, meta_cache.clone(), base_fs, table_path.clone())
                    .await
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
            let mut test_ref_2 =
                open_sstable::<Test>(&option, meta_cache.clone(), base_fs, table_path.clone())
                    .await
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
                open_sstable::<Test>(&option, meta_cache.clone(), base_fs, table_path.clone())
                    .await
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
