use std::{marker::PhantomData, ops::Bound, sync::Arc};

use arrow::{
    array::AsArray,
    datatypes::{DataType, Int8Type},
};
use fusio::{dynamic::DynFile, DynRead};
use fusio_parquet::reader::AsyncReader;
use futures_util::StreamExt;
use parquet::{
    arrow::{
        arrow_reader::{
            statistics::StatisticsConverter, ArrowReaderBuilder, ArrowReaderOptions, RowSelection,
            RowSelector,
        },
        async_reader::{AsyncFileReader, AsyncReader as ParquetAsyncReader},
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    },
    errors::Result as ParquetResult,
    file::{
        metadata::{ParquetMetaData, RowGroupMetaData},
        page_index::index::Index,
        statistics::Statistics,
    },
    format::PageLocation,
};
use parquet_lru::{BoxedFileReader, DynLruCache};
use ulid::Ulid;

use super::{arrows::get_range_filter, scan::SsTableScan};
use crate::{
    fs::generate_file_id,
    record::{Key, Record, Schema},
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
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
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
        let filter = unsafe { get_range_filter::<R>(schema_descriptor, range, ts) };

        Ok(SsTableScan::new(
            builder.with_row_filter(filter).build()?,
            projection_mask,
            full_schema,
        ))
    }

    #[allow(unused)]
    pub(crate) async fn scan_with_row_group_skip<'scan>(
        self,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
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
        let filter = unsafe { get_range_filter::<R>(schema_descriptor, range, ts) };

        let metadata = builder.metadata();

        let selected_row_groups = Self::select_row_groups(metadata, range);

        Ok(SsTableScan::new(
            builder
                .with_row_groups(selected_row_groups)
                .with_row_filter(filter)
                .build()?,
            projection_mask,
            full_schema,
        ))
    }

    #[allow(unused)]
    pub(crate) async fn scan_with_page_skip<'scan>(
        self,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<SsTableScan<'scan, R>, parquet::errors::ParquetError> {
        let mut builder = self
            .into_parquet_builder(limit, projection_mask.clone())
            .await?;

        let schema_descriptor = builder.metadata().file_metadata().schema_descr();
        let full_schema = builder.schema().clone();

        // Safety: filter's lifetime relies on range's lifetime, sstable must not live longer than
        // it
        let filter = unsafe { get_range_filter::<R>(schema_descriptor, range, ts) };

        let metadata = builder.metadata();

        let selected_row_groups = Self::select_row_groups(metadata, range);

        if let Some((_, _)) = metadata.column_index().zip(metadata.offset_index()) {
            let selection = Self::apply_filter(metadata, range, selected_row_groups.clone());
            builder = builder.with_row_selection(selection);
        }

        Ok(SsTableScan::new(
            builder
                .with_row_groups(selected_row_groups)
                .with_row_filter(filter)
                .build()?,
            projection_mask,
            full_schema,
        ))
    }

    fn select_row_groups<'scan>(
        metadata: &ParquetMetaData,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
    ) -> Vec<usize> {
        let mut selected_row_groups = vec![];

        for (idx, row_group) in metadata.row_groups().iter().enumerate() {
            let Some(sorting_cols) = row_group.sorting_columns() else {
                selected_row_groups.push(idx);
                continue;
                // unreachable!()
            };
            let primary_key_index = sorting_cols.last().unwrap().column_idx as usize;
            let col_meta = row_group.column(primary_key_index);
            col_meta.column_type();

            if let Some(statistics) = col_meta.statistics() {
                let meets = match statistics {
                    Statistics::Boolean(_value_statistics) => true,
                    Statistics::Int32(value_statistics) => {
                        let start = range.0.map(|v| v.as_i32());
                        let end = range.1.map(|v| v.as_i32());
                        let max = *value_statistics.max_opt().unwrap_or(&i32::MAX);
                        let min = *value_statistics.min_opt().unwrap_or(&i32::MIN);

                        let scope = crate::scope::Scope {
                            min,
                            max,
                            gen: generate_file_id(),
                            wal_ids: None,
                        };
                        scope.meets_range((start.as_ref(), end.as_ref()))
                    }
                    Statistics::Int64(value_statistics) => {
                        let start = range.0.map(|v| v.as_i64());
                        let end = range.1.map(|v| v.as_i64());
                        let max = *value_statistics.max_opt().unwrap_or(&i64::MAX);
                        let min = *value_statistics.min_opt().unwrap_or(&i64::MIN);

                        let scope = crate::scope::Scope {
                            min,
                            max,
                            gen: generate_file_id(),
                            wal_ids: None,
                        };
                        scope.meets_range((start.as_ref(), end.as_ref()))
                    }
                    Statistics::Int96(_) => true,
                    Statistics::Float(_value_statistics) => true,
                    Statistics::Double(_value_statistics) => true,
                    Statistics::ByteArray(value_statistics) => {
                        let start = range.0.map(|v| v.to_bytes());
                        let end = range.1.map(|v| v.to_bytes());
                        let min = value_statistics.min_bytes_opt().unwrap();
                        if let Some(max) = value_statistics.max_bytes_opt() {
                            let scope = crate::scope::Scope {
                                min,
                                max,
                                gen: generate_file_id(),
                                wal_ids: None,
                            };
                            scope.meets_range((start.as_ref(), end.as_ref()))
                        } else {
                            match end {
                                Bound::Included(key) => min <= key,
                                Bound::Excluded(key) => min < key,
                                Bound::Unbounded => true,
                            }
                        }
                    }
                    Statistics::FixedLenByteArray(_) => true,
                };

                if meets {
                    selected_row_groups.push(idx);
                }
            }
        }

        selected_row_groups
    }

    fn apply_filter<'scan>(
        metadata: &ParquetMetaData,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
        selected_row_groups: Vec<usize>,
    ) -> RowSelection {
        if selected_row_groups.is_empty() {
            unreachable!()
        }
        let row_group = metadata.row_group(0);
        let Some(sorting_cols) = row_group.sorting_columns() else {
            unreachable!()
        };
        let primary_key_index = sorting_cols.last().unwrap().column_idx as usize;

        let mut selection = RowSelection::from_consecutive_ranges([].into_iter(), 0);
        let column_indexes = metadata.column_index().unwrap();
        let offsets = metadata.offset_index().unwrap();
        for row_group_idx in selected_row_groups {
            let row_group = metadata.row_group(row_group_idx);
            let index = &column_indexes[row_group_idx][primary_key_index];
            let offset = &offsets[row_group_idx][primary_key_index];
            let row_counts = Self::row_group_page_row_counts(row_group, &offset.page_locations);
            let meets = match index {
                Index::NONE => todo!(),
                Index::BOOLEAN(_native_index) => unimplemented!(),
                Index::INT32(index) => {
                    let mut meets = Vec::with_capacity(row_counts.len());
                    let start = range.0.map(|v| v.as_i32());
                    let end = range.1.map(|v| v.as_i32());
                    for page_index in index.indexes.iter() {
                        let max = *page_index.max().unwrap_or(&i32::MAX);
                        let min = *page_index.min().unwrap_or(&i32::MIN);
                        let scope = crate::scope::Scope {
                            min,
                            max,
                            gen: generate_file_id(),
                            wal_ids: None,
                        };

                        meets.push(scope.meets_range((start.as_ref(), end.as_ref())));
                    }
                    meets
                }
                Index::INT64(_native_index) => unimplemented!(),
                Index::INT96(_native_index) => unimplemented!(),
                Index::FLOAT(_native_index) => unimplemented!(),
                Index::DOUBLE(_native_index) => unimplemented!(),
                Index::BYTE_ARRAY(index) => {
                    let mut meets = Vec::with_capacity(row_counts.len());
                    let start = range.0.map(|v| v.to_bytes());
                    let end = range.1.map(|v| v.to_bytes());
                    for page_index in index.indexes.iter() {
                        let min = page_index.min_bytes().unwrap_or(&[]);
                        if let Some(max) = page_index.max_bytes() {
                            let scope = crate::scope::Scope {
                                min,
                                max,
                                gen: generate_file_id(),
                                wal_ids: None,
                            };
                            meets.push(scope.meets_range((start.as_ref(), end.as_ref())));
                        } else {
                            match end {
                                Bound::Included(key) => meets.push(min <= key),
                                Bound::Excluded(key) => meets.push(min < key),
                                Bound::Unbounded => meets.push(true),
                            }
                        }
                    }
                    meets
                }
                Index::FIXED_LEN_BYTE_ARRAY(_) => unimplemented!(),
            };

            let mut selectors = Vec::with_capacity(row_counts.len());
            for (row_count, select) in row_counts.iter().zip(meets) {
                if select {
                    selectors.push(RowSelector::select(*row_count));
                } else {
                    selectors.push(RowSelector::skip(*row_count));
                };
            }
            selection = selection.intersection(&selectors.into());
        }

        selection
    }

    /// return the row counts of each data page in the given row group
    fn row_group_page_row_counts(
        row_group: &RowGroupMetaData,
        page_offsets: &[PageLocation],
    ) -> Vec<usize> {
        if page_offsets.is_empty() {
            return vec![];
        }

        let total_rows = row_group.num_rows() as usize;
        let mut row_counts = Vec::with_capacity(page_offsets.len());
        page_offsets.windows(2).for_each(|pages| {
            let start = pages[0].first_row_index as usize;
            let end = pages[1].first_row_index as usize;
            row_counts.push(end - start);
        });
        row_counts.push(total_rows - page_offsets.last().unwrap().first_row_index as usize);
        row_counts
    }

    #[allow(unused)]
    fn apply_filter2<'scan>(
        metadata: &ParquetMetaData,
        arrow_schema: &arrow::datatypes::Schema,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
        selected_row_groups: Vec<usize>,
    ) -> Vec<usize> {
        if selected_row_groups.is_empty() {
            unreachable!()
        }
        let row_group = metadata.row_group(0);
        let Some(sorting_cols) = row_group.sorting_columns() else {
            unreachable!()
        };
        let primary_key_index = sorting_cols.last().unwrap().column_idx as usize;
        let col_meta = row_group.column(primary_key_index);
        let primary_key_name = col_meta.column_descr().name();
        metadata.row_group(0).sorting_columns();

        let convert = StatisticsConverter::try_new(
            primary_key_name,
            arrow_schema,
            metadata.file_metadata().schema_descr(),
        )
        .unwrap();
        let mins = convert
            .data_page_mins(
                metadata.column_index().unwrap(),
                metadata.offset_index().unwrap(),
                selected_row_groups.iter(),
            )
            .unwrap();
        let maxs = convert
            .data_page_maxes(
                metadata.column_index().unwrap(),
                metadata.offset_index().unwrap(),
                selected_row_groups.iter(),
            )
            .unwrap();

        match mins.data_type() {
            DataType::Boolean => todo!(),
            DataType::Int8 => {
                let min_values = mins.as_primitive::<Int8Type>();
                let max_values = maxs.as_primitive::<Int8Type>();
                for (idx, min, max) in
                    min_values
                        .iter()
                        .zip(max_values)
                        .enumerate()
                        .map(|(idx, (min, max))| {
                            (
                                idx,
                                min.expect("primary key can not be null"),
                                max.expect("primary key can not be null"),
                            )
                        })
                {
                    let start = range.0.map(|v| v.as_i32() as i8);
                    let end = range.1.map(|v| v.as_i32() as i8);

                    // let col_index = metadata.offset_index().unwrap()[0][0];
                    let scope = crate::scope::Scope {
                        min,
                        max,
                        gen: generate_file_id(),
                        wal_ids: None,
                    };
                    // if scope.meets_range((start.as_ref(), end.as_ref())) {
                    // } else {
                    // }
                }
            }
            DataType::Int16 => todo!(),
            DataType::Int32 => todo!(),
            DataType::Int64 => todo!(),
            DataType::UInt8 => todo!(),
            DataType::UInt16 => todo!(),
            DataType::UInt32 => todo!(),
            DataType::UInt64 => todo!(),
            DataType::Float16 => todo!(),
            DataType::Float32 => todo!(),
            DataType::Float64 => todo!(),
            DataType::Date32 => todo!(),
            DataType::Date64 => todo!(),
            DataType::Binary => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::BinaryView => todo!(),
            DataType::Utf8 => todo!(),
            DataType::LargeUtf8 => todo!(),
            DataType::Utf8View => todo!(),
            DataType::List(field) => todo!(),
            _ => unimplemented!(),
        };

        selected_row_groups
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
        file::properties::{EnabledStatistics, WriterProperties},
        format::SortingColumn,
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
                .set_compression(Compression::LZ4)
                // .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                // .set_column_bloom_filter_enabled(column_paths.clone(), true)
                // .set_sorting_columns(Some(sorting_columns))
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .set_sorting_columns(Some(vec![
                    SortingColumn::new(1_i32, true, true),
                    SortingColumn::new(2_i32, false, true),
                ]))
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

#[cfg(all(test, feature = "tokio"))]
mod predicate_tests {
    use std::{ops::Bound, sync::Arc};

    use fusio::{disk::LocalFs, DynFs};
    use fusio_log::Path;
    use fusio_parquet::writer::AsyncWriter;
    use futures::StreamExt;
    use parquet::{
        arrow::{ArrowSchemaConverter, AsyncArrowWriter, ProjectionMask},
        basic::Compression,
        file::properties::{EnabledStatistics, WriterProperties},
    };

    use crate::{
        fs::FileType,
        inmem::immutable::tests::TestSchema,
        ondisk::sstable::tests::open_sstable,
        record::Schema,
        tests::{get_test_record_batch_with_size, Test},
        DbOption,
    };

    async fn try_load_data(options: DbOption, record_size: usize) {
        let parquet_dir_path = "./db_path/data";
        let parquet_path = "./db_path/data/data.parquet";
        let parquet_file_path = std::path::Path::new(parquet_path);
        let dir_path = std::path::Path::new(parquet_dir_path);

        let fs = Arc::new(LocalFs {}) as Arc<dyn DynFs>;

        if !parquet_file_path.exists() {
            if !dir_path.exists() {
                std::fs::create_dir_all(parquet_dir_path).unwrap();
            }
            let base_path = Path::from_filesystem_path(parquet_dir_path).unwrap();

            let schema = TestSchema {};
            let table_path = base_path.child("data.parquet");

            let record_batch = get_test_record_batch_with_size(options.clone(), record_size).await;
            let mut writer = AsyncArrowWriter::try_new(
                AsyncWriter::new(
                    fs.open_options(&table_path, FileType::Parquet.open_options(false))
                        .await
                        .unwrap(),
                ),
                schema.arrow_schema().clone(),
                Some(options.write_parquet_properties.clone()),
            )
            .unwrap();

            writer.write(&record_batch).await.unwrap();
            writer.close().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_scan_predicate_push_down() {
        let parquet_dir_path = "./db_path/data";
        let parquet_path = "./db_path/data/data.parquet";

        let fs = Arc::new(LocalFs {}) as Arc<dyn DynFs>;
        let schema = TestSchema {};
        let (column_paths, sorting_columns) = schema.primary_key_path();

        let options = DbOption::new(parquet_dir_path.into(), &schema)
            .disable_wal()
            .write_parquet_option(
                WriterProperties::builder()
                    .set_max_row_group_size(1024 * 16)
                    // .set_data_page_row_count_limit(100)
                    .set_data_page_size_limit(1024 * 4)
                    .set_compression(Compression::LZ4)
                    .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                    .set_column_bloom_filter_enabled(column_paths.clone(), true)
                    .set_sorting_columns(Some(sorting_columns))
                    .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                    .build(),
            );

        let record_size = 1024 * 1024 * 8;
        try_load_data(options.clone(), record_size as usize).await;

        let table_path = Path::from_filesystem_path(parquet_path).unwrap();

        let mut ranges = vec![];
        let mut rand = fastrand::Rng::new();
        for _ in 0..10 {
            let left = rand.u32(0..record_size);
            let right = rand.u32(left..record_size);

            ranges.push((format!("{:08}", left), format!("{:08}", right)));
        }

        let with_filter = {
            let start = std::time::Instant::now();
            for (lower, upper) in ranges.iter() {
                let mut scan = open_sstable::<Test>(&fs, &table_path)
                    .await
                    .scan(
                        (Bound::Included(lower), Bound::Included(upper)),
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

                while scan.next().await.transpose().unwrap().is_some() {}
            }
            start.elapsed().as_millis() as f64 / 1000.0
        };
        let with_row_group = {
            let start = std::time::Instant::now();
            for (lower, upper) in ranges.iter() {
                let mut scan = open_sstable::<Test>(&fs, &table_path)
                    .await
                    .scan_with_row_group_skip(
                        (Bound::Included(lower), Bound::Included(upper)),
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

                while scan.next().await.transpose().unwrap().is_some() {}
            }
            start.elapsed().as_millis() as f64 / 1000.0
        };
        let with_page_filter = {
            let start = std::time::Instant::now();
            for (lower, upper) in ranges.iter() {
                let mut scan = open_sstable::<Test>(&fs, &table_path)
                    .await
                    .scan_with_page_skip(
                        (Bound::Included(lower), Bound::Included(upper)),
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

                while scan.next().await.transpose().unwrap().is_some() {}
            }
            start.elapsed().as_millis() as f64 / 1000.0
        };

        println!("-------------------------------------------");
        println!(
            "read parquet with filter: {:.4}",
            with_filter / (ranges.len() as f64)
        );
        println!(
            "read parquet with filter and row group: {:.4}",
            with_row_group / (ranges.len() as f64)
        );
        println!(
            "read parquet with filter and page filter: {:.4}",
            with_page_filter / (ranges.len() as f64)
        );
        println!("-------------------------------------------");
    }
}
