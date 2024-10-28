use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use foyer::{Cache, HybridCache};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};

use crate::fs::FileId;

pub(crate) type MetaCache = Arc<Cache<FileId, Arc<ParquetMetaData>>>;
pub(crate) type RangeCache = HybridCache<(FileId, Range<usize>), Bytes>;

pub(crate) struct CacheReader<R> {
    gen: FileId,
    inner: R,
    range_cache: RangeCache,
    meta_cache: MetaCache,
}

impl<R> CacheReader<R> {
    pub(crate) fn new(
        meta_cache: MetaCache,
        range_cache: RangeCache,
        gen: FileId,
        inner: R,
    ) -> CacheReader<R> {
        Self {
            gen,
            inner,
            range_cache,
            meta_cache,
        }
    }
}

impl<R: AsyncFileReader> AsyncFileReader for CacheReader<R> {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let key = (self.gen, range);
            if let Some(entry) = self
                .range_cache
                .get(&key)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?
            {
                return Ok(entry.value().clone());
            }

            let bytes = self.inner.get_bytes(key.1.clone()).await?;
            let entry = self.range_cache.insert(key, bytes);
            Ok(entry.value().clone())
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            if let Some(entry) = self.meta_cache.get(&self.gen) {
                return Ok(entry.value().clone());
            }

            let meta = self
                .inner
                .get_metadata()
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?;
            let entry = self.meta_cache.insert(self.gen, meta);

            Ok(entry.value().clone())
        }
        .boxed()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        collections::Bound,
        sync::{
            atomic::{AtomicUsize, Ordering::SeqCst},
            Arc,
        },
    };

    use fusio::{dynamic::DynFile, path::Path, Error, IoBuf, IoBufMut, Read, Write};
    use futures_util::StreamExt;
    use parquet::arrow::{arrow_to_parquet_schema, ProjectionMask};
    use tempfile::TempDir;
    use ulid::Ulid;

    use crate::{
        compaction::tests::build_parquet_table,
        fs::FileType,
        ondisk::sstable::SsTable,
        record::{Record, RecordInstance},
        tests::Test,
        version::set::VersionSet,
        wal::log::LogType,
        DbOption,
    };

    struct CountFile {
        inner: Box<dyn DynFile>,
        read_count: Arc<AtomicUsize>,
    }

    impl Read for CountFile {
        async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
            self.read_count.fetch_add(1, SeqCst);
            self.inner.read_exact_at(buf, pos).await
        }

        async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
            self.read_count.fetch_add(1, SeqCst);
            self.inner.read_to_end_at(buf, pos).await
        }

        async fn size(&self) -> Result<u64, Error> {
            self.inner.size().await
        }
    }

    impl Write for CountFile {
        async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
            self.inner.write_all(buf).await
        }

        async fn flush(&mut self) -> Result<(), Error> {
            self.inner.flush().await
        }

        async fn close(&mut self) -> Result<(), Error> {
            self.inner.close().await
        }
    }

    #[tokio::test]
    async fn test_cache_read() {
        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::<Test>::from(Path::from_filesystem_path(temp_dir.path()).unwrap());
        let fs = option.base_fs.clone().parse().unwrap();
        fs.create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let table_gen = Ulid::new();
        build_parquet_table::<Test>(
            &option,
            table_gen,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 1.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 2.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 3.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            &RecordInstance::Normal,
            0,
            &fs,
        )
        .await
        .unwrap();

        let read_count = Arc::new(AtomicUsize::new(0));
        let table_path = option.table_path(&table_gen, 0);
        let (meta_cache, range_cache) = VersionSet::build_cache(&option).await.unwrap();

        for _ in 0..1000 {
            let mut scan = SsTable::<Test>::open(
                Box::new(CountFile {
                    inner: fs
                        .open_options(&table_path, FileType::Parquet.open_options(true))
                        .await
                        .unwrap(),
                    read_count: read_count.clone(),
                }),
                table_gen,
                true,
                range_cache.clone(),
                meta_cache.clone(),
            )
            .await
            .unwrap()
            .scan(
                (Bound::Unbounded, Bound::Unbounded),
                0_u32.into(),
                None,
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 3, 4],
                ),
            )
            .await
            .unwrap();

            let entry_0 = scan.next().await.unwrap().unwrap();
            assert_eq!(entry_0.get().unwrap().vstring, "1");
            assert_eq!(entry_0.get().unwrap().vu32, Some(0));
            assert_eq!(entry_0.get().unwrap().vbool, Some(true));

            let entry_1 = scan.next().await.unwrap().unwrap();
            assert_eq!(entry_1.get().unwrap().vstring, "2");
            assert_eq!(entry_1.get().unwrap().vu32, Some(0));
            assert_eq!(entry_1.get().unwrap().vbool, Some(true));

            let entry_2 = scan.next().await.unwrap().unwrap();
            assert_eq!(entry_2.get().unwrap().vstring, "3");
            assert_eq!(entry_2.get().unwrap().vu32, Some(0));
            assert_eq!(entry_2.get().unwrap().vbool, Some(true));
        }

        assert_eq!(read_count.load(SeqCst), 9);
    }
}
