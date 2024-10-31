use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use foyer::{
    Cache, CacheBuilder, DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder, LruConfig,
};
use fusio_parquet::reader::AsyncReader;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};
use ulid::Ulid;

use crate::{CacheError, CacheReader, MetaCache, RangeCache};

#[derive(Debug, Clone)]
pub struct FoyerMetaCache(Cache<Ulid, Arc<ParquetMetaData>>);
#[derive(Debug, Clone)]
pub struct FoyerRangeCache(HybridCache<(Ulid, Range<usize>), Bytes>);

pub struct FoyerReader {
    gen: Ulid,
    inner: AsyncReader,
    range_cache: FoyerRangeCache,
    meta_cache: FoyerMetaCache,
}

impl MetaCache for FoyerMetaCache {
    fn get(&self, gen: &Ulid) -> Option<Arc<ParquetMetaData>> {
        self.0.get(gen).map(|entry| entry.value().clone())
    }

    fn insert(&self, gen: Ulid, data: Arc<ParquetMetaData>) -> Arc<ParquetMetaData> {
        self.0.insert(gen, data).value().clone()
    }
}

impl RangeCache for FoyerRangeCache {
    async fn get(&self, key: &(Ulid, Range<usize>)) -> Result<Option<Bytes>, CacheError> {
        Ok(self.0.get(key).await?.map(|entry| entry.value().clone()))
    }

    fn insert(&self, key: (Ulid, Range<usize>), bytes: Bytes) -> Bytes {
        self.0.insert(key, bytes).value().clone()
    }
}

impl CacheReader for FoyerReader {
    type MetaCache = FoyerMetaCache;
    type RangeCache = FoyerRangeCache;

    fn new(
        meta_cache: Self::MetaCache,
        range_cache: Self::RangeCache,
        gen: Ulid,
        inner: AsyncReader,
    ) -> Self {
        Self {
            gen,
            inner,
            range_cache,
            meta_cache,
        }
    }

    async fn build_caches(
        cache_path: impl AsRef<std::path::Path> + Send,
        cache_meta_capacity: usize,
        cache_meta_shards: usize,
        cache_meta_ratio: f64,
        cache_range_memory: usize,
        cache_range_disk: usize,
    ) -> Result<(Self::MetaCache, Self::RangeCache), CacheError> {
        let meta_cache = CacheBuilder::new(cache_meta_capacity)
            .with_shards(cache_meta_shards)
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: cache_meta_ratio,
            })
            .build();
        let range_cache = HybridCacheBuilder::new()
            .memory(cache_range_memory)
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(cache_path).with_capacity(cache_range_disk),
            )
            .build()
            .await
            .map_err(CacheError::from)?;
        Ok((FoyerMetaCache(meta_cache), FoyerRangeCache(range_cache)))
    }
}

impl AsyncFileReader for FoyerReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let key = (self.gen, range);
            if let Some(bytes) = self
                .range_cache
                .get(&key)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?
            {
                return Ok(bytes);
            }

            let bytes = self.inner.get_bytes(key.1.clone()).await?;
            Ok(self.range_cache.insert(key, bytes))
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            if let Some(meta) = self.meta_cache.get(&self.gen) {
                return Ok(meta);
            }

            let meta = self
                .inner
                .get_metadata()
                .await
                .map_err(|e| parquet::errors::ParquetError::External(From::from(e)))?;

            Ok(self.meta_cache.insert(self.gen, meta))
        }
        .boxed()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        fs::File,
        ops::Range,
        sync::{
            atomic::{AtomicUsize, Ordering::SeqCst},
            Arc,
        },
    };

    use arrow::{
        array::{BooleanArray, RecordBatch, StringArray, UInt32Array},
        datatypes::{DataType, Field, Schema},
    };
    use fusio::{
        dynamic::DynFile, fs::OpenOptions, path::Path, Error, IoBuf, IoBufMut, Read, Write,
    };
    use fusio_dispatch::FsOptions;
    use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
    use parquet::arrow::{async_reader::AsyncFileReader, AsyncArrowWriter};
    use tempfile::TempDir;
    use ulid::Ulid;

    use crate::{foyer_reader::FoyerReader, CacheReader};

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

        let parquet_path = {
            let path = temp_dir.path().join("test.parquet");
            let _ = File::create(&path).unwrap();

            Path::from_filesystem_path(&path).unwrap()
        };
        let fs = FsOptions::Local.parse().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_null", DataType::Boolean, false),
            Field::new("_ts", DataType::UInt32, false),
            Field::new("vstring", DataType::Utf8, false),
            Field::new("vu32", DataType::UInt32, false),
            Field::new("vbool", DataType::Boolean, true),
        ]));
        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(
                    &parquet_path,
                    OpenOptions::default().read(true).write(true).create(true),
                )
                .await
                .unwrap(),
            ),
            schema.clone(),
            None,
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(BooleanArray::from(vec![false, false, false])),
                        Arc::new(UInt32Array::from(vec![0, 1, 2])),
                        Arc::new(StringArray::from(vec!["a", "b", "c"])),
                        Arc::new(UInt32Array::from(vec![0, 1, 2])),
                        Arc::new(BooleanArray::from(vec![true, true, true])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        writer.close().await.unwrap();

        let read_count = Arc::new(AtomicUsize::new(0));
        let (meta_cache, range_cache) = FoyerReader::build_caches(
            temp_dir.path().join("cache"),
            32,
            4,
            0.1,
            64 * 1024 * 1024,
            254 * 1024 * 1024,
        )
        .await
        .unwrap();

        let gen = Ulid::new();
        for _ in 0..1000 {
            let file = fs
                .open_options(&parquet_path, OpenOptions::default().read(true))
                .await
                .unwrap();
            let content_len = file.size().await.unwrap();

            let mut reader = FoyerReader::new(
                meta_cache.clone(),
                range_cache.clone(),
                gen,
                AsyncReader::new(
                    Box::new(CountFile {
                        inner: file,
                        read_count: read_count.clone(),
                    }),
                    content_len,
                )
                .await
                .unwrap(),
            );

            let _ = AsyncFileReader::get_metadata(&mut reader).await.unwrap();
            let _ = AsyncFileReader::get_bytes(&mut reader, Range { start: 0, end: 10 })
                .await
                .unwrap();
        }

        assert_eq!(read_count.load(SeqCst), 2);
    }
}
