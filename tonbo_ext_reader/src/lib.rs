use std::{fmt::Debug, io, ops::Range, sync::Arc};

use bytes::Bytes;
use fusio_parquet::reader::AsyncReader;
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};
use thiserror::Error;
use ulid::Ulid;

pub mod foyer_reader;
pub mod lru_reader;

pub trait MetaCache: Sync + Send + Clone + Debug {
    fn get(&self, gen: &Ulid) -> Option<Arc<ParquetMetaData>>;

    fn insert(&self, gen: Ulid, data: Arc<ParquetMetaData>) -> Arc<ParquetMetaData>;
}

pub trait RangeCache: Sync + Send + Clone + Debug {
    fn get(
        &self,
        key: &(Ulid, Range<usize>),
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, CacheError>> + Send;

    fn insert(&self, key: (Ulid, Range<usize>), bytes: Bytes) -> Bytes;
}

pub trait CacheReader: AsyncFileReader + Unpin {
    type MetaCache: MetaCache;
    type RangeCache: RangeCache;

    fn new(
        meta_cache: Self::MetaCache,
        range_cache: Self::RangeCache,
        gen: Ulid,
        inner: AsyncReader,
    ) -> Self;

    #[allow(clippy::too_many_arguments)]
    fn build_caches(
        cache_path: impl AsRef<std::path::Path> + Send,
        cache_meta_capacity: usize,
        cache_meta_shards: usize,
        cache_meta_ratio: f64,
        cache_range_memory: usize,
        cache_range_disk: usize,
        cache_range_capacity: usize,
        cache_range_shards: usize,
    ) -> impl std::future::Future<Output = Result<(Self::MetaCache, Self::RangeCache), CacheError>> + Send;
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache io error: {0}")]
    Io(#[from] io::Error),
    #[error("foyer error: {0}")]
    Foyer(#[from] anyhow::Error),
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

    use crate::{foyer_reader::FoyerReader, lru_reader::LruReader, CacheReader};

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

    async fn inner_test_cache_read<R: CacheReader>() {
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
        let (meta_cache, range_cache) = R::build_caches(
            temp_dir.path().join("cache"),
            32,
            4,
            0.1,
            64 * 1024 * 1024,
            254 * 1024 * 1024,
            128,
            16,
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

            let mut reader = R::new(
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

    #[tokio::test]
    async fn test_cache_read() {
        inner_test_cache_read::<FoyerReader>().await;
        inner_test_cache_read::<LruReader>().await;
    }
}
