use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use futures_core::future::BoxFuture;
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::Result,
    file::metadata::ParquetMetaData,
};

use crate::LruCache;

pub struct BoxedFileReader {
    inner: Box<dyn AsyncFileReader>,
}

impl BoxedFileReader {
    pub fn new<T: AsyncFileReader + 'static>(inner: T) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl AsyncFileReader for BoxedFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }
}

pub trait DynLruCache<K> {
    fn get_reader(&self, key: K, reader: BoxedFileReader) -> BoxFuture<'_, BoxedFileReader>;
}

impl<K, C> DynLruCache<K> for C
where
    K: 'static + Send,
    C: LruCache<K> + Sized + Send + Sync,
{
    fn get_reader(&self, key: K, reader: BoxedFileReader) -> BoxFuture<'_, BoxedFileReader> {
        Box::pin(async move { BoxedFileReader::new(self.get_reader(key, reader).await) })
    }
}
