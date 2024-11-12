#[cfg(feature = "foyer")]
pub mod foyer;

use std::{future::Future, marker::PhantomData};

use parquet::{arrow::async_reader::AsyncFileReader, errors::Result};
use thiserror::Error;

#[derive(Default)]
pub struct Options {
    meta_capacity: usize,
    data_capacity: usize,
}

impl Options {
    pub fn meta_capacity(mut self, meta_capacity: usize) -> Self {
        self.meta_capacity = meta_capacity;
        self
    }

    pub fn data_capacity(mut self, data_capacity: usize) -> Self {
        self.data_capacity = data_capacity;
        self
    }
}

pub trait LruCache<K>: Clone + Send + Sync + 'static {
    type LruReader<R: AsyncFileReader + 'static>: AsyncFileReader + 'static;

    fn new(options: Options) -> impl Future<Output = Result<Self, Error>> + Send;

    fn get_reader<R>(&self, key: K, reader: R) -> impl Future<Output = Self::LruReader<R>> + Send
    where
        R: AsyncFileReader + 'static;
}

#[derive(Clone, Default)]
pub struct NoopCache<K> {
    _phantom: PhantomData<K>,
}

impl<K> LruCache<K> for NoopCache<K>
where
    K: Send + Sync + Clone + 'static,
{
    type LruReader<R: AsyncFileReader + 'static> = R;

    async fn new(_options: Options) -> Result<Self, Error> {
        Ok(Self {
            _phantom: PhantomData,
        })
    }

    async fn get_reader<R: AsyncFileReader>(&self, _key: K, reader: R) -> R {
        reader
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("External lru implementation error: {0}")]
    External(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}
