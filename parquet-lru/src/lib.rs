mod r#dyn;
#[cfg(feature = "foyer")]
pub mod foyer;

use std::{future::Future, marker::PhantomData};

use parquet::arrow::async_reader::AsyncFileReader;

pub use crate::r#dyn::*;

pub trait LruCache<K>
where
    K: 'static,
{
    type LruReader<R>: AsyncFileReader + 'static
    where
        R: AsyncFileReader + 'static;

    fn get_reader<R>(&self, key: K, reader: R) -> impl Future<Output = Self::LruReader<R>> + Send
    where
        R: AsyncFileReader + 'static;
}

#[derive(Default)]
pub struct NoCache<K> {
    _phantom: PhantomData<K>,
}

impl<K> Clone for NoCache<K> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

unsafe impl<K> Send for NoCache<K> {}

unsafe impl<K> Sync for NoCache<K> {}

impl<K> LruCache<K> for NoCache<K>
where
    K: 'static,
{
    type LruReader<R>
        = R
    where
        R: AsyncFileReader + 'static;

    #[allow(clippy::manual_async_fn)]
    fn get_reader<R>(&self, _key: K, reader: R) -> impl Future<Output = R> + Send
    where
        R: AsyncFileReader,
    {
        async move { reader }
    }
}
