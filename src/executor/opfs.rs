use std::{error::Error, future::Future, sync::Arc};

use async_lock;
use fusio::{MaybeSend, MaybeSync};
use wasm_bindgen::prelude::*;

use super::{Executor, JoinHandle, RwLock};

#[wasm_bindgen]
pub struct OpfsExecutor;

impl Default for OpfsExecutor {
    fn default() -> Self {
        Self
    }
}

impl OpfsExecutor {
    pub fn new() -> Self {
        Self
    }
}

pub struct OpfsJoinHandle<R> {
    _phantom: std::marker::PhantomData<R>,
}

impl<R> JoinHandle<R> for OpfsJoinHandle<R>
where
    R: MaybeSend,
{
    async fn join(self) -> Result<R, Box<dyn Error>> {
        // In WASM with spawn_local, we can't join the spawned task
        // This is a limitation of the WASM environment
        Err("Cannot join spawned tasks in WASM".into())
    }
}

impl<T> RwLock<T> for Arc<async_lock::RwLock<T>>
where
    T: MaybeSend + MaybeSync,
{
    type ReadGuard<'a>
        = async_lock::RwLockReadGuard<'a, T>
    where
        Self: 'a;

    type WriteGuard<'a>
        = async_lock::RwLockWriteGuard<'a, T>
    where
        Self: 'a;

    async fn read(&self) -> Self::ReadGuard<'_> {
        async_lock::RwLock::read(self).await
    }

    async fn write(&self) -> Self::WriteGuard<'_> {
        async_lock::RwLock::write(self).await
    }
}

impl Executor for OpfsExecutor {
    type JoinHandle<R>
        = OpfsJoinHandle<R>
    where
        R: MaybeSend;

    type RwLock<T>
        = Arc<async_lock::RwLock<T>>
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        wasm_bindgen_futures::spawn_local(async move {
            let _ = future.await;
        });
        OpfsJoinHandle {
            _phantom: std::marker::PhantomData,
        }
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        Arc::new(async_lock::RwLock::new(value))
    }
}
