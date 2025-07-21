#[cfg(feature = "tokio")]
pub mod tokio;

use std::{
    error::Error,
    future::Future,
    ops::{Deref, DerefMut},
};

use fusio::{MaybeSend, MaybeSync};

pub trait JoinHandle<R> {
    fn join(self) -> impl Future<Output = Result<R, Box<dyn Error>>> + MaybeSend;
}

pub trait RwLock<T> {
    type ReadGuard<'a>: Deref<Target = T> + 'a
    where
        Self: 'a;

    type WriteGuard<'a>: DerefMut<Target = T> + 'a
    where
        Self: 'a;

    fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + MaybeSend;

    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + MaybeSend;
}

pub trait Executor {
    type JoinHandle<R>: JoinHandle<R>
    where
        R: MaybeSend;

    type RwLock<T>: RwLock<T>
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend;

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync;
}
