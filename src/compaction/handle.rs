//! Unified handle for background compaction workers.

use std::marker::PhantomData;

use fusio::executor::Executor;
use futures::future::AbortHandle;

/// Handle to a background compaction worker.
///
/// Provides control over the worker lifecycle. The worker is automatically
/// aborted when the handle is dropped.
pub(crate) struct CompactionHandle<E: Executor> {
    abort: AbortHandle,
    _marker: PhantomData<E>,
}

impl<E: Executor> CompactionHandle<E> {
    /// Create a new compaction handle.
    pub(crate) fn new(abort: AbortHandle, _join: Option<E::JoinHandle<()>>) -> Self {
        Self {
            abort,
            _marker: PhantomData,
        }
    }
}

impl<E: Executor> Drop for CompactionHandle<E> {
    fn drop(&mut self) {
        self.abort.abort();
    }
}
