//! Unified handle for background compaction workers.

use std::marker::PhantomData;

use fusio::executor::Executor;
use futures::{channel::mpsc, future::AbortHandle};

/// Handle to a background compaction worker.
///
/// Provides control over the worker lifecycle. The worker is automatically
/// aborted when the handle is dropped.
#[derive(Debug)]
pub(crate) enum CompactionTrigger {
    Kick,
}

pub(crate) struct CompactionHandle<E: Executor> {
    abort: AbortHandle,
    trigger: Option<mpsc::Sender<CompactionTrigger>>,
    _marker: PhantomData<E>,
}

impl<E: Executor> CompactionHandle<E> {
    /// Create a new compaction handle.
    pub(crate) fn new(
        abort: AbortHandle,
        _join: Option<E::JoinHandle<()>>,
        trigger: Option<mpsc::Sender<CompactionTrigger>>,
    ) -> Self {
        Self {
            abort,
            trigger,
            _marker: PhantomData,
        }
    }

    /// Best-effort trigger to nudge the compaction worker.
    pub(crate) fn kick(&self) {
        if let Some(sender) = &self.trigger {
            let mut sender = sender.clone();
            let _ = sender.try_send(CompactionTrigger::Kick);
        }
    }
}

impl<E: Executor> Drop for CompactionHandle<E> {
    fn drop(&mut self) {
        self.abort.abort();
    }
}
