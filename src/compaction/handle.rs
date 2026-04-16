//! Unified handle for background compaction workers.

use std::{marker::PhantomData, sync::Mutex};

use fusio::executor::Executor;
#[cfg(test)]
use fusio::executor::JoinHandle;
#[cfg(test)]
use futures::SinkExt;
use futures::{channel::mpsc, future::AbortHandle};

use crate::observability::log_debug;

/// Handle to a background compaction worker.
///
/// Provides control over the worker lifecycle. The worker is automatically
/// aborted when the handle is dropped.
#[derive(Debug)]
pub(crate) enum CompactionTrigger {
    Kick,
    Shutdown,
}

pub(crate) struct CompactionHandle<E: Executor> {
    abort: Option<AbortHandle>,
    join: Mutex<Option<E::JoinHandle<()>>>,
    trigger: Option<mpsc::Sender<CompactionTrigger>>,
    _marker: PhantomData<E>,
}

impl<E: Executor> CompactionHandle<E> {
    /// Create a new compaction handle.
    pub(crate) fn new(
        abort: AbortHandle,
        join: Option<E::JoinHandle<()>>,
        trigger: Option<mpsc::Sender<CompactionTrigger>>,
    ) -> Self {
        Self {
            abort: Some(abort),
            join: Mutex::new(join),
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
        log_debug!(component = "compaction", event = "compaction_kick",);
    }

    /// Gracefully stop the compaction worker and wait for it to exit.
    #[cfg(test)]
    pub(crate) async fn shutdown(mut self) {
        if let Some(mut sender) = self.trigger.take() {
            let _ = sender.send(CompactionTrigger::Shutdown).await;
        }
        let join = self
            .join
            .lock()
            .expect("compaction join mutex poisoned")
            .take();
        if let Some(join) = join {
            let _ = join.join().await;
        }
        self.abort.take();
    }
}

impl<E: Executor> Drop for CompactionHandle<E> {
    fn drop(&mut self) {
        if let Some(sender) = &self.trigger {
            let mut sender = sender.clone();
            let _ = sender.try_send(CompactionTrigger::Shutdown);
        }
        if let Some(abort) = self.abort.take() {
            abort.abort();
        }
        let _ = self
            .join
            .lock()
            .expect("compaction join mutex poisoned")
            .take();
        log_debug!(component = "compaction", event = "compaction_shutdown",);
    }
}
