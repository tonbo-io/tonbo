//! Asynchronous write-ahead log framework scaffolding.
//!
//! This module exposes the public API surface described in RFC 0002 and acts
//! as the coordination layer between ingest, the frame encoder, storage
//! backends, and recovery. Implementations will live in the sibling modules.

use std::{fmt, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

use fusio::executor::{Executor, Timer};
use typed_arrow::arrow_array::RecordBatch;

use crate::db::Mode;

pub mod backend;
pub mod frame;
pub mod metrics;
pub mod replay;
// Writer logic will live here once the async queue is implemented.

/// Sync policy controlling how frequently the WAL forces durability.
#[derive(Debug, Clone)]
pub enum WalSyncPolicy {
    /// Call fsync after every committed frame.
    Always,
    /// Call fsync once the specified number of bytes have been appended.
    IntervalBytes(usize),
    /// Call fsync on a wall-clock cadence.
    IntervalTime(Duration),
    /// Do not explicitly sync (development/testing only).
    Disabled,
}

impl Default for WalSyncPolicy {
    fn default() -> Self {
        WalSyncPolicy::IntervalTime(Duration::from_millis(50))
    }
}

/// Configuration for enabling the WAL on a `DB` instance.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory in which WAL segments are created.
    pub dir: PathBuf,
    /// Maximum size for a single WAL segment before rotation.
    pub segment_max_bytes: usize,
    /// Flush interval for the writer's buffer.
    pub flush_interval: Duration,
    /// Durability policy applied after writes.
    pub sync: WalSyncPolicy,
    /// Soft cap on retained WAL bytes.
    pub retention_bytes: Option<usize>,
    /// Capacity of the writer's bounded queue.
    pub queue_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("wal"),
            segment_max_bytes: 64 * 1024 * 1024,
            flush_interval: Duration::from_millis(10),
            sync: WalSyncPolicy::default(),
            retention_bytes: None,
            queue_size: 65_536,
        }
    }
}

/// Result type shared across WAL operations.
pub type WalResult<T> = Result<T, WalError>;

/// Errors surfaced by the WAL subsystem.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// Underlying storage backend failure.
    #[error("wal backend error: {0}")]
    Backend(String),
    /// The WAL is not currently enabled.
    #[error("wal not enabled")]
    Disabled,
    /// Module has not been fully implemented yet.
    #[error("wal feature is not implemented: {0}")]
    Unimplemented(&'static str),
}

/// Acks emitted once a WAL submission satisfies the configured durability policy.
#[derive(Debug)]
pub struct WalAck {
    /// Sequence number of the durable frame.
    pub seq: u64,
    /// Bytes flushed while satisfying the policy.
    pub bytes_flushed: usize,
    /// Time elapsed between enqueue and durability.
    pub elapsed: Duration,
}

/// Logical payloads accepted by the WAL.
#[derive(Debug)]
pub enum WalPayload {
    /// Dynamic mode batch along with the commit timestamp.
    DynBatch {
        /// Arrow batch storing the row data.
        batch: RecordBatch,
        /// Commit timestamp captured at enqueue.
        commit_ts: u64,
    },
    /// Reserved for typed row append once compile-time dispatch returns.
    #[allow(dead_code)]
    TypedRows {
        /// Placeholder for the typed payload.
        _todo: (),
    },
    /// Reserved for future control frames.
    #[allow(dead_code)]
    Control {
        /// Placeholder for control payload.
        _todo: (),
    },
}

/// Handle returned when enabling the WAL on a `DB`.
#[derive(Debug)]
pub struct WalHandle<E> {
    /// Placeholder for internal state until implementation is provided.
    _inner: Arc<()>,
    _exec: PhantomData<E>,
}

impl<E> Clone for WalHandle<E> {
    fn clone(&self) -> Self {
        Self {
            _inner: Arc::clone(&self._inner),
            _exec: PhantomData,
        }
    }
}

impl<E> WalHandle<E> {
    pub(crate) fn placeholder() -> Self {
        Self {
            _inner: Arc::new(()),
            _exec: PhantomData,
        }
    }

    /// Enqueue a payload to the WAL writer.
    pub async fn submit(&self, _payload: WalPayload) -> WalResult<WalTicket<E>> {
        Err(WalError::Unimplemented("wal::WalHandle::submit"))
    }

    /// Force manual rotation of the active WAL segment.
    pub async fn rotate(&self) -> WalResult<()> {
        Err(WalError::Unimplemented("wal::WalHandle::rotate"))
    }
}

/// Ticket returned after a successful submission.
pub struct WalTicket<E> {
    /// Sequence number assigned to the payload.
    pub seq: u64,
    _exec: PhantomData<E>,
}

impl<E> WalTicket<E>
where
    E: Executor + Timer,
{
    pub(crate) fn new(seq: u64) -> Self {
        Self {
            seq,
            _exec: PhantomData,
        }
    }

    /// Resolve once durability is achieved.
    pub async fn durable(&self) -> WalResult<WalAck> {
        Err(WalError::Unimplemented("WalTicket::durable"))
    }
}

impl<E> fmt::Debug for WalTicket<E>
where
    E: Executor + Timer,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalTicket")
            .field("seq", &self.seq)
            .field("durable", &"Unimplemented")
            .finish()
    }
}

/// Extension methods on the `DB` to enable/disable the WAL.
pub trait WalExt<M: Mode, E: Executor + Timer> {
    /// Enable the WAL with the provided configuration and executor.
    fn enable_wal(&mut self, _cfg: WalConfig) -> WalResult<WalHandle<E>>;

    /// Disable the WAL and wait for outstanding work to drain.
    fn disable_wal(&mut self) -> WalResult<()>;

    /// Access the active WAL handle, if any.
    fn wal(&self) -> Option<&WalHandle<E>>;
}

impl<M, E> WalExt<M, E> for crate::db::DB<M, E>
where
    M: Mode,
    E: Executor + Timer,
{
    fn enable_wal(&mut self, cfg: WalConfig) -> WalResult<WalHandle<E>> {
        self.set_wal_config(Some(cfg));
        let _ = self.executor();
        let handle = WalHandle::placeholder();
        self.set_wal_handle(Some(handle.clone()));
        Ok(handle)
    }

    fn disable_wal(&mut self) -> WalResult<()> {
        self.set_wal_handle(None);
        Ok(())
    }

    fn wal(&self) -> Option<&WalHandle<E>> {
        self.wal_handle()
    }
}
