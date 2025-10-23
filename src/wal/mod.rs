//! Asynchronous write-ahead log framework scaffolding.
//!
//! This module exposes the public API surface described in RFC 0002 and acts
//! as the coordination layer between ingest, the frame encoder, storage
//! backends, and recovery. Implementations will live in the sibling modules.

use std::{
    fmt,
    marker::PhantomData,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field};

use fusio::{
    DynFs,
    executor::{Executor, JoinHandle, Timer},
    impls::disk::TokioFs,
    path::Path,
};
use futures::{
    SinkExt,
    channel::{mpsc, oneshot},
    executor::block_on,
};

use crate::{
    db::Mode, inmem::immutable::memtable::MVCC_TOMBSTONE_COL, mvcc::Timestamp,
    wal::metrics::WalMetrics,
};

pub mod frame;
pub mod metrics;
pub mod replay;
pub mod storage;
pub mod writer;
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
#[derive(Debug, Clone, thiserror::Error)]
pub enum WalError {
    /// Underlying storage backend failure.
    #[error("wal backend error: {0}")]
    Backend(String),
    /// Payload serialization or deserialization failure.
    #[error("wal frame codec error: {0}")]
    Codec(String),
    /// The WAL data on disk is corrupt or truncated.
    #[error("wal frame is corrupt: {0}")]
    Corrupt(&'static str),
    /// The WAL is not currently enabled.
    #[error("wal not enabled")]
    Disabled,
    /// Module has not been fully implemented yet.
    #[error("wal feature is not implemented: {0}")]
    Unimplemented(&'static str),
}

/// Acks emitted once a WAL submission satisfies the configured durability policy.
#[derive(Debug, Clone)]
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
        commit_ts: Timestamp,
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

struct WalHandleInner<E>
where
    E: Executor + Timer,
{
    sender: mpsc::Sender<writer::WriterMsg>,
    queue_depth: Arc<AtomicUsize>, // current queue occupancy
    next_payload_seq: AtomicU64,   /* logical seq handed to each submitted payload (embedded in
                                    * frames) */
    join: Mutex<Option<E::JoinHandle<WalResult<()>>>>,
}

impl<E> WalHandleInner<E>
where
    E: Executor + Timer,
{
    fn new(
        sender: mpsc::Sender<writer::WriterMsg>,
        queue_depth: Arc<AtomicUsize>,
        start_seq: u64,
        join: E::JoinHandle<WalResult<()>>,
    ) -> Self {
        Self {
            sender,
            queue_depth,
            next_payload_seq: AtomicU64::new(start_seq),
            join: Mutex::new(Some(join)),
        }
    }

    fn clone_sender(&self) -> mpsc::Sender<writer::WriterMsg> {
        self.sender.clone()
    }

    fn close_channel(&self) {
        let mut sender = self.sender.clone();
        sender.close_channel();
    }

    fn take_join(&self) -> Option<E::JoinHandle<WalResult<()>>> {
        self.join.lock().expect("wal join mutex poisoned").take()
    }
}

/// Handle returned when enabling the WAL on a `DB`.
pub struct WalHandle<E>
where
    E: Executor + Timer,
{
    inner: Arc<WalHandleInner<E>>,
}

impl<E> Clone for WalHandle<E>
where
    E: Executor + Timer,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E> fmt::Debug for WalHandle<E>
where
    E: Executor + Timer,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalHandle").finish()
    }
}

impl<E> WalHandle<E>
where
    E: Executor + Timer,
{
    fn from_parts(
        sender: mpsc::Sender<writer::WriterMsg>,
        queue_depth: Arc<AtomicUsize>,
        join: E::JoinHandle<WalResult<()>>,
        start_seq: u64,
    ) -> Self {
        Self {
            inner: Arc::new(WalHandleInner::new(sender, queue_depth, start_seq, join)),
        }
    }

    /// Enqueue a payload to the WAL writer.
    pub async fn submit(&self, payload: WalPayload) -> WalResult<WalTicket<E>> {
        let payload_seq = self.inner.next_payload_seq.fetch_add(1, Ordering::SeqCst);
        let (ack_tx, ack_rx) = oneshot::channel();
        let enqueued_at = Instant::now();

        self.inner.queue_depth.fetch_add(1, Ordering::SeqCst);

        let msg = writer::WriterMsg::Enqueue {
            payload_seq,
            payload,
            enqueued_at,
            ack_tx,
        };

        let mut sender = self.inner.clone_sender();
        if let Err(_err) = sender.send(msg).await {
            self.inner.queue_depth.fetch_sub(1, Ordering::SeqCst);
            return Err(WalError::Backend("wal writer task closed".into()));
        }

        Ok(WalTicket {
            seq: payload_seq,
            receiver: ack_rx,
            _exec: PhantomData,
        })
    }

    /// Force manual rotation of the active WAL segment.
    pub async fn rotate(&self) -> WalResult<()> {
        Err(WalError::Unimplemented("wal::WalHandle::rotate"))
    }

    /// Shut down the writer task and wait for completion.
    pub fn shutdown(self) -> WalResult<()> {
        self.inner.close_channel();
        if let Some(join) = self.inner.take_join() {
            let join_result = block_on(async move { join.join().await })
                .map_err(|err| WalError::Backend(err.to_string()))?;
            join_result?
        }
        Ok(())
    }
}

/// Append a `_tombstone` boolean column to `batch`, returning a new batch for WAL storage.
pub fn batch_with_tombstones(
    batch: &RecordBatch,
    tombstones: Option<&[bool]>,
) -> WalResult<RecordBatch> {
    let rows = batch.num_rows();
    let values = match tombstones {
        Some(bits) => {
            if bits.len() != rows {
                return Err(WalError::Codec(
                    "tombstone bitmap length mismatch record batch".to_string(),
                ));
            }
            bits.to_vec()
        }
        None => vec![false; rows],
    };

    if batch
        .schema()
        .fields()
        .iter()
        .any(|f| f.name() == MVCC_TOMBSTONE_COL)
    {
        return Err(WalError::Codec(
            "record batch already contains _tombstone column".to_string(),
        ));
    }

    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false).into());

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(BooleanArray::from(values)) as _);

    let schema = Arc::new(arrow_schema::Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|err| WalError::Codec(err.to_string()))
}

/// Remove `_tombstone` column, returning the stripped batch and bitmap when present.
pub fn strip_tombstone_column(batch: RecordBatch) -> WalResult<(RecordBatch, Option<Vec<bool>>)> {
    let schema = batch.schema();
    if let Some(idx) = schema
        .fields()
        .iter()
        .position(|f| f.name() == MVCC_TOMBSTONE_COL)
    {
        let array = batch.column(idx);
        let bools = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| WalError::Codec("_tombstone column not boolean".to_string()))?;
        let bitmap = (0..bools.len())
            .map(|i| bools.value(i))
            .collect::<Vec<bool>>();
        let stripped_fields = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, f)| f.clone())
            .collect::<Vec<_>>();
        let stripped_columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, col)| col.clone())
            .collect::<Vec<_>>();
        let stripped = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(stripped_fields)),
            stripped_columns,
        )
        .map_err(|err| WalError::Codec(err.to_string()))?;
        Ok((stripped, Some(bitmap)))
    } else {
        Ok((batch, None))
    }
}

/// Ticket returned after a successful submission.
pub struct WalTicket<E> {
    /// Sequence number assigned to the payload.
    pub seq: u64,
    receiver: oneshot::Receiver<WalResult<WalAck>>,
    _exec: PhantomData<E>,
}

impl<E> WalTicket<E>
where
    E: Executor + Timer,
{
    /// Resolve once durability is achieved.
    pub async fn durable(self) -> WalResult<WalAck> {
        match self.receiver.await {
            Ok(result) => result,
            Err(_) => Err(WalError::Backend("wal writer dropped ack".into())),
        }
    }
}

impl<E> fmt::Debug for WalTicket<E>
where
    E: Executor + Timer,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalTicket").field("seq", &self.seq).finish()
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
        let fs: Arc<dyn DynFs> = Arc::new(TokioFs);
        let wal_root = Path::from_filesystem_path(&cfg.dir)
            .map_err(|err| WalError::Backend(err.to_string()))?;
        let storage = storage::WalStorage::new(fs, wal_root);

        let metrics = Arc::new(E::rw_lock(WalMetrics::default()));
        let writer = writer::spawn_writer(
            Arc::clone(self.executor()),
            storage,
            cfg.clone(),
            Arc::clone(&metrics),
            0,
            frame::INITIAL_FRAME_SEQ,
        );
        let (sender, queue_depth, join) = writer.into_parts();
        let handle = WalHandle::from_parts(sender, queue_depth, join, frame::INITIAL_FRAME_SEQ);

        self.set_wal_config(Some(cfg));
        self.set_wal_handle(Some(handle.clone()));
        Ok(handle)
    }

    fn disable_wal(&mut self) -> WalResult<()> {
        if let Some(handle) = self.wal_handle().cloned() {
            self.set_wal_handle(None);
            handle.shutdown()?;
        } else {
            self.set_wal_handle(None);
        }
        Ok(())
    }

    fn wal(&self) -> Option<&WalHandle<E>> {
        self.wal_handle()
    }
}
