//! Asynchronous write-ahead log framework scaffolding.
//!
//! This module exposes the public API surface described in RFC 0002 and acts
//! as the coordination layer between ingest, the frame encoder, storage
//! backends, and recovery. Implementations will live in the sibling modules.

use std::{
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
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
#[derive(Clone)]
pub struct WalConfig {
    /// Directory in which WAL segments are created.
    pub dir: Path,
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
    /// Filesystem implementation backing the WAL directory.
    pub filesystem: Arc<dyn DynFs>,
}

impl Default for WalConfig {
    fn default() -> Self {
        let dir = Path::parse("wal").expect("static wal path");
        Self {
            dir,
            segment_max_bytes: 64 * 1024 * 1024,
            flush_interval: Duration::from_millis(10),
            sync: WalSyncPolicy::default(),
            retention_bytes: None,
            queue_size: 65_536,
            filesystem: Arc::new(TokioFs),
        }
    }
}

impl fmt::Debug for WalConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fs_tag = self.filesystem.file_system();
        f.debug_struct("WalConfig")
            .field("dir", &self.dir)
            .field("segment_max_bytes", &self.segment_max_bytes)
            .field("flush_interval", &self.flush_interval)
            .field("sync", &self.sync)
            .field("retention_bytes", &self.retention_bytes)
            .field("queue_size", &self.queue_size)
            .field("filesystem", &fs_tag)
            .finish()
    }
}

/// Result type shared across WAL operations.
pub type WalResult<T> = Result<T, WalError>;

/// Errors surfaced by the WAL subsystem.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WalError {
    /// Underlying storage layer failure.
    #[error("wal storage error: {0}")]
    Storage(String),
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
    /// Dynamic mode batch along with the commit timestamp (batch already includes `_tombstone`).
    DynBatch {
        /// Arrow batch storing the row data with `_tombstone` column appended.
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

/// Append a `_tombstone` boolean column, returning a new batch for WAL storage.
pub fn append_tombstone_column(
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
    columns.push(Arc::new(BooleanArray::from(values)) as ArrayRef);

    let schema = Arc::new(arrow_schema::Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|err| WalError::Codec(err.to_string()))
}

/// Remove `_tombstone` column, returning the stripped batch and bitmap.
pub(crate) fn split_tombstone_column(batch: RecordBatch) -> WalResult<(RecordBatch, Vec<bool>)> {
    let schema = batch.schema();
    let idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == MVCC_TOMBSTONE_COL)
        .ok_or_else(|| WalError::Codec("_tombstone column missing".to_string()))?;

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

    Ok((stripped, bitmap))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::inmem::immutable::memtable::MVCC_TOMBSTONE_COL;

    fn int_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let data = Arc::new(Int32Array::from(vec![1, 2])) as _;
        RecordBatch::try_new(schema, vec![data]).expect("batch")
    }

    #[test]
    fn append_tombstone_rejects_length_mismatch() {
        let batch = int_batch();
        let err = append_tombstone_column(&batch, Some(&[true])).expect_err("length mismatch");
        assert!(
            matches!(err, WalError::Codec(msg) if msg.contains("tombstone bitmap length mismatch"))
        );
    }

    #[test]
    fn append_tombstone_rejects_existing_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false),
        ]));
        let data = Arc::new(Int32Array::from(vec![1])) as _;
        let tombstones = Arc::new(BooleanArray::from(vec![false])) as _;
        let batch = RecordBatch::try_new(schema, vec![data, tombstones]).expect("batch");
        let err = append_tombstone_column(&batch, None).expect_err("existing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("already contains _tombstone")));
    }

    #[test]
    fn split_tombstone_requires_column() {
        let batch = int_batch();
        let err = split_tombstone_column(batch).expect_err("missing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("_tombstone column missing")));
    }
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
            return Err(WalError::Storage("wal writer task closed".into()));
        }

        Ok(WalTicket {
            seq: payload_seq,
            receiver: ack_rx,
            _exec: PhantomData,
        })
    }

    /// Append a record batch with its tombstone bitmap to the WAL.
    pub async fn append(
        &self,
        batch: &RecordBatch,
        tombstones: &[bool],
        commit_ts: Timestamp,
    ) -> WalResult<WalTicket<E>> {
        if batch.num_rows() != tombstones.len() {
            return Err(WalError::Codec(
                "tombstone bitmap length mismatch record batch".to_string(),
            ));
        }
        let wal_batch = append_tombstone_column(batch, Some(tombstones))?;
        self.submit(WalPayload::DynBatch {
            batch: wal_batch,
            commit_ts,
        })
        .await
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
                .map_err(|err| WalError::Storage(err.to_string()))?;
            join_result?
        }
        Ok(())
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
            Err(_) => Err(WalError::Storage("wal writer dropped ack".into())),
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
        let storage = storage::WalStorage::new(Arc::clone(&cfg.filesystem), cfg.dir.clone());

        let tail_metadata = block_on(storage.tail_metadata())?;
        let next_payload_seq = tail_metadata
            .as_ref()
            .and_then(|meta| meta.last_provisional_id)
            .map(|last| last.saturating_add(1))
            .unwrap_or(frame::INITIAL_FRAME_SEQ);
        let initial_frame_seq = tail_metadata
            .as_ref()
            .and_then(|meta| meta.last_frame_seq)
            .map(|seq| seq.saturating_add(1))
            .unwrap_or(frame::INITIAL_FRAME_SEQ);

        let metrics = Arc::new(E::rw_lock(WalMetrics::default()));
        let writer = writer::spawn_writer(
            Arc::clone(self.executor()),
            storage,
            cfg.clone(),
            Arc::clone(&metrics),
            0,
            initial_frame_seq,
        );
        let (sender, queue_depth, join) = writer.into_parts();
        let handle = WalHandle::from_parts(sender, queue_depth, join, next_payload_seq);

        self.set_wal_config(Some(cfg.clone()));
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
