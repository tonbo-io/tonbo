//! Asynchronous write-ahead log framework scaffolding.
//!
//! This module exposes the public API surface described in RFC 0002 and acts
//! as the coordination layer between ingest, the frame encoder, storage
//! backends, and recovery. Implementations will live in the sibling modules.

use std::{
    fmt,
    hash::Hash,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use fusio::{
    DynFs,
    executor::{Executor, JoinHandle, Timer},
    path::Path,
};
use futures::{
    SinkExt,
    channel::{mpsc, oneshot},
    executor::block_on,
};

use crate::{
    db::Mode,
    inmem::immutable::memtable::{MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL},
    mvcc::Timestamp,
    wal::metrics::WalMetrics,
};

pub mod frame;
pub mod manifest_ext;
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

/// Recovery behavior adopted when scanning existing WAL segments.
///
/// See `docs/rfcs/0002-wal.md#recovery-modes` for a deeper discussion of the trade-offs. Only the
/// point-in-time style semantics are implemented today; stricter or more lenient policies surface
/// `WalError::Unimplemented`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalRecoveryMode {
    /// Stop replay at the first truncated or unreadable frame and surface the events committed up
    /// to that point.
    #[default]
    PointInTime,
    /// Stop replay at the first truncated or unreadable frame, treating tail damage as the result
    /// of an interrupted write rather than hard corruption. Equivalent to
    /// [`WalRecoveryMode::PointInTime`] today, but reserved for future logic that may apply
    /// tail-specific heuristics (for example, allowing zero-length CRC mismatches at the end of the
    /// final segment).
    TolerateCorruptedTail,
    /// Treat any I/O or codec error as fatal corruption, aborting replay immediately. Not yet
    /// implemented.
    AbsoluteConsistency,
    /// Skip over corrupted frames and continue replaying subsequent data. Not yet implemented.
    SkipCorrupted,
}

/// Configuration for enabling the WAL on a `DB` instance.
#[derive(Clone)]
pub struct WalConfig {
    /// Directory in which WAL segments are created.
    pub dir: Path,
    /// Maximum size for a single WAL segment before rotation.
    pub segment_max_bytes: usize,
    /// Maximum time a single WAL segment may remain active before rotation. `None` disables
    /// age-based rotation.
    pub segment_max_age: Option<Duration>,
    /// Flush interval for the writer's buffer.
    pub flush_interval: Duration,
    /// Durability policy applied after writes.
    pub sync: WalSyncPolicy,
    /// Replay behavior adopted during recovery (defaults to [`WalRecoveryMode::PointInTime`]).
    pub recovery: WalRecoveryMode,
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
            segment_max_age: None,
            flush_interval: Duration::from_millis(10),
            sync: WalSyncPolicy::default(),
            recovery: WalRecoveryMode::default(),
            retention_bytes: None,
            queue_size: 65_536,
            filesystem: crate::fs::local_fs(),
        }
    }
}

impl fmt::Debug for WalConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fs_tag = self.filesystem.file_system();
        f.debug_struct("WalConfig")
            .field("dir", &self.dir)
            .field("segment_max_bytes", &self.segment_max_bytes)
            .field("segment_max_age", &self.segment_max_age)
            .field("flush_interval", &self.flush_interval)
            .field("sync", &self.sync)
            .field("recovery", &self.recovery)
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
    /// Tombstone bitmap length does not match the batch row count.
    #[error("tombstone bitmap length mismatch: expected {expected}, got {actual}")]
    TombstoneLengthMismatch {
        /// Expected number of rows in the batch.
        expected: usize,
        /// Actual bitmap entries provided.
        actual: usize,
    },
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

/// Dynamic ingest payload handed to WAL commands.
#[derive(Debug, Clone)]
pub struct DynBatchPayload {
    /// Record batch supplied by the caller (without MVCC columns).
    pub batch: RecordBatch,
    /// Arrow column containing the `_commit_ts` values for the batch.
    pub commit_ts_column: ArrayRef,
    /// Arrow column containing the `_tombstone` bitmap for the batch.
    pub tombstone_column: ArrayRef,
}

impl DynBatchPayload {
    /// Number of rows carried by the batch.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Logical commands accepted by the WAL writer queue.
#[derive(Debug, Clone)]
pub enum WalCommand {
    /// Open an explicit transaction.
    TxnBegin {
        /// Provisional identifier reserved for this transaction.
        provisional_id: u64,
    },
    /// Append rows to an explicit transaction.
    TxnAppend {
        /// Provisional identifier associated with the transaction.
        provisional_id: u64,
        /// Ingest payload containing the data and MVCC columns.
        payload: DynBatchPayload,
    },
    /// Commit an explicit transaction.
    TxnCommit {
        /// Provisional identifier associated with the transaction.
        provisional_id: u64,
        /// Commit timestamp.
        commit_ts: Timestamp,
    },
    /// Abort an explicit transaction.
    TxnAbort {
        /// Provisional identifier associated with the transaction.
        provisional_id: u64,
    },
}

/// Append `_commit_ts` and `_tombstone` columns, returning a new batch for WAL storage.
pub(crate) fn append_mvcc_columns(
    batch: &RecordBatch,
    commit_ts: &ArrayRef,
    tombstones: &ArrayRef,
) -> WalResult<RecordBatch> {
    let rows = batch.num_rows();

    if commit_ts.len() != rows {
        return Err(WalError::Codec(
            "commit_ts column length mismatch record batch".to_string(),
        ));
    }

    let commit_array = commit_ts
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| WalError::Codec("_commit_ts column not u64".to_string()))?;
    if commit_array.null_count() > 0 {
        return Err(WalError::Codec("null commit_ts value".to_string()));
    }

    if commit_array.is_empty()
        || commit_array
            .iter()
            .all(|value| value.map(|v| v == commit_array.value(0)).unwrap_or(false))
    {
        // ok
    } else {
        return Err(WalError::Codec(
            "commit_ts column contained varying values".to_string(),
        ));
    }

    if tombstones.len() != rows {
        return Err(WalError::Codec(
            "tombstone bitmap length mismatch record batch".to_string(),
        ));
    }

    let tombstone_array = tombstones
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| WalError::Codec("_tombstone column not boolean".to_string()))?;
    if tombstone_array.null_count() > 0 {
        return Err(WalError::Codec("null tombstone value".to_string()));
    }

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

    if batch
        .schema()
        .fields()
        .iter()
        .any(|f| f.name() == MVCC_COMMIT_COL)
    {
        return Err(WalError::Codec(
            "record batch already contains _commit_ts column".to_string(),
        ));
    }

    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false).into());
    fields.push(Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false).into());

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::clone(commit_ts));
    columns.push(Arc::clone(tombstones));

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|err| WalError::Codec(err.to_string()))
}

/// Remove MVCC columns, returning the stripped batch, commit timestamps, and bitmap.
pub(crate) fn split_mvcc_columns(
    batch: RecordBatch,
) -> WalResult<(RecordBatch, Option<Timestamp>, ArrayRef, ArrayRef)> {
    let schema = batch.schema();
    let commit_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == MVCC_COMMIT_COL)
        .ok_or_else(|| WalError::Codec("_commit_ts column missing".to_string()))?;
    let tombstone_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == MVCC_TOMBSTONE_COL)
        .ok_or_else(|| WalError::Codec("_tombstone column missing".to_string()))?;

    let commit_array = batch.column(commit_idx);
    let commit_column = commit_array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| WalError::Codec("_commit_ts column not u64".to_string()))?;
    if commit_column.null_count() > 0 {
        return Err(WalError::Codec("null commit_ts value".to_string()));
    }
    let commit_ts = if commit_column.is_empty() {
        None
    } else {
        let first = commit_column.value(0);
        if commit_column
            .iter()
            .any(|value| value.map(|v| v != first).unwrap_or(true))
        {
            return Err(WalError::Codec(
                "commit_ts column contained varying values".to_string(),
            ));
        }
        Some(Timestamp::from(first))
    };

    let tombstone_array = batch.column(tombstone_idx);
    let tombstone_column = tombstone_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| WalError::Codec("_tombstone column not boolean".to_string()))?;
    if tombstone_column.null_count() > 0 {
        return Err(WalError::Codec("null tombstone value".to_string()));
    }

    let stripped_fields = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != commit_idx && *idx != tombstone_idx)
        .map(|(_, field)| field.clone())
        .collect::<Vec<_>>();
    let stripped_columns = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != commit_idx && *idx != tombstone_idx)
        .map(|(_, column)| column.clone())
        .collect::<Vec<_>>();

    let stripped = RecordBatch::try_new(Arc::new(Schema::new(stripped_fields)), stripped_columns)
        .map_err(|err| WalError::Codec(err.to_string()))?;

    Ok((
        stripped,
        commit_ts,
        Arc::clone(commit_array),
        Arc::clone(tombstone_array),
    ))
}

struct WalHandleInner<E>
where
    E: Executor + Timer,
{
    sender: mpsc::Sender<writer::WriterMsg>,
    queue_depth: Arc<AtomicUsize>, // current queue occupancy
    next_payload_seq: AtomicU64,   // monotonic sequence handed to payloads and commands
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

    fn next_payload_seq(&self) -> u64 {
        self.next_payload_seq.fetch_add(1, Ordering::SeqCst)
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

    /// Allocate the next provisional identifier suitable for WAL commands.
    pub fn next_provisional_id(&self) -> u64 {
        self.inner.next_payload_seq()
    }

    /// Enqueue a command to the WAL writer.
    pub async fn submit_command(&self, command: WalCommand) -> WalResult<WalTicket<E>> {
        let submission_seq = self.inner.next_payload_seq();
        self.enqueue_command(command, submission_seq).await
    }

    async fn enqueue_command(
        &self,
        command: WalCommand,
        submission_seq: u64,
    ) -> WalResult<WalTicket<E>> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let enqueued_at = Instant::now();

        self.inner.queue_depth.fetch_add(1, Ordering::SeqCst);

        let msg = writer::WriterMsg::Enqueue {
            _submission_seq: submission_seq,
            command,
            enqueued_at,
            ack_tx,
        };

        let mut sender = self.inner.clone_sender();
        if let Err(_err) = sender.send(msg).await {
            self.inner.queue_depth.fetch_sub(1, Ordering::SeqCst);
            return Err(WalError::Storage("wal writer task closed".into()));
        }

        Ok(WalTicket {
            seq: submission_seq,
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
            return Err(WalError::TombstoneLengthMismatch {
                expected: batch.num_rows(),
                actual: tombstones.len(),
            });
        }
        let provisional_id = self.next_provisional_id();
        let append_ticket = self
            .txn_append(provisional_id, batch, tombstones, commit_ts)
            .await?;
        append_ticket.durable().await?;
        self.txn_commit(provisional_id, commit_ts).await
    }

    /// Force manual rotation of the active WAL segment.
    pub async fn rotate(&self) -> WalResult<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let mut sender = self.inner.clone_sender();
        let msg = writer::WriterMsg::Rotate { ack_tx };

        if let Err(_err) = sender.send(msg).await {
            return Err(WalError::Storage("wal writer task closed".into()));
        }

        ack_rx
            .await
            .map_err(|_| WalError::Storage("wal writer task closed".into()))?
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

    #[cfg(test)]
    pub(crate) fn test_from_parts(
        sender: mpsc::Sender<writer::WriterMsg>,
        queue_depth: Arc<AtomicUsize>,
        join: E::JoinHandle<WalResult<()>>,
        start_seq: u64,
    ) -> Self {
        Self::from_parts(sender, queue_depth, join, start_seq)
    }

    /// Log a transaction begin marker.
    pub async fn txn_begin(&self, provisional_id: u64) -> WalResult<WalTicket<E>> {
        self.enqueue_command(WalCommand::TxnBegin { provisional_id }, provisional_id)
            .await
    }

    /// Log a transaction append frame carrying a batch and tombstone bitmap.
    pub async fn txn_append(
        &self,
        provisional_id: u64,
        batch: &RecordBatch,
        tombstones: &[bool],
        commit_ts: Timestamp,
    ) -> WalResult<WalTicket<E>> {
        if batch.num_rows() != tombstones.len() {
            return Err(WalError::TombstoneLengthMismatch {
                expected: batch.num_rows(),
                actual: tombstones.len(),
            });
        }
        let commit_values =
            Arc::new(UInt64Array::from(vec![commit_ts.get(); batch.num_rows()])) as ArrayRef;
        let tombstone_values = Arc::new(BooleanArray::from(tombstones.to_vec())) as ArrayRef;
        let payload = DynBatchPayload {
            batch: batch.clone(),
            commit_ts_column: Arc::clone(&commit_values),
            tombstone_column: Arc::clone(&tombstone_values),
        };
        let command = WalCommand::TxnAppend {
            provisional_id,
            payload,
        };
        self.enqueue_command(command, provisional_id).await
    }

    /// Log a transaction commit marker.
    pub async fn txn_commit(
        &self,
        provisional_id: u64,
        commit_ts: Timestamp,
    ) -> WalResult<WalTicket<E>> {
        self.enqueue_command(
            WalCommand::TxnCommit {
                provisional_id,
                commit_ts,
            },
            provisional_id,
        )
        .await
    }

    /// Log a transaction abort marker.
    pub async fn txn_abort(&self, provisional_id: u64) -> WalResult<WalTicket<E>> {
        self.enqueue_command(WalCommand::TxnAbort { provisional_id }, provisional_id)
            .await
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let data = Arc::new(Int32Array::from(vec![1, 2])) as _;
        RecordBatch::try_new(schema, vec![data]).expect("batch")
    }

    fn commit_array(ts: u64, len: usize) -> ArrayRef {
        Arc::new(UInt64Array::from(vec![ts; len])) as ArrayRef
    }

    fn tombstone_array(values: &[bool]) -> ArrayRef {
        Arc::new(BooleanArray::from(values.to_vec())) as ArrayRef
    }

    #[test]
    fn append_mvcc_rejects_length_mismatch() {
        let batch = sample_batch();
        let commit = commit_array(1, batch.num_rows());
        let tombstones = tombstone_array(&[true]);
        let err = append_mvcc_columns(&batch, &commit, &tombstones).expect_err("length mismatch");
        assert!(
            matches!(err, WalError::Codec(msg) if msg.contains("tombstone bitmap length mismatch"))
        );
    }

    #[test]
    fn append_mvcc_rejects_existing_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let data = Arc::new(Int32Array::from(vec![1])) as _;
        let commit = Arc::new(UInt64Array::from(vec![1_u64])) as _;
        let batch = RecordBatch::try_new(schema, vec![data, commit]).expect("batch");
        let commit = commit_array(2, batch.num_rows());
        let tombstones = tombstone_array(&[false]);
        let err = append_mvcc_columns(&batch, &commit, &tombstones).expect_err("existing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("already contains _commit_ts")));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false),
        ]));
        let data = Arc::new(Int32Array::from(vec![1])) as _;
        let tombstones = Arc::new(BooleanArray::from(vec![false])) as _;
        let batch = RecordBatch::try_new(schema, vec![data, tombstones]).expect("batch");
        let commit = commit_array(3, batch.num_rows());
        let tombstones = tombstone_array(&[false]);
        let err = append_mvcc_columns(&batch, &commit, &tombstones).expect_err("existing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("already contains _tombstone")));
    }

    #[test]
    fn split_tombstone_requires_column() {
        let batch = sample_batch();
        let err = split_mvcc_columns(batch).expect_err("missing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("_commit_ts column missing")));
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
    M::Key: Eq + Hash + Clone,
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
