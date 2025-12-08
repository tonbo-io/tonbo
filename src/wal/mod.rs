//! Asynchronous write-ahead log framework scaffolding.
//!
//! This module exposes the public API surface described in RFC 0002 and acts
//! as the coordination layer between ingest, the frame encoder, storage
//! backends, and recovery. Implementations will live in the sibling modules.

use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use fusio::{
    DynFs,
    disk::LocalFs,
    executor::{Executor, Instant, JoinHandle, Timer},
    path::Path,
};
use futures::{
    SinkExt,
    channel::{mpsc, oneshot},
};
use ulid::Ulid;

use crate::{
    id::FileId,
    inmem::immutable::memtable::{MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL},
    mvcc::Timestamp,
    wal::metrics::WalMetrics,
};

pub mod frame;
pub mod manifest_ext;
pub mod metrics;
pub mod replay;
pub mod state;
pub mod storage;
pub mod writer;
pub use state::WalSegmentBounds;
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
    /// Backend used to create and append WAL segments.
    pub segment_backend: Arc<dyn DynFs>,
    /// Optional store used for `state.json` persistence.
    pub state_store: Option<Arc<dyn state::WalStateStore>>,
    /// When enabled, WAL pruning only reports planned deletions without removing objects.
    pub prune_dry_run: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        let dir = Path::parse("wal").expect("static wal path");
        let filesystem = Arc::new(LocalFs {});
        Self {
            dir,
            segment_max_bytes: 64 * 1024 * 1024,
            segment_max_age: None,
            flush_interval: Duration::from_millis(10),
            sync: WalSyncPolicy::default(),
            recovery: WalRecoveryMode::default(),
            retention_bytes: None,
            queue_size: 65_536,
            segment_backend: filesystem,
            state_store: None,
            prune_dry_run: false,
        }
    }
}

impl fmt::Debug for WalConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fs_tag = self.segment_backend.file_system();
        f.debug_struct("WalConfig")
            .field("dir", &self.dir)
            .field("segment_max_bytes", &self.segment_max_bytes)
            .field("segment_max_age", &self.segment_max_age)
            .field("flush_interval", &self.flush_interval)
            .field("sync", &self.sync)
            .field("recovery", &self.recovery)
            .field("retention_bytes", &self.retention_bytes)
            .field("queue_size", &self.queue_size)
            .field("segment_backend", &fs_tag)
            .field("state_store_present", &self.state_store.is_some())
            .field("prune_dry_run", &self.prune_dry_run)
            .finish()
    }
}

impl WalConfig {
    /// Replace the store used for `state.json` bookkeeping.
    #[must_use]
    pub fn with_state_store(mut self, store: Arc<dyn state::WalStateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Disable `state.json` persistence (primarily for tests).
    #[must_use]
    pub fn without_state_store(mut self) -> Self {
        self.state_store = None;
        self
    }

    /// Enable or disable WAL prune dry-run mode.
    #[must_use]
    pub fn with_prune_dry_run(mut self, dry_run: bool) -> Self {
        self.prune_dry_run = dry_run;
        self
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
    /// State file persistence failed.
    #[error("wal state error: {0}")]
    State(String),
    /// Module has not been fully implemented yet.
    #[error("wal feature is not implemented: {0}")]
    Unimplemented(&'static str),
}

/// Acks emitted once a WAL submission satisfies the configured durability policy.
#[derive(Debug, Clone)]
pub struct WalAck {
    /// Sequence number of the first frame written for the command.
    pub first_seq: u64,
    /// Sequence number of the final frame written for the command.
    pub last_seq: u64,
    /// Bytes flushed while satisfying the policy.
    pub bytes_flushed: usize,
    /// Time elapsed between enqueue and durability.
    pub elapsed: Duration,
}

/// Snapshot describing WAL segment metadata without touching the filesystem.
#[derive(Debug, Clone)]
pub struct WalSnapshot {
    /// Completed segments ordered from oldest to newest.
    pub sealed_segments: Vec<WalSegmentBounds>,
    /// Active segment metadata when the current file already contains frames.
    pub active_segment: Option<WalSegmentBounds>,
}

impl WalSnapshot {
    /// Iterate over all segments (sealed + active when present) in order.
    pub fn iter(&self) -> impl Iterator<Item = &WalSegmentBounds> {
        self.sealed_segments
            .iter()
            .chain(self.active_segment.iter())
    }
}

/// Dynamic ingest payload handed to WAL commands.
#[derive(Debug, Clone)]
pub enum DynBatchPayload {
    /// Full row payload with a uniform `_commit_ts` column (no tombstones).
    Row {
        /// Record batch supplied by the caller (without MVCC columns).
        batch: RecordBatch,
        /// Arrow column containing the `_commit_ts` values for the batch.
        commit_ts_column: ArrayRef,
    },
    /// Key-only delete payload encoded with the delete schema.
    Delete {
        /// Record batch carrying key columns + `_commit_ts`.
        batch: RecordBatch,
    },
}

impl DynBatchPayload {
    /// Number of rows carried by the batch.
    pub fn num_rows(&self) -> usize {
        match self {
            DynBatchPayload::Row { batch, .. } | DynBatchPayload::Delete { batch } => {
                batch.num_rows()
            }
        }
    }
}

/// Logical commands accepted by the WAL writer queue.
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum WalCommand {
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

/// Append the `_commit_ts` column, returning a new batch for WAL storage.
pub(crate) fn append_commit_column(
    batch: &RecordBatch,
    commit_ts: &ArrayRef,
) -> WalResult<RecordBatch> {
    let rows = batch.num_rows();
    let schema = batch.schema();

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

    if schema.fields().iter().any(|f| f.name() == MVCC_COMMIT_COL) {
        return Err(WalError::Codec(
            "record batch already contains _commit_ts column".to_string(),
        ));
    }

    if schema
        .fields()
        .iter()
        .any(|f| f.name() == MVCC_TOMBSTONE_COL)
    {
        return Err(WalError::Codec(
            "record batch already contains _tombstone column".to_string(),
        ));
    }

    let mut fields = schema.fields().to_vec();
    fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false).into());

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::clone(commit_ts));

    let metadata = schema.metadata().clone();
    let updated_schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    RecordBatch::try_new(updated_schema, columns).map_err(|err| WalError::Codec(err.to_string()))
}

/// Remove the `_commit_ts` column, returning the stripped batch and commit metadata.
pub(crate) fn split_commit_column(
    batch: RecordBatch,
) -> WalResult<(RecordBatch, Option<Timestamp>, ArrayRef)> {
    let schema = batch.schema();
    let commit_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == MVCC_COMMIT_COL)
        .ok_or_else(|| WalError::Codec("_commit_ts column missing".to_string()))?;

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

    let stripped_fields = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != commit_idx)
        .map(|(_, field)| field.clone())
        .collect::<Vec<_>>();
    let stripped_columns = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != commit_idx)
        .map(|(_, column)| column.clone())
        .collect::<Vec<_>>();

    let metadata = schema.metadata().clone();
    let stripped_schema = Arc::new(Schema::new_with_metadata(stripped_fields, metadata));
    let stripped = RecordBatch::try_new(stripped_schema, stripped_columns)
        .map_err(|err| WalError::Codec(err.to_string()))?;

    Ok((stripped, commit_ts, Arc::clone(commit_array)))
}

struct WalHandleInner<E>
where
    E: Executor + Timer + Clone,
{
    sender: mpsc::Sender<writer::WriterMsg>,
    queue_depth: Arc<AtomicUsize>, // current queue occupancy
    next_payload_seq: AtomicU64,   // monotonic sequence handed to payloads and commands
    join: Mutex<Option<E::JoinHandle<WalResult<()>>>>,
    metrics: Arc<E::RwLock<WalMetrics>>,
}

impl<E> WalHandleInner<E>
where
    E: Executor + Timer + Clone,
{
    fn new(
        sender: mpsc::Sender<writer::WriterMsg>,
        queue_depth: Arc<AtomicUsize>,
        start_seq: u64,
        join: E::JoinHandle<WalResult<()>>,
        metrics: Arc<E::RwLock<WalMetrics>>,
    ) -> Self {
        Self {
            sender,
            queue_depth,
            next_payload_seq: AtomicU64::new(start_seq),
            join: Mutex::new(Some(join)),
            metrics,
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

    fn metrics(&self) -> Arc<E::RwLock<WalMetrics>> {
        Arc::clone(&self.metrics)
    }
}

/// Handle returned when enabling the WAL on a `DB`.
pub struct WalHandle<E>
where
    E: Executor + Timer + Clone,
{
    inner: Arc<WalHandleInner<E>>,
}

impl<E> Clone for WalHandle<E>
where
    E: Executor + Timer + Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E> fmt::Debug for WalHandle<E>
where
    E: Executor + Timer + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalHandle").finish()
    }
}

impl<E> WalHandle<E>
where
    E: Executor + Timer + Clone,
{
    fn from_parts(
        sender: mpsc::Sender<writer::WriterMsg>,
        queue_depth: Arc<AtomicUsize>,
        join: E::JoinHandle<WalResult<()>>,
        start_seq: u64,
        metrics: Arc<E::RwLock<WalMetrics>>,
    ) -> Self {
        Self {
            inner: Arc::new(WalHandleInner::new(
                sender,
                queue_depth,
                start_seq,
                join,
                metrics,
            )),
        }
    }

    /// Allocate the next provisional identifier suitable for WAL commands.
    pub fn next_provisional_id(&self) -> u64 {
        self.inner.next_payload_seq()
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

    /// Fetch a snapshot describing the WAL segments tracked by the writer.
    pub async fn snapshot(&self) -> WalResult<WalSnapshot> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let mut sender = self.inner.clone_sender();
        let msg = writer::WriterMsg::Snapshot { ack_tx };

        if let Err(_err) = sender.send(msg).await {
            return Err(WalError::Storage("wal writer task closed".into()));
        }

        ack_rx
            .await
            .map_err(|_| WalError::Storage("wal writer task closed".into()))?
    }

    /// Shut down the writer task and wait for completion.
    pub async fn shutdown(self) -> WalResult<()> {
        self.inner.close_channel();
        if let Some(join) = self.inner.take_join() {
            match join.join().await {
                Ok(join_result) => join_result.map_err(|err| WalError::Storage(err.to_string()))?,
                Err(err) => {
                    #[cfg(target_arch = "wasm32")]
                    {
                        // WebExecutor join handles are intentionally unjoinable on wasm; treat this
                        // as a best-effort shutdown so disable_wal_async() does not fail
                        // spuriously.
                        let _ = err;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        return Err(WalError::Storage(err.to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    /// Access the metrics collected by the WAL writer.
    pub fn metrics(&self) -> Arc<E::RwLock<WalMetrics>> {
        self.inner.metrics()
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn test_from_parts(
        sender: mpsc::Sender<writer::WriterMsg>,
        queue_depth: Arc<AtomicUsize>,
        join: E::JoinHandle<WalResult<()>>,
        start_seq: u64,
        metrics: Arc<E::RwLock<WalMetrics>>,
    ) -> Self {
        Self::from_parts(sender, queue_depth, join, start_seq, metrics)
    }

    /// Log a transaction begin marker.
    pub async fn txn_begin(&self, provisional_id: u64) -> WalResult<WalTicket<E>> {
        self.enqueue_command(WalCommand::TxnBegin { provisional_id }, provisional_id)
            .await
    }

    /// Log a transaction append frame carrying live rows only.
    pub(crate) async fn txn_append(
        &self,
        provisional_id: u64,
        batch: &RecordBatch,
        commit_ts: Timestamp,
    ) -> WalResult<WalTicket<E>> {
        let commit_values =
            Arc::new(UInt64Array::from(vec![commit_ts.get(); batch.num_rows()])) as ArrayRef;
        let payload = DynBatchPayload::Row {
            batch: batch.clone(),
            commit_ts_column: Arc::clone(&commit_values),
        };
        self.txn_append_payload(provisional_id, payload).await
    }

    /// Log a key-only delete frame.
    pub async fn txn_append_delete(
        &self,
        provisional_id: u64,
        batch: RecordBatch,
    ) -> WalResult<WalTicket<E>> {
        let payload = DynBatchPayload::Delete { batch };
        self.txn_append_payload(provisional_id, payload).await
    }

    /// Log a transaction commit marker.
    pub(crate) async fn txn_commit(
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

    async fn txn_append_payload(
        &self,
        provisional_id: u64,
        payload: DynBatchPayload,
    ) -> WalResult<WalTicket<E>> {
        let command = WalCommand::TxnAppend {
            provisional_id,
            payload,
        };
        self.enqueue_command(command, provisional_id).await
    }
}

pub(crate) fn wal_segment_file_id(seq: u64) -> FileId {
    let mut bytes = [0u8; 16];
    bytes[8..16].copy_from_slice(&seq.to_be_bytes());
    Ulid::from_bytes(bytes)
}

/// Ticket returned after a successful submission.
#[must_use = "propagate WAL spans into GC pinning before dropping"]
pub struct WalTicket<E> {
    /// Sequence number assigned to the payload.
    pub seq: u64,
    receiver: oneshot::Receiver<WalResult<WalAck>>,
    _exec: PhantomData<E>,
}

impl<E> WalTicket<E>
where
    E: Executor + Timer + Clone,
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
    E: Executor + Timer + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalTicket").field("seq", &self.seq).finish()
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use fusio::executor::tokio::TokioExecutor;
    use futures::StreamExt;

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let data = Arc::new(Int32Array::from(vec![1, 2])) as _;
        RecordBatch::try_new(schema, vec![data]).expect("batch")
    }

    fn commit_array(ts: u64, len: usize) -> ArrayRef {
        Arc::new(UInt64Array::from(vec![ts; len])) as ArrayRef
    }

    #[test]
    fn append_mvcc_rejects_length_mismatch() {
        let batch = sample_batch();
        let commit = commit_array(1, batch.num_rows() - 1);
        let err = append_commit_column(&batch, &commit).expect_err("length mismatch");
        assert!(
            matches!(err, WalError::Codec(msg) if msg.contains("commit_ts column length mismatch"))
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
        let err = append_commit_column(&batch, &commit).expect_err("existing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("already contains _commit_ts")));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false),
        ]));
        let data = Arc::new(Int32Array::from(vec![1])) as _;
        let tombstones = Arc::new(BooleanArray::from(vec![false])) as _;
        let batch = RecordBatch::try_new(schema, vec![data, tombstones]).expect("batch");
        let commit = commit_array(3, batch.num_rows());
        let err = append_commit_column(&batch, &commit).expect_err("existing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("already contains _tombstone")));
    }

    #[test]
    fn split_commit_requires_column() {
        let batch = sample_batch();
        let err = split_commit_column(batch).expect_err("missing column");
        assert!(matches!(err, WalError::Codec(msg) if msg.contains("_commit_ts column missing")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_returns_both_acks_in_order() {
        use crate::wal::{frame, writer};

        let executor = Arc::new(TokioExecutor::default());
        let (sender, mut receiver) = mpsc::channel(4);
        let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let join: tokio::task::JoinHandle<WalResult<()>> = executor.spawn(async move {
            let mut next_seq = frame::INITIAL_FRAME_SEQ;
            while let Some(msg) = receiver.next().await {
                match msg {
                    writer::WriterMsg::Enqueue {
                        command, ack_tx, ..
                    } => {
                        let (first_seq, last_seq, advance) = match command {
                            WalCommand::TxnAppend { .. } => {
                                (next_seq, next_seq.saturating_add(1), 2)
                            }
                            WalCommand::TxnCommit { .. } => (next_seq, next_seq, 1),
                            _ => (next_seq, next_seq, 1),
                        };
                        next_seq = next_seq.saturating_add(advance);
                        let ack = WalAck {
                            first_seq,
                            last_seq,
                            bytes_flushed: 0,
                            elapsed: Duration::from_millis(0),
                        };
                        let _ = ack_tx.send(Ok(ack));
                    }
                    writer::WriterMsg::Rotate { ack_tx } => {
                        let _ = ack_tx.send(Ok(()));
                    }
                    writer::WriterMsg::Snapshot { ack_tx } => {
                        let snapshot = WalSnapshot {
                            sealed_segments: Vec::new(),
                            active_segment: None,
                        };
                        let _ = ack_tx.send(Ok(snapshot));
                    }
                }
            }

            #[cfg(all(test, target_arch = "wasm32", feature = "web"))]
            mod wasm_tests {
                use std::sync::{Arc, atomic::AtomicUsize};

                use fusio::executor::web::WebExecutor;
                use wasm_bindgen_test::*;

                use super::*;

                wasm_bindgen_test_configure!(run_in_browser);

                #[wasm_bindgen_test]
                async fn shutdown_treats_unjoinable_handles_as_ok() {
                    let exec = WebExecutor::new();
                    let (tx, _rx) = futures::channel::mpsc::channel(1);
                    let queue_depth = Arc::new(AtomicUsize::new(0));
                    let join = exec.spawn(async { Ok(()) });
                    let metrics = exec.rw_lock(WalMetrics::default());
                    let inner = WalHandleInner::new(
                        tx,
                        queue_depth,
                        frame::INITIAL_FRAME_SEQ,
                        join,
                        metrics,
                    );
                    let handle = WalHandle {
                        inner: Arc::new(inner),
                    };

                    handle
                        .shutdown()
                        .await
                        .expect("shutdown should not fail on unjoinable wasm handles");
                }
            }
            Ok(())
        });

        let metrics = Arc::new(TokioExecutor::rw_lock(WalMetrics::default()));
        let handle: WalHandle<TokioExecutor> = WalHandle::test_from_parts(
            sender,
            queue_depth,
            join,
            frame::INITIAL_FRAME_SEQ,
            metrics,
        );

        let batch = sample_batch();
        let provisional_id = handle.next_provisional_id();
        let commit_ts = Timestamp::new(1);

        let append_ack = handle
            .txn_append(provisional_id, &batch, commit_ts)
            .await
            .expect("append ticket")
            .durable()
            .await
            .expect("append ack");

        let commit_ack = handle
            .txn_commit(provisional_id, commit_ts)
            .await
            .expect("commit ticket")
            .durable()
            .await
            .expect("commit ack");

        assert!(append_ack.first_seq <= append_ack.last_seq);
        assert!(commit_ack.first_seq <= commit_ack.last_seq);
        assert!(append_ack.last_seq < commit_ack.first_seq);
        assert_eq!(append_ack.first_seq, frame::INITIAL_FRAME_SEQ);
        assert_eq!(append_ack.last_seq, frame::INITIAL_FRAME_SEQ + 1);
        assert_eq!(commit_ack.first_seq, frame::INITIAL_FRAME_SEQ + 2);
        assert_eq!(commit_ack.last_seq, frame::INITIAL_FRAME_SEQ + 2);
    }
}

/// Extension methods on the `DB` to enable/disable the WAL.
pub trait WalExt<FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Enable the WAL with the provided configuration and executor.
    fn enable_wal(&mut self, _cfg: WalConfig)
    -> impl Future<Output = WalResult<WalHandle<E>>> + '_;

    /// Disable the WAL and wait for outstanding work to drain.
    fn disable_wal(&mut self) -> impl Future<Output = WalResult<()>> + '_;

    /// Access the active WAL handle, if any.
    fn wal(&self) -> Option<&WalHandle<E>>;
}

impl<FS, E> WalExt<FS, E> for crate::db::DbInner<FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    async fn enable_wal(&mut self, cfg: WalConfig) -> WalResult<WalHandle<E>> {
        let storage = storage::WalStorage::new(Arc::clone(&cfg.segment_backend), cfg.dir.clone());
        let wal_state_handle = storage.load_state_handle(cfg.state_store.as_ref()).await?;
        let wal_state_hint = wal_state_handle
            .as_ref()
            .and_then(|handle| handle.state().last_segment_seq);

        let tail_metadata = storage.tail_metadata_with_hint(wal_state_hint).await?;
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
        let handle = WalHandle::from_parts(
            sender,
            queue_depth,
            join,
            next_payload_seq,
            Arc::clone(&metrics),
        );

        self.set_wal_config(Some(cfg.clone()));
        self.set_wal_handle(Some(handle.clone()));
        Ok(handle)
    }

    fn disable_wal(&mut self) -> impl Future<Output = WalResult<()>> + '_ {
        self.disable_wal_async()
    }

    fn wal(&self) -> Option<&WalHandle<E>> {
        self.wal_handle()
    }
}
