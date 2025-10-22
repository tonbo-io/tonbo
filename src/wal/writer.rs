//! Asynchronous WAL writer task and queue plumbing.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use fusio::{
    Write,
    dynamic::fs::DynFile,
    error::Error as FusioError,
    executor::{Executor, RwLock, Timer},
};
use futures::{
    StreamExt,
    channel::{mpsc, oneshot},
};

use crate::wal::{
    WalAck, WalConfig, WalError, WalPayload, WalResult, WalSyncPolicy,
    frame::{self, Frame, encode_payload},
    metrics::WalMetrics,
    storage::{WalSegment, WalStorage},
};

/// Message dispatched to the writer loop.
pub(crate) enum WriterMsg {
    /// Append a payload to the WAL.
    Enqueue {
        /// Frame sequence number assigned to the first emitted frame.
        seq: u64,
        /// Logical payload to encode.
        payload: WalPayload,
        /// Instant at which the payload was enqueued (used for latency metrics).
        enqueued_at: Instant,
        /// Sender to resolve once durability is satisfied.
        ack_tx: oneshot::Sender<WalResult<WalAck>>,
    },
}

impl WriterMsg {
    #[cfg(test)]
    fn queued(
        seq: u64,
        payload: WalPayload,
        enqueued_at: Instant,
        ack_tx: oneshot::Sender<WalResult<WalAck>>,
    ) -> Self {
        Self::Enqueue {
            seq,
            payload,
            enqueued_at,
            ack_tx,
        }
    }
}

/// Handle returned by [`spawn_writer`] to allow enqueueing work and joining the task.
pub(crate) struct WriterHandle<E>
where
    E: Executor + Timer,
{
    /// Bounded sender feeding the writer task.
    pub(crate) sender: mpsc::Sender<WriterMsg>,
    /// Shared counter tracking the approximate queue depth.
    pub(crate) queue_depth: Arc<AtomicUsize>,
    join: E::JoinHandle<WalResult<()>>,
}

impl<E> WriterHandle<E>
where
    E: Executor + Timer,
{
    /// Consume the handle and return its constituent pieces.
    pub(crate) fn into_parts(
        self,
    ) -> (
        mpsc::Sender<WriterMsg>,
        Arc<AtomicUsize>,
        E::JoinHandle<WalResult<()>>,
    ) {
        (self.sender, self.queue_depth, self.join)
    }
}

/// Spawn the asynchronous writer task and return a [`WriterHandle`] for coordination.
pub(crate) fn spawn_writer<E>(
    exec: Arc<E>,
    #[allow(dead_code)]
    #[allow(dead_code)]
    storage: WalStorage,
    cfg: WalConfig,
    metrics: Arc<E::RwLock<WalMetrics>>,
    initial_segment_seq: u64,
    initial_frame_seq: u64,
) -> WriterHandle<E>
where
    E: Executor + Timer,
{
    let (sender, receiver) = mpsc::channel(cfg.queue_size);
    let queue_depth = Arc::new(AtomicUsize::new(0));

    let fut = run_writer_loop::<E>(
        storage,
        cfg,
        metrics,
        receiver,
        Arc::clone(&queue_depth),
        initial_segment_seq,
        initial_frame_seq,
    );

    let join = exec.spawn(fut);

    WriterHandle {
        sender,
        queue_depth,
        join,
    }
}

async fn run_writer_loop<E>(
    storage: WalStorage,
    cfg: WalConfig,
    metrics: Arc<E::RwLock<WalMetrics>>,
    mut receiver: mpsc::Receiver<WriterMsg>,
    queue_depth: Arc<AtomicUsize>,
    initial_segment_seq: u64,
    initial_frame_seq: u64,
) -> WalResult<()>
where
    E: Executor + Timer,
{
    let mut ctx: WriterContext<E> =
        WriterContext::new(storage, cfg, metrics, queue_depth, initial_segment_seq).await?;
    ctx.next_frame_seq = initial_frame_seq.max(frame::INITIAL_FRAME_SEQ);

    while let Some(msg) = receiver.next().await {
        match msg {
            WriterMsg::Enqueue {
                seq,
                payload,
                enqueued_at,
                ack_tx,
            } => {
                ctx.queue_depth.fetch_sub(1, Ordering::SeqCst);
                ctx.update_queue_depth_metric().await;
                match ctx.handle_enqueue(seq, payload, enqueued_at).await {
                    Ok(ack) => {
                        let _ = ack_tx.send(Ok(ack));
                    }
                    Err(err) => {
                        let _ = ack_tx.send(Err(err.clone()));
                        return Err(err);
                    }
                }
            }
        }
    }

    ctx.flush_and_sync_for_shutdown().await?;
    ctx.queue_depth.store(0, Ordering::SeqCst);
    ctx.update_queue_depth_metric().await;

    Ok(())
}

struct WriterContext<E>
where
    E: Executor + Timer,
{
    #[allow(dead_code)]
    storage: WalStorage,
    cfg: WalConfig,
    metrics: Arc<E::RwLock<WalMetrics>>,
    queue_depth: Arc<AtomicUsize>,
    #[allow(dead_code)]
    segment_seq: u64,
    segment: WalSegment,
    segment_bytes: usize,
    bytes_since_sync: usize,
    last_sync: Instant,
    next_frame_seq: u64,
}

impl<E> WriterContext<E>
where
    E: Executor + Timer,
{
    async fn new(
        storage: WalStorage,
        cfg: WalConfig,
        metrics: Arc<E::RwLock<WalMetrics>>,
        queue_depth: Arc<AtomicUsize>,
        segment_seq: u64,
    ) -> WalResult<Self> {
        let mut segment = storage.open_segment(segment_seq).await?;
        let existing_bytes = {
            let file = segment.file_mut();
            file.size()
                .await
                .map_err(|err| backend_err("determine wal segment size", err))?
        } as usize;

        Ok(Self {
            storage,
            cfg,
            metrics,
            queue_depth,
            segment_seq,
            segment,
            segment_bytes: existing_bytes,
            bytes_since_sync: 0,
            last_sync: Instant::now(),
            next_frame_seq: frame::INITIAL_FRAME_SEQ,
        })
    }

    async fn handle_enqueue(
        &mut self,
        seq: u64,
        payload: WalPayload,
        enqueued_at: Instant,
    ) -> WalResult<WalAck> {
        let mut frames = encode_payload(payload, seq)?;
        if frames.is_empty() {
            return Err(WalError::Corrupt("wal payload produced no frames"));
        }

        let mut bytes_written = 0usize;
        for frame in frames.drain(..) {
            let frame_bytes = self.write_frame(frame).await?;
            bytes_written = bytes_written.saturating_add(frame_bytes);
        }

        self.segment_bytes = self.segment_bytes.saturating_add(bytes_written);
        self.bytes_since_sync = self.bytes_since_sync.saturating_add(bytes_written);

        self.flush_if_needed().await?;
        let sync_performed = self.maybe_sync().await?;

        if bytes_written > 0 {
            self.record_bytes_written(bytes_written).await;
        }
        if sync_performed {
            self.record_sync().await;
        }

        let ack = WalAck {
            seq,
            bytes_flushed: bytes_written,
            elapsed: enqueued_at.elapsed(),
        };
        Ok(ack)
    }

    async fn write_frame(&mut self, frame: Frame) -> WalResult<usize> {
        let seq = self.next_frame_seq;
        self.next_frame_seq = self.next_frame_seq.saturating_add(1);
        let buf = frame.into_bytes(seq);
        let len = buf.len();
        let (result, _buf) = self.segment.file_mut().write_all(buf).await;
        result.map_err(|err| backend_err("write wal frame", err))?;
        Ok(len)
    }

    async fn flush_if_needed(&mut self) -> WalResult<()> {
        // For the MVP we flush on every enqueue to keep semantics simple.
        self.segment
            .file_mut()
            .flush()
            .await
            .map_err(|err| backend_err("flush wal segment", err))
    }

    async fn maybe_sync(&mut self) -> WalResult<bool> {
        match self.cfg.sync {
            WalSyncPolicy::Always => {
                self.sync_all().await?;
                self.bytes_since_sync = 0;
                self.last_sync = Instant::now();
                Ok(true)
            }
            WalSyncPolicy::IntervalBytes(threshold) => {
                if self.bytes_since_sync >= threshold {
                    self.sync_data().await?;
                    self.bytes_since_sync = 0;
                    self.last_sync = Instant::now();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            WalSyncPolicy::IntervalTime(interval) => {
                let now = Instant::now();
                if now.duration_since(self.last_sync) >= interval {
                    self.sync_data().await?;
                    self.last_sync = now;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            WalSyncPolicy::Disabled => Ok(false),
        }
    }

    async fn sync_data(&mut self) -> WalResult<()> {
        perform_sync(self.segment.file_mut(), SyncVariant::Data).await
    }

    async fn sync_all(&mut self) -> WalResult<()> {
        perform_sync(self.segment.file_mut(), SyncVariant::All).await
    }

    async fn flush_and_sync_for_shutdown(&mut self) -> WalResult<()> {
        self.flush_if_needed().await?;
        if !matches!(self.cfg.sync, WalSyncPolicy::Disabled) {
            let _ = self.sync_all().await;
        }
        Ok(())
    }

    async fn record_bytes_written(&self, bytes: usize) {
        let mut guard = self.metrics.write().await;
        guard.record_bytes_written(bytes as u64);
    }

    async fn record_sync(&self) {
        let mut guard = self.metrics.write().await;
        guard.record_sync();
    }

    async fn update_queue_depth_metric(&self) {
        let depth = self.queue_depth.load(Ordering::SeqCst);
        let mut guard = self.metrics.write().await;
        guard.record_queue_depth(depth);
    }
}

enum SyncVariant {
    Data,
    All,
}

async fn perform_sync(_file: &mut Box<dyn DynFile>, _variant: SyncVariant) -> WalResult<()> {
    // TODO: Integrate fusio durability hooks once dynamic FileSync is exposed.
    Ok(())
}

fn backend_err(action: &str, err: FusioError) -> WalError {
    WalError::Backend(format!("failed to {action}: {err}"))
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };

    use fusio::{DynFs, executor::BlockingExecutor, impls::mem::fs::InMemoryFs, path::Path};
    use futures::{channel::oneshot, executor::LocalPool, task::LocalSpawnExt};
    use typed_arrow::{
        arrow_array::{ArrayRef, Int64Array, RecordBatch},
        arrow_schema::{DataType, Field, Schema},
    };

    use super::*;
    use crate::wal::{WalPayload, WalResult};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let data: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        RecordBatch::try_new(schema, vec![data]).expect("valid batch")
    }

    #[test]
    fn submit_and_drain_on_shutdown() {
        let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
        let root = Path::parse("wal-test").expect("path");
        let storage = WalStorage::new(fs, root);

        let cfg = WalConfig {
            queue_size: 4,
            sync: WalSyncPolicy::Always,
            ..WalConfig::default()
        };

        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));
        let metrics_reader = Arc::clone(&metrics);

        let (mut sender, receiver) = mpsc::channel(cfg.queue_size);
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let queue_depth_writer = Arc::clone(&queue_depth);

        let result_cell: Rc<RefCell<Option<WalResult<()>>>> = Rc::new(RefCell::new(None));
        let result_cell_clone = Rc::clone(&result_cell);

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        spawner
            .spawn_local(async move {
                let result = run_writer_loop::<BlockingExecutor>(
                    storage,
                    cfg,
                    Arc::clone(&metrics),
                    receiver,
                    queue_depth_writer,
                    0,
                    frame::INITIAL_FRAME_SEQ,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn");

        let payload = WalPayload::DynBatch {
            batch: sample_batch(),
            commit_ts: 42,
        };

        let seq = frame::INITIAL_FRAME_SEQ;
        let (ack_tx, ack_rx) = oneshot::channel();
        queue_depth.fetch_add(1, Ordering::SeqCst);
        sender
            .try_send(WriterMsg::queued(seq, payload, Instant::now(), ack_tx))
            .expect("send");

        let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack_cell_clone = Rc::clone(&ack_cell);
        spawner
            .spawn_local(async move {
                let ack = ack_rx.await.expect("oneshot");
                *ack_cell_clone.borrow_mut() = Some(ack);
            })
            .expect("spawn ack");

        sender.close_channel();
        pool.run();

        let ack = ack_cell
            .borrow()
            .clone()
            .expect("ack result")
            .expect("ack ok");
        assert_eq!(ack.seq, seq);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let metrics_guard = futures::executor::block_on(metrics_reader.read());
        assert_eq!(metrics_guard.queue_depth, 0);
        assert!(metrics_guard.bytes_written > 0);
    }

    #[test]
    fn queue_backpressure_and_metrics() {
        let cfg = WalConfig {
            queue_size: 1,
            sync: WalSyncPolicy::Always,
            ..WalConfig::default()
        };

        let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
        let root = Path::parse("wal-backpressure").expect("path");
        let storage = WalStorage::new(fs, root);
        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));
        let metrics_reader = Arc::clone(&metrics);

        let (mut sender, receiver) = mpsc::channel(cfg.queue_size);
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let queue_depth_writer = Arc::clone(&queue_depth);

        let result_cell: Rc<RefCell<Option<WalResult<()>>>> = Rc::new(RefCell::new(None));
        let result_cell_clone = Rc::clone(&result_cell);

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        spawner
            .spawn_local(async move {
                let result = run_writer_loop::<BlockingExecutor>(
                    storage,
                    cfg,
                    Arc::clone(&metrics),
                    receiver,
                    queue_depth_writer,
                    0,
                    frame::INITIAL_FRAME_SEQ,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let seq1 = frame::INITIAL_FRAME_SEQ;
        let (ack1_tx, ack1_rx) = oneshot::channel();
        queue_depth.fetch_add(1, Ordering::SeqCst);
        sender
            .try_send(WriterMsg::queued(
                seq1,
                WalPayload::DynBatch {
                    batch: sample_batch(),
                    commit_ts: 1,
                },
                Instant::now(),
                ack1_tx,
            ))
            .expect("first send");

        let ack1_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack1_cell_clone = Rc::clone(&ack1_cell);
        spawner
            .spawn_local(async move {
                let ack = ack1_rx.await.expect("oneshot1");
                *ack1_cell_clone.borrow_mut() = Some(ack);
            })
            .expect("spawn ack1");

        pool.run_until_stalled();

        let ack1 = ack1_cell
            .borrow()
            .clone()
            .expect("ack1 result")
            .expect("ack1 ok");
        assert_eq!(ack1.seq, seq1);
        assert_eq!(queue_depth.load(Ordering::SeqCst), 0);

        queue_depth.fetch_add(1, Ordering::SeqCst);
        let (ack2_tx, ack2_rx) = oneshot::channel();
        sender
            .try_send(WriterMsg::queued(
                seq1 + 1,
                WalPayload::DynBatch {
                    batch: sample_batch(),
                    commit_ts: 2,
                },
                Instant::now(),
                ack2_tx,
            ))
            .expect("second send");

        let ack2_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack2_cell_clone = Rc::clone(&ack2_cell);
        spawner
            .spawn_local(async move {
                let ack = ack2_rx.await.expect("oneshot2");
                *ack2_cell_clone.borrow_mut() = Some(ack);
            })
            .expect("spawn ack2");

        sender.close_channel();
        pool.run();

        let ack2 = ack2_cell
            .borrow()
            .clone()
            .expect("ack2 result")
            .expect("ack2 ok");
        assert_eq!(ack2.seq, seq1 + 1);

        let metrics_guard = futures::executor::block_on(metrics_reader.read());
        assert_eq!(metrics_guard.queue_depth, 0);
        assert!(metrics_guard.bytes_written > 0);
        assert_eq!(metrics_guard.sync_operations, 2);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());
    }
}
