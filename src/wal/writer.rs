//! Asynchronous WAL writer task and queue plumbing.

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use fusio::{
    Write,
    dynamic::MaybeSendFuture,
    error::Error as FusioError,
    executor::{Executor, RwLock, Timer},
    fs::FileSystemTag,
    path::path_to_local,
};
use futures::{
    StreamExt,
    channel::{mpsc, oneshot},
    future::{Fuse, FutureExt},
};

use crate::{
    mvcc::Timestamp,
    wal::{
        WalAck, WalCommand, WalConfig, WalError, WalResult, WalSnapshot, WalSyncPolicy,
        frame::{self, Frame, encode_command},
        metrics::WalMetrics,
        state::{WalSegmentBounds, WalStateHandle},
        storage::{SegmentDescriptor, TailMetadata, WalSegment, WalStorage},
        wal_segment_file_id,
    },
};

// Wrapper around executor-specific sleep futures so we can store them in the timer slot.
type SleepFuture = Pin<Box<dyn MaybeSendFuture<Output = ()>>>;
// Optional holder for the active sleep future (fused so we can poll it safely multiple times).
type SleepSlot = Option<Fuse<SleepFuture>>;

/// Message dispatched to the writer loop.
pub(crate) enum WriterMsg {
    /// Append a command to the WAL.
    ///
    /// We model messages as an enum even though only `Enqueue` exists today so
    /// future command variants (e.g. rotation, flush) can ride the same queue
    /// without changing its type.
    Enqueue {
        /// Logical sequence assigned by the submitter (embedded in frame payloads).
        _submission_seq: u64,
        /// Logical command to encode.
        command: WalCommand,
        /// Instant at which the payload was enqueued (used for latency metrics).
        enqueued_at: Instant,
        /// Sender to resolve once durability is satisfied.
        ack_tx: oneshot::Sender<WalResult<WalAck>>,
    },
    /// Force rotation of the active WAL segment.
    Rotate {
        /// Sender to notify once the rotation completes (or fails).
        ack_tx: oneshot::Sender<WalResult<()>>,
    },
    /// Return a snapshot describing WAL metadata without touching storage.
    Snapshot {
        /// Sender to deliver the snapshot to.
        ack_tx: oneshot::Sender<WalResult<WalSnapshot>>,
    },
}

impl WriterMsg {
    #[cfg(test)]
    fn queued(
        _submission_seq: u64,
        command: WalCommand,
        enqueued_at: Instant,
        ack_tx: oneshot::Sender<WalResult<WalAck>>,
    ) -> Self {
        Self::Enqueue {
            _submission_seq,
            command,
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
    pub(crate) queue_depth: Arc<AtomicUsize>, // current queue occupancy
    join: E::JoinHandle<WalResult<()>>,
}

#[allow(unused)]
struct WriterLoopStart {
    segment_seq: u64,
    frame_seq: u64,
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
        WriterLoopInit {
            exec: Arc::clone(&exec),
            storage,
            cfg,
            metrics,
            queue_depth: Arc::clone(&queue_depth),
            initial_segment_seq,
            initial_frame_seq,
        },
        receiver,
    );

    let join = exec.spawn(fut);

    WriterHandle {
        sender,
        queue_depth,
        join,
    }
}

struct WriterLoopInit<E: Executor> {
    exec: Arc<E>,
    storage: WalStorage,
    cfg: WalConfig,
    metrics: Arc<E::RwLock<WalMetrics>>,
    queue_depth: Arc<AtomicUsize>,
    initial_segment_seq: u64,
    initial_frame_seq: u64,
}

async fn run_writer_loop<E>(
    init: WriterLoopInit<E>,
    mut receiver: mpsc::Receiver<WriterMsg>,
) -> WalResult<()>
where
    E: Executor + Timer,
{
    let WriterLoopInit {
        exec,
        storage,
        cfg,
        metrics,
        queue_depth,
        initial_segment_seq,
        initial_frame_seq,
    } = init;
    let mut ctx: WriterContext<E> = WriterContext::new(
        exec,
        storage,
        cfg,
        metrics,
        queue_depth,
        initial_segment_seq,
    )
    .await?;
    ctx.next_frame_seq = ctx
        .next_frame_seq
        .max(initial_frame_seq.max(frame::INITIAL_FRAME_SEQ));

    let mut timer: SleepSlot = None;
    ctx.recompute_timer(&mut timer);

    loop {
        if let Some(mut timer_future) = timer.as_mut() {
            futures::select_biased! {
                _ = timer_future => {
                    timer = None;
                    match ctx.handle_timer_elapsed().await {
                        Ok(TimerTickOutcome { sync_performed }) => {
                            if sync_performed {
                                ctx.record_sync().await;
                            }
                            ctx.recompute_timer(&mut timer);
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }
                msg = receiver.next() => {
                    match msg {
                        Some(WriterMsg::Enqueue { _submission_seq: _, command, enqueued_at, ack_tx }) => {
                            ctx.queue_depth.fetch_sub(1, Ordering::SeqCst);
                            ctx.update_queue_depth_metric().await;
                            match ctx.handle_enqueue(command, enqueued_at).await {
                                Ok(HandleOutcome { ack, sync_performed, timer_directive }) => {
                                    if sync_performed {
                                        ctx.record_sync().await;
                                    }
                                    ctx.apply_timer_directive(timer_directive, &mut timer);
                                    let _ = ack_tx.send(Ok(ack));
                                }
                                Err(err) => {
                                    ctx.apply_timer_directive(TimerDirective::Cancel, &mut timer);
                                    let _ = ack_tx.send(Err(err.clone()));
                                    return Err(err);
                                }
                            }
                        }
                        Some(WriterMsg::Rotate { ack_tx }) => {
                            match ctx.handle_rotation_request().await {
                                Ok(rotation) => {
                                    if rotation.sync_performed {
                                        ctx.record_sync().await;
                                    }
                                    if rotation.performed {
                                        ctx.apply_timer_directive(TimerDirective::Cancel, &mut timer);
                                    } else {
                                        ctx.recompute_timer(&mut timer);
                                    }
                                    let _ = ack_tx.send(Ok(()));
                                }
                                Err(err) => {
                                    ctx.apply_timer_directive(TimerDirective::Cancel, &mut timer);
                                    let _ = ack_tx.send(Err(err.clone()));
                                    return Err(err);
                                }
                            }
                        }
                        Some(WriterMsg::Snapshot { ack_tx }) => {
                            let snapshot = ctx.snapshot();
                            let _ = ack_tx.send(Ok(snapshot));
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        } else {
            match receiver.next().await {
                Some(WriterMsg::Enqueue {
                    _submission_seq: _,
                    command,
                    enqueued_at,
                    ack_tx,
                }) => {
                    ctx.queue_depth.fetch_sub(1, Ordering::SeqCst);
                    ctx.update_queue_depth_metric().await;
                    match ctx.handle_enqueue(command, enqueued_at).await {
                        Ok(HandleOutcome {
                            ack,
                            sync_performed,
                            timer_directive,
                        }) => {
                            if sync_performed {
                                ctx.record_sync().await;
                            }
                            ctx.apply_timer_directive(timer_directive, &mut timer);
                            let _ = ack_tx.send(Ok(ack));
                        }
                        Err(err) => {
                            let _ = ack_tx.send(Err(err.clone()));
                            return Err(err);
                        }
                    }
                }
                Some(WriterMsg::Rotate { ack_tx }) => match ctx.handle_rotation_request().await {
                    Ok(rotation) => {
                        if rotation.sync_performed {
                            ctx.record_sync().await;
                        }
                        if rotation.performed {
                            ctx.apply_timer_directive(TimerDirective::Cancel, &mut timer);
                        } else {
                            ctx.recompute_timer(&mut timer);
                        }
                        let _ = ack_tx.send(Ok(()));
                    }
                    Err(err) => {
                        let _ = ack_tx.send(Err(err.clone()));
                        return Err(err);
                    }
                },
                Some(WriterMsg::Snapshot { ack_tx }) => {
                    let snapshot = ctx.snapshot();
                    let _ = ack_tx.send(Ok(snapshot));
                }
                None => break,
            }
        }
    }

    let shutdown_synced = ctx.flush_and_sync_for_shutdown().await?;
    if shutdown_synced {
        ctx.record_sync().await;
    }
    ctx.queue_depth.store(0, Ordering::SeqCst);
    ctx.update_queue_depth_metric().await;

    Ok(())
}

struct WriterContext<E>
where
    E: Executor + Timer,
{
    exec: Arc<E>,
    storage: WalStorage,
    cfg: WalConfig,
    fs_tag: FileSystemTag,
    metrics: Arc<E::RwLock<WalMetrics>>,
    queue_depth: Arc<AtomicUsize>,
    segment_seq: u64,
    segment: WalSegment,
    segment_bytes: usize,
    segment_opened_at: Instant,
    rotation_deadline: Option<Instant>,
    next_sync_deadline: Option<Instant>,
    scheduled_deadline: Option<Instant>,
    bytes_since_sync: usize,
    last_sync: Instant,
    last_flush: Instant,
    next_frame_seq: u64,
    next_segment_seq: u64,
    completed_segments: VecDeque<SegmentMeta>,
    active_segment: Option<WalSegmentBounds>,
    state: Option<WalStateHandle>,
    state_dirty: bool,
}

impl<E> WriterContext<E>
where
    E: Executor + Timer,
{
    async fn new(
        exec: Arc<E>,
        storage: WalStorage,
        cfg: WalConfig,
        metrics: Arc<E::RwLock<WalMetrics>>,
        queue_depth: Arc<AtomicUsize>,
        segment_seq: u64,
    ) -> WalResult<Self> {
        let fs_tag = storage.fs().file_system();

        let mut state = storage.load_state_handle(cfg.state_store.as_ref()).await?;

        let tail = storage
            .tail_metadata_with_hint(
                state
                    .as_ref()
                    .and_then(|handle| handle.state().last_segment_seq),
            )
            .await?;

        let mut next_frame_seq = frame::INITIAL_FRAME_SEQ;
        let (segment_seq, mut segment_bytes, mut next_segment_seq) =
            if let Some(ref tail_meta) = tail {
                if let Some(last_seq) = tail_meta.last_frame_seq {
                    next_frame_seq = last_seq.saturating_add(1);
                }
                (
                    tail_meta.active.seq,
                    tail_meta.active.bytes,
                    tail_meta.active.seq.saturating_add(1),
                )
            } else {
                (segment_seq, 0usize, segment_seq.saturating_add(1))
            };

        let mut segment = storage.open_segment(segment_seq).await?;
        if segment_bytes == 0 {
            segment_bytes = self_initial_size(fs_tag, segment.file_mut()).await?;
        }

        if let Some(handle) = state.as_ref() {
            if let Some(seq) = handle.state().last_frame_seq {
                next_frame_seq = next_frame_seq.max(seq.saturating_add(1));
            }
            if let Some(seg_seq) = handle.state().last_segment_seq {
                next_segment_seq = next_segment_seq.max(seg_seq.saturating_add(1));
            }
        }

        let (completed_segments, active_segment, mut state_dirty) =
            Self::rehydrate_segment_metadata(&storage, &tail, state.as_ref()).await?;

        if let Some(handle) = state.as_mut()
            && Self::align_state_with_segments(handle, &completed_segments, active_segment.as_ref())
        {
            state_dirty = true;
        }

        let mut ctx = Self {
            exec,
            storage,
            cfg,
            fs_tag,
            metrics,
            queue_depth,
            segment_seq,
            segment,
            segment_bytes,
            segment_opened_at: Instant::now(),
            rotation_deadline: None,
            next_sync_deadline: None,
            scheduled_deadline: None,
            bytes_since_sync: 0,
            last_sync: Instant::now(),
            last_flush: Instant::now(),
            next_frame_seq,
            next_segment_seq,
            completed_segments,
            active_segment,
            state,
            state_dirty,
        };
        if ctx.segment_bytes > 0
            && let Some(max_age) = ctx.cfg.segment_max_age
        {
            let now = Instant::now();
            ctx.segment_opened_at = now;
            if let Some(deadline) = now.checked_add(max_age) {
                ctx.rotation_deadline = Some(deadline);
            }
        }
        ctx.enforce_retention_limit().await?;
        Ok(ctx)
    }

    async fn handle_enqueue(
        &mut self,
        command: WalCommand,
        enqueued_at: Instant,
    ) -> WalResult<HandleOutcome> {
        let was_empty = self.segment_bytes == 0;
        let commit_hint = match &command {
            WalCommand::TxnCommit { commit_ts, .. } => Some(*commit_ts),
            _ => None,
        };
        let mut frames = encode_command(command)?;
        if frames.is_empty() {
            return Err(WalError::Corrupt("wal payload produced no frames"));
        }

        let mut bytes_written = 0usize;
        let first_frame_seq = self.next_frame_seq;
        for frame in frames.drain(..) {
            let frame_bytes = self.write_frame(frame).await?;
            bytes_written = bytes_written.saturating_add(frame_bytes);
        }

        self.segment_bytes = self.segment_bytes.saturating_add(bytes_written);
        self.bytes_since_sync = self.bytes_since_sync.saturating_add(bytes_written);

        if was_empty
            && self.segment_bytes > 0
            && let Some(max_age) = self.cfg.segment_max_age
        {
            let now = Instant::now();
            self.segment_opened_at = now;
            if let Some(deadline) = now.checked_add(max_age) {
                self.rotation_deadline = Some(deadline);
            } else {
                self.rotation_deadline = Some(Instant::now());
            }
        }

        self.flush_if_needed(false).await?;
        let rotation = self.maybe_rotate().await?;
        let sync_outcome = self.maybe_sync().await?;

        if bytes_written > 0 {
            self.record_bytes_written(bytes_written).await;
        }

        let durable_seq = self.current_frame_seq();
        self.record_frame_progress(durable_seq, commit_hint);
        self.persist_state_if_dirty().await?;

        let ack = WalAck {
            first_seq: first_frame_seq,
            last_seq: durable_seq,
            bytes_flushed: bytes_written,
            elapsed: enqueued_at.elapsed(),
        };
        let sync_performed = rotation.sync_performed || sync_outcome.performed;
        let timer_directive = if rotation.performed {
            TimerDirective::Cancel
        } else {
            sync_outcome.timer_directive
        };
        Ok(HandleOutcome {
            ack,
            sync_performed,
            timer_directive,
        })
    }

    async fn write_frame(&mut self, frame: Frame) -> WalResult<usize> {
        let seq = self.next_frame_seq;
        self.next_frame_seq = self.next_frame_seq.saturating_add(1);
        let buf = frame.into_bytes(seq);
        let len = buf.len();
        let (result, _buf) = self.segment.file_mut().write_all(buf).await;
        result.map_err(|err| backend_err("write wal frame", err))?;
        self.touch_active_segment(seq);
        Ok(len)
    }

    async fn flush_if_needed(&mut self, force: bool) -> WalResult<()> {
        let should_flush = force
            || self.cfg.flush_interval.is_zero()
            || self.last_flush.elapsed() >= self.cfg.flush_interval;
        if should_flush {
            self.segment
                .file_mut()
                .flush()
                .await
                .map_err(|err| backend_err("flush wal segment", err))?;
            self.last_flush = Instant::now();
        }
        Ok(())
    }

    async fn maybe_sync(&mut self) -> WalResult<SyncOutcome> {
        match self.cfg.sync {
            WalSyncPolicy::Always => {
                self.sync_all().await?;
                self.bytes_since_sync = 0;
                self.last_sync = Instant::now();
                Ok(SyncOutcome {
                    performed: true,
                    timer_directive: TimerDirective::Cancel,
                })
            }
            WalSyncPolicy::IntervalBytes(threshold) => {
                if self.bytes_since_sync >= threshold {
                    self.sync_data().await?;
                    self.bytes_since_sync = 0;
                    self.last_sync = Instant::now();
                    Ok(SyncOutcome {
                        performed: true,
                        timer_directive: TimerDirective::Cancel,
                    })
                } else {
                    Ok(SyncOutcome {
                        performed: false,
                        timer_directive: TimerDirective::None,
                    })
                }
            }
            WalSyncPolicy::IntervalTime(interval) => {
                if self.bytes_since_sync > 0 {
                    Ok(SyncOutcome {
                        performed: false,
                        timer_directive: TimerDirective::Schedule(interval),
                    })
                } else {
                    Ok(SyncOutcome {
                        performed: false,
                        timer_directive: TimerDirective::Cancel,
                    })
                }
            }
            WalSyncPolicy::Disabled => Ok(SyncOutcome {
                performed: false,
                timer_directive: TimerDirective::None,
            }),
        }
    }

    async fn maybe_rotate(&mut self) -> WalResult<RotationOutcome> {
        if self.cfg.segment_max_bytes == 0 || self.segment_bytes < self.cfg.segment_max_bytes {
            return Ok(RotationOutcome {
                performed: false,
                sync_performed: false,
            });
        }
        if self.segment_bytes == 0 {
            return Ok(RotationOutcome {
                performed: false,
                sync_performed: false,
            });
        }

        self.rotate_active_segment().await
    }

    async fn rotate_active_segment(&mut self) -> WalResult<RotationOutcome> {
        self.flush_if_needed(true).await?;

        let mut sync_performed = false;
        if !matches!(self.cfg.sync, WalSyncPolicy::Disabled) {
            self.sync_all().await?;
            self.bytes_since_sync = 0;
            self.last_sync = Instant::now();
            sync_performed = true;
        }

        let old_path = self.segment.path().clone();
        let old_bytes = self.segment_bytes;
        let sealed_seq = self.segment_seq;

        let new_seq = self.next_segment_seq;
        let mut new_segment = self.storage.open_segment(new_seq).await?;
        let new_bytes = self_initial_size(self.fs_tag, new_segment.file_mut()).await?;

        self.close_active_segment().await?;
        let old_segment = std::mem::replace(&mut self.segment, new_segment);
        drop(old_segment);

        self.segment_seq = new_seq;
        self.segment_bytes = new_bytes;
        self.next_segment_seq = new_seq.saturating_add(1);
        let now = Instant::now();
        self.last_flush = now;
        self.segment_opened_at = now;
        self.rotation_deadline = None;
        if self.segment_bytes > 0
            && let Some(max_age) = self.cfg.segment_max_age
            && let Some(deadline) = now.checked_add(max_age)
        {
            self.rotation_deadline = Some(deadline);
        }

        let descriptor = SegmentDescriptor {
            seq: sealed_seq,
            path: old_path,
            bytes: old_bytes,
        };
        let bounds = if let Some(bounds) = self.active_segment.take() {
            bounds
        } else {
            Self::load_bounds_from_storage(&self.storage, &descriptor).await?
        };
        self.completed_segments
            .push_back(SegmentMeta::new(descriptor, bounds.clone()));
        self.record_sealed_segment(bounds);
        self.enforce_retention_limit().await?;

        Ok(RotationOutcome {
            performed: true,
            sync_performed,
        })
    }

    async fn handle_rotation_request(&mut self) -> WalResult<RotationOutcome> {
        if self.segment_bytes == 0 {
            return Ok(RotationOutcome {
                performed: false,
                sync_performed: false,
            });
        }

        let outcome = self.rotate_active_segment().await?;
        self.persist_state_if_dirty().await?;
        Ok(outcome)
    }

    fn snapshot(&self) -> WalSnapshot {
        WalSnapshot {
            sealed_segments: self
                .completed_segments
                .iter()
                .map(|meta| meta.bounds.clone())
                .collect(),
            active_segment: self.active_segment.clone(),
        }
    }

    async fn sync_data(&mut self) -> WalResult<()> {
        let path = self.segment.path().clone();
        perform_sync(self.fs_tag, &path, SyncVariant::Data).await
    }

    async fn sync_all(&mut self) -> WalResult<()> {
        let path = self.segment.path().clone();
        perform_sync(self.fs_tag, &path, SyncVariant::All).await
    }

    async fn flush_and_sync_for_shutdown(&mut self) -> WalResult<bool> {
        self.flush_if_needed(true).await?;
        let mut synced = false;
        if !matches!(self.cfg.sync, WalSyncPolicy::Disabled) {
            self.sync_all().await?;
            self.bytes_since_sync = 0;
            self.last_sync = Instant::now();
            synced = true;
        }
        self.close_active_segment().await?;
        self.persist_state_if_dirty().await?;
        Ok(synced)
    }

    async fn record_bytes_written(&self, bytes: usize) {
        let mut guard = self.metrics.write().await;
        guard.record_bytes_written(bytes as u64);
    }

    async fn record_sync(&self) {
        let mut guard = self.metrics.write().await;
        guard.record_sync();
    }

    fn touch_active_segment(&mut self, seq: u64) {
        let bounds = self.active_segment.get_or_insert_with(|| {
            let file_id = wal_segment_file_id(self.segment_seq);
            WalSegmentBounds::new(self.segment_seq, file_id, seq, seq)
        });
        bounds.extend_to(seq);
        if let Some(handle) = self.state.as_mut() {
            handle.state_mut().set_active_segment(bounds.clone());
            self.state_dirty = true;
        }
    }

    async fn update_queue_depth_metric(&self) {
        let depth = self.queue_depth.load(Ordering::SeqCst);
        let mut guard = self.metrics.write().await;
        guard.record_queue_depth(depth);
    }

    fn total_retained_bytes(&self) -> usize {
        let completed: usize = self
            .completed_segments
            .iter()
            .map(|meta| meta.bytes())
            .sum();
        completed.saturating_add(self.segment_bytes)
    }

    fn remove_sealed_segment_metadata(&mut self, seq: u64) {
        if let Some(handle) = self.state.as_mut() {
            let existed = handle
                .state()
                .sealed_segments()
                .iter()
                .any(|segment| segment.seq == seq);
            if existed {
                handle
                    .state_mut()
                    .retain_sealed_segments(|segment| segment.seq != seq);
                self.state_dirty = true;
            }
        }
    }

    async fn load_bounds_from_storage(
        storage: &WalStorage,
        descriptor: &SegmentDescriptor,
    ) -> WalResult<WalSegmentBounds> {
        let Some(bounds) = storage.segment_frame_bounds(&descriptor.path).await? else {
            return Err(WalError::Corrupt(
                "wal segment contained no frames despite non-zero length",
            ));
        };
        Ok(WalSegmentBounds::new(
            descriptor.seq,
            wal_segment_file_id(descriptor.seq),
            bounds.first_seq,
            bounds.last_seq,
        ))
    }

    async fn enforce_retention_limit(&mut self) -> WalResult<()> {
        if let Some(limit) = self.cfg.retention_bytes {
            while self.total_retained_bytes() > limit {
                if let Some(evicted) = self.completed_segments.pop_front() {
                    self.remove_sealed_segment_metadata(evicted.seq());
                    self.storage.remove_segment(evicted.path()).await?;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn close_active_segment(&mut self) -> WalResult<()> {
        self.segment
            .file_mut()
            .close()
            .await
            .map_err(|err| backend_err("close wal segment", err))
    }

    fn current_frame_seq(&self) -> u64 {
        self.next_frame_seq.saturating_sub(1)
    }

    fn record_frame_progress(&mut self, seq: u64, commit_ts: Option<Timestamp>) {
        if let Some(handle) = self.state.as_mut() {
            let state = handle.state_mut();
            state.set_frame_seq(seq);
            if let Some(ts) = commit_ts {
                state.set_commit_ts(ts);
            }
            self.state_dirty = true;
        }
    }

    fn record_sealed_segment(&mut self, bounds: WalSegmentBounds) {
        if let Some(handle) = self.state.as_mut() {
            handle.state_mut().set_segment_seq(bounds.seq);
            handle.state_mut().upsert_sealed_segment(bounds);
            handle.state_mut().clear_active_segment();
            self.state_dirty = true;
        }
    }

    async fn persist_state_if_dirty(&mut self) -> WalResult<()> {
        if self.state_dirty {
            if let Some(handle) = self.state.as_mut() {
                handle.persist().await?;
            }
            self.state_dirty = false;
        }
        Ok(())
    }

    fn apply_timer_directive(&mut self, directive: TimerDirective, timer_slot: &mut SleepSlot) {
        match directive {
            TimerDirective::None => {}
            TimerDirective::Cancel => {
                self.next_sync_deadline = None;
            }
            TimerDirective::Schedule(interval) => {
                let now = Instant::now();
                let deadline = now.checked_add(interval).unwrap_or(now);
                self.next_sync_deadline = match self.next_sync_deadline {
                    Some(existing) => Some(existing.min(deadline)),
                    None => Some(deadline),
                };
            }
        }
        self.recompute_timer(timer_slot);
    }

    fn recompute_timer(&mut self, timer_slot: &mut SleepSlot) {
        let next_deadline =
            Self::earliest_deadline(self.next_sync_deadline, self.rotation_deadline);

        match next_deadline {
            Some(deadline) => {
                let now = Instant::now();
                let duration = deadline.saturating_duration_since(now);
                *timer_slot = Some(self.exec.sleep(duration).fuse());
                self.scheduled_deadline = Some(deadline);
            }
            None => {
                *timer_slot = None;
                self.scheduled_deadline = None;
            }
        }
    }

    async fn rehydrate_segment_metadata(
        storage: &WalStorage,
        tail: &Option<TailMetadata>,
        state: Option<&WalStateHandle>,
    ) -> WalResult<(VecDeque<SegmentMeta>, Option<WalSegmentBounds>, bool)> {
        let mut completed = VecDeque::new();
        let mut active = None;
        let mut state_dirty = false;

        let mut sealed_lookup: HashMap<u64, WalSegmentBounds> = state
            .map(|handle| {
                handle
                    .sealed_segments()
                    .iter()
                    .cloned()
                    .map(|segment| (segment.seq, segment))
                    .collect()
            })
            .unwrap_or_default();

        if let Some(tail_meta) = tail {
            for descriptor in tail_meta.completed.iter().cloned() {
                if descriptor.bytes == 0 {
                    continue;
                }
                let bounds = match sealed_lookup.remove(&descriptor.seq) {
                    Some(bounds) => bounds,
                    None => {
                        state_dirty = true;
                        Self::load_bounds_from_storage(storage, &descriptor).await?
                    }
                };
                completed.push_back(SegmentMeta::new(descriptor, bounds));
            }

            if tail_meta.active.bytes > 0 {
                active = Some(Self::load_bounds_from_storage(storage, &tail_meta.active).await?);
                state_dirty = true;
            }
        }

        Ok((completed, active, state_dirty))
    }

    fn align_state_with_segments(
        state: &mut WalStateHandle,
        completed: &VecDeque<SegmentMeta>,
        active: Option<&WalSegmentBounds>,
    ) -> bool {
        let desired: Vec<_> = completed.iter().map(|meta| meta.bounds.clone()).collect();
        let mut dirty = false;
        if state.state().sealed_segments() != desired {
            state.state_mut().replace_sealed_segments(desired);
            dirty = true;
        }
        let aligns = match (state.active_segment(), active) {
            (Some(existing), Some(next)) => *existing == *next,
            (None, None) => true,
            _ => false,
        };
        if !aligns {
            if let Some(bounds) = active.cloned() {
                state.state_mut().set_active_segment(bounds);
            } else {
                state.state_mut().clear_active_segment();
            }
            dirty = true;
        }
        dirty
    }

    fn earliest_deadline(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
        match (a, b) {
            (Some(x), Some(y)) => Some(x.min(y)),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        }
    }

    async fn handle_timer_elapsed(&mut self) -> WalResult<TimerTickOutcome> {
        self.scheduled_deadline = None;
        let mut sync_performed = false;
        let now = Instant::now();

        if let Some(deadline) = self.rotation_deadline
            && deadline <= now
        {
            if self.segment_bytes > 0 {
                let rotation = self.rotate_active_segment().await?;
                if rotation.sync_performed {
                    sync_performed = true;
                }
                self.persist_state_if_dirty().await?;
            }
            self.rotation_deadline = None;
        }

        if let Some(deadline) = self.next_sync_deadline
            && deadline <= now
        {
            self.next_sync_deadline = None;
            if self.bytes_since_sync > 0 {
                self.sync_data().await?;
                self.bytes_since_sync = 0;
                self.last_sync = Instant::now();
                sync_performed = true;
            }
        }

        Ok(TimerTickOutcome { sync_performed })
    }
}

async fn self_initial_size(
    fs_tag: FileSystemTag,
    file: &mut dyn fusio::dynamic::fs::DynFile,
) -> WalResult<usize> {
    match file.size().await {
        Ok(len) => Ok(len as usize),
        Err(err) if fs_tag == FileSystemTag::S3 && is_not_found(&err) => Ok(0),
        Err(err) => Err(backend_err("determine wal segment size", err)),
    }
}

fn is_not_found(err: &FusioError) -> bool {
    fn inner_contains(not_found: &str, err: &dyn Error) -> bool {
        if err.to_string().contains(not_found) {
            return true;
        }
        let mut current = err;
        while let Some(source) = current.source() {
            if source.to_string().contains(not_found) {
                return true;
            }
            current = source;
        }
        false
    }

    match err {
        FusioError::Remote(inner) | FusioError::Other(inner) => {
            inner_contains("404", inner.as_ref()) || inner_contains("NotFound", inner.as_ref())
        }
        FusioError::Io(io_err) => io_err.kind() == io::ErrorKind::NotFound,
        _ => false,
    }
}

enum SyncVariant {
    Data,
    All,
}

async fn perform_sync(
    fs_tag: FileSystemTag,
    segment_path: &fusio::path::Path,
    variant: SyncVariant,
) -> WalResult<()> {
    match fs_tag {
        FileSystemTag::Local => {
            let local_path = path_to_local(segment_path).map_err(|err| {
                WalError::Storage(format!("failed to resolve wal segment path: {err}"))
            })?;
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&local_path)
                .map_err(|err| {
                    WalError::Storage(format!(
                        "failed to reopen wal segment {} for sync: {}",
                        local_path.display(),
                        err
                    ))
                })?;
            match variant {
                SyncVariant::Data => file.sync_data().map_err(|err| {
                    WalError::Storage(format!(
                        "failed to fdatasync wal segment {}: {}",
                        local_path.display(),
                        err
                    ))
                }),
                SyncVariant::All => file.sync_all().map_err(|err| {
                    WalError::Storage(format!(
                        "failed to fsync wal segment {}: {}",
                        local_path.display(),
                        err
                    ))
                }),
            }
        }
        FileSystemTag::Memory | FileSystemTag::S3 => Ok(()),
        _ => Err(WalError::Storage(format!(
            "wal backend {:?} does not support durability sync",
            fs_tag
        ))),
    }
}

fn backend_err(action: &str, err: FusioError) -> WalError {
    WalError::Storage(format!("failed to {action}: {err}"))
}

struct HandleOutcome {
    ack: WalAck,
    sync_performed: bool,
    timer_directive: TimerDirective,
}

struct SyncOutcome {
    performed: bool,
    timer_directive: TimerDirective,
}

struct RotationOutcome {
    performed: bool,
    sync_performed: bool,
}

#[derive(Debug, Clone, Copy)]
enum TimerDirective {
    None,
    Cancel,
    Schedule(Duration),
}

struct TimerTickOutcome {
    sync_performed: bool,
}

#[derive(Clone, Debug)]
struct SegmentMeta {
    descriptor: SegmentDescriptor,
    bounds: WalSegmentBounds,
}

impl SegmentMeta {
    fn new(descriptor: SegmentDescriptor, bounds: WalSegmentBounds) -> Self {
        Self { descriptor, bounds }
    }

    fn path(&self) -> &fusio::path::Path {
        &self.descriptor.path
    }

    fn bytes(&self) -> usize {
        self.descriptor.bytes
    }

    fn seq(&self) -> u64 {
        self.descriptor.seq
    }
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
        time::{Duration, Instant},
    };

    use arrow_array::{ArrayRef, UInt64Array};
    use fusio::{
        DynFs, executor::BlockingExecutor, fs::FsCas, impls::mem::fs::InMemoryFs, path::Path,
    };
    use futures::{channel::oneshot, executor::LocalPool, task::LocalSpawnExt};
    use typed_arrow::{
        arrow_array::{Int64Array, RecordBatch},
        arrow_schema::{DataType, Field, Schema},
    };

    use super::*;
    use crate::{
        mvcc::Timestamp,
        wal::{
            DynBatchPayload, WalCommand, WalResult,
            state::{FsWalStateStore, WalStateStore},
        },
    };

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let data = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as _;
        RecordBatch::try_new(schema, vec![data]).expect("valid batch")
    }

    fn queue_autocommit(
        sender: &mut mpsc::Sender<WriterMsg>,
        queue_depth: &Arc<AtomicUsize>,
        seq_append: u64,
        seq_commit: u64,
        commands: (WalCommand, WalCommand),
    ) -> (
        oneshot::Receiver<WalResult<WalAck>>,
        oneshot::Receiver<WalResult<WalAck>>,
    ) {
        let (append_command, commit_command) = commands;

        let (append_ack_tx, append_ack_rx) = oneshot::channel();
        queue_depth.fetch_add(1, Ordering::SeqCst);
        sender
            .try_send(WriterMsg::queued(
                seq_append,
                append_command,
                Instant::now(),
                append_ack_tx,
            ))
            .expect("append send");

        let (commit_ack_tx, commit_ack_rx) = oneshot::channel();
        queue_depth.fetch_add(1, Ordering::SeqCst);
        sender
            .try_send(WriterMsg::queued(
                seq_commit,
                commit_command,
                Instant::now(),
                commit_ack_tx,
            ))
            .expect("commit send");

        (append_ack_rx, commit_ack_rx)
    }

    fn sample_commands(
        batch: &RecordBatch,
        commit_ts: u64,
        provisional_id: u64,
    ) -> (WalCommand, WalCommand) {
        let commit_array: ArrayRef =
            Arc::new(UInt64Array::from(vec![commit_ts; batch.num_rows()])) as ArrayRef;
        let payload = DynBatchPayload::Row {
            batch: batch.clone(),
            commit_ts_column: commit_array,
        };
        let append = WalCommand::TxnAppend {
            provisional_id,
            payload,
        };
        let commit = WalCommand::TxnCommit {
            provisional_id,
            commit_ts: Timestamp::new(commit_ts),
        };
        (append, commit)
    }

    fn in_memory_env(
        queue_size: usize,
        sync: WalSyncPolicy,
        root: &str,
    ) -> (WalStorage, WalConfig) {
        let backend = Arc::new(InMemoryFs::new());
        let fs_dyn: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let storage = WalStorage::new(Arc::clone(&fs_dyn), Path::parse(root).expect("path"));
        let mut cfg = WalConfig::default();
        cfg.queue_size = queue_size;
        cfg.sync = sync;
        cfg.segment_backend = fs_dyn;
        cfg.state_store = Some(Arc::new(FsWalStateStore::new(fs_cas)));
        (storage, cfg)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn state_json_tracks_commit_progress() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_writer: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let root = Path::parse("wal-state-progress").expect("path");
        let storage = WalStorage::new(Arc::clone(&fs_writer), root.clone());

        let mut cfg = WalConfig::default();
        cfg.queue_size = 2;
        cfg.sync = WalSyncPolicy::Always;
        cfg.segment_backend = fs_writer;
        let state_store: Arc<dyn WalStateStore> = Arc::new(FsWalStateStore::new(fs_cas));
        cfg.state_store = Some(Arc::clone(&state_store));

        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack_cell_clone = Rc::clone(&ack_cell);

        let provisional_id = 5;
        let (append_ack_rx, commit_ack_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            provisional_id,
            provisional_id + 1,
            sample_commands(&sample_batch(), 123, provisional_id),
        );

        spawner
            .spawn_local(async move {
                let _ = append_ack_rx.await.expect("append ack");
            })
            .expect("spawn append ack");

        spawner
            .spawn_local(async move {
                let ack = commit_ack_rx.await.expect("commit ack oneshot");
                *ack_cell_clone.borrow_mut() = Some(ack);
            })
            .expect("spawn ack");

        pool.run_until_stalled();
        sender.close_channel();
        pool.run();

        let ack = ack_cell
            .borrow()
            .clone()
            .expect("ack result")
            .expect("ack ok");
        assert_eq!(ack.last_seq, frame::INITIAL_FRAME_SEQ + 1);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let state_handle = WalStateHandle::load(state_store, &root)
            .await
            .expect("load state");
        let state = state_handle.state().clone();
        assert_eq!(state.last_frame_seq, Some(frame::INITIAL_FRAME_SEQ + 1));
        assert_eq!(state.last_commit_ts, Some(123));
        assert!(state.last_segment_seq.is_none());
        assert!(state.sealed_segments.is_empty());
        let active = state
            .active_segment
            .as_ref()
            .expect("active segment metadata present");
        assert_eq!(active.seq, 0);
        assert_eq!(active.first_frame, frame::INITIAL_FRAME_SEQ);
        assert_eq!(active.last_frame, frame::INITIAL_FRAME_SEQ + 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn state_tracks_sealed_segment_metadata() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_writer: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let root = Path::parse("wal-state-sealed").expect("path");
        let storage = WalStorage::new(Arc::clone(&fs_writer), root.clone());

        let mut cfg = WalConfig::default();
        cfg.queue_size = 2;
        cfg.sync = WalSyncPolicy::Always;
        cfg.segment_backend = fs_writer;
        let state_store: Arc<dyn WalStateStore> = Arc::new(FsWalStateStore::new(fs_cas));
        cfg.state_store = Some(Arc::clone(&state_store));

        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let provisional_id = 9;
        let (append_ack_rx, commit_ack_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            provisional_id,
            provisional_id + 1,
            sample_commands(&sample_batch(), 77, provisional_id),
        );

        spawner
            .spawn_local(async move {
                let _ = append_ack_rx.await.expect("append ack");
            })
            .expect("spawn append ack");

        let rotate_ack: Rc<RefCell<Option<WalResult<()>>>> = Rc::new(RefCell::new(None));
        let rotate_clone = Rc::clone(&rotate_ack);
        spawner
            .spawn_local(async move {
                let ack = commit_ack_rx.await.expect("commit ack oneshot");
                ack.expect("commit ack ok");
            })
            .expect("spawn commit ack");

        pool.run_until_stalled();
        let (rotate_tx, rotate_rx) = oneshot::channel();
        sender
            .try_send(WriterMsg::Rotate { ack_tx: rotate_tx })
            .expect("send rotate");
        spawner
            .spawn_local(async move {
                let ack = rotate_rx.await.expect("rotate ack");
                *rotate_clone.borrow_mut() = Some(ack);
            })
            .expect("spawn rotate ack");

        pool.run_until_stalled();
        sender.close_channel();
        pool.run();

        assert!(rotate_ack.borrow().clone().expect("rotate result").is_ok());

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let state_handle = WalStateHandle::load(state_store, &root)
            .await
            .expect("load state");
        let state = state_handle.state().clone();
        assert!(state.active_segment.is_none());
        assert_eq!(state.sealed_segments.len(), 1);
        let sealed = &state.sealed_segments[0];
        assert_eq!(sealed.seq, 0);
        assert_eq!(sealed.first_frame, frame::INITIAL_FRAME_SEQ);
        assert_eq!(sealed.last_frame, frame::INITIAL_FRAME_SEQ + 1);
    }

    #[test]
    fn snapshot_reports_sealed_and_active_segments() {
        let (storage, cfg) = in_memory_env(4, WalSyncPolicy::Always, "wal-snapshot");

        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        // Write first batch and rotate so it becomes sealed.
        let first_batch = sample_batch();
        let (append_ack_rx, commit_ack_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            100,
            101,
            sample_commands(&first_batch, 200, 100),
        );

        spawner
            .spawn_local(async move {
                let _ = append_ack_rx.await.expect("append ack");
            })
            .expect("await append");

        let rotate_done: Rc<RefCell<Option<WalResult<()>>>> = Rc::new(RefCell::new(None));
        let rotate_clone = Rc::clone(&rotate_done);
        spawner
            .spawn_local(async move {
                let ack = commit_ack_rx.await.expect("commit ack");
                ack.expect("commit ok");
            })
            .expect("await commit");

        pool.run_until_stalled();
        let (rotate_tx, rotate_rx) = oneshot::channel();
        sender
            .try_send(WriterMsg::Rotate { ack_tx: rotate_tx })
            .expect("send rotate");
        spawner
            .spawn_local(async move {
                let ack = rotate_rx.await.expect("rotate ack");
                *rotate_clone.borrow_mut() = Some(ack);
            })
            .expect("spawn rotate ack");

        // Append a second batch so the active segment has metadata.
        let (append2_ack_rx, commit2_ack_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            200,
            201,
            sample_commands(&first_batch, 300, 200),
        );

        spawner
            .spawn_local(async move {
                let _ = append2_ack_rx.await.expect("append2 ack");
            })
            .expect("await append2");

        let snapshot_cell: Rc<RefCell<Option<WalSnapshot>>> = Rc::new(RefCell::new(None));
        let snapshot_clone = Rc::clone(&snapshot_cell);
        spawner
            .spawn_local(async move {
                let ack = commit2_ack_rx.await.expect("commit2 ack");
                ack.expect("commit2 ok");
            })
            .expect("await commit2");

        pool.run_until_stalled();
        let (snap_tx, snap_rx) = oneshot::channel();
        sender
            .try_send(WriterMsg::Snapshot { ack_tx: snap_tx })
            .expect("send snapshot");
        spawner
            .spawn_local(async move {
                let snapshot = snap_rx.await.expect("snapshot ack").expect("snapshot ok");
                *snapshot_clone.borrow_mut() = Some(snapshot);
            })
            .expect("spawn snapshot wait");

        sender.close_channel();
        pool.run();

        assert!(rotate_done.borrow().clone().expect("rotate result").is_ok());
        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let snapshot = snapshot_cell.borrow().clone().expect("snapshot captured");
        assert_eq!(snapshot.sealed_segments.len(), 1);
        let sealed = &snapshot.sealed_segments[0];
        assert_eq!(sealed.seq, 0);
        assert_eq!(sealed.first_frame, frame::INITIAL_FRAME_SEQ);
        assert_eq!(sealed.last_frame, frame::INITIAL_FRAME_SEQ + 1);
        let active = snapshot
            .active_segment
            .as_ref()
            .expect("active segment present");
        assert_eq!(active.seq, 1);
        assert_eq!(active.first_frame, frame::INITIAL_FRAME_SEQ + 2);
        assert_eq!(active.last_frame, frame::INITIAL_FRAME_SEQ + 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn submit_and_drain_on_shutdown() {
        let (storage, cfg) = in_memory_env(4, WalSyncPolicy::Always, "wal-test");

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn");

        let base = sample_batch();
        let payload_seq = 777;
        let (append_ack_rx, commit_ack_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            payload_seq,
            payload_seq + 1,
            sample_commands(&base, 42, payload_seq),
        );

        let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack_cell_clone = Rc::clone(&ack_cell);
        spawner
            .spawn_local(async move {
                let append_ack = append_ack_rx.await.expect("append ack");
                append_ack.expect("append ack ok");
                let commit_ack = commit_ack_rx.await.expect("commit ack");
                *ack_cell_clone.borrow_mut() = Some(commit_ack);
            })
            .expect("spawn ack");

        sender.close_channel();
        pool.run();

        let ack = ack_cell
            .borrow()
            .clone()
            .expect("ack result")
            .expect("ack ok");
        assert_eq!(ack.last_seq, frame::INITIAL_FRAME_SEQ + 1);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let metrics_guard = metrics_reader.read().await;
        assert_eq!(metrics_guard.queue_depth, 0);
        assert!(metrics_guard.bytes_written > 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queue_backpressure_and_metrics() {
        let (storage, cfg) = in_memory_env(1, WalSyncPolicy::Always, "wal-backpressure");
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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let seq1 = 42;
        let (append_rx1, commit_rx1) = queue_autocommit(
            &mut sender,
            &queue_depth,
            seq1,
            seq1 + 1,
            sample_commands(&sample_batch(), 1, seq1),
        );

        let ack1_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack1_cell_clone = Rc::clone(&ack1_cell);
        spawner
            .spawn_local(async move {
                let append_ack = append_rx1.await.expect("append ack1");
                append_ack.expect("append ack1 ok");
                let commit_ack = commit_rx1.await.expect("commit ack1");
                *ack1_cell_clone.borrow_mut() = Some(commit_ack);
            })
            .expect("spawn ack1");

        pool.run_until_stalled();

        let ack1 = ack1_cell
            .borrow()
            .clone()
            .expect("ack1 result")
            .expect("ack1 ok");
        assert_eq!(ack1.last_seq, frame::INITIAL_FRAME_SEQ + 1);
        assert_eq!(queue_depth.load(Ordering::SeqCst), 0);

        let (append_rx2, commit_rx2) = queue_autocommit(
            &mut sender,
            &queue_depth,
            seq1 + 2,
            seq1 + 3,
            sample_commands(&sample_batch(), 2, seq1 + 2),
        );

        let ack2_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack2_cell_clone = Rc::clone(&ack2_cell);
        spawner
            .spawn_local(async move {
                let append_ack = append_rx2.await.expect("append ack2");
                append_ack.expect("append ack2 ok");
                let commit_ack = commit_rx2.await.expect("commit ack2");
                *ack2_cell_clone.borrow_mut() = Some(commit_ack);
            })
            .expect("spawn ack2");

        sender.close_channel();
        pool.run();

        let ack2 = ack2_cell
            .borrow()
            .clone()
            .expect("ack2 result")
            .expect("ack2 ok");
        assert_eq!(ack2.last_seq, frame::INITIAL_FRAME_SEQ + 3);

        let metrics_guard = metrics_reader.read().await;
        assert_eq!(metrics_guard.queue_depth, 0);
        assert!(metrics_guard.bytes_written > 0);
        assert!(metrics_guard.sync_operations >= 2);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn interval_time_policy_triggers_sync_without_additional_writes() {
        let (storage, cfg) = in_memory_env(
            2,
            WalSyncPolicy::IntervalTime(Duration::from_millis(0)),
            "wal-interval-time",
        );

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let (append_rx, commit_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            99,
            100,
            sample_commands(&sample_batch(), 11, 99),
        );

        let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack_cell_clone = Rc::clone(&ack_cell);
        spawner
            .spawn_local(async move {
                let append_ack = append_rx.await.expect("append ack");
                append_ack.expect("append ack ok");
                let commit_ack = commit_rx.await.expect("commit ack");
                *ack_cell_clone.borrow_mut() = Some(commit_ack);
            })
            .expect("spawn ack");

        sender.close_channel();
        pool.run();

        let ack = ack_cell
            .borrow()
            .clone()
            .expect("ack result")
            .expect("ack ok");
        assert_eq!(ack.last_seq, frame::INITIAL_FRAME_SEQ + 1);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let metrics_guard = metrics_reader.read().await;
        assert_eq!(metrics_guard.queue_depth, 0);
        assert!(metrics_guard.bytes_written > 0);
        assert!(metrics_guard.sync_operations >= 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn interval_bytes_policy_honors_threshold() {
        let (storage, cfg) =
            in_memory_env(4, WalSyncPolicy::IntervalBytes(1), "wal-interval-bytes");
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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let (append_rx, commit_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            7,
            8,
            sample_commands(&sample_batch(), 21, 7),
        );

        let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack_cell_clone = Rc::clone(&ack_cell);
        spawner
            .spawn_local(async move {
                let append_ack = append_rx.await.expect("append ack");
                append_ack.expect("append ack ok");
                let commit_ack = commit_rx.await.expect("commit ack");
                *ack_cell_clone.borrow_mut() = Some(commit_ack);
            })
            .expect("spawn ack");

        sender.close_channel();
        pool.run();

        let ack = ack_cell
            .borrow()
            .clone()
            .expect("ack result")
            .expect("ack ok");
        assert_eq!(ack.last_seq, frame::INITIAL_FRAME_SEQ + 1);

        let metrics_guard = metrics_reader.read().await;
        assert_eq!(metrics_guard.queue_depth, 0);
        assert!(metrics_guard.bytes_written > 0);
        assert!(metrics_guard.sync_operations >= 1);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn manual_rotation_creates_new_segment() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_reader: Arc<dyn DynFs> = backend.clone();
        let fs_writer: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let root = Path::parse("wal-manual-rotation").expect("path");
        let storage_reader = WalStorage::new(fs_reader, root.clone());
        let storage_writer = WalStorage::new(Arc::clone(&fs_writer), root.clone());

        let mut cfg = WalConfig::default();
        cfg.queue_size = 4;
        cfg.segment_max_bytes = 1024;
        cfg.sync = WalSyncPolicy::Always;
        cfg.segment_backend = fs_writer;
        let state_store: Arc<dyn WalStateStore> = Arc::new(FsWalStateStore::new(fs_cas));
        cfg.state_store = Some(Arc::clone(&state_store));

        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage: storage_writer,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let (append_rx, commit_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            17,
            18,
            sample_commands(&sample_batch(), 55, 17),
        );

        let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack_cell_clone = Rc::clone(&ack_cell);
        spawner
            .spawn_local(async move {
                let append_ack = append_rx.await.expect("append ack");
                append_ack.expect("append ack ok");
                let commit_ack = commit_rx.await.expect("commit ack");
                *ack_cell_clone.borrow_mut() = Some(commit_ack);
            })
            .expect("spawn ack");

        pool.run_until_stalled();

        let (rotate_tx, rotate_rx) = oneshot::channel();
        sender
            .try_send(WriterMsg::Rotate { ack_tx: rotate_tx })
            .expect("send rotate");

        let rotate_cell: Rc<RefCell<Option<WalResult<()>>>> = Rc::new(RefCell::new(None));
        let rotate_cell_clone = Rc::clone(&rotate_cell);
        spawner
            .spawn_local(async move {
                let res = rotate_rx.await.expect("rotate oneshot");
                *rotate_cell_clone.borrow_mut() = Some(res);
            })
            .expect("spawn rotate listener");

        pool.run_until_stalled();

        sender.close_channel();
        pool.run();

        let ack = ack_cell
            .borrow()
            .clone()
            .expect("ack result")
            .expect("ack ok");
        assert_eq!(ack.last_seq, frame::INITIAL_FRAME_SEQ + 1);

        rotate_cell
            .borrow()
            .clone()
            .expect("rotate result")
            .expect("rotate ok");

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let segments = storage_reader
            .list_segments()
            .await
            .expect("list segments after manual rotation");
        assert!(
            segments.len() >= 2,
            "manual rotation should create a new segment"
        );
        assert_eq!(segments[0].seq, 0);
        assert!(
            segments[0].bytes > 0,
            "sealed segment should retain written bytes"
        );
        assert_eq!(segments[1].seq, 1);

        let state_handle = WalStateHandle::load(state_store, storage_reader.root())
            .await
            .expect("load state after rotation");
        let state = state_handle.state();
        assert_eq!(state.last_segment_seq, Some(0));
        assert_eq!(state.last_frame_seq, Some(frame::INITIAL_FRAME_SEQ + 1));
        assert_eq!(state.last_commit_ts, Some(55));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn time_based_rotation_seals_segment() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_reader: Arc<dyn DynFs> = backend.clone();
        let fs_writer: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let root = Path::parse("wal-time-rotation").expect("path");
        let storage_reader = WalStorage::new(fs_reader, root.clone());
        let storage_writer = WalStorage::new(Arc::clone(&fs_writer), root.clone());

        let mut cfg = WalConfig::default();
        cfg.queue_size = 4;
        cfg.segment_max_age = Some(Duration::from_millis(0));
        cfg.sync = WalSyncPolicy::Always;
        cfg.segment_backend = fs_writer;
        cfg.state_store = Some(Arc::new(FsWalStateStore::new(fs_cas)));

        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage: storage_writer,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let (append_rx, commit_rx) = queue_autocommit(
            &mut sender,
            &queue_depth,
            31,
            32,
            sample_commands(&sample_batch(), 90, 31),
        );

        let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
        let ack_cell_clone = Rc::clone(&ack_cell);
        spawner
            .spawn_local(async move {
                let append_ack = append_rx.await.expect("append ack");
                append_ack.expect("append ack ok");
                let commit_ack = commit_rx.await.expect("commit ack");
                *ack_cell_clone.borrow_mut() = Some(commit_ack);
            })
            .expect("spawn ack listener");

        pool.run_until_stalled();
        pool.run_until_stalled();

        sender.close_channel();
        pool.run();

        let ack = ack_cell
            .borrow()
            .clone()
            .expect("ack result")
            .expect("ack ok");
        assert_eq!(ack.last_seq, frame::INITIAL_FRAME_SEQ + 1);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let segments = storage_reader
            .list_segments()
            .await
            .expect("list segments after time rotation");
        assert!(
            segments.len() >= 2,
            "time rotation should seal current segment"
        );
        assert!(
            segments[0].bytes > 0,
            "sealed segment should retain written bytes"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn segment_rotation_enforces_retention() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_reader: Arc<dyn DynFs> = backend.clone();
        let fs_writer: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let root = Path::parse("wal-rotation-retention").expect("path");
        let storage_reader = WalStorage::new(fs_reader, root.clone());
        let storage_writer = WalStorage::new(Arc::clone(&fs_writer), root.clone());

        let mut cfg = WalConfig::default();
        cfg.queue_size = 4;
        cfg.segment_max_bytes = 1;
        cfg.retention_bytes = Some(1);
        cfg.sync = WalSyncPolicy::Disabled;
        cfg.segment_backend = fs_writer;
        cfg.state_store = Some(Arc::new(FsWalStateStore::new(fs_cas)));

        let metrics = Arc::new(BlockingExecutor::rw_lock(WalMetrics::default()));

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
                    WriterLoopInit {
                        exec: Arc::new(BlockingExecutor::default()),
                        storage: storage_writer,
                        cfg,
                        metrics: Arc::clone(&metrics),
                        queue_depth: queue_depth_writer,
                        initial_segment_seq: 0,
                        initial_frame_seq: frame::INITIAL_FRAME_SEQ,
                    },
                    receiver,
                )
                .await;
                *result_cell_clone.borrow_mut() = Some(result);
            })
            .expect("spawn writer");

        let mut enqueue_payload = |seq_base: u64, commit_ts: u64| {
            let (append_rx, commit_rx) = queue_autocommit(
                &mut sender,
                &queue_depth,
                seq_base,
                seq_base + 1,
                sample_commands(&sample_batch(), commit_ts, seq_base),
            );

            let ack_cell: Rc<RefCell<Option<WalResult<WalAck>>>> = Rc::new(RefCell::new(None));
            let ack_cell_clone = Rc::clone(&ack_cell);
            spawner
                .spawn_local(async move {
                    let append_ack = append_rx.await.expect("append ack");
                    append_ack.expect("append ack ok");
                    let commit_ack = commit_rx.await.expect("commit ack");
                    *ack_cell_clone.borrow_mut() = Some(commit_ack);
                })
                .expect("spawn ack listener");

            ack_cell
        };

        let ack1_cell = enqueue_payload(10, 1);
        let ack2_cell = enqueue_payload(12, 2);

        sender.close_channel();
        pool.run();

        let ack1 = ack1_cell
            .borrow()
            .clone()
            .expect("ack1 result")
            .expect("ack1 ok");
        let ack2 = ack2_cell
            .borrow()
            .clone()
            .expect("ack2 result")
            .expect("ack2 ok");
        assert!(ack2.last_seq > ack1.last_seq);

        let writer_result = result_cell.borrow().clone().expect("writer result");
        assert!(writer_result.is_ok());

        let segments = storage_reader
            .list_segments()
            .await
            .expect("list segments after rotation");
        assert_eq!(
            segments.len(),
            1,
            "retention should keep only latest segment"
        );
        assert!(
            segments[0]
                .path
                .as_ref()
                .ends_with("wal-00000000000000000004.tonwal")
        );
    }
}
