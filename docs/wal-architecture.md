# Tonbo WAL Architecture Sketch

> Status: scaffolding only — persistence logic intentionally deferred to the
> durability team. This document captures the intended shape so the remaining
> work can land without reshaping public APIs.

## Goals

- Provide a **single async entry point** for write-ahead logging that DB callers
  can rely on (`WalHandle::submit` / `WalTicket::durable`).
- Keep the door open for **multiple storage backends** (local filesystem,
  object stores) without committing to an implementation yet.
- Let the CTO codify **trait bounds and type plumbing** (executor, timer,
  fusio I/O) ahead of time so implementation teams can “color inside the lines”.

## Components in the Crate

| Module / Type | Responsibility | Current State |
| --- | --- | --- |
| `wal::WalConfig` | User-supplied knobs: directory, segment limits, queue size, sync policy. | Fully defined. |
| `wal::WalSyncPolicy` | Encodes fsync policy (always, bytes, time, disabled). | Fully defined. |
| `wal::WalPayload` | Logical frames the WAL accepts (`DynBatch` today; typed/control reserved). | Defined. |
| `wal::WalHandle<E>` | Handle stored on `DB`; owns future submit/rotate plumbing. | Constructible via `placeholder()`, methods return `WalError::Unimplemented`. |
| `wal::WalTicket<E>` | Represents an enqueued frame plus a future durability acknowledgement. | Exposes `durable()` stub returning `WalError::Unimplemented`. |
| `wal::backend` | Facade around `Arc<dyn fusio::DynFs>` for future segment work. | All functions return `WalError::Unimplemented`. |
| `wal::frame` | Frame header, discriminants, Arrow payload modeling. | Encode/decode left as TODOs. |
| `wal::replay` | Recovery scanner API for boot-time WAL replay. | Methods return `WalError::Unimplemented`. |
| `wal::metrics` | Placeholder for queue/sync telemetry. | Empty scaffolding. |

## Intended Call Flow

1. **Enable**: `DB::enable_wal(cfg)` (via `WalExt`) will eventually:
   - Create a `WalHandle` with real writer state (today: placeholder).
   - Bootstrap the storage directory and spawn the async writer on the executor.
   - Run recovery (`replay::Replayer::scan`) before allowing new ingest.
2. **Submit**: `DB::ingest` calls `WalHandle::append(&batch, &tombstones, commit_ts)`,
   which internally enqueues `WalPayload::DynBatch { .. }`.
   - When implementation lands, this will push into the writer queue, assign a
     sequence number, and return a `WalTicket`.
3. **Durability Ack**: `WalTicket::durable().await` yields a `WalAck` describing
   the durable sequence, bytes flushed, and elapsed time.
4. **Rotate / Disable**: `WalHandle::rotate()` and `WalExt::disable_wal()` will
   seal the active segment, drain the queue, and release resources.

All numbered steps are annotated in code with `WalError::Unimplemented` so the
team knows exactly where to land follow-up patches.

## Executor & Async Boundaries

- `DB<M, E>` carries `Arc<E>` where `E: fusio::executor::Executor + Timer`.
- `WalHandle<E>` and `WalTicket<E>` are parameterised on the same executor.
- The future writer loop will spawn tasks through this executor and use the
  timer to enforce `WalSyncPolicy::IntervalTime`.

## Implementation Hand-off Notes

- Keep submit fast: enqueue payload + return ticket immediately; offload IO to
  a single writer task.
- Store state needed by durability (`seq`, queued bytes, sync counters) inside
  the eventual writer struct; the current placeholder keeps the signature ready.
- Use `WalError::Unimplemented("...")` markers as breadcrumbs; replace them with
  concrete error handling when implementing each layer.
- Update `docs/rfcs/0002-wal.md` when behavior deviates from this sketch; the
  RFC already references the executor-specific API.

The scaffolding is now in place; follow-on engineers can implement the writer,
storage, and replay logic without refactoring the public surface. The CTO moves
on once the architecture and documentation are clear.
