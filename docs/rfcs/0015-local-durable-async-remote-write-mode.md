# RFC: Local-Durable Async-Remote Write Mode

- Status: Draft
- Authors: Tonbo team
- Created: 2026-04-17
- Area: WAL, durability, recovery, object storage

## Summary

Define an explicit write mode for S3-backed Tonbo deployments in which a write is considered durable once it has been persisted to local disk, while publication of that write to remote object storage proceeds asynchronously. The goal is to aggressively reduce foreground write latency in object-store-backed deployments without obscuring the durability boundary or weakening recovery semantics by accident.

## Motivation

Current benchmarking shows that remote object-store operations still dominate the foreground write path in S3-backed workloads. Recent write-path optimizations materially improved write latency, especially in same-region S3, but they did not eliminate the fundamental cost of waiting for remote publication before acknowledging a write.

A mode that acknowledges writes after durable local persistence can remove the largest remaining latency source from the foreground path. That makes S3-backed Tonbo more viable for ingest-heavy and mixed workloads where write latency matters directly.

This only makes sense if the durability contract is explicit. Acknowledging after local-disk persistence but before remote publication is not the same contract as acknowledging after remote durability. Recovery, visibility, and node-loss behavior must therefore be defined as first-class semantics rather than treated as an implementation detail.

## Goals

- Define an explicit write mode in which a write is durable after local-disk persistence.
- Remove remote object-store publication from the foreground write critical path in that mode.
- Preserve correctness across crash and restart when remote publication is incomplete.
- Define the visibility relationship between locally durable state and remotely published state.
- Keep the durability contract explicit at the API and configuration level.

## Non-Goals

- Replacing the existing remote-durable mode.
- Hiding weaker immediate durability behind the current S3-backed mode.
- Defining the final implementation shape of schedulers, queues, or background workers.
- Solving every future multi-node replication policy in this RFC.

## Design

### Write Modes

Tonbo exposes distinct durability modes rather than overloading one mode with ambiguous behavior.

| Mode | Ack point | Immediate durability scope | Remote visibility |
| --- | --- | --- | --- |
| `remote_durable` | after remote durability completes | remote backing store | after ack |
| `local_durable_async_remote` | after durable local-disk persistence completes | local node disk | after async publication |

The new mode is only correct if the caller can accept that a write acknowledged in this mode may not yet exist in remote object storage.

### Durability Contract

In `local_durable_async_remote` mode:

- a write is acknowledged after it is durably recorded on local disk
- durable means the local WAL state required for recovery has reached the configured local sync boundary
- remote upload, remote commit, and manifest publication are not required before ack
- the system must expose that the write is locally durable but may still be remotely unpublished

This mode therefore provides local durability first and remote durability later.

### Visibility Model

The system distinguishes between local durability and globally published visibility.

- Local readers on the same node may observe locally durable writes before remote publication completes.
- Readers that rely only on remote-published state must not observe data until publication is complete.
- Manifest visibility remains the authority for remotely published shared state.
- Local recovery state must not be mistaken for globally published state.

This preserves a clean separation between what is durable on one node and what has become part of the shared object-store-backed database state.

### Recovery Semantics

Recovery must account for data that was acknowledged locally but not yet published remotely.

After a node restart in `local_durable_async_remote` mode:

- Tonbo replays locally durable WAL state first.
- Recovery determines which locally durable writes have already been published remotely and which remain pending.
- Pending locally durable writes are re-enqueued for remote publication.
- Publication must be idempotent so recovery can safely retry after partial progress.
- Remote manifest state remains the source of truth for what is globally published.

The recovery contract is therefore: acknowledged local writes survive local crash and restart on the same durable node, even if remote publication was incomplete at the time of failure.

### Node-Loss Semantics

This mode makes node durability scope explicit.

If the node and its durable local disk are lost before remote publication completes:

- acknowledged but unpublished writes may be lost
- this is permitted by the mode contract
- the mode must therefore be documented as local-durable rather than remote-durable

This is the key tradeoff that buys lower write latency.

### Staging Boundary

The minimum required staging boundary is the WAL.

- foreground writes must become recoverable from local durable WAL state
- remote publication may later materialize WAL-derived state into remote WAL objects, SSTs, manifests, or a combination consistent with Tonbo's storage model

This RFC does not require that every intermediate representation be finalized before ack. It requires that the acknowledged write be recoverable and publishable from local durable state.

### Backpressure and Lag

Async remote publication introduces a new lag surface between local durable state and remote published state.

The system must define bounded behavior when remote publication falls behind:

- local staged bytes and pending operations must be measurable
- the mode may apply backpressure once local staging exceeds configured limits
- the system may reject new writes if it cannot preserve the local durability contract safely

This keeps the mode from silently turning remote outages into unbounded local accumulation.

### API and Configuration Expectations

The mode must be explicit in both builder configuration and documentation.

The caller should be able to distinguish clearly between:

- remote durability before ack
- local durability before ack with async remote publication

The semantic distinction matters more than the naming details. The API must make it hard to opt into weaker immediate durability accidentally.

## Alternatives Considered

### Keep remote publication on the foreground path

This preserves the strongest object-store-backed durability semantics, but it keeps the main latency wall on the write path. The current benchmark results suggest this leaves substantial performance on the table.

### Hide local-first behavior behind the existing S3-backed mode

This would be simpler operationally, but it makes durability semantics ambiguous. That is not acceptable for a database durability feature.

### Treat local disk as a cache only

A pure cache model does not help enough if acknowledged writes still depend on remote publication. The purpose of this mode is to move the ack point earlier, which requires local durability rather than ephemeral caching.

## Future Work

- Define exact publication units and idempotency keys.
- Define observability for unpublished local-durable backlog.
- Define whether this mode is single-node only at first or can extend to richer replication topologies later.
- Define benchmark surfaces that compare `remote_durable` and `local_durable_async_remote` directly.
- Define operational controls for draining, fencing, and safe shutdown with outstanding unpublished state.
