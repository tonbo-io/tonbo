# RFC: Garbage Collection and Retention

- Status: Draft
- Authors: Tonbo team
- Created: 2026-03-13
- Area: Manifest, Compaction, Retention, Observability

## Summary

Define Tonbo's garbage collection model for obsolete WAL and SST objects on
local filesystems and object storage. The design makes manifest reachability the
source of truth for reclaim safety, treats GC work items as durable background
maintenance records, and rolls the feature out in stages so users get
meaningful value before the full serverless-safe design is complete. The RFC
also defines the user-facing guarantees of each phase, the observability needed
to operate GC, and the benchmark requirements needed to set sane defaults.

## Motivation

Tonbo already publishes new immutable objects during flush and compaction, but
it does not yet provide a complete product story for reclaiming the old ones.
This creates four problems:

- physical bytes can grow well above logical live data until old objects are
  reclaimed,
- current compaction output can identify obsolete inputs, but there is no
  standalone GC contract for when they may be safely deleted,
- retention, time travel, and active readers are not yet tied into one clear
  reclaim model,
- benchmarking and operator tuning need first-class GC metrics rather than ad
  hoc probes.

GC therefore needs to be treated as a core storage subsystem, not as a minor
follow-on to compaction.

## Goals

- Define a snapshot-safe reclaim contract for WAL and SST objects.
- Define the root set that protects objects from deletion.
- Define the durable unit of GC work and the worker execution model.
- Define a staged rollout where each phase delivers a clear user-facing benefit.
- Define the observability and benchmark requirements needed to tune GC.
- Preserve Tonbo's object-store-first, write-new-only design.

## Non-Goals

- Finalizing compaction heuristics or level sizing policies.
- Designing range tombstones in this RFC.
- Defining a global coordinator service outside manifest-backed leases.
- Covering every future sidecar format in detail; this RFC defines the object
  model they must fit into.
- Replacing the logging and metrics layering RFCs; this RFC builds on them.

## Design

### Terminology

- **Root set**: the set of metadata references that currently protect physical
  objects from deletion.
- **Retained version**: a committed manifest version kept for time travel,
  policy retention, or explicit pinning.
- **Active snapshot**: a reader-visible snapshot that has not yet released its
  protection over the referenced version.
- **GC work item**: a durable record that some objects became reclaim
  candidates for a specific reason.
- **Reclaim lag**: the time between an object becoming obsolete and being
  physically reclaimed.

### Scope of GC

Tonbo GC is responsible for reclaiming four classes of objects:

- obsolete WAL segments no longer needed by any retained version,
- obsolete SST data objects replaced by flush or compaction outputs,
- obsolete delete-sidecar or future SST-adjacent sidecar objects,
- orphaned uploads that were written but never became manifest-visible.

The first live phases focus on obsolete WAL and SST objects. Orphan cleanup is a
follow-on repair lane because it does not share the same user-facing retention
semantics.

### Safety Model

GC safety is defined by **reachability**, not by compaction output alone.

An object is eligible for deletion only if all of the following are true:

1. It is not referenced by the current HEAD version.
2. It is not referenced by any retained historical version.
3. It is not protected by an active snapshot or other explicit pin.
4. It is not still inside any configured grace or retention window.

Compaction, flush, or WAL-floor movement may identify reclaim candidates, but
those signals are only advisory. The final delete decision must revalidate
against the root set at delete time.

This keeps GC idempotent and crash-safe:

- if compaction crashes after publishing a work item, the worker can recheck
  reachability later,
- if a worker crashes after deleting some objects, retries can treat missing
  objects as already reclaimed,
- if manifest state changes between plan and sweep, revalidation prevents
  deleting a newly protected object.

### Root Set

The root set for GC consists of:

- the current HEAD version,
- retained historical versions kept by policy,
- active reader snapshots,
- explicit user pins such as future checkpoints or tagged versions.

This RFC treats the root set as the authoritative protection surface for GC.
Any future feature that promises historical visibility must attach itself here.

### Retention Semantics

Tonbo retention applies to committed manifest versions, not directly to SST or
WAL objects. Physical objects remain protected as long as they are reachable
from any retained version or explicit pin.

Tonbo retains historical versions using both time-based and count-based
retention:

- `max_ttl`: retain versions newer than `now - max_ttl`
- `max_versions`: retain at least the newest `max_versions` committed versions

A committed version is retained if any of the following are true:

1. it is the current HEAD version,
2. it is explicitly pinned by an active snapshot, checkpoint, tag, or other
   root-set extension,
3. it is within `max_ttl`,
4. it is among the newest `max_versions` committed versions.

A committed version becomes GC-eligible only when all of the above are false.

This is intentionally conservative. The time-based rule gives users a
predictable time-travel window, while the count-based rule protects low-write
tables from losing too many rollback points when writes are infrequent.

Recommended provisional defaults for the first release are:

- `max_ttl = 24h`
- `max_versions = 10`

These defaults should be refined once GC and retention benchmarks are in place.

### GC Work Items

Tonbo should represent GC work as durable, append-only work items rather than a
single mutable table-scoped plan.

Semantically, each work item contains:

- the table it belongs to,
- why the objects became obsolete,
- when they became obsolete,
- the objects that may now be reclaim candidates,
- current worker ownership or lease state,
- retry or failure history.

The important contract is that GC work items are **durable hints**, not the
deletion authority. They make background cleanup incremental and measurable, but
the worker still validates reachability before removal.

The object payload should be generic enough to represent:

- SST data files,
- delete sidecars,
- future SST sidecars or indexes,
- WAL segment files,
- orphaned uploads discovered by repair scans.

### Transitional GC Work Model

Tonbo adopts a phased GC work model.

In early reclaim phases, Tonbo may represent pending GC as a single mutable
per-table plan (`GcPlanState`). This plan is a durable hint only. It is not the
authority for deletion safety, and it may be overwritten by newer producers
before a worker drains it. Losing or replacing a hint may delay reclaim, but it
must not permit unsafe deletion because workers always revalidate candidates
against the current root set at sweep time.

This transitional model is acceptable for:

- observability-only phases,
- conservative single-process reclaim,
- local embedded deployments where reclaim liveness is best-effort.

Once Tonbo introduces durable background GC with retries, leasing, or
multi-process/serverless workers, it must migrate from the per-table mutable
plan to append-only GC work items.

In the append-only model, each work item records:

- table identity,
- obsolescence reason,
- time of obsolescence,
- candidate objects,
- lease or ownership state,
- retry and completion state.

This migration changes GC liveness and operability, but not GC safety: in both
models, manifest reachability and root-set revalidation remain the source of
truth for delete eligibility.

### Execution Model

Tonbo GC follows a `record -> claim -> revalidate -> sweep -> acknowledge`
model.

- **Record**: flush, compaction, WAL-floor movement, or repair scanning emits a
  GC work item.
- **Claim**: a worker leases a work item for bounded time.
- **Revalidate**: the worker computes the current root set and filters any
  still-protected objects out of the candidate set.
- **Sweep**: the worker deletes the remaining objects through Fusio, treating
  missing objects as already reclaimed.
- **Acknowledge**: the worker records completion, retry, or retention blocking.

This model is intentionally compatible with both embedded and serverless
execution:

- embedded deployments can run a small in-process worker,
- multi-process deployments can use manifest-backed leases,
- object storage remains the source of truth for visibility.

### Rollout Path and User-Facing Guarantees

GC should be shipped as a sequence of capability phases rather than one large
feature.

| Phase | Engine capability | Customer gets | Not promised yet |
| --- | --- | --- | --- |
| 1. Observability | GC metrics and backlog accounting | Users can see obsolete bytes, reclaim lag, and blocked reclaim instead of guessing | No automatic space reclaim |
| 2. Conservative local reclaim | Single-process safe GC using manifest reachability, local snapshot protection, and a per-table mutable GC plan as a best-effort hint | Local and embedded deployments start recovering space after compaction | No durable reclaim backlog, no multi-process or serverless snapshot guarantees |
| 3. Durable background GC | Append-only work items, leases, retries, and automatic drain | Cleanup happens without manual operator action and reclaim work survives worker turnover | Retention and long-lived distributed readers still conservative |
| 4. Distributed snapshot-safe GC | Manifest-backed reader protection and root-set enforcement across processes | Safe GC for serverless and shared-object-store deployments | Aggressive retention tuning may still be disabled |
| 5. Retention controls | Enforced TTL / max-version policies and explicit pins | Users can trade storage cost against time-travel window predictably | Delete-heavy workloads may still retain excess history |
| 6. Delete-heavy optimization | Tombstone-aware reclamation and stronger compaction/GC cooperation | Better storage and read amplification on overwrite/delete workloads | Final defaults still depend on benchmark results |
| 7. Benchmarked defaults | Benchmark-backed cadence and retention defaults | Operators get documented expectations and sane defaults | N/A |

### Observability Requirements

GC is not complete unless it is observable.

Tonbo should expose GC metrics that answer:

- how much obsolete data is pending reclaim,
- how long reclaim is taking,
- what fraction of reclaim is blocked by retention or readers,
- how many delete operations succeeded, retried, or failed,
- how much physical amplification remains after compaction.

These metrics should integrate with the layered observability design so they can
be used in:

- benchmark artifacts,
- local debugging,
- operator polling,
- production dashboards.

### Verification and Benchmark Requirements

GC needs both correctness validation and economic benchmarking.

Correctness coverage should prove:

- GC never deletes objects still reachable from the root set,
- retries and partial deletes are idempotent,
- restart after worker crash preserves correctness,
- retention and pins delay reclaim as specified.

Benchmark coverage should prove:

- how long obsolete data remains before reclaim,
- how retention changes physical amplification,
- whether GC work disturbs read latency,
- what delete request and storage-cost patterns result from the chosen cadence.

The benchmark program should therefore treat GC as a first-class engine scenario
rather than as an implementation footnote.

## Alternatives Considered

### Compaction-only delete with no reachability revalidation

Rejected because it ties correctness to one producer path and makes crashes,
retention changes, and late-arriving reader protection harder to reason about.

### Full prefix sweep with no durable work items

Rejected as the primary model because it is harder to measure reclaim lag and
harder to attribute delete work back to the events that created obsolete data.
It remains useful as a repair and orphan-discovery lane.

### Global coordinator outside the manifest

Rejected because it conflicts with Tonbo's object-store-first, stateless
execution model. Manifest-backed leases are sufficient for the coordination
needed here.

## Current Implementation Recommendations

The following recommendations reflect the current Tonbo phase and should guide
near-term implementation unless new evidence from benchmarks or prototype work
forces a revision.

1. **Do not block producers on GC bookkeeping**
   Flush, compaction, and WAL-floor movement should not block user-visible
   completion on durable GC work recording. Reclaim bookkeeping may lag or be
   rediscovered later, but reclaim correctness must continue to depend on
   root-set revalidation rather than producer-side waiting.

2. **Use process-local reader protection for the current single-process phase**
   For embedded and single-process reclaim, process-local snapshot/reader
   protection is acceptable. Manifest-backed reader protection becomes mandatory
   before Tonbo allows SST deletion in shared-storage, cloud, or multi-process
   deployments where the deleting process cannot directly observe every reader.

3. **Keep WAL candidate discovery floor-driven**
   WAL reclaim candidate discovery should remain driven by the WAL floor, since
   WAL retention is naturally more linear than SST reachability. Execution,
   retry, and observability can still converge later on shared GC worker
   machinery, but WAL does not need to abandon floor-based discovery to fit the
   general model.

4. **Treat retention defaults as provisional**
   The proposed defaults (`max_ttl = 24h`, `max_versions = 10`) are useful
   placeholders, but they should remain benchmark-driven tuning levers rather
   than hard commitments at this stage.

5. **Model checkpoints and tags as root-set pins from the beginning**
   Even if public checkpoint/tag APIs ship later, Tonbo should treat them as
   ordinary root-set pins in the design from the start so GC semantics do not
   need a later conceptual rewrite.

6. **Keep orphan repair separate from the steady-state reclaim lane**
   Orphan discovery is closer to repair/audit work than to normal reclaim and
   should remain a separate periodic lane unless implementation experience shows
   that combining the two materially simplifies operations without harming
   object-store cost or latency behavior.

7. **Require delete-rate limiting before calling GC production-ready**
   GC should not be allowed to issue unbounded delete traffic. Some form of
   delete-rate limiting or service isolation is required before benchmark
   results are treated as representative of production behavior.

## Open Questions

1. Are there any narrow failure modes where a producer should still wait for GC
   work recording even though the general recommendation is non-blocking?
2. What is the smallest manifest-backed reader-protection mechanism that is safe
   enough for the first shared-storage or cloud deployment?
3. How much shared worker machinery should WAL GC reuse without losing the
   clarity of floor-driven WAL candidate discovery?
4. What retention defaults do benchmarks justify once reclaim lag, storage
   amplification, and delete cost are measured on realistic workloads?
5. Can checkpoint/tag pin semantics be introduced internally now without adding
   public API or metadata churn that would distract from baseline GC delivery?
6. What delete-rate limit and service-isolation settings keep GC from distorting
   read/write latency under production-like load?

## Future Work

- Range tombstones and their reclaim semantics.
- Cross-table resource isolation and per-tenant GC accounting.
- Adaptive GC cadence based on backlog, request cost, or read-tail impact.
- Additional benchmark scenarios once the baseline GC window is instrumented and
  stable.
