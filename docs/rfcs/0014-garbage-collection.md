# RFC: Root-Set Garbage Collection

- Status: Draft
- Authors: Tonbo team
- Created: 2026-03-13
- Updated: 2026-03-16
- Area: Manifest, Compaction, Retention

## Summary

Define a simple GC model for obsolete SST objects based on one rule:
**delete only what is unreachable from the manifest root set**. The first
implementation targets correctness for the **single-process Tonbo model**:
one process owns writes, retention, and GC, while many readers may observe
retained manifest versions through that process. WAL GC stays on its current
floor-driven path and is not merged into the SST GC design. This RFC is
explicitly not a distributed GC design; a distributed Tonbo deployment will
require a different mental model rather than a small extension of this one.

## Motivation

Tonbo already publishes new immutable objects through manifest CAS, but the
current SST GC story is still centered on a single mutable `GcPlanState`. That
shape is not sufficient for the target correctness model:

- a compaction outcome can identify obsolete SST inputs, but that alone does not
  prove they are safe to delete,
- a single mutable GC plan can be overwritten, which is acceptable for a hint
  but not for the source of truth,
- single-writer, multiple-reader correctness needs a clear reclaim rule now,
  before more worker machinery or distributed coordination is added,
- WAL GC is already reasonably isolated and should not be destabilized by trying
  to force one unified GC framework too early.

The immediate problem is therefore narrower than “full distributed GC”: Tonbo
needs a small, defensible reclaim contract that is correct under manifest
versioning and can evolve later.

## Goals

- Define the root set that protects SST objects from deletion.
- Make the root set the only authority for SST deletion.
- Deliver a first implementation that is correct for the single-process Tonbo
  deployment model.
- Keep WAL GC standalone and floor-driven.
- Separate must-have behavior from later enhancements.
- Require observability and benchmarking as part of each GC story.
- Define a minimal metrics set that lets us measure GC cost and benefit.

## Non-Goals

- Unifying WAL GC and SST GC under one worker model.
- Designing append-only GC work-item queues in this RFC.
- Adding checkpoint-aware GC semantics now.
- Designing distributed or multi-process GC semantics.
- Finalizing rate limiting, adaptive cadence, or benchmark-tuned defaults.
- Covering orphan cleanup and repair scans in detail.

## Design

### Current Problem

Today compaction can produce a single GC plan containing obsolete SST and WAL
references. That is useful as a local hint, but it is not a sound correctness
boundary for SST deletion even in a single-process system:

- readers reason from committed manifest versions,
- compaction only sees one moment in time,
- the mutable GC plan can be replaced by later work.

The consequence is simple: **SST GC must not trust the GC plan as deletion
authority**.

### Root Set

The SST GC root set is:

- the current HEAD version,
- any explicit hard pins that already exist in the single-process deployment.

That is the entire must-have root set for the first implementation.

The first release intentionally does **not** require:

- explicit reader pin records,
- checkpoint-aware pins,
- distributed reader registries,
- separate pin objects in the manifest.

The first milestone deliberately avoids adding new retention semantics. If later
evidence shows the initial root set is too small or too blunt, explicit pins or
retention-driven root sets can be added as follow-up work.

This should not be read as a distributed design placeholder. It is a deliberate
single-process simplification:

- one process decides retention,
- one process runs GC,
- one process has authority to translate retained versions into reclaimable
  files.

Once Tonbo supports distributed writers, distributed readers, or independent GC
workers, this simplification stops being sufficient.

### Safety Rule

An SST data file or SST sidecar is eligible for deletion only if it is
unreachable from every version in the root set.

Equivalently:

1. build the root set,
2. collect every SST object path reachable from that root set,
3. delete only manifest-published SST objects that are not in that reachable
   set.

Candidate discovery from compaction is only a hint. It can speed up what the
sweeper looks at first, but it cannot decide safety.

This keeps the safety rule simple:

- manifest versions define visibility,
- the current root set defines which versions remain protected,
- reachability defines deletability.

### Single-Process Correctness

The first GC milestone is correctness for one Tonbo process managing manifest
versions, the current root set, and GC.

The contract is:

- the owning process may publish new SSTs and remove old SST references from
  HEAD,
- SST GC may only delete objects that are unreachable from the current root
  set.

This means the implementation can be conservative without being wrong. If GC
keeps extra files for a while, that is a liveness or cost issue. If it deletes a
file still reachable from the current root set, that is a correctness failure.

### Why This Model Stops at One Process

This RFC intentionally does not claim that the same mental model scales to a
distributed Tonbo deployment.

In a distributed deployment, at least these assumptions break:

- the process performing GC cannot assume it sees every active reader,
- the local root set may no longer be a sufficient proxy for reader protection,
- independent workers may race on planning, deletion, and version visibility,
- reclaim safety may depend on explicit distributed coordination rather than on
  local reachability plus a locally computed root set.

That means the distributed design is not just “this RFC plus a reader pin.” It
will require a different mental model with different safety boundaries.

### Retention Is Deferred

Version-retention policy is intentionally not part of the first SST GC
milestone.

That means:

- no `max_ttl` requirement in this RFC,
- no `max_versions` requirement in this RFC,
- no promise yet about time-travel retention as part of the initial GC design.

If Tonbo later wants retention-driven root sets, that should be added as a
follow-up change after the basic single-process GC path is working and measured.

### WAL GC Is Separate

WAL GC remains floor-driven and standalone.

This RFC does not change the current WAL model:

- `WalFloor` in the manifest remains the protection boundary,
- WAL candidate discovery remains sequence-based,
- WAL pruning remains independent of SST reachability sweeps.

The only shared concept between WAL GC and SST GC is the manifest as metadata
authority. Trying to merge them now would make the SST design more complicated
without improving the immediate correctness target.

### Execution Model

The initial SST GC execution model is intentionally minimal:

1. load HEAD and the current root set from the manifest,
2. compute the reachable SST object set,
3. compare that set with known manifest-published SST objects,
4. delete unreachable objects,
5. treat missing objects as already reclaimed.

This may run in a simple in-process worker or via a manually triggered path.
The implementation may use compaction output as an optimization hint, but not as
deletion authority.

Writers do not need to wait for GC bookkeeping for correctness. If the first
implementation blocks briefly around local cleanup because that is the simplest
way to ship, that is acceptable, provided that:

- publish visibility is still decided by manifest CAS,
- delete failures only delay reclaim,
- GC never becomes part of the commit success condition.

### Must-Haves vs Enhancements

| Category | Must have now | Enhancement later |
| --- | --- | --- |
| Safety authority | Root set from HEAD and existing hard pins | Retention-driven root sets, explicit pins, reader registries, leases |
| Reader protection | Single-process protection from the current root set | Fine-grained pinned versions |
| SST discovery | Reachability over manifest-published SSTs | Durable per-event GC queues |
| WAL GC | Keep current floor-driven path | Optional operational convergence only if proven useful |
| Benchmarking | Baseline-first benchmark story before enablement | Remote-store simulations, cadence tuning |
| Retention policy | Deferred | `max_ttl`, `max_versions`, time-travel retention |
| Checkpoints | Out of GC scope; copy a chosen version elsewhere | Rich checkpoint/tag integration |
| Metrics | Minimal cost/benefit metrics | Full backlog, lease, and rate-limit dashboards |
| Rate limiting | Undefined for now | Delete throttling and service isolation |

For clarity, the right reading of the “enhancement later” column is:

- items that strengthen the single-process design without changing its core
  mental model,
- not a promise that distributed GC can be reached by incrementally extending
  this RFC.

### Minimal Metrics

The first implementation should measure only what helps judge whether GC is
worth running and whether it harms the foreground path:

- bytes deleted,
- SST objects deleted,
- reclaim run duration,
- write latency before/after enabling GC,
- read latency before/after enabling GC,
- physical size before/after GC.

These metrics are enough to answer the practical question: what storage savings
do we get, and what latency cost do we pay?

### Baseline-First Benchmarking

GC should not be enabled by default based only on correctness tests. Each GC
milestone should start with a benchmark-baseline story that captures the
pre-change behavior, then re-run the same scenarios after implementation.

The benchmark workflow should follow two simple rules:

1. capture a baseline before the GC change or before enabling the new path,
2. compare against that baseline after the change using the same workload.

The initial benchmark set should answer:

- what is write latency before and after GC,
- what is read latency before and after GC,
- how many bytes and objects GC reclaims,
- how much persistent size remains before and after GC,
- how much extra manifest or object-store work GC causes.

This is intentionally close to how neighboring projects already work:

- `aisle` keeps explicit baseline docs and Criterion comparisons,
- `fusio-manifest` wraps benchmark storage paths with counters so I/O behavior is
  directly measured.

Tonbo should do the same for GC: define the baseline first, then use that
baseline to justify shipping or enabling the feature.

### Observability Requirement

Observability is part of the definition of done for every GC story.

At minimum, each story should specify:

- what counters or timings are added,
- how benchmark runs will capture them,
- which correctness or cost question those measurements answer.

If a story changes GC behavior but does not say how to observe or benchmark that
change, the story is incomplete.

### Distributed Tonbo

When Tonbo moves beyond the single-process deployment model, GC should be
re-specified in a new RFC instead of treating this one as the distributed
baseline.

That future RFC should start by redefining:

- who owns reader protection,
- who owns delete authority,
- what metadata forms the distributed root set,
- what coordination model replaces the single-process simplifications used here.

### Checkpoints

GC does not need checkpoint-specific semantics in the first design.

A checkpoint can stay simple: copy a chosen version and its referenced data to a
separate location, bucket, or prefix. Once copied, that checkpoint is outside
the online GC contract for the source database.

### Failure Model

The narrow failure model for the first implementation is:

- if GC crashes before delete, nothing is reclaimed,
- if GC crashes after deleting some files, retry is safe because missing files
  are treated as already deleted,
- if the root set is computed incorrectly, SST GC can become unsafe,
- if the root set is conservative, reclaim is delayed.

The main correctness work is therefore not “build more GC machinery”; it is
“compute and retain the right root set”.

## Alternatives Considered

### Treat Compaction Output as the Deletion Authority

Rejected because compaction does not define the full reader-visible protection
surface. It can suggest likely garbage, but it cannot replace root-set
reachability.

### Unify WAL GC and SST GC Immediately

Rejected because WAL GC already has a simpler and more stable safety boundary.
Forcing a shared worker or queue model now would make the SST design harder to
ship without improving correctness.

### Add Explicit Reader Pins Before Shipping Any SST GC

Rejected for now because it delays the first correct implementation. The first
milestone should not pull in extra reader-protection semantics before the basic
single-process GC path exists.

## Open Questions

1. Should the first SST sweep run only as an explicit maintenance action, or as
   a lightweight in-process background loop?
2. Should Tonbo keep GC benchmarks in the main crate, or in a dedicated bench
   workspace similar to adjacent repos once the scenarios grow?

## Future Work

- Explicit pinned versions for long-lived readers.
- Retention-driven root sets using `max_ttl` and `max_versions`.
- Distributed reader protection for shared-storage multi-process deployments.
- Durable GC work queues if liveness or observability requires them.
- Checkpoint tags and named pins if the product needs in-place retained copies.
- Delete throttling once benchmark data exists.
- Orphan discovery and repair scans as a separate maintenance lane.
