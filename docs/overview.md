# Overview

**Tonbo** is an **open-source, Arrow-native embedded database** designed for **serverless and edge-first online data analytics**.
It provides a lightweight, in-process engine that lets developers run analytical queries **close to their applications** — whether in cloud functions, edge runtimes, or browser environments — while keeping data stored in **open formats (Apache Arrow and Parquet)** on **object storage**.

Tonbo combines the flexibility of embedded databases with the scalability and efficiency of modern data lakes:

- **Arrow/Parquet native:** fully compatible with Arrow data types and columnar semantics.
- **Edge-first runtime:** embeddable into Deno, Node.js, or other serverless runtimes as stateless query engines.
- **Object-storage optimized:** interacts natively with S3-compatible backends and fully leverages object storage throughput, parallelism, and cost efficiency.
- **Unified analytics layer:** supports real-time, scalable analytics with foundation for pluggable index types (vector, inverted, graph planned for future releases).
- **Composable with existing engines:** designed for interoperability with query engines; DataFusion TableProvider and Postgres FDW integrations are on the roadmap.

Unlike conventional databases that abstract storage behind a local filesystem, Tonbo directly orchestrates Arrow and Parquet files on object storage, enabling fine-grained compaction, tiered caching, and zero-copy reads across distributed environments.

## I/O

Tonbo’s I/O layer is powered by **[Fusio](https://github.com/tonbo-io/fusio)**, an async I/O substrate built to unify local files, object storage, and browser environments under one consistent interface.
It ensures that Tonbo can read and write Arrow- and Parquet-based data efficiently—no matter whether it’s running on a local node, a serverless edge function, or a browser runtime.

### Core Design Goals

- **Runtime-portable, Arrow-first:**
  Fusio adapts to multiple async runtimes—including **tokio**, **monoio**, and **io_uring**—without changing Arrow/Parquet semantics or introducing runtime-specific dependencies.
  This allows Tonbo to achieve low-latency, non-blocking I/O across Linux, serverless, and embedded environments while keeping the same Arrow-native core.

- **Object-storage-native:**
  Fusio treats **S3-compatible storage as a first-class backend**, not just a remote filesystem.
  It fully exploits object storage throughput, parallel uploads, and low cost through multipart uploads, conditional commits, and lazy reads.

- **Unified durability semantics:**
  A consistent durability model defines when data is visible and safe across backends.
  Local disks use `fsync`/`fdatasync`, while S3 maps to “multipart complete” commits; both follow the same contract (`Flush -> Data -> All -> Commit`).

### Why It Matters

Fusio allows Tonbo’s storage engine (WAL, manifest, Parquet SSTs) to operate seamlessly on both **local disks and object storage**, removing the traditional boundary between embedded databases and data lakes.
Developers can run the same workload—compaction, checkpointing, or indexing—anywhere the code runs, without changing configuration or sacrificing consistency.

### Conceptual Diagram

```
       +---------------- Tonbo Engine (WAL / SST / Compaction) ----------------+
       |                                                                       |
       |               Unified Async I/O Traits: Read / Write / Fs             |
       |                               |                                       |
       +-------------------------------+---------------------------------------+
                                       |
                 Fusio Runtime Abstraction & Durability Model
                                       |
               +-----------+-----------------------+--------------+
               | Local Disk | Object Store (S3/R2) | Browser OPFS |
               +-----------+-----------------------+--------------+
```

### Full Async

Tonbo’s I/O layer, powered by **Fusio**, is *fully asynchronous from end to end*. Every read, write, and filesystem operation returns a `Future`, implemented using lightweight traits (`Read`, `Write`, `Fs`, `Executor`, `Timer`) that abstract away the underlying runtime. Fusio unifies **poll-based runtimes** (like `tokio`) and **completion-based runtimes** (`tokio-uring`, `monoio`) through zero-cost trait dispatch, enabling Tonbo to achieve native async I/O performance on any platform.

This design removes runtime awareness from Tonbo’s core: the database can run on a thread-pool executor, an edge worker, or even a WASM environment with OPFS, all while maintaining non-blocking behavior and consistent durability semantics.

## Metrics & Benchmarks

Tonbo exposes a stable metrics snapshot API (`DB::metrics_snapshot()`) that powers both
benchmarks and production monitoring. The benchmark harness relies on public APIs only and
currently covers scenario runs (write-only, read-only, mixed, compaction) plus component
benches (memtable, WAL, SST encode, iterator). Cache metrics are reserved until a cache
implementation lands, so the snapshot leaves that section empty for now.

## First-class Object Storage

Tonbo treats **object storage as the primary substrate** for durability and coordination—not just as a backup layer.
Because objects are **immutable**, Tonbo’s storage engine is built around an **append-only, merge-tree architecture**: every write produces new segment or Parquet files, and background compaction merges them into larger, optimized datasets.
This design eliminates in-place mutation and makes Tonbo naturally compatible with S3-style semantics.

### Core Design Goals

- **Immutable storage model:**
  Data is never rewritten. Segments are appended and compacted asynchronously, ensuring durability and easy recovery without locks or distributed transactions.
- **Conditional commits:**
  Metadata updates (manifests, checkpoints, GC plans) rely on **conditional PUTs**—atomic “compare-and-swap” operations using object-store versioning or ETags.
  This provides **atomic visibility** and replaces centralized coordinators like etcd.
- **Serverless concurrency:**
  Any function or worker can participate in writing or compacting, as shared state is synchronized purely through object storage.
  The system remains stateless and horizontally scalable.

### Why It Matters

By combining immutable objects with atomic metadata commits, Tonbo achieves the essential properties of a **serverless database**:
durable global state, stateless execution, and deterministic recovery—all built directly on object storage primitives.

### Conceptual Diagram

```
     +---------------------- Tonbo Storage Engine ----------------------+
     |   Write-Ahead Log (WAL)   ->   Append-only Segments (SSTables)    |
     |                                +   Conditional Manifest          |
     +------------------------------------------------------------------+
                                     |
                         Object Storage (S3/R2/MinIO)
            Immutable Objects + Conditional PUT + Atomic Versioning
```

## Merge-Tree

Tonbo adopts a **columnar LSM (Log-Structured Merge) Tree** architecture tailored for object storage. Rather than rewriting data in place, Tonbo continuously **appends new immutable segments**  (Arrow/Parquet files) and **merges them asynchronously** through compaction. This merge-tree model unifies **streaming ingestion**, **snapshot isolation**, and **object-storage persistence**, making Tonbo capable of supporting real-time analytics over immutable, versioned data.

It forms the backbone of Tonbo’s serverless engine:

- **Write path** -> append-only, durable to object storage via Fusio.
- **Read path** -> read and scan data,  let users describe what data they want and supports skip data efficiently.
- **Compaction path** -> columnar merge of Parquet segments with preserved statistics and filters.
- **Manifest path** -> atomic snapshot lineage managed across distributed write / read process.

### Why It Matters

The merge-tree architecture is what makes Tonbo compatible with object storage and viable as a serverless database. Traditional B-Tree or page-based designs rely on in-place mutation and random writes, which are incompatible with immutable, eventually consistent object stores. Tonbo’s columnar LSM Tree, in contrast, writes only new immutable segments and atomically updates manifests—perfectly aligning with S3’s append-only, conditional-commit model.

This design brings three critical advantages:

- **Immutability as a feature:** every write produces new Parquet segments; object storage’s versioning and durability become part of the database’s safety model.
- **Stateless compute:** since state lives entirely in immutable objects and manifests, any function or node can serve as a temporary writer or compactor.
- **Scalable economics:** separating compute from storage allows data to persist cheaply on S3, while compute scales elastically through serverless runtimes.

In short, the merge-tree turns object storage from a passive file system into an active, versioned database substrate—enabling Tonbo’s vision of an open, serverless analytics layer.

### Conceptual Diagram

```
                    +------------- Tonbo Merge-Tree Engine ---------------+
                    |                                                     |
        Write Path  |  WAL -> MemTable(mutable->immutable) -> L0 SSTables |
                    |               ^                   ^                 |
        Read Path   |   Snapshot <- Manifest <- Compaction Scheduler      |
                    |                   v                                 |
                    |          L1..Ln SSTables (Parquet on S3)            |
                    +-----------------------------------------------------+
                                         |
                                   Object Storage
                Immutable Parquet Segments + Conditional Manifest Versions
```

### Data Model

Tonbo’s merge-tree storage model organizes data into several logical components that together maintain durability, consistency, and efficient read/write performance.

- **MemTable**
  - *Mutable*: an in-memory, ordered key–value structure that handles live writes and reads. Each insert or delete is first appended to the WAL, then applied here for immediate visibility. When the memtable reaches its size threshold, it becomes immutable.
  - *Immutable*: a frozen snapshot of a memtable, converted to Arrow columnar format for efficient read and scan. It supports range queries and projection via index and Arrow record batches.

- **SsTable**
  - *Level 0*: unsorted SSTables generated from flushed immutable memtables. Multiple Level 0 files may overlap in key range and are compacted regularly into higher levels.
  - *Level N*: sorted, non-overlapping SSTables stored in Parquet format. Higher levels represent progressively compacted, read-optimized datasets distributed across local disk or object storage.

- **WAL**
  A sequential log that records every write before it reaches memory or disk. It guarantees durability across failures and supports both single and batched writes (`Full`, `First`, `Middle`, `Last`). The WAL exists in three layers—memory buffer, local filesystem, and remote object storage—flushed asynchronously via Fusio.

- **Manifest**
  The manifest is the database’s versioned metadata log. It tracks all SST files and their levels, deleted WALs, and checkpoints, maintaining atomic snapshot lineage. Updates to the manifest use conditional commits on object storage, ensuring atomic visibility across distributed processes.

Together, these layers define Tonbo’s logical data model: mutable writes flow through WAL -> MemTable -> immutable flush -> SsTable -> Manifest, forming an append-only, versioned architecture optimized for object storage and serverless execution.

### Write Path

Tonbo’s write path is fully asynchronous and append-only, ensuring durability and low write latency.
It begins when a user issues a write operation and ends once the data is durably persisted as Level-0 SSTables through minor compaction.

1. **WAL append:**
   Each write is first recorded in the Write-Ahead Log to guarantee durability and recoverability. The WAL is asynchronously flushed through Fusio to local or remote storage.

2. **MemTable update:**
   The same entry is inserted into the in-memory `MutableMemTable`, which maintains a sorted view of active data for immediate reads.

3. **Freeze and flush:**
   When the MemTable reaches its configured threshold, it is frozen into an `ImmutableMemTable` and queued for flush.

4. **Minor compaction:**
   Frozen memtables are merged and serialized into Parquet-based SSTables (Level 0). These new SSTs are uploaded via Fusio and referenced in the manifest using conditional commits, marking the data as durable and visible.

#### WAL

Tonbo’s WAL ensures durability before data enters the in-memory structures. Writes are first appended into a buffered sequence of log entries and grouped into **immutable segments** (objects) suited for storage on S3-like backends. Each segment is uploaded via Fusio’s async I/O layer and atomically published using conditional PUT/CAS, ensuring correct global ordering even in serverless, multi-writer environments. A lightweight WAL manifest tracks segment sequence numbers, offsets, and visibility state, enabling reliable crash recovery: upon startup, Tonbo replays segments into the MemTable before proceeding to flush/compaction. This design simulates a continuous append-only log over object storage—bridging the durability of a traditional WAL with the scalability of S3.

##### Configuring the WAL via `DbBuilder`

By default, durable builders create a `WalConfig` using layout-derived defaults (directory, filesystem bindings, state store). Most deployments don’t need additional tuning:

```rust,no_run
use tonbo::db::{DB, DbBuildError, DbBuilder, DynMode};

async fn bootstrap(schema: arrow_schema::SchemaRef) -> Result<(), DbBuildError> {
    let _db: DB<DynMode, _> = DbBuilder::from_schema_key_name(schema, "id")?
        .on_disk("/srv/tonbo")
        .build()
        .await?;
    Ok(())
}
```

When low-level tweaks are required (e.g., forcing synchronous fsyncs for benchmarking), call `DbBuilder::wal_config` with a [`db::WalConfig`] struct or the convenience setters:

```rust,no_run
use std::time::Duration;
use tonbo::db::{DB, DbBuildError, DbBuilder, DynMode, WalConfig, WalSyncPolicy};

async fn bootstrap(schema: arrow_schema::SchemaRef) -> Result<(), DbBuildError> {
    let overrides = WalConfig::default()
        .sync_policy(WalSyncPolicy::Always)
        .flush_interval(Duration::from_millis(1));
    let _db: DB<DynMode, _> = DbBuilder::from_schema_key_name(schema, "id")?
        .on_disk("/srv/tonbo")
        .wal_config(overrides)
        .build()
        .await?;
    Ok(())
}
```

### Compaction Path

Tonbo’s compaction path runs fully asynchronously, transforming short-lived in-memory segments into durable, query-optimized SSTables stored on object storage.

#### Minor Compaction

When a memtable freezes, its immutable snapshot is merged and serialized directly into Level-0 Parquet SSTables. These SSTs are written via Fusio to S3 (or configured storage) and atomically published in the manifest through conditional commits, ensuring visibility without local staging files.

#### Major Compaction

> **Status:** The compaction executor and planner are implemented; background scheduling is pending (#550).

Major compaction merges L0..Ln SSTs when level thresholds are exceeded. The process pulls SSTs from object storage, merges them using streaming readers, uploads the compacted results back to S3, and updates the manifest atomically. Old SSTs are retired asynchronously through garbage collection. Currently triggered via admin API; automatic background scheduling is in progress.

#### Design Characteristics

- **Stateless and async:** any process can perform compaction safely.
- **Object-storage optimized:** no object mutation—new SSTs are appended and atomically switched.
- **Crash-safe lineage:** every compaction produces a new manifest version, ensuring consistent recovery.
- **Heuristic-first triggers:** minor compaction is automatic by default; major compaction background loop is being added (#550).

#### Garbage Collection

> **Status:** WAL floor tracking implemented; SST GC worker pending (#547).

Tonbo's GC is **manifest-driven and snapshot-safe**, designed for object storage:

- **Reachability by manifest:** only delete WAL/SST objects **not referenced** by the HEAD manifest or any **retained versions** (time-travel).
- **Snapshot grace:** respect active snapshot timestamps so **no in-use object** is collected.
- **Write-new only:** incomplete/unpublished objects are never visible and can be safely removed; GC targets **superseded SSTs**, **old WAL fragments**, and **orphaned uploads**.
- **Asynchronous & idempotent:** runs in the background (via compactor or a small GC worker) as a simple **plan -> sweep** routine; safe to retry, no local state.
- **Serverless-friendly:** correctness lives in **object storage + manifest**; GC reduces read amplification and object/list costs without centralized coordination.

### Read Path

Tonbo executes reads in **two phases** to minimize remote I/O while preserving snapshot/MVCC semantics over the merge-tree.

**Phase 1 — Plan & Prune (manifest-driven)**
Resolve a **snapshot manifest** (HEAD or a specific version), then build a minimal scan plan by pruning:

- sources (txn/mutable/immutable, L0..Ln SSTs),
- row-groups/segments via min/max & stats,
- columns via projection,
- ranges & Parquet bloom filters (sidecar indexes like inverted/vector are planned),
  producing the exact set of objects/row-groups/columns for Phase 2.

```
================= Phase 1: Plan & Prune =================

+----------------------------+
| Manifest Snapshot          |
| (HEAD or specific version) |
+-----+----------------------+
      |
      v
+-----+------+
|   Planner  |
| normalize  |
| predicates |
+-----+------+
      |
      |
      |        +-----------------------------+  +-----------------------------+
      |        | Sources                     |  | Sidecar Indexes (planned)   |
      |        | Txn / Mem / Imm / L0..Ln    |  | vector / inverted / bitmap  |
      |        | metadata + file statistics  |  +-------------+---------------+
      |        +-----------+-----------------+                |
      |                    |                                  |
      |                    |                                  |
      |                    |                                  |
      |                    |                                  |
      |                    |                                  |
      |                    |                                  |
      |                    |                                  |
      +--------+-----------+----------------------------------+
               |
               v
      +--------+--------+
      |     Pruner       |
      |  levels / stats  |
      |  page indexes    |
      |  PK range filter |
      +--------+--------+
                  |
                  v
      +--------+--------+
      |   PLAN (RowSet,  |
      |   Projection,    |
      |   Residuals)     |
      +------------------+
```

**Phase 2 — Merge & Materialize (columnar streaming)**
Open only the planned sources and perform a **k-way, heap-driven async merge** across txn/mutable/immutable and L0..Ln:

- enforce **key order** (asc/desc) with bounded memory (one pre-peek per source),
- apply **MVCC** (snapshot timestamp) and **latest-wins** across overlaps,
- support **early termination** (e.g., `LIMIT n`) and backpressure,
- stream **only needed column chunks**, then package into Arrow `RecordBatch`es.

```
==================== Phase 2: Merge & Materialize ====================

+-----------+-----------+
| Open planned objects  |
| via Fusio             |
+-----------+-----------+
            |
            v
+-----------------------+
| MergeStream           |
| k-way, MVCC, ordered  |
| early LIMIT, backpress|
+-----------+-----------+
            |
            v
+-----------------------+
| PackageStream         |
| columnar packer       |
+-----------+-----------+
            |
            v
+-----------------------+
| Arrow RecordBatches   |
+-----------+-----------+
            |
            v
+-----------------------+
| Consumer              |
| (Tonbo Rust API)      |
+-----------------------+
```

#### Scan Pipeline

```
Selected sources (txn/mutable/immutable/L0..Ln)
          |
          v
MergeStream --(k-way, MVCC, ordered, LIMIT/backpressure)--> PackageStream --> Arrow RecordBatches
```

#### Why this design

The two-phase plan+scan **cuts object-storage bytes and tail latency**, and the on-line async merge reconciles in-flight memory with multi-level SSTs under a snapshot—ideal for **edge/FaaS** cold starts (small plan, precise scan).

## MVCC

Tonbo’s MVCC is built for serverless, object-storage–native operation: all durable state is **append-only** (WAL fragments, Parquet SSTs) and **visibility is defined by the manifest** via **conditional (CAS) commits**.

### Core Model

- **Monotonic timestamp (`Ts`)**: strictly increasing logical time assigned to writes (per entry or per batch).
- **Snapshot**: `(manifest_version, Ts_snapshot)` captured at read/txn begin.
- **Visibility (per key)**: an entry is visible iff it’s referenced by the **snapshot’s manifest** and `write_ts ≤ Ts_snapshot`, and not tombstoned by any `delete_ts ≤ Ts_snapshot`.
- **Write-new only**: new generations are created and then **atomically published** by manifest CAS; no in-place mutation.

### Write & Publish

- **Durability**: append to **WAL fragment** (immutable object) -> apply to **Mutable MemTable** -> on freeze/flush, create **L0 SSTs** and **CAS-publish** manifest edits.
- **Idempotent publish**: manifest CAS makes retries safe; incomplete uploads remain invisible.

### Transactions

- **Optimistic, short-lived** (seconds): begin captures a snapshot; writes stage locally.
- **Commit**:
  - per-key **conflict check** vs. snapshot (no newer committed version),
  - **atomic batch** apply with a single manifest CAS,
  - optional **idempotency key** to make retries safe across cold starts.
- **Commit modes** (tunable):
  - `strict`: wait for the WAL writer to report durability before acknowledging the client (manifest edits are not in the transactional path yet but will piggyback here later).
  - `fast`: return once WAL frames are queued; durability is asynchronous, so a crash before fsync can drop acknowledged commits.

Public API (dynamic mode today):

```rust,no_run
use std::sync::Arc;
use fusio::executor::BlockingExecutor;
use tonbo::db::{CommitAckMode, DB, DbBuilder, DynMode, Transaction};
use tonbo::key::KeyOwned;

async fn run_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(build_arrow_schema()); // application-defined helper
    let executor = Arc::new(BlockingExecutor);
    let mut db: DB<DynMode, _> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .with_commit_ack_mode(CommitAckMode::Fast) // optional: trade durability for latency
        .in_memory("txn")?
        .build_with_executor(executor.clone())
        .await?;

    let mut tx: Transaction = db.begin_transaction().await?;
    let incoming_batch = build_record_batch(); // supply your Arrow RecordBatch
    tx.upsert_batch(&incoming_batch)?;
    tx.delete(KeyOwned::from("row_to_remove"))?;
    tx.commit(&mut db).await?;
    Ok(())
}
```

### Compaction Interaction

- Compaction **rewrites** data into new SSTs and flips visibility by **manifest CAS**; it does not alter `Ts` or history. Older snapshots remain readable until GC.

### Recovery & GC

- **Recovery**: load the latest good manifest; replay ordered WAL fragments into the mutable memtable so WAL-backed rows participate in the read path immediately; ignore objects not referenced by any committed manifest.
- **GC**: manifest drives retirement of obsolete WAL/SST generations with retention windows and safety checks.

### Invariants

- **Manifest defines visibility** (authoritative control plane).
- **Append-only data** (WAL/SST) + **conditional publish** (CAS).
- **Monotonic `Ts` + latest-wins** under a snapshot.
- **Readers/Writers stateless**; durability and ordering live in object storage + manifest.

## Checkpoints, Snapshots & Catalog Versioning

Tonbo makes **time-travel, rollback, and dataset freezing** first-class by publishing **data and catalog (schema & indexes)** together at a checkpoint. Reads are **snapshot-consistent** for both planes.

### Primitives

* **Snapshot (read)**: a stable `(manifest_version, read_ts, schema_set)` view; sessions pin it for reproducible scans. *Implemented via `snapshot_at()` and `list_versions()` APIs.*
* **Checkpoint (write)**: `flush -> L0 -> CAS publish` atomically switches **SST visibility + catalog edits**. *Basic checkpoint flow works.* Named tags (e.g., `episode:123`) are planned (#554).
* **Time travel**: address by **version / timestamp** within retention. *Implemented.* Addressing by tag and **pins** to prevent GC are planned (#554).
* **Export / restore**: materialize a tagged version; optionally move `HEAD` back to a prior version. *Planned feature.*

### Catalog-aware DDL

* **Additive (safe)**: add NULLable columns, widen types, add indexes -> publish at checkpoint; old files read missing cols as NULL/default.
* **Destructive** (drop/rename/narrow/vector-dim change):
  * **Soft drop (default)**: hide in catalog at checkpoint; background compaction rewrites; old snapshots remain readable until GC.
  * **Hard drop (compliance)**: checkpoint carries a rewrite plan; compactor rewrites immediately; tighten retention to retire prior versions early.

### Sidecar indexes (planned)

> **Note:** Sidecar indexes are a planned feature, not yet implemented.

* Vector / inverted / bitmap indexes will be **versioned with SST generations** and carry the snapshot's `schema_id`; rebuild when schema requires.
* Indexes will be **snapshot-aware** (no per-row MVCC), switching at checkpoint boundaries for efficiency.

### Ops defaults

* Retention window (e.g., 7–30 days); **pin** episodes/releases.
* Checkpoint before destructive DDL and at episode/task boundaries.
* Compaction publishes new versions; old versions readable until GC.

### Why this matters

* **Reproducible reads**: evaluation/audit see the exact **data+schema** used.
* **Safe rollback**: one operation flips both planes back.
* **Dataset freezing**: tag and export stable cuts for fine-tune/A/B runs.

> This unified layer is the **first step** toward agent-grade version control: practical snapshots/time-travel, atomic checkpoints, catalog awareness, and pinned tags—while keeping the online path **append-only + CAS-atomic**.

## Data & File Formats

Tonbo is **Arrow-native in memory** and **Parquet on disk**: Arrow gives a typed, zero-copy columnar API; Parquet gives durable, columnar SSTs with statistics for pushdown and pruning.

### Arrow (in-memory / API boundary)

- **Schema-first**: tables are defined by Arrow schemas (names, types, nullability).
- **RecordBatch I/O**: writes ingest Arrow arrays/RecordBatches; reads materialize RecordBatches (projection & predicate pushdown preserved).
- **Zero-copy interop**: Arrow is the contract to query engines (DataFusion, FDW/FFI), minimizing marshaling.

### Parquet (SST on object storage)

- **One SST = one Parquet object**: immutable; new generations are made and **CAS-published** in the manifest.
- **Sorted by primary key**: compaction rewrites SSTs with stable key order to bound read amplification.
- **Row groups & pages**: use min/max stats, column indexes/bloom for **Phase-1** pruning.
- **Page-level index & page sizing (object-storage aware)**: we **heavily rely on page-level indexes** for fine-grained pruning; to reduce remote round-trips and exploit S3 range-GET throughput, **increase Parquet data page size** beyond defaults — **~8–16 MB per data page** is a good starting point (trade-off: larger pages may over-read when predicates are extremely selective).
- **Encodings & compression**: Parquet encodings (dict, RLE/bitpack) and compression (ZSTD/LZ4) are writer options.
- **Sidecar indexes (planned)**: vector / inverted / graph indexes will be stored as sibling objects keyed to the SST generation (not yet implemented).

### Schema evolution

- **Additive** (add nullable columns / widen types) supported; missing columns read as null/default.
- **Destructive** (drop/rename/retighten) requires rewrite via compaction to keep snapshots consistent.

## Runtime

> **Note:** WASM and browser support are experimental; OPFS integration is in progress. Production use is recommended on native runtimes (Tokio).

Tonbo runs on **async Rust** with a **Fusio-based Executor / Timer / FS** stack, so the same engine works in servers, serverless/edge, and the **browser**.

- **Web/WASM feature toggle**: build with `--features web` to switch Fusio to the `WebExecutor` + `wasm-http` stack (optionally `web-opfs`). Tokio is disabled in this mode, `JoinHandle::join` is intentionally unjoinable on the web, and the background compaction loop runs via the Fusio executor/timer abstraction on all targets (Tokio or WebExecutor).

- **OPFS (browser, WASM)**
  First-class storage backend. Core ops (`open/read_at/write_all/size/close`) map to OPFS handles.
  Durability mapping: `Flush/Data/All` -> flush/close (browsers don’t expose `fsync`); `Commit` & `DirSync` -> no-op.

- **Deno**
  Prefer **OPFS** (same paths, e.g. `Path::from_opfs_path("...")`).
  If OPFS is unavailable, fall back to **Deno FS** (sandboxed local dir) **or** go fully remote with **S3/R2** via `wasm-http` (where `Commit` = multipart-complete -> visible).

- **Config sketch**
  - Browser/Deno (with OPFS): `DbOption::new(...).base_fs(OPFS)`
  - Deno (no OPFS): `base_fs(Local)` *or* `base_fs(S3)`

- **Why this matters**
  Edge-first portability with one API: identical `Read/Write/Fs` and durability semantics, no engine-specific forks; easy bridge to object storage in browser/Deno or native runtimes.

## Query

Arrow-native, **pushdown-first**, **snapshot-consistent** query layer. It resolves a manifest snapshot, plans precisely, scans minimally, and streams Arrow `RecordBatches` across edge and serverless runtimes. One small core serves multiple interfaces.

### Plan -> Scan

```
              Rust API
           (Tonbo native)
                 |
     +-------------------------+
     |   Predicate Adapter     |
     +-------------------------+
                 |
     +-------------------------+
     | PLAN: resolve snapshot  |
     | prune -> RowSet+Proj+RS |
     +-------------------------+
          ^                 ^
          |                 |
+----------------+   +------------------+
| Manifest       |   | Sidecar Indexes  |
| (HEAD/version) |   | (planned)        |
+----------------+   +------------------+
                 |
     +-------------------------+
     | SCAN: k-way merge       |
     | page prune, early LIMIT |
     | residual recheck (SV)   |
     +-------------------------+
                 |
     +-------------------------+
     | Arrow RecordBatches     |
     +-------------------------+
```

### Sources & precedence

```
[ Snapshot: manifest + read_ts ]
             |
     +---+---+---+---+---+
     |   |   |   |   |   |
   Txn  Mut Imm  L0  L1..Ln
     \    \   \  /     /
      \    \   \/     /
       \    \  /\    /
        +--------------+
        | Merge & MVCC |
        | latest-wins  |
        +--------------+
               |
   +---------------------------+
   | Ordered stream (PK sort) |
   | with early LIMIT         |
   +---------------------------+
```

### Key points

- **Interfaces:** Native Rust API today; Postgres FDW, DataFusion `TableProvider`, and JS/Python SDKs are on the roadmap.
- **Execution:** Two phases — **Plan** builds RowSet and projection and marks residuals; **Scan** does k-way merge across sources with page pruning, projection, early LIMIT, and residual recheck.
- **Pushdown:** Filter, projection, limit, order by primary key. Filters classified as **Eq**, **Neq**, **In**, etc.; exact parts drive RowSet and Parquet page pruning.
- **Pruning & indexes:** Uses manifest snapshot, primary key ranges, segment and column stats, Parquet page indexes; sidecar indexes (vector, inverted, bitmap) are planned for future releases.
- **Correctness:** Manifest-defined snapshot plus read timestamp; append-only WAL and SST with CAS publishes; compaction rewrites only; time-travel within GC window.
- **Performance:** Columnar only-read, larger Parquet pages for object storage efficiency, cached plans and RowSets, tunable batch size, simple stats-based hints; heavy aggregations and joins live in callers.

## Manifest

Tonbo’s **manifest** is the authoritative source of truth that coordinates **stateless readers/writers** and the **long-lived compactor** over object storage. Data lives in immutable objects (WAL fragments, Parquet SSTs); the manifest defines **visibility** via **conditional (CAS) commits**.

### Purpose

- **Atomic visibility:** publish new data by switching manifest references (no in-place mutation).
- **Global snapshot:** readers resolve a stable version (HEAD or by ID) for snapshot isolation.
- **Coordination hub:** writers/compactors/GC advertise and claim work via manifest-adjacent records (plans, leases) using CAS.
- **Recovery anchor:** after crashes, reload the last good manifest and continue; incomplete objects remain invisible.

### Data Model

- **Version (V\_n):** lists visible SST scopes per level (L0..Ln), referenced WAL generations, optional sidecar indexes, and watermarks (timestamps).
- **HEAD:** a pointer to the current Version (V\_n). Moving HEAD is an **atomic** operation (CAS).
- **Version Edits:** minimal deltas applied atomically:
  - `Add{ level, scope }` (introduce new SSTs)
  - `Remove{ level, gen }` (retire old SSTs)
  - `LatestTimestamp{ ts }` (advance logical time watermark)
  - `Rewrite{ snapshot }` (periodic snapshotting to bound recovery time/size)

### Operations & Concurrency

- **CAS publish:** writers and compactors submit edits with an **expected version/ETag**; on conflict, back off, refresh, retry (idempotent by edit/intent ID).
- **Snapshot & rewrite:** after N edits or size threshold, compact the log into a self-contained version and CAS-move HEAD.
- **Leased plans (optional):** compaction/GC plans are stored as objects with **lease + TTL** and updated via CAS to avoid duplicate execution.

### Interoperability with Serverless Roles

- **Reader (stateless):** fetch HEAD (or a specific version), execute two-phase read on the referenced SSTs; zero dependence on local state.
- **Writer (stateless):** persist WAL fragment -> produce L0 SSTs -> **CAS** `Add{L0,...}` (+ optional WAL retire hints). Retries are safe at the manifest boundary.
- **Compactor (long-lived):** claim a plan (CAS), read referenced SSTs, build new SSTs, then **atomically** publish `Add{L+1,...} + Remove{L,...}`. Readers on older snapshots remain correct until GC.

### Guarantees

- **Atomic, monotonic versions:** readers never see partial updates; HEAD only moves forward.
- **Idempotent commits:** duplicate publishes do not corrupt state.
- **Append-only correctness:** all durable state is new objects + manifest switches.
- **Time travel & retention:** keep recent versions for rollback/analysis; GC removes unreachable generations when safe.

### Why it fits distributed serverless

- **No central coordinator:** correctness is enforced by **object-store CAS** (e.g., ETag/If-Match) and immutable uploads (MPU complete -> visible).
- **Stateless scale-out:** any node can read, write, or compact; the manifest serializes visibility without shared process state.
- **Resilience by construction:** crashes leave HEAD unchanged; incomplete or orphaned objects are invisible until a clean publish.

### Conceptual sketch

```
Writers   ─┐       +-----------+      CAS      +------------------+
           ├─ Edits│  Version  │ ────────────▶ │       HEAD       |
Compactor ─┘       │   V_n     │               +------------------+
                   │ (SST refs,│
                   │ WAL, idx) │
                   +-----------+
                         │
                         ▼
               Immutable Objects (object storage)
        (WAL fragments, Parquet SSTs, sidecar indexes)
```



## Serverless Execution Topology

Tonbo runs as a **manifest-orchestrated** system over **object storage**. The **manifest** on S3 is the control plane; data lives as **immutable WAL fragments and Parquet SSTs**. Compute roles are decoupled: **Readers** and **Writers** are **stateless** (safe for edge/FaaS), while the **Compactor** is a **long-lived, state-minimal service** that advances merges and cleanup. All coordination happens through **conditional (CAS) manifest commits**—no central coordinator is required.

### Roles & Responsibilities

- **Reader (stateless)**
  Loads a **snapshot manifest** (HEAD or a specific version) and performs columnar scans with projection/predicate pushdown. Works well in **edge functions** and short-lived containers.
- **Writer (stateless)**
  Appends to **batched WAL fragments** (immutable objects), updates the in-memory MemTable, then flushes to **L0 SSTs** (Parquet) and **atomically publishes** visibility via **CAS manifest edits**. Retries are idempotent at the manifest boundary.
- **Compactor (long-lived, state-minimal)**
  Periodically evaluates thresholds, **pulls SSTs from object storage**, merges locally via streaming, **uploads new SSTs**, and publishes a new manifest version via CAS. Old objects are retired by GC plans. Can scale out via **lease/plan claiming** to avoid duplicate work.

### Lifecycle at a Glance

- **Write path:** WAL fragment (object) -> MemTable -> freeze -> **Minor compaction** to L0 SSTs (direct to S3) -> **CAS manifest update**.
- **Read path:** Resolve manifest snapshot -> parallel, columnar scans with pushdown -> optional local caching.
- **Compaction path (background):** Select L/L+1 SSTs -> merge -> upload new SSTs -> **CAS manifest update** -> GC old objects.

**Compaction defaults**
- **Minor compaction:** enabled by default; flushes once ~4 sealed immutables accumulate, emitting L0 SSTs. Opt-out via `DbBuilder::disable_minor_compaction` is intended for tests/bulk-load tooling only.
- **Major compaction:** not scheduled automatically yet; invoke the admin trigger or an opt-in loop when available. The planner/executor path is wired, but background scheduling is still landing.

### Why this fits Edge Compute & Shared Storage

- **Immutable by design:** write-new-only + **conditional commits** match object storage semantics—no in-place mutation.
- **Stateless compute:** readers/writers run anywhere (edge/FaaS/containers), cold starts fetch just the manifest.
- **Simple recovery:** incomplete uploads are invisible; previously committed versions remain readable.
- **Elastic & economical:** durable data on **S3**, compute scales independently; compaction can be a managed service to keep read amplification low.
- **High-throughput analytics:** columnar layout + statistics/filters + pushdown leverage object storage bandwidth and concurrency.

### Conceptual Diagram
```
        +---------------------- Compute Roles -----------------------+
        |  Reader (stateless)  |  Writer (stateless)                 |
        |  snapshot + pushdown |  WAL -> L0 flush -> CAS publish     |
        +----------|-----------+-----------|-------------------------+
                   |                       |
                   v                       v
        +------------------------- Object Storage ---------------------------+
        |   WAL fragments (immutable)  |   Parquet SSTs (L0..Ln)  | Manifest |
        |         append-only          |     write-new only       |  (CAS)   |
        +-------------------^-------------------------^-----------+----------+
                            |                         |
                            |                         |
                 +----------+-----------+             |
                 |  Compactor (long-    |             |
                 |  lived service,      |-------------+
                 |  state-minimal)      |
                 |  merge & publish via |
                 |  CAS manifest        |
                 +----------------------+
```
