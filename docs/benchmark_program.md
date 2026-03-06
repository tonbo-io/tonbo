# Tonbo Benchmark Program

This document defines the broader benchmark program around Tonbo's current
compaction-focused harness. The goal is to evaluate Tonbo as a live analytical
engine on object storage, not only as a quiesced embedded analytical engine.

## Program Shape

Tonbo should take inspiration from the benchmark structure used by other open
source databases, not just their workload names.

- From object-store-native systems such as SlateDB: split microbenchmarks from
  a long-running engine bencher, add mixed workloads, contention tests, system
  resource monitoring, nightly trend runs, and explicit topology disclosure.
- From LSM-oriented systems such as RocksDB and LevelDB: use a named workload
  taxonomy, force deterministic phase ordering, emit richer metrics, and
  explain regime changes such as fixed overhead, large values, sync cost, and
  compaction debt clearly.
- From server-oriented systems such as libSQL: add endpoint-facing and
  freshness-facing benchmarks, not only storage-engine internals.

Tonbo should therefore have three benchmark layers:

- `micro`: narrow internal costs such as scan planning, stream init, merge
  init, package init, manifest open, and WAL sync.
- `engine`: object-store-backed workload scenarios with mixed traffic,
  compaction, GC, freshness, and durability.
- `surface`: query, open, freshness, or API benchmarks that look like a user
  workload rather than only an engine test.

Every benchmark artifact should include:

- `topology`: runner region, bucket region, same VPC/AZ or not, public/private
  path, cold/warm run, median RTT
- `live state`: logical bytes, physical bytes, visible SST count, obsolete SST
  count, WAL bytes, manifest bytes
- `request economics`: GET/HEAD/range-GET/PUT counts, bytes/request, estimated
  request cost
- `latency and tail`: mean, p50, p95, p99, p99.9
- `engine pressure`: compaction backlog, GC backlog, WAL queue depth,
  freshness lag, durable-ack lag, CPU, RSS, network MB/s

## Instrumentation Track

The benchmark plan only works if regressions are diagnosable. Tonbo should add
instrumentation in parallel with benchmark work so a slow or unstable result
can be explained without ad hoc debugging.

User why:

- If a benchmark fails to explain where latency or cost comes from, it does not
  build trust.
- If a mixed-load result regresses, we need to say whether the problem is WAL
  durability, scan setup, object-store requests, compaction pressure, or GC
  lag.

Tonbo why:

- The current benchmark already shows an object-store latency floor, but not
  its source.
- Mixed workloads will be hard to reason about unless the engine and the
  harness expose phase-level timing and queue pressure.
- Instrumentation lowers the cost of both benchmark development and future
  product debugging.

### Instrumentation Principles

- Always record end-to-end latency, and add phase-level timers so the top-line
  number is explainable.
  End-to-end latency is the primary user-facing number and should lead the
  report. Phase-level timers are what make that number actionable by showing
  whether time is going to setup, object-store requests, merge work,
  packaging, WAL sync, or manifest work.
- Record both logical work and physical work.
  Logical work describes what the user asked Tonbo to do, such as rows returned
  or live bytes scanned. Physical work describes what Tonbo and the storage
  backend actually paid for, such as bytes read, requests issued, objects
  created, or bytes rewritten by compaction.
- Keep metric names stable across scenarios so results are comparable.
  If one benchmark reports `prepare_ms`, another reports `scan_setup_ms`, and a
  third folds setup into total latency, the benchmark suite becomes hard to
  compare and easy to misread. Stable names create one shared vocabulary across
  all runs.
- Emit machine-readable artifacts first; derive charts and summaries from them
  later.
  Structured outputs such as JSON or TSV should be the source of truth.
  Charts, markdown summaries, and dashboards should be generated from that raw
  data so the same run can be reinterpreted later without rerunning the
  benchmark.
- Every benchmark run should be traceable back to the manifest state, WAL mode,
  compaction settings, and topology.
  This is broader than just recording a random seed. Tonbo should capture
  enough engine, storage, workload, and deployment context to explain whether a
  result came from the code path, the storage layout, or the network path.

### Engine Modules To Instrument

#### Read path

Why:

- Users experience read latency as one number, but Tonbo needs to know whether
  time is spent in snapshot resolution, planning, stream construction, remote
  open, merge, or packaging.

Add:

- snapshot resolution time
- plan and prune time
- stream-open time per source
- merge init time
- package init time
- rows scanned vs rows returned
- bytes read per source
- object requests per scan

Use:

- explain fixed overhead vs data-size effects
- explain why object-store runs are flat
- isolate whether setup or consume dominates

#### WAL and commit path

Why:

- Users care about durable-ack latency and fresh-read visibility; Tonbo needs
  to know where that path stalls.

Add:

- enqueue to durable latency
- queue depth over time
- bytes per WAL frame and per segment
- sync duration
- commit wait reason counters
- ack-to-visible lag

Use:

- explain stream-ingest results
- compare `strict` vs `fast`
- identify whether durability cost is batching, sync, or publication

#### Flush and minor compaction

Why:

- Fresh data benchmarks will be shaped heavily by sealing and minor compaction,
  not just by reads.

Add:

- seal trigger reason
- time from seal to flush start
- flush duration
- output SST count and bytes
- rows in, rows out
- overlap or amplification indicators

Use:

- tune seal thresholds
- explain read freshness and SST explosion
- detect small-object amplification

#### Major compaction

Why:

- Users only care when compaction affects read tails or storage cost; Tonbo
  needs to expose that mechanism directly.

Add:

- compaction job wait time
- execution time
- bytes read and bytes written
- input SST count and output SST count
- obsolete SST count produced
- WAL floor movement
- backlog depth over time

Use:

- explain read-during-compaction results
- calculate write amp and cleanup lag
- identify whether planner or executor is the bottleneck

#### GC and retention

Why:

- Cleanup cost is part of the object-store product story, not an internal
  footnote.

Add:

- obsolete bytes pending delete
- obsolete object count pending delete
- time from obsolete to reclaimed
- delete request counts and latency
- retained bytes due to snapshots or retention

Use:

- explain physical amplification windows
- set GC cadence and retention defaults
- distinguish delayed reclaim from ineffective compaction

#### Manifest and metadata path

Why:

- On object storage, metadata and version movement can be a large fixed cost.

Add:

- HEAD fetch latency
- manifest decode latency
- CAS publish latency
- CAS retry count
- visible version size

Use:

- explain constant setup floors
- identify metadata bottlenecks under mixed load

### Benchmark Engine To Instrument

#### Harness phase timing

Add:

- setup time
- warmup time
- steady-state window time
- teardown time
- per-iteration phase timers

Use:

- separate engine cost from harness overhead
- keep small-workload results honest

#### Topology capture

Add:

- runner region
- bucket region
- same VPC or AZ flag
- endpoint type
- cold or warm run flag
- median RTT probe

Use:

- stop over-attributing latency to Tonbo when it comes from path placement

#### System resource monitoring

Add:

- process CPU
- RSS
- disk throughput
- network throughput
- runtime queue depth if available

Use:

- explain whether a regression is CPU-bound, memory-bound, or network-bound

#### Request accounting

Add:

- GET, HEAD, range-GET, PUT, DELETE counts
- bytes per request type
- request failures and retries

Use:

- explain object-store economics
- connect page size and flush size to request amplification

#### Run configuration snapshot

Add:

- scenario name
- commit mode
- sync policy
- seal thresholds
- compaction settings
- page size
- batch size
- retention settings
- git revision

Use:

- make every run reproducible
- make cross-run comparisons defensible

### Rollout Priority

1. Read-path phase timers and request accounting
2. WAL and commit-path timers
3. Topology capture and harness/system metrics
4. Minor and major compaction accounting
5. GC and retention metrics
6. Richer manifest and CAS instrumentation

### Minimum Instrumentation Required Before Each Scenario

- `interleaved_freshness_read_write`
  - read-path phase timers
  - WAL queue depth
  - ack-to-visible lag
  - request counts
- `durable_parallel_stream_ingest`
  - durable-ack timing
  - WAL segment and sync metrics
  - recovery timing
- `deployment_topology_request_amplification`
  - topology capture
  - request accounting
  - manifest-open timing
- `read_after_compaction_byte_sweep`
  - prepare vs consume timing
  - logical vs physical bytes
  - visible SST count
- `parallel_readers_during_background_compaction`
  - compaction backlog
  - compaction bytes read and written
  - reader tail latency by phase
- `gc_lag_storage_amplification_window`
  - obsolete bytes
  - reclaim delay
  - delete request counts and latency

## Priority Logic

The order should follow the user journey, not Tonbo internals:

1. Can I write fresh data and query it immediately?
2. What does durability cost me?
3. Is the latency floor coming from Tonbo or the object-store path?
4. At what data volume do Tonbo's optimizations start to matter?
5. Does background maintenance hurt live traffic?
6. What does cleanup do to my bill and latency?

## Integrated Scenario Roadmap

### 1. `interleaved_freshness_read_write`

Why first: users care first about whether fresh writes become queryable quickly
under real mixed load, because that is the live-analytics promise.

Tonbo why: this is the core positioning test that separates Tonbo from a
quiesced embedded analytical engine.

Reference pattern:

- concurrent mixed-workload benchmarkers
- freshness-facing query workloads

Workload:

- Run 4 writer tasks and 16 reader tasks for 15 minutes on object storage.
- Writers commit 2,000-row Arrow batches every 100 ms with 20% key overlap and
  5% deletes.
- Readers issue "last 5 minutes for tenant" scans every 250 ms with about 1%
  selectivity.
- Compare `strict` and `fast` commit-ack modes.

Metrics:

- commit latency
- freshness lag from durable ack to first visible read
- read mean, p95, p99
- WAL queue depth
- compaction backlog
- visible SST count
- request counts
- bytes read and written
- logical live bytes

Decision:

- default commit-ack mode
- seal thresholds
- minor-compaction cadence
- whether Tonbo can credibly claim live freshness on object storage

Timing:

- Short-term

### 2. `durable_parallel_stream_ingest`

Why second: users next ask what durability costs in throughput and latency when
they treat the database like a stream sink, not a batch loader.

Tonbo why: Tonbo's WAL and object-store durability path are central to the
product, so this is a primary proof, not an edge case.

Reference pattern:

- durable vs non-durable write comparisons
- sync-vs-async framing

Workload:

- Run 8 parallel writer streams for 20 minutes.
- Compare batch sizes of 1, 100, and 1,000 rows.
- Compare WAL sync policies like `Always`, `Interval(10 ms)`, and
  `Interval(50 ms)`.
- Inject crash-and-recover every 60 seconds after acknowledged writes.

Metrics:

- ack p50, p95, p99
- durable lag
- ingest throughput
- WAL segment size
- sync latency
- queue depth
- recovery time
- lost acknowledged rows
- cost per MiB ingested

Decision:

- default WAL sync policy
- batching window
- whether Tonbo needs stronger coalescing or local staging to make durable
  ingest viable

Timing:

- Short-term

### 3. `deployment_topology_request_amplification`

Why third: users need to know whether slow object-store results are caused by
Tonbo, by many small requests, or by the deployment path between compute and
storage.

Tonbo why: this is the main credibility gap in current results because the
object-store latency floor is visible but not explained.

Reference pattern:

- explicit topology reporting
- parameterized harness discipline

Workload:

- Keep logical live-set fixed at 1 GiB.
- Compare three physical layouts such as micro pages or segments, medium
  pages, and object-store-optimized large pages.
- Run the same selective range scan from same-AZ in-VPC, same-region public
  path, and cross-region when possible.

Metrics:

- GET, HEAD, and range-GET counts
- bytes per request
- mean and p99 latency
- manifest-open time
- scan-plan time
- estimated request cost per GiB scanned
- median RTT
- network throughput

Decision:

- default Parquet page size
- minimum flush size
- benchmark deployment requirements
- whether public-path and colocated results must be reported separately

Timing:

- Short-term if infra is available, otherwise Later

### 4. `read_after_compaction_byte_sweep`

Why fourth: users need to know when Tonbo's structural optimizations start to
pay off, because small datasets often look dominated by constant overhead.

Tonbo why: this explains the current benchmark correctly and prevents weak
conclusions from tiny workloads.

Reference pattern:

- byte-oriented reporting
- regime-change explanation style

Workload:

- Run `baseline` and `quiesced` read scenarios at fixed logical live-set
  targets such as 8 MiB, 32 MiB, 128 MiB, 512 MiB, 2 GiB, and 8 GiB on both
  `local` and `object_store`.
- Keep query shape fixed with a narrow recent-range filter and projection.

Metrics:

- logical live bytes before and after compaction
- read bytes
- request counts
- visible SST count
- prepare vs consume latency
- mean and p99
- CPU time

Decision:

- where the fixed-cost floor stops dominating
- what public benchmark scale should be used
- how Tonbo should explain compaction value in public benchmark material

Timing:

- Short-term

### 5. `parallel_readers_during_background_compaction`

Why fifth: users care about whether background maintenance causes latency spikes
while they are reading live data.

Tonbo why: compaction only matters strategically once it is measured as a
user-visible tail-latency risk or benefit.

Reference pattern:

- read-during-write style workloads
- deterministic phase sequencing

Workload:

- Seed an overlap-heavy L0 state with roughly 1 GiB logical data and dozens of
  visible SSTs.
- Start major compaction.
- Run 8, 32, and 64 concurrent readers for 10 minutes, each issuing a scan
  every 200 ms with a small recent-range filter and narrow projection.

Metrics:

- reader p50, p95, p99
- compaction throughput
- bytes rewritten
- visible SST count over time
- backlog depth
- latency spikes around manifest changes
- request counts

Decision:

- whether compaction needs throttling
- whether compaction needs isolation
- whether compaction needs separate service-class treatment to preserve
  live-read tails

Timing:

- Later

### 6. `gc_lag_storage_amplification_window`

Why sixth: users eventually ask what cleanup and retention do to storage cost,
object count, and read stability after compaction publishes new data.

Tonbo why: it turns the current physical-byte caveat into an operational
benchmark and exposes whether Tonbo's cleanup model is good enough for
object-storage economics.

Reference pattern:

- write amplification and compaction accounting
- background system monitoring

Workload:

- Sustain overwrite and delete traffic through repeated compaction cycles with
  retention windows like 0, 5, and 30 minutes while a light analytical read
  load stays active.

Metrics:

- logical vs physical bytes
- obsolete object count
- time-to-reclaim
- delete request count
- read latency during GC
- tombstone density

Decision:

- GC cadence
- retention defaults
- whether reader registry or stronger watermarking is required before broader
  claims

Timing:

- Later
