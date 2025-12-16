# RFC: Performance Benchmarking Strategy for Tonbo

**Status:** Draft
**Authors:** Tonbo Team
**Target:** Next minor release
**Inspired by:** Luca Palmieri – *Rewrite, Optimize, Repeat*
**Scope:** Storage engine performance (disk + S3)

---

## 1. Motivation

Tonbo is a storage engine. For such systems, correctness is necessary but not sufficient:
**performance is a first-class product feature**.

As Tonbo evolves (LSM layout changes, WAL refactors, object store abstractions, Arrow/Parquet optimizations), we need a systematic way to:

- Detect **performance regressions**
- Validate **performance invariants** at the component level
- Understand **why** performance changes
- Compare:
  - Tonbo versions (main vs release)
  - Storage backends (disk vs S3)
  - Workload shapes (write-heavy, read-heavy, mixed)

This RFC proposes a benchmarking strategy inspired by Luca Palmieri’s approach:
**iterate by measuring, optimizing, and validating — repeatedly and deliberately**.

---

## 2. Goals and Non-Goals

### 2.1 Goals

- Define what “performance” means for Tonbo
- Establish **repeatable, versioned benchmarks**
- Support **multiple storage backends** (disk, S3 via localstack, real S3)
- Enable execution in:
  - Local development
  - CI (GitHub Actions)
- Catch regressions at:
  - **Component level**
  - **System / scenario level**
- Enable comparison across Tonbo versions

### 2.2 Non-Goals (for now)

- Dashboards or visualization
- Distributed or multi-node benchmarks
- Perfectly stable absolute numbers in CI
- Benchmarking S3 itself (network variability is accepted)

---

## 3. Terminology and Benchmark Taxonomy

This RFC intentionally uses **two benchmark classes**, not “micro vs macro”.

### 3.1 Component Benchmarks (formerly “microbenchmarks”)

**Purpose**

- Validate performance contracts of individual engine components
- Detect regressions caused by refactors or algorithm changes
- Compare alternative implementations

Component benchmarks define **performance contracts** for engine subsystems.
A contract specifies expected asymptotic behavior and acceptable regression bounds.

**Properties**

- Narrow scope
- Near-isolation (minimal dependencies, no full system orchestration)
- Stable inputs
- Fast to execute
- Suitable for CI (deterministic inputs, bounded runtime)
- Implemented with `criterion`, `iai`, or similar tools

**Examples (Tonbo-specific)**

- Memtable insert throughput
- Memtable lookup latency
- Bloom filter check cost
- SST iterator merge cost
- Block cache hit / miss latency
- WAL append latency
- Arrow / Parquet encode-decode throughput

**Rule**

> Improvements in component benchmarks must **not** regress scenario benchmarks.

---

### 3.2 Scenario Benchmarks (system / product benchmarks)

**Purpose**

- Measure user-visible performance
- Detect emergent behavior across components
- Validate architectural decisions

**Properties**

- End-to-end
- Multi-component
- Slower to run
- Configuration-driven
- Backend-agnostic

**Examples**

- Sustained sequential writes triggering flush + compaction
- Random reads under cache pressure
- Mixed read/write workloads
- Disk vs S3 comparison under identical workloads

**Rule**

> Scenario benchmarks are the **final authority** on performance.

---

## 4. Design Principles

1. **Scenario benchmarks define truth**
2. **Component benchmarks explain truth**
3. Benchmarks are **production artifacts**
4. Benchmarks are **backend-agnostic**
5. Configuration is **data, not code**
6. Benchmarks evolve alongside Tonbo

---

## 5. Benchmark Architecture

The benchmark harness is responsible for orchestration, configuration, and measurement.
Tonbo engine code should remain largely unaware of benchmark execution.

### 5.1 Repository Layout

```text
tonbo/
├── benches/
│   ├── scenarios/
│   │   ├── write_only.rs
│   │   ├── read_only.rs
│   │   └── mixed.rs
│   ├── components/
│   │   ├── memtable.rs
│   │   ├── bloom.rs           # deferred
│   │   ├── cache.rs           # deferred
│   │   └── iter.rs            # deferred
│   └── harness/
│       ├── config.rs
│       ├── backend.rs
│       ├── workload.rs
│       └── metrics.rs

> **Status (dev branch)**: `write_only`, `read_only`, and `mixed` scenarios exist; compaction scenario is not yet implemented. Component coverage is limited to `memtable` via ingest; bloom/cache/iterator benches are deferred. Bench diagnostics require a feature flag and are intentionally off-by-default.
