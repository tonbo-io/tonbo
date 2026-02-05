# RFC: Compaction Correctness Gate

- Status: Draft
- Authors: Tonbo team
- Created: 2026-02-03
- Area: Compaction, Read Path, Testing

## Summary

Define a correctness gate for compaction that validates read consistency and MVCC semantics before and after compaction. The gate uses a small reference oracle plus deterministic scenarios as a minimal phase, and provides a structure for later expansion into model-based randomized sequences and broader invariants.

## Motivation

Compaction rewrites physical layout and can silently break logical correctness. A dedicated gate is needed to prove that compaction does not change visible results for a fixed snapshot and does not resurrect deleted or superseded values. This RFC establishes the semantic contract, the minimal gate, and a path to expand coverage as the compaction pipeline evolves.

## Goals

- Specify compaction correctness invariants in terms of visible read results.
- Deliver a minimal, deterministic gate that is reproducible and debuggable.
- Provide an oracle model that aligns with Tonbo MVCC semantics.
- Define a path to expand from deterministic scenarios to model-based randomized tests.

## Non-Goals

- Performance benchmarking or tuning.
- CI gating or long-running fuzzing (added later once validated).
- Full coverage of crash recovery or manifest/GC interactions (follow-on work).
- Guaranteeing correctness for future range tombstones or remote compaction (follow-on work).

## Design

### Invariants

The correctness gate validates that **logical results are unchanged by compaction** for a fixed snapshot. Invariants are grouped by phase to allow a minimal starting point.

**Phase 0 (minimal gate):**

1. **Snapshot consistency**  
   For a fixed logical snapshot, reads before and after compaction return identical results.

2. **No resurrection + last-write-wins**  
   A key deleted (or overwritten) before a snapshot does not reappear, and the latest visible version at that snapshot is returned.

3. **Range completeness**  
   Range scans at a fixed snapshot return the same key set and values before and after compaction.

**Phase 1+ (expanded invariants):**

4. **Tombstone pruning safety**  
   Pruning tombstones does not expose older versions that should remain hidden.

5. **Iterator/seek stability**  
   Iterator ordering and seek semantics remain stable across compaction.

6. **Reopen + snapshot durability**  
   Snapshot consistency remains valid across DB reopen boundaries.

### Oracle Model

The gate uses a small in-memory MVCC oracle to represent logical truth:

- **State model**: key -> ordered versions. Each version stores `(commit_ts, tombstone, value)`.
- **Visibility**: for snapshot `ts`, the visible version is the highest `commit_ts <= ts`.
- **Tie-breaks**: delete wins on equal `commit_ts` (consistent with SST merge semantics).
- **Read semantics**:
  - `get(key, ts)` returns the visible version or not-found.
  - `scan(range, ts)` returns all visible keys in order.

The oracle does not model compaction mechanics; it models only logical results so the compaction pipeline can change freely as long as results match.

### Test Types

**Deterministic scenarios (Phase 0):**

| Scenario | Purpose | Invariants |
| --- | --- | --- |
| Overwrite chain | Validate last-write-wins at fixed snapshot | 1, 2 |
| Delete-heavy | Validate no resurrection | 1, 2 |
| Range scan with deletes | Validate range completeness | 1, 3 |
| Cross-segment overlap | Validate compaction rewrite invariance | 1, 2, 3 |

Each scenario:
1. Build a controlled write/delete sequence.
2. Take snapshot `ts`.
3. Read (get/scan) before compaction; compare to oracle.
4. Force compaction.
5. Read after compaction at the same `ts`; compare to oracle.

**Model-based randomized sequences (Phase 1+):**

- Generate sequences of operations: put, delete, flush, compact, get, scan.
- Use seeded RNG; log seed and operation trace for reproduction.
- Validate after each read, and at periodic pre/post compaction checkpoints.

### Comparison with Other Systems

- TigerBeetle uses a model-based fuzzer for its LSM tree and compares scan/get results against a reference model.
- LevelDB maintains a ModelDB (in-memory map) and compares iterator results at intervals and across snapshots.
- RocksDB stress tests maintain an expected-state oracle that verifies latest values across runs.
- Turso uses proptest and differential oracles to compare results against SQLite, plus explicit property checks in concurrency simulation.

Tonbo adopts the same principle: compare logical results to a minimal oracle, then expand via randomized sequences once the baseline gate is validated.

### Reproducibility and Debuggability

The gate must emit enough context to replay failures:

- RNG seed (if randomized).
- Operation trace (human-readable and machine-parsable).
- Snapshot timestamps used for assertions.
- Diff of expected vs actual results (keys and versions).

### Scope and Coverage

Initial coverage targets **minor compaction** and the current read path across mutable + immutable + SST layers. Major compaction, manifest/GC, and remote execution are validated in Phase 1+ after the minimal gate is stable.

## References

- [Model-based testing](https://en.wikipedia.org/wiki/Model-based_testing)
- [Property-based testing](https://en.wikipedia.org/wiki/Property_testing)
- [Differential testing](https://en.wikipedia.org/wiki/Differential_testing)
- [Test oracle](https://en.wikipedia.org/wiki/Test_oracle)

## Future Work

- Add tombstone pruning safety tests and MVCC watermark checks.
- Extend coverage to major compaction, manifest updates, and GC integration.
- Enable CI gating and nightly fuzzing once test stability is confirmed.
- Add crash/reopen validation as part of the gate.
- Add range tombstone semantics when supported.
