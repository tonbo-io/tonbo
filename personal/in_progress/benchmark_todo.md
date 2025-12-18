- [ ] Add compaction scenario benchmark that exercises flush/merge/GC and captures write amplification (missing under `benches/scenarios`).
- [ ] Lift scenario harness to support `concurrency > 1` for multi-writer and mixed read/write load shapes.
- [ ] Rework component benchmarks into true microbenches (criterion/iai) that target memtable/WAL/iterator/bloom/cache APIs directly instead of the public ingest path.
- [ ] Expand benchmark diagnostics to include compaction, read-path, cache metrics, and write amplification rather than emitting “unsupported” placeholders.
- [ ] Enforce regression checks for component benches and multi-backend (disk + S3/localstack) runs with thresholds; extend CI coverage accordingly.
- [ ] Stamp benchmark outputs with the commit SHA unconditionally so runs stay versioned even without `GIT_COMMIT` in the environment.

## Post-release follow-ups
- [ ] Proposal B: evolve the compaction benchmarking harness toward RocksDB-style phase orchestration (fill/soak/major/read-verify), per-level stats export, multi-backend runs (disk + S3/localstack), regression thresholds for write amplification/stall latency, and bench-only hooks for compaction knobs without widening the production API surface.
