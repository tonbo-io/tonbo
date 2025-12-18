# Benchmarks

See [RFC 0012](rfcs/0012-performance-benchmarking.md) for design rationale and terminology.

## Quick Start

```bash
# Run all benchmarks (scenarios + components)
cargo run -p tonbo-bench-runner -- --profile ci

# Run scenarios only
cargo run -p tonbo-bench-runner -- --mode scenario --profile ci

# Run with custom config
cargo run -p tonbo-bench-runner -- --mode scenario --config benches/harness/configs/ci-write-only.yaml

# Dry-run (print commands without executing)
cargo run -p tonbo-bench-runner -- --profile ci --dry-run
```

## Runner Options

```
--mode        scenario | component | memtable | wal | sst | iterator | all (default: all)
--profile     local | ci | deep (default: local)
--config      Path to YAML config (required unless using --profile ci)
--dry-run     Print commands without executing
```

## Manual Execution

```bash
# Scenarios
RUSTFLAGS="--cfg tonbo_bench" cargo bench --bench tonbo_scenarios -- --config benches/harness/configs/ci-write-only.yaml

# Components
RUSTFLAGS="--cfg tonbo_bench" cargo bench --bench tonbo_components -- --config benches/harness/configs/ci-write-only.yaml
```

## Regression Comparison

```bash
cargo run -p bench-compare -- \
  --current target/bench-results \
  --baseline benchmarks/baselines/scenarios \
  --thresholds benches/harness/configs/regression-thresholds.yaml \
  --missing-baseline fail
```

## Report Generation

```bash
cargo run -p bench-report -- \
  --results-dir target/bench-results \
  --output-md target/bench-reports/perf-report.md \
  --output-json target/bench-reports/perf-report.json
```

## Output

Results are written to: `target/bench-results/<run-id>/<bench-target>/<storage-substrate>/<benchmark>.json`

## Available Configs

| Config | Description |
|--------|-------------|
| `ci-write-only.yaml` | Small sequential write (CI) |
| `ci-mixed.yaml` | Small mixed read/write (CI) |
| `deep-disk.yaml` | Longer run with diagnostics |
| `baseline-*.yaml` | Baseline generation configs |

## Sample Config

```yaml
backend:
  type: disk
  path: target/bench-tmp
workload:
  type: sequential_write  # or: read_only, mixed
  num_records: 3000
  value_size_bytes: 256
runtime:
  concurrency: 1
diagnostics:
  enabled: false  # set true for detailed metrics
```
