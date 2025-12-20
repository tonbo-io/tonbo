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
--diagnostics-sample-ms  Override diagnostics sampling interval (ms)
--diagnostics-max-samples  Override max number of diagnostics samples
```

## Subcommands

```
# Report summary/trends from past runs
report --results-dir <path> [--current-run <id>] [--baseline-run <id>] [--limit <n>]

# Regression compare (thresholded)
compare --current <path> --baseline <path> --thresholds <yaml> [--report <path>]

# Parameter sweep runner
sweep --config <sweep-config.yaml> [--output <path>] [--format csv|jsonl]
```

## Manual Execution

```bash
# Scenarios
cargo bench --features test --bench tonbo_scenarios -- --config benches/harness/configs/ci-write-only.yaml

# Components
cargo bench --features test --bench tonbo_components -- --config benches/harness/configs/ci-write-only.yaml
```

## Regression Comparison

```bash
cargo run -p tonbo-bench-runner -- compare \
  --current target/bench-results/<run-id> \
  --baseline benchmarks/baselines/scenarios \
  --thresholds benches/harness/configs/regression-thresholds.yaml \
  --missing-baseline fail
```

## Report Generation

```bash
cargo run -p tonbo-bench-runner -- report \
  --results-dir target/bench-results \
  --output-md target/bench-reports/perf-report.md \
  --output-json target/bench-reports/perf-report.json
```

## Sweep Config (Example)

```yaml
run:
  mode: component
  config: benches/harness/configs/deep-disk.yaml
parameters:
  - name: wal_sync_policy
    env: TONBO_BENCH_WAL_SYNC
    values: ["always", "interval_ms:1", "disabled"]
output:
  format: csv
  path: target/bench-results/sweeps/wal-sync.csv
```

Use `env` to vary environment variables or `config_path` (dot-separated) to override YAML fields.

```bash
cargo run -p tonbo-bench-runner -- sweep --config benches/harness/sweeps/wal-sync.yaml
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
  sample_interval_ms: 1000  # optional time-series sampling
  max_samples: 120  # optional cap on samples
```
