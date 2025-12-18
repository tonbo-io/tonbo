# Benchmarks (Phase 5 – history + reporting)

- Config-driven: YAML passed via `--config`.
- Scenario support: `sequential_write`/`write_only`, `read_only`, `mixed`.
- Backends: disk; S3/LocalStack harness is available locally but **not** wired into CI yet.
- Component support: `memtable_insert` remains as in Phase 1.
- Output schema is backward compatible and includes `backend_details`, `workload_type`, and optional `diagnostics` when enabled.
- Regression detection (Phase 4): results are compared against baselines in `benchmarks/baselines/` using `bench-compare`.
- History + reporting (Phase 5): `bench-report` consumes existing JSON outputs to produce Markdown/JSON history summaries.

## One-command runner (Gap #1)

Use the runner to avoid manual `RUSTFLAGS` setup and run scenario/component benches from repo root:

```bash
# Run both scenario + component benches with a config (recommended)
cargo run -p tonbo-bench-runner -- --mode all --config benches/harness/configs/ci-write-only.yaml

# Scenario only
cargo run -p tonbo-bench-runner -- --mode scenario --config benches/harness/configs/ci-write-only.yaml

# Dry-run to see the exact commands/env (no execution)
cargo run -p tonbo-bench-runner -- --mode all --config benches/harness/configs/ci-write-only.yaml --dry-run

# CI preset without specifying a config (uses ci-write-only.yaml)
cargo run -p tonbo-bench-runner -- --mode scenario --profile ci
```

Behavior:
- Sets `RUSTFLAGS="--cfg tonbo_bench"` for benchmark subprocesses only.
- Prints the exact command + env before running; exits non-zero on failure.
- `--mode {scenario|component|all}` (default: `all`), `--profile {local|ci}` (default: `local`), `--dry-run` to print without executing.
- Scenario/component benches still honor your YAML config contents; runner does not change output paths or metrics.

Common failure modes:
- Missing `--config` (or absent default for profile/local) → fix path or use `--profile ci`.
- Config path typo → runner errors before invoking cargo.
- Network issues for S3/LocalStack configs → resolve endpoint/creds; runner will surface cargo’s exit status.
- Deep runs: use `--profile deep` (future) or point at `benches/harness/configs/deep-disk.yaml` for longer, diagnostics-enabled runs.

Runner usage summary (help-style)
- Command: `cargo run -p tonbo-bench-runner -- [--mode {scenario|component|memtable|wal|sst|iterator|all}] [--config <path>] [--profile {local|ci|deep}] [--dry-run]`
- Defaults: `--mode all`, `--profile local`, `--config` required unless `--profile ci` (uses `benches/harness/configs/ci-write-only.yaml` for both suites), `--profile deep` uses `benches/harness/configs/deep-disk.yaml`.
- Suite-specific configs: `--scenario-config` and `--component-config` override `--config` so you can run different YAMLs for scenario vs component when using `--mode all`.
- Modes: `scenario` (scenarios only), `component` (all components), `memtable` / `wal` / `sst` / `iterator` (only that component), `all` (both suites; components default to memtable/wal/sst/iterator).
- Behavior: sets `RUSTFLAGS="--cfg tonbo_bench"` for subprocesses; prints the exact command + env; exits non-zero on failure.
- Outputs: JSON results under `target/bench-results/<run-id>/<bench-target>/<storage-substrate>/…` (run-id and substrate already handled by the bench harness).
- Post-run summary: runner lists new run directories and JSON files with a concise metric snippet (e.g., ops/sec, bytes/sec, wall_time_ms) to point you to artifacts.

## Run

```bash
cargo bench --bench tonbo_scenarios -- --config bench-config.yaml
cargo bench --bench tonbo_components -- --config bench-config.yaml

# Enable engine diagnostics (Phase 3) – add the feature flag
cargo bench --bench tonbo_scenarios -- --config bench-config.yaml

# Regression comparison (Phase 4)
cargo run -p bench-compare -- \
  --current target/bench-results \
  --baseline benchmarks/baselines/scenarios \
  --thresholds benches/harness/configs/regression-thresholds.yaml \
  --missing-baseline fail

# Report generation (Phase 5)
cargo run -p bench-report -- \
  --results-dir target/bench-results \
  --limit 10 \
  --output-md target/bench-reports/perf-report.md \
  --output-json target/bench-reports/perf-report.json

**Note:** `runtime.concurrency` is currently limited to `1` in the harness; higher values will abort.
```

## Output

- Results: `target/bench-results/<run-id>/<bench-target>/<storage-substrate>/<benchmark>.json`
- Temp data: `target/bench-tmp/tonbo-bench-*/` for disk runs; S3 prefixes are cleaned after runs.
- Regression reports: optional `target/bench-results/regression-report.json` emitted by the comparator.
- History/reporting artifacts: `target/bench-reports/perf-report.md` (and `.json` when requested).

# Engine diagnostics

- Bench-only diagnostics are compiled only for benches/tests; they do not ship in normal builds.

## Sample Configs

Disk write-only:
```yaml
backend:
  type: disk
  path: target/bench-tmp
workload:
  type: sequential_write
  num_records: 100000
  value_size_bytes: 256
runtime:
  concurrency: 1
```

S3 (LocalStack) read-only:
```yaml
backend:
  type: s3
  s3:
    endpoint: http://localhost:4566
    bucket: tonbo-bench
    region: us-east-1
    prefix: local-test
workload:
  type: read_only
  num_records: 1000
  value_size_bytes: 256
  warmup_records: 100
runtime:
  concurrency: 1
```

Mixed (disk) example:
```yaml
backend:
  type: disk
  path: target/bench-tmp
workload:
  type: mixed
  num_records: 2000
  value_size_bytes: 128
  read_ratio: 0.7
  write_ratio: 0.3
runtime:
  concurrency: 1
```

Diagnostics toggle (default: off):
```yaml
diagnostics:
  enabled: true
  level: basic # basic | full (future expansion)
```

## LocalStack usage

- Provide dummy credentials via `TONBO_S3_ACCESS_KEY`/`TONBO_S3_SECRET_KEY` (or standard AWS envs) and set `backend.s3.endpoint` to the LocalStack endpoint.
- The harness creates the bucket if missing and scopes all objects under a per-run prefix; cleanup is best-effort via prefix deletion.
- CI currently runs disk-only; S3/LocalStack is supported for local runs only. No real AWS calls are issued in CI.

## CI behavior (Phase 4)

- `.github/workflows/benchmarks.yml` runs small disk-only scenario benches using configs in `benches/harness/configs/ci-*.yaml`.
- `.github/workflows/benchmarks-nightly.yml` runs disk-only `ycsb_b` via `benches/harness/configs/nightly-ycsb-b.yaml`.
- `bench-compare` checks results against repo-tracked baselines in `benchmarks/baselines/scenarios/` using thresholds from `benches/harness/configs/regression-thresholds.yaml`.
- Missing baselines now fail the run; keep `benchmarks/baselines/*` in sync when adding new benches.
- Exit codes: `0` pass, `1` regression beyond threshold, `2` invalid inputs/missing baseline/parsing failure. CI fails on `1` or `2`.
- Artifacts: `target/bench-results/*.json` plus `regression-report.json` for debugging.

## Baselines

- Primary strategy: repo-tracked JSON files under `benchmarks/baselines/scenarios/`.
- Manual refresh workflow: trigger `.github/workflows/benchmarks-baseline.yml` to produce fresh baseline JSONs as artifacts; download and commit explicitly. No auto-commit to keep updates deliberate.
- Thresholds live in `benches/harness/configs/regression-thresholds.yaml`. Defaults cover throughput/latency metrics; per-benchmark overrides narrow tolerance where needed.

## Presets (Phase 5)

- Presets live at `benches/harness/configs/presets.yaml` and mirror YCSB-like mixes:
  - `ycsb_a` (50/50 mixed), `ycsb_b` (95/5 read-heavy), `ycsb_c` (read-only), `write_heavy` (sequential write).
- To use a preset, merge it into a scenario config (or generate a config on the fly):

```bash
cat benches/harness/configs/presets.yaml | yq '.presets.ycsb_a' > /tmp/bench.yaml
cat /tmp/bench.yaml - <<'EOF' > /tmp/bench.yaml
backend:
  type: disk
  path: target/bench-tmp
EOF
cargo bench --bench tonbo_scenarios -- --config /tmp/bench.yaml
```

Presets are configuration helpers only; they do not change the harness schema.

## Reporting (Phase 5)

- `bench-report` consumes existing Phase 4 JSON outputs (no schema changes) and emits Markdown and optional JSON:
  - Run metadata (run id, timestamp when encoded, commit when present)
  - Current vs baseline delta table
  - Trend summary over the last N runs (defaults to 10, bounded by available runs)
- Selection rules:
  - Current run: latest timestamped run id by default (or `--current-run <id>`)
  - Baseline: previous run by default (or `--baseline-run <id>`)
  - Trend window: last `--limit` runs up to the current run
- History strategy: primary store is CI artifacts (per-run JSON + report). Git repo remains clean; download artifacts for ad-hoc analysis.
