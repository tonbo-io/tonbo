# Benchmarks (Phase 5 – history + reporting)

- Config-driven: YAML passed via `--config`.
- Scenario support: `sequential_write`/`write_only`, `read_only`, `mixed`.
- Backends: disk and S3 (LocalStack in CI) through the benchmark harness.
- Component support: `memtable_insert` remains as in Phase 1.
- Output schema is backward compatible and includes `backend_details`, `workload_type`, and optional `diagnostics` when enabled.
- Regression detection (Phase 4): results are compared against baselines in `benchmarks/baselines/` using `bench-compare`.
- History + reporting (Phase 5): `bench-report` consumes existing JSON outputs to produce Markdown/JSON history summaries.

## Run

```bash
cargo bench --bench tonbo_scenarios -- --config bench-config.yaml
cargo bench --bench tonbo_components -- --config bench-config.yaml

# Enable engine diagnostics (Phase 3) – add the feature flag
cargo bench --features bench-diagnostics --bench tonbo_scenarios -- --config bench-config.yaml

# Regression comparison (Phase 4)
cargo run -p bench-compare -- \
  --current target/bench-results \
  --baseline benchmarks/baselines/scenarios \
  --thresholds benches/harness/configs/regression-thresholds.yaml

# Report generation (Phase 5)
cargo run -p bench-report -- \
  --results-dir target/bench-results \
  --limit 10 \
  --output-md target/bench-reports/perf-report.md \
  --output-json target/bench-reports/perf-report.json
```

## Output

- Results: `target/bench-results/<timestamp>-<benchmark>.json`
- Temp data: `target/bench-tmp/tonbo-bench-*/` for disk runs; S3 prefixes are cleaned after runs.
- Regression reports: optional `target/bench-results/regression-report.json` emitted by the comparator.
- History/reporting artifacts: `target/bench-reports/perf-report.md` (and `.json` when requested).

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
- CI uses LocalStack only; no real AWS calls are issued in CI.

## CI behavior (Phase 4)

- `.github/workflows/benchmarks.yml` runs small disk-only scenario benches using configs in `benches/harness/configs/ci-*.yaml`.
- `bench-compare` checks results against repo-tracked baselines in `benchmarks/baselines/scenarios/` using thresholds from `benches/harness/configs/regression-thresholds.yaml`.
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
