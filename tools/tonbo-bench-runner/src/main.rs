//! Consolidated runner for Tonbo benchmarks (run/report/compare).
//!
//! Recon notes:
//! - Current benches run via `cargo bench --bench tonbo_scenarios -- --config <path>` and `cargo
//!   bench --bench tonbo_components -- --config <path>`.
//! - Bench subprocesses enable `--features test` to access test helpers when needed.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "tonbo-bench-runner")]
#[command(about = "One-command runner for Tonbo scenario and component benchmarks")]
#[command(args_conflicts_with_subcommands = true)]
#[command(subcommand_required = false)]
struct Args {
    #[command(subcommand)]
    command: Option<CommandKind>,
    #[command(flatten)]
    run: RunArgs,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    /// Generate a markdown/JSON report from benchmark results.
    Report(ReportArgs),
    /// Compare benchmark results against a baseline.
    Compare(CompareArgs),
}

#[derive(Debug, clap::Args, Clone)]
struct RunArgs {
    /// Which bench suite(s) to run.
    #[arg(long, value_enum, default_value = "all")]
    mode: Mode,
    /// Path to YAML bench config. Required for scenarios; defaults for --profile ci.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Optional scenario-only config path (overrides --config for scenarios).
    #[arg(long)]
    scenario_config: Option<PathBuf>,
    /// Optional component-only config path (overrides --config for components).
    #[arg(long)]
    component_config: Option<PathBuf>,
    /// Profile presets (currently only affects default config resolution).
    #[arg(long, value_enum, default_value = "local")]
    profile: Profile,
    /// Print commands without executing them.
    #[arg(long)]
    dry_run: bool,
    /// Write a markdown summary for new runs to this path (optional).
    #[arg(long)]
    report_md: Option<PathBuf>,
    /// Override diagnostics sampling interval (milliseconds).
    #[arg(long)]
    diagnostics_sample_ms: Option<u64>,
    /// Override maximum number of diagnostics samples.
    #[arg(long)]
    diagnostics_max_samples: Option<usize>,
}

#[derive(Clone, Copy, Debug, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Scenario,
    Component,
    #[serde(rename = "memtable")]
    #[value(name = "memtable")]
    ComponentMemtable,
    #[serde(rename = "wal")]
    #[value(name = "wal")]
    ComponentWal,
    #[serde(rename = "sst")]
    #[value(name = "sst")]
    ComponentSst,
    #[serde(rename = "iterator")]
    #[value(name = "iterator")]
    ComponentIterator,
    All,
}

#[derive(Clone, Copy, Debug, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Profile {
    Local,
    Ci,
    Deep,
}

#[derive(Debug)]
struct BenchCommand {
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    description: String,
}

#[derive(Debug, Error)]
enum RunnerError {
    #[error("config file not found: {0}")]
    MissingConfig(String),
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Some(CommandKind::Report(report)) => run_report(report),
        Some(CommandKind::Compare(compare)) => run_compare(compare),
        None => run_bench(args.run),
    }
}

fn run_bench(args: RunArgs) -> Result<()> {
    let configs = resolve_configs(&args)?;
    let commands = build_commands(&args, &configs, &[])?;

    if args.dry_run {
        for cmd in &commands {
            println!("{}", format_command(cmd));
        }
        return Ok(());
    }

    let before_runs = list_run_dirs(default_results_root());

    for cmd in &commands {
        println!("Running: {}", format_command(cmd));
        let status = spawn_command(cmd)?;
        if !status.success() {
            anyhow::bail!(
                "command failed ({}): exit code {:?}",
                cmd.description,
                status.code()
            );
        }
    }

    let after_runs = list_run_dirs(default_results_root());
    let run_summaries = collect_run_summaries(&before_runs, &after_runs);
    print_run_summaries(&run_summaries);
    if let Some(path) = args.report_md.as_ref() {
        let markdown = render_markdown_report(&run_summaries);
        if path.as_os_str() == "-" {
            println!("{markdown}");
        } else {
            write_markdown_report(&markdown, path)?;
            println!("Wrote report to {}", path.display());
        }
    }

    Ok(())
}

/// Report generator for benchmark performance history.
#[derive(Debug, clap::Args)]
struct ReportArgs {
    /// Directory containing benchmark result JSON files.
    #[arg(long)]
    results_dir: PathBuf,
    /// Run ID to use as the "current" run (defaults to latest timestamp).
    #[arg(long)]
    current_run: Option<String>,
    /// Run ID to use as baseline (defaults to previous run if available).
    #[arg(long)]
    baseline_run: Option<String>,
    /// Number of runs to include in trend calculations.
    #[arg(long, default_value_t = 10)]
    limit: usize,
    /// Output Markdown path.
    #[arg(long, default_value = "target/bench-reports/perf-report.md")]
    output_md: PathBuf,
    /// Optional machine-readable JSON report path.
    #[arg(long)]
    output_json: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
struct CompareArgs {
    /// Path to current benchmark results (file or directory).
    #[arg(long)]
    current: PathBuf,
    /// Path to baseline benchmark results (file or directory).
    #[arg(long)]
    baseline: PathBuf,
    /// YAML thresholds file.
    #[arg(long)]
    thresholds: PathBuf,
    /// Optional path to write a machine-readable report (JSON).
    #[arg(long)]
    report: Option<PathBuf>,
    /// Behavior when a benchmark is missing in the baseline.
    #[arg(long, default_value = "fail", value_parser = ["warn", "fail"])]
    missing_baseline: String,
}

fn run_report(args: ReportArgs) -> Result<()> {
    if args.limit == 0 {
        return Err(anyhow!("--limit must be > 0"));
    }

    let runs = load_runs(&args.results_dir)?;
    if runs.is_empty() {
        return Err(anyhow!(
            "no benchmark runs found under {}",
            args.results_dir.display()
        ));
    }

    let sorted_ids: BTreeSet<String> = runs.keys().cloned().collect();
    let current_id = select_current_run(&sorted_ids, args.current_run.as_deref())?;
    let baseline_id = select_baseline_run(&sorted_ids, &current_id, args.baseline_run.as_deref());
    let trend_ids = select_trend_ids(&sorted_ids, &current_id, args.limit);

    let current = runs
        .get(&current_id)
        .ok_or_else(|| anyhow!("selected current run {} not found", current_id))?;
    let baseline = baseline_id.as_ref().and_then(|id| runs.get(id));

    let summary = baseline
        .map(|b| build_summary(b, current))
        .unwrap_or_default();
    let trend = build_trend(&runs, &current_id, &trend_ids);

    write_history_markdown_report(
        &args.output_md,
        current,
        baseline_id.as_deref(),
        &summary,
        &trend,
        &trend_ids,
    )?;
    if let Some(json_path) = &args.output_json {
        write_history_json_report(
            json_path,
            current,
            baseline_id.as_deref(),
            &summary,
            &trend,
            &trend_ids,
        )?;
    }

    Ok(())
}

fn run_compare(args: CompareArgs) -> Result<()> {
    if args.missing_baseline != "warn" && args.missing_baseline != "fail" {
        anyhow::bail!("missing_baseline must be 'warn' or 'fail'");
    }

    let thresholds_file = load_thresholds(&args.thresholds)?;
    if !thresholds_file.regression.enabled {
        println!("regression checks disabled via thresholds file");
        return Ok(());
    }

    let baseline = load_run(&args.baseline).context("load baseline results")?;
    let current = load_run(&args.current).context("load current results")?;

    let (samples, exit_code) =
        compare_runs(&baseline, &current, &thresholds_file.regression, &args);
    render_compare_report(&samples);
    if let Some(report_path) = args.report
        && let Err(err) = write_machine_report(&report_path, &samples)
    {
        eprintln!("failed to write report to {}: {err}", report_path.display());
    }

    if exit_code == 0 {
        Ok(())
    } else {
        std::process::exit(exit_code);
    }
}

fn build_commands(
    args: &RunArgs,
    configs: &ResolvedConfigs,
    extra_env: &[(String, String)],
) -> Result<Vec<BenchCommand>> {
    let mut commands = Vec::new();
    let components = mode_components(args.mode);
    let diagnostics_env = diagnostics_env(args, extra_env);

    match args.mode {
        Mode::Scenario | Mode::All => {
            let config = configs
                .scenario
                .as_ref()
                .ok_or_else(|| RunnerError::MissingConfig("scenario requires --config".into()))?;
            commands.push(BenchCommand {
                program: "cargo".to_string(),
                args: vec![
                    "bench".into(),
                    "--features".into(),
                    "test".into(),
                    "--bench".into(),
                    "tonbo_scenarios".into(),
                    "--".into(),
                    "--config".into(),
                    config.display().to_string(),
                ],
                env: diagnostics_env.clone(),
                description: format!("scenario benches with config {}", config.display()),
            });
        }
        _ => {}
    }

    match args.mode {
        Mode::Component
        | Mode::All
        | Mode::ComponentMemtable
        | Mode::ComponentWal
        | Mode::ComponentSst
        | Mode::ComponentIterator => {
            let config = configs
                .component
                .as_ref()
                .ok_or_else(|| RunnerError::MissingConfig("component requires --config".into()))?;
            let mut args_vec = vec![
                "bench".into(),
                "--features".into(),
                "test".into(),
                "--bench".into(),
                "tonbo_components".into(),
                "--".into(),
                "--config".into(),
            ];
            args_vec.push(config.display().to_string());
            args_vec.push("--component".into());
            args_vec.push(components.join(","));
            commands.push(BenchCommand {
                program: "cargo".to_string(),
                args: args_vec,
                env: diagnostics_env.clone(),
                description: format!(
                    "component benches with config {} components={}",
                    config.display(),
                    components.join(",")
                ),
            });
        }
        _ => {}
    }

    Ok(commands)
}

fn diagnostics_env(args: &RunArgs, extra_env: &[(String, String)]) -> Vec<(String, String)> {
    let mut envs = Vec::new();
    if let Some(ms) = args.diagnostics_sample_ms {
        envs.push(("TONBO_BENCH_DIAG_SAMPLE_MS".to_string(), ms.to_string()));
    }
    if let Some(max) = args.diagnostics_max_samples {
        envs.push(("TONBO_BENCH_DIAG_MAX_SAMPLES".to_string(), max.to_string()));
    }
    envs.extend_from_slice(extra_env);
    envs
}

fn mode_components(mode: Mode) -> Vec<String> {
    match mode {
        Mode::ComponentMemtable => vec!["memtable".into()],
        Mode::ComponentWal => vec!["wal".into()],
        Mode::ComponentSst => vec!["sst".into()],
        Mode::ComponentIterator => vec!["iterator".into()],
        _ => vec![
            "memtable".into(),
            "wal".into(),
            "sst".into(),
            "iterator".into(),
        ],
    }
}

#[derive(Debug)]
struct ResolvedConfigs {
    scenario: Option<PathBuf>,
    component: Option<PathBuf>,
}

fn resolve_configs(args: &RunArgs) -> Result<ResolvedConfigs> {
    let default_ci = repo_root().join("benches/harness/configs/ci.yaml");
    let base = args.config.clone();
    let scenario = args.scenario_config.clone().or_else(|| base.clone());
    let component = args.component_config.clone().or(base);

    let (scenario, component) = match args.profile {
        Profile::Ci => (
            scenario.or(Some(default_ci.clone())),
            component.or(Some(default_ci)),
        ),
        Profile::Deep => (
            scenario.or_else(|| Some(repo_root().join("benches/harness/configs/deep-disk.yaml"))),
            component.or_else(|| Some(repo_root().join("benches/harness/configs/deep-disk.yaml"))),
        ),
        Profile::Local => (scenario, component),
    };

    validate_config("scenario", &scenario, &args.mode)?;
    validate_config("component", &component, &args.mode)?;

    Ok(ResolvedConfigs {
        scenario,
        component,
    })
}

fn repo_root() -> PathBuf {
    // Walk ancestors from the crate dir to locate the repo root that carries bench configs.
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    for ancestor in manifest_dir.ancestors() {
        if ancestor.join("benches/harness/configs").exists() {
            return ancestor.to_path_buf();
        }
    }
    manifest_dir.to_path_buf()
}

fn validate_config(kind: &str, config: &Option<PathBuf>, mode: &Mode) -> Result<()> {
    let needed = matches!(
        (kind, mode),
        ("scenario", Mode::Scenario | Mode::All)
            | (
                "component",
                Mode::Component
                    | Mode::All
                    | Mode::ComponentMemtable
                    | Mode::ComponentWal
                    | Mode::ComponentSst
                    | Mode::ComponentIterator
            )
    );
    if needed {
        let cfg = config
            .as_ref()
            .ok_or_else(|| RunnerError::MissingConfig(format!("{kind} requires --config")))?;
        if !cfg.exists() {
            return Err(RunnerError::MissingConfig(cfg.display().to_string()).into());
        }
    }
    Ok(())
}

fn spawn_command(cmd: &BenchCommand) -> Result<std::process::ExitStatus> {
    let mut command = Command::new(&cmd.program);
    command.args(&cmd.args);
    for (k, v) in &cmd.env {
        command.env(k, v);
    }
    command
        .status()
        .with_context(|| format!("failed to start {}", cmd.description))
}

fn format_command(cmd: &BenchCommand) -> String {
    let envs = cmd
        .env
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");
    let args = cmd.args.join(" ");
    if envs.is_empty() {
        format!("{} {}", cmd.program, args)
    } else {
        format!("{envs} {} {}", cmd.program, args)
    }
}

fn default_results_root() -> PathBuf {
    PathBuf::from("target/bench-results")
}

fn list_run_dirs(root: PathBuf) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(entries) = fs::read_dir(&root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            }
        }
    }
    dirs
}

fn collect_run_summaries(before: &[PathBuf], after: &[PathBuf]) -> Vec<RunSummary> {
    let before_set: HashSet<_> = before.iter().collect();
    let mut new_runs: Vec<&PathBuf> = after.iter().filter(|p| !before_set.contains(p)).collect();
    if new_runs.is_empty() && !after.is_empty() {
        // If we can't detect new ones (e.g., rerun), fall back to latest by name.
        new_runs.push(
            after
                .iter()
                .max_by_key(|p| p.file_name().map(|s| s.to_os_string()).unwrap_or_default())
                .unwrap(),
        );
    }

    new_runs
        .into_iter()
        .map(|run| {
            let files = list_json_files(run);
            let summaries = files
                .into_iter()
                .map(|file| load_summary(&file))
                .filter_map(|res| res.transpose())
                .collect::<Result<Vec<_>>>()
                .unwrap_or_default();
            RunSummary {
                run_dir: run.clone(),
                files: summaries,
            }
        })
        .collect()
}

fn print_run_summaries(runs: &[RunSummary]) {
    if runs.is_empty() {
        println!("No benchmark outputs found in target/bench-results");
        return;
    }
    println!("Run summary:");
    for run in runs {
        let rel_run = run.run_dir.strip_prefix(".").unwrap_or(&run.run_dir);
        println!("  - run: {}", rel_run.display());
        if run.files.is_empty() {
            println!("    (no JSON outputs found)");
            continue;
        }
        for summary in &run.files {
            let rel = summary
                .path
                .strip_prefix(".")
                .unwrap_or(&summary.path)
                .display();
            println!(
                "    * {name} [{kind}] target={target} backend={backend} substr={substr} \
                 {metrics}{diag} ({path})",
                name = summary.benchmark_name,
                kind = summary.benchmark_type,
                target = summary.bench_target.as_deref().unwrap_or("unknown"),
                backend = summary.backend.as_deref().unwrap_or("unknown"),
                substr = summary.storage_substrate.as_deref().unwrap_or("unknown"),
                metrics = summary.metrics_summary,
                diag = if summary.diagnostics_summary.is_empty() {
                    "".to_string()
                } else {
                    format!(" diag={}", summary.diagnostics_summary)
                },
                path = rel
            );
        }
    }
}

fn list_json_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().map(|ext| ext == "json").unwrap_or(false) {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    files
}

#[derive(Debug, Deserialize)]
struct SummaryResult {
    benchmark_name: String,
    benchmark_type: String,
    #[serde(default)]
    bench_target: Option<String>,
    #[serde(default)]
    storage_substrate: Option<String>,
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    metrics: serde_json::Value,
    #[serde(default)]
    diagnostics: Option<serde_json::Value>,
}

struct FileSummary {
    benchmark_name: String,
    benchmark_type: String,
    bench_target: Option<String>,
    storage_substrate: Option<String>,
    backend: Option<String>,
    metrics_summary: String,
    diagnostics_summary: String,
    path: PathBuf,
}

struct RunSummary {
    run_dir: PathBuf,
    files: Vec<FileSummary>,
}

fn load_summary(path: &Path) -> Result<Option<FileSummary>> {
    let data = fs::read(path)?;
    let parsed: SummaryResult = match serde_json::from_slice(&data) {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };
    let metrics_summary = summarize_metrics(&parsed.metrics);
    let diagnostics_summary = summarize_diagnostics(&parsed.diagnostics);
    Ok(Some(FileSummary {
        benchmark_name: parsed.benchmark_name,
        benchmark_type: parsed.benchmark_type,
        bench_target: parsed.bench_target,
        storage_substrate: parsed.storage_substrate,
        backend: parsed.backend,
        metrics_summary,
        diagnostics_summary,
        path: path.to_path_buf(),
    }))
}

fn summarize_metrics(metrics: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    let keys = [
        "ops_per_sec",
        "bytes_per_sec",
        "bytes_per_sec_write",
        "bytes_per_sec_read",
        "wall_time_ms",
    ];
    for key in keys {
        if let Some(val) = metrics.get(key) {
            if let Some(num) = val.as_f64() {
                parts.push(format!("{key}={num:.2}"));
            } else if let Some(num) = val.as_u64() {
                parts.push(format!("{key}={num}"));
            }
        }
    }
    if parts.is_empty() {
        "metrics=n/a".into()
    } else {
        parts.join(" ")
    }
}

fn summarize_diagnostics(diag: &Option<serde_json::Value>) -> String {
    let Some(val) = diag else {
        return String::new();
    };
    let mut parts = Vec::new();
    if let Some(wa) = val.get("write_amplification")
        && let (Some(logical), Some(physical)) = (
            wa.get("logical_bytes_written"),
            wa.get("physical_bytes_written"),
        )
        && let (Some(l), Some(p)) = (logical.as_u64(), physical.as_u64())
    {
        parts.push(format!("wa: logical={}B physical={}B", l, p));
    }
    if let Some(wal) = val.get("wal")
        && let Some(sync) = wal.get("sync_operations").and_then(|v| v.as_u64())
    {
        parts.push(format!("wal_syncs={sync}"));
    }
    if let Some(wal) = val.get("wal")
        && let Some(lat) = wal.get("append_latency")
    {
        if let Some(mean) = lat.get("mean_us").and_then(|v| v.as_f64()) {
            parts.push(format!("wal_p50_us={mean:.0}"));
        }
        if let Some(max) = lat.get("max_us").and_then(|v| v.as_u64()) {
            parts.push(format!("wal_max_us={max}"));
        }
    }
    if let Some(flush) = val.get("flush") {
        if let Some(bytes_per_sec) = flush.get("bytes_per_sec").and_then(|v| v.as_f64()) {
            parts.push(format!("flush_bps={bytes_per_sec:.0}"));
        }
        if let Some(count) = flush.get("count").and_then(|v| v.as_u64()) {
            parts.push(format!("flush_count={count}"));
        }
    }
    parts.join(" ")
}

fn render_markdown_report(runs: &[RunSummary]) -> String {
    let mut out = String::new();
    if runs.is_empty() {
        out.push_str("# Bench Report\n\nNo benchmark outputs found.\n");
    } else {
        out.push_str("# Bench Report\n\n");
        for run in runs {
            let rel_run = run.run_dir.strip_prefix(".").unwrap_or(&run.run_dir);
            out.push_str(&format!("## Run {}\n\n", rel_run.display()));
            if run.files.is_empty() {
                out.push_str("- No JSON outputs found\n\n");
            } else {
                let (scenarios, components): (Vec<_>, Vec<_>) = run
                    .files
                    .iter()
                    .partition(|f| f.benchmark_type == "scenario");
                if !scenarios.is_empty() {
                    out.push_str("### Scenarios\n");
                    for f in scenarios {
                        let rel = f.path.strip_prefix(".").unwrap_or(&f.path).display();
                        out.push_str(&format!(
                            "- `{}` target={} backend={} substrate={} {}\n",
                            f.benchmark_name,
                            f.bench_target.as_deref().unwrap_or("unknown"),
                            f.backend.as_deref().unwrap_or("unknown"),
                            f.storage_substrate.as_deref().unwrap_or("unknown"),
                            f.metrics_summary,
                        ));
                        if !f.diagnostics_summary.is_empty() {
                            out.push_str(&format!("  - diag: {}\n", f.diagnostics_summary));
                        }
                        out.push_str(&format!("  - file: `{}`\n", rel));
                    }
                    out.push('\n');
                }
                if !components.is_empty() {
                    out.push_str("### Components\n");
                    for f in components {
                        let rel = f.path.strip_prefix(".").unwrap_or(&f.path).display();
                        out.push_str(&format!(
                            "- `{}` target={} backend={} substrate={} {}\n",
                            f.benchmark_name,
                            f.bench_target.as_deref().unwrap_or("unknown"),
                            f.backend.as_deref().unwrap_or("unknown"),
                            f.storage_substrate.as_deref().unwrap_or("unknown"),
                            f.metrics_summary,
                        ));
                        if !f.diagnostics_summary.is_empty() {
                            out.push_str(&format!("  - diag: {}\n", f.diagnostics_summary));
                        }
                        out.push_str(&format!("  - file: `{}`\n", rel));
                    }
                    out.push('\n');
                }
            }
        }
    }
    out
}

fn write_markdown_report(content: &str, path: &Path) -> Result<()> {
    fs::write(path, content)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct RawBenchResult {
    #[serde(default)]
    benchmark_name: String,
    #[serde(default)]
    metrics: Value,
    #[serde(default)]
    git_commit: Option<String>,
}

#[derive(Debug)]
struct HistoryRun {
    run_id: String,
    timestamp: Option<DateTime<Utc>>,
    commit: Option<String>,
    benches: BTreeMap<String, BenchMetrics>,
}

#[derive(Debug)]
struct BenchMetrics {
    metrics: BTreeMap<String, f64>,
}

#[derive(Debug)]
struct SummaryRow {
    benchmark: String,
    metric: String,
    baseline: f64,
    current: f64,
    delta: f64,
}

#[derive(Debug)]
struct TrendRow {
    benchmark: String,
    metric: String,
    min_delta: f64,
    median_delta: f64,
    max_delta: f64,
    samples: usize,
}

fn load_runs(results_dir: &Path) -> Result<BTreeMap<String, HistoryRun>> {
    let mut entries = Vec::new();
    let mut stack = vec![results_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let listing = match fs::read_dir(&dir) {
            Ok(ls) => ls,
            Err(err) => {
                eprintln!("skipping {}: list error: {err}", dir.display());
                continue;
            }
        };
        for entry in listing.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            entries.push(path);
        }
    }
    entries.sort();

    let mut runs: BTreeMap<String, HistoryRun> = BTreeMap::new();
    for path in entries {
        let rel = match path.strip_prefix(results_dir) {
            Ok(rel) => rel,
            Err(_) => {
                eprintln!("skipping {}: not under results dir", path.display());
                continue;
            }
        };
        if let Some(name) = rel.file_name().and_then(|s| s.to_str())
            && name == "regression-report.json"
        {
            continue;
        }
        let mut rel_components = rel.components();
        let Some(run_component) = rel_components.next() else {
            eprintln!("skipping {}: missing run component", path.display());
            continue;
        };
        let run_id = run_component.as_os_str().to_string_lossy().to_string();

        let content = match fs::read_to_string(&path) {
            Ok(buf) => buf,
            Err(err) => {
                eprintln!("skipping {}: read error: {err}", path.display());
                continue;
            }
        };
        let doc: RawBenchResult = match serde_json::from_str(&content) {
            Ok(doc) => doc,
            Err(err) => {
                eprintln!("skipping {}: parse error: {err}", path.display());
                continue;
            }
        };
        let benchmark_name = doc.benchmark_name.clone();
        let metrics = extract_numeric_metrics(&doc.metrics);
        if metrics.is_empty() {
            eprintln!("skipping {}; no numeric metrics", path.display());
            continue;
        }
        let timestamp = parse_timestamp(&run_id);
        let bench_entry = BenchMetrics { metrics };

        let run = runs
            .entry(run_id.to_string())
            .or_insert_with(|| HistoryRun {
                run_id: run_id.to_string(),
                timestamp,
                commit: doc.git_commit.clone(),
                benches: BTreeMap::new(),
            });
        run.timestamp = run.timestamp.or(timestamp);
        run.commit = run.commit.clone().or(doc.git_commit.clone());
        run.benches.insert(benchmark_name.to_string(), bench_entry);
    }

    Ok(runs)
}

fn parse_timestamp(run_id: &str) -> Option<DateTime<Utc>> {
    if run_id.len() < 14 {
        return None;
    }
    DateTime::parse_from_str(&(run_id[..14].to_string() + " +0000"), "%Y%m%d%H%M%S %z")
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn select_current_run(sorted_ids: &BTreeSet<String>, explicit: Option<&str>) -> Result<String> {
    if let Some(id) = explicit {
        if sorted_ids.contains(id) {
            return Ok(id.to_string());
        }
        return Err(anyhow!("requested current run {id} not found"));
    }
    sorted_ids
        .iter()
        .max()
        .cloned()
        .ok_or_else(|| anyhow!("no runs available"))
}

fn select_baseline_run(
    sorted_ids: &BTreeSet<String>,
    current: &str,
    explicit: Option<&str>,
) -> Option<String> {
    if let Some(id) = explicit {
        return sorted_ids.get(id).cloned();
    }
    let before_current: Vec<&String> = sorted_ids
        .iter()
        .filter(|id| id.as_str() < current)
        .collect();
    before_current.last().map(|s| (*s).clone())
}

fn select_trend_ids(sorted_ids: &BTreeSet<String>, current: &str, limit: usize) -> Vec<String> {
    let mut ids: Vec<String> = sorted_ids
        .iter()
        .filter(|id| id.as_str() <= current)
        .cloned()
        .collect();
    ids.sort();
    if ids.len() > limit {
        ids.drain(0..(ids.len() - limit));
    }
    ids
}

fn build_summary(baseline: &HistoryRun, current: &HistoryRun) -> Vec<SummaryRow> {
    let mut rows = Vec::new();
    for (bench, cur_metrics) in &current.benches {
        let Some(base_metrics) = baseline.benches.get(bench) else {
            continue;
        };
        for (metric, cur_value) in &cur_metrics.metrics {
            if let Some(base_value) = base_metrics.metrics.get(metric) {
                if *base_value == 0.0 {
                    continue;
                }
                let delta = (cur_value - base_value) / base_value;
                rows.push(SummaryRow {
                    benchmark: bench.clone(),
                    metric: metric.clone(),
                    baseline: *base_value,
                    current: *cur_value,
                    delta,
                });
            }
        }
    }
    rows.sort_by(|a, b| {
        (a.benchmark.as_str(), a.metric.as_str()).cmp(&(b.benchmark.as_str(), b.metric.as_str()))
    });
    rows
}

fn build_trend(
    runs: &BTreeMap<String, HistoryRun>,
    current_id: &str,
    trend_ids: &[String],
) -> Vec<TrendRow> {
    let mut rows = Vec::new();
    let mut metrics_index: BTreeMap<(String, String), Vec<(String, f64)>> = BTreeMap::new();
    for run_id in trend_ids {
        if let Some(run) = runs.get(run_id) {
            for (bench, metrics) in &run.benches {
                for (metric, value) in &metrics.metrics {
                    metrics_index
                        .entry((bench.clone(), metric.clone()))
                        .or_default()
                        .push((run_id.clone(), *value));
                }
            }
        }
    }

    for ((bench, metric), samples) in metrics_index {
        let Some(current_value) = samples
            .iter()
            .find(|(id, _)| id == current_id)
            .map(|(_, v)| *v)
        else {
            continue;
        };
        let mut deltas: Vec<f64> = samples
            .iter()
            .map(|(_, v)| {
                if current_value == 0.0 {
                    0.0
                } else {
                    (v - current_value) / current_value
                }
            })
            .collect();
        if deltas.is_empty() {
            continue;
        }
        deltas.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let min_delta = *deltas.first().unwrap_or(&0.0);
        let max_delta = *deltas.last().unwrap_or(&0.0);
        let median_delta = {
            let mid = deltas.len() / 2;
            if deltas.len().is_multiple_of(2) {
                (deltas[mid - 1] + deltas[mid]) / 2.0
            } else {
                deltas[mid]
            }
        };
        rows.push(TrendRow {
            benchmark: bench,
            metric,
            min_delta,
            median_delta,
            max_delta,
            samples: deltas.len(),
        });
    }

    rows.sort_by(|a, b| {
        (a.benchmark.as_str(), a.metric.as_str()).cmp(&(b.benchmark.as_str(), b.metric.as_str()))
    });
    rows
}

fn write_history_markdown_report(
    path: &Path,
    current: &HistoryRun,
    baseline_id: Option<&str>,
    summary: &[SummaryRow],
    trend: &[TrendRow],
    trend_ids: &[String],
) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create report directory {}", parent.display()))?;
    }

    let mut out = String::new();
    out.push_str("# Performance Report\n\n");
    out.push_str("## Run Metadata\n\n");
    out.push_str(&format!("- current run: `{}`\n", current.run_id));
    if let Some(ts) = current.timestamp {
        out.push_str(&format!("- timestamp (UTC): `{}`\n", ts));
    }
    if let Some(commit) = &current.commit {
        out.push_str(&format!("- git commit: `{commit}`\n"));
    }
    if let Some(base) = baseline_id {
        out.push_str(&format!("- baseline run: `{base}`\n"));
    } else {
        out.push_str("- baseline run: none (first or single run)\n");
    }
    out.push_str(&format!(
        "- trend window: last {} run(s): {}\n\n",
        trend_ids.len(),
        trend_ids.join(", ")
    ));

    out.push_str("## Summary (current vs baseline)\n\n");
    if summary.is_empty() {
        out.push_str("_no baseline available or no overlapping metrics_\n\n");
    } else {
        out.push_str("| Benchmark | Metric | Baseline | Current | Delta (%) |\n");
        out.push_str("|---|---:|---:|---:|---:|\n");
        for row in summary {
            out.push_str(&format!(
                "| {} | {} | {:.4} | {:.4} | {:+.2}% |\n",
                row.benchmark,
                row.metric,
                row.baseline,
                row.current,
                row.delta * 100.0
            ));
        }
        out.push('\n');
    }

    out.push_str("## Trends (last N runs vs current)\n\n");
    if trend.is_empty() {
        out.push_str("_insufficient data for trends_\n");
    } else {
        out.push_str(
            "| Benchmark | Metric | Min Delta (%) | Median Delta (%) | Max Delta (%) | Samples |\n",
        );
        out.push_str("|---|---:|---:|---:|---:|---:|\n");
        for row in trend {
            out.push_str(&format!(
                "| {} | {} | {:+.2}% | {:+.2}% | {:+.2}% | {} |\n",
                row.benchmark,
                row.metric,
                row.min_delta * 100.0,
                row.median_delta * 100.0,
                row.max_delta * 100.0,
                row.samples
            ));
        }
    }

    fs::write(path, out).with_context(|| format!("write markdown report {}", path.display()))?;
    Ok(())
}

fn write_history_json_report(
    path: &Path,
    current: &HistoryRun,
    baseline_id: Option<&str>,
    summary: &[SummaryRow],
    trend: &[TrendRow],
    trend_ids: &[String],
) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create json report directory {}", parent.display()))?;
    }

    let json = serde_json::json!({
        "schema_version": "1",
        "current_run": current.run_id,
        "baseline_run": baseline_id,
        "timestamp": current.timestamp.map(|t| t.to_rfc3339()),
        "git_commit": current.commit,
        "trend_window": trend_ids,
        "summary": summary.iter().map(|r| serde_json::json!({
            "benchmark": r.benchmark,
            "metric": r.metric,
            "baseline": r.baseline,
            "current": r.current,
            "delta": r.delta,
        })).collect::<Vec<_>>(),
        "trends": trend.iter().map(|r| serde_json::json!({
            "benchmark": r.benchmark,
            "metric": r.metric,
            "min_delta": r.min_delta,
            "median_delta": r.median_delta,
            "max_delta": r.max_delta,
            "samples": r.samples,
        })).collect::<Vec<_>>(),
    });

    let buf = serde_json::to_vec_pretty(&json)?;
    fs::write(path, buf).with_context(|| format!("write json report {}", path.display()))?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ThresholdsFile {
    #[serde(default)]
    regression: RegressionThresholds,
}

#[derive(Debug, Deserialize)]
struct RegressionThresholds {
    #[serde(default = "RegressionThresholds::default_enabled")]
    enabled: bool,
    #[serde(default)]
    default: BTreeMap<String, f64>,
    #[serde(default)]
    per_benchmark: BTreeMap<String, BTreeMap<String, f64>>,
}

impl Default for RegressionThresholds {
    fn default() -> Self {
        Self {
            enabled: true,
            default: BTreeMap::new(),
            per_benchmark: BTreeMap::new(),
        }
    }
}

impl RegressionThresholds {
    fn default_enabled() -> bool {
        true
    }
}

#[derive(Debug)]
struct CompareRun {
    benchmarks: BTreeMap<String, BTreeMap<String, f64>>,
}

#[derive(Debug)]
struct MetricSample {
    benchmark: String,
    metric: String,
    baseline: f64,
    current: f64,
    delta: f64,
    threshold: f64,
    status: MetricStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricStatus {
    Pass,
    Fail,
}

fn load_thresholds(path: &Path) -> Result<ThresholdsFile> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read thresholds at {}", path.display()))?;
    let cfg: ThresholdsFile = serde_yaml::from_str(&content)
        .with_context(|| format!("parse YAML thresholds at {}", path.display()))?;
    Ok(cfg)
}

fn collect_json_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("list directory {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_json_files_recursive(&path, files)?;
        } else if file_type.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            files.push(path);
        }
    }
    Ok(())
}

fn load_run(path: &Path) -> Result<CompareRun> {
    let mut files = Vec::new();
    let meta = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if meta.is_file() {
        files.push(path.to_path_buf());
    } else if meta.is_dir() {
        collect_json_files_recursive(path, &mut files)?;
        files.sort();
    } else {
        return Err(anyhow!("unsupported path type: {}", path.display()));
    }

    if files.is_empty() {
        return Err(anyhow!(
            "no benchmark result files found under {}",
            path.display()
        ));
    }

    let mut benchmarks: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for file in files {
        let content =
            fs::read_to_string(&file).with_context(|| format!("read {}", file.display()))?;
        let result: RawBenchResult = match serde_json::from_str(&content) {
            Ok(res) => res,
            Err(err) => {
                eprintln!("skipping {}: {err}", file.display());
                continue;
            }
        };
        let metrics = extract_numeric_metrics(&result.metrics);
        benchmarks.insert(result.benchmark_name.clone(), metrics);
    }

    if benchmarks.is_empty() {
        return Err(anyhow!(
            "no valid benchmark entries found under {}",
            path.display()
        ));
    }

    Ok(CompareRun { benchmarks })
}

fn compare_runs(
    baseline: &CompareRun,
    current: &CompareRun,
    thresholds: &RegressionThresholds,
    args: &CompareArgs,
) -> (Vec<MetricSample>, i32) {
    let mut samples = Vec::new();
    let mut exit_code = 0;

    for (bench_name, current_metrics) in &current.benchmarks {
        let mut threshold_map = thresholds.default.clone();
        if let Some(overrides) = thresholds.per_benchmark.get(bench_name) {
            for (k, v) in overrides {
                threshold_map.insert(k.clone(), *v);
            }
        }
        if threshold_map.is_empty() {
            continue;
        }

        let Some(base_metrics) = baseline.benchmarks.get(bench_name) else {
            eprintln!("baseline missing benchmark {bench_name}");
            if args.missing_baseline == "fail" {
                exit_code = exit_code.max(2);
            }
            continue;
        };

        for (metric, threshold) in threshold_map {
            let Some(base_value) = base_metrics.get(&metric) else {
                eprintln!("baseline missing metric {metric} for benchmark {bench_name}");
                continue;
            };
            let Some(cur_value) = current_metrics.get(&metric) else {
                eprintln!("current run missing metric {metric} for benchmark {bench_name}");
                continue;
            };
            if *base_value == 0.0 {
                eprintln!(
                    "baseline value for {bench_name}/{metric} is zero; skipping to avoid division \
                     by zero"
                );
                continue;
            }
            let delta = (cur_value - base_value) / base_value;
            let status = if threshold >= 0.0 {
                if delta > threshold {
                    MetricStatus::Fail
                } else {
                    MetricStatus::Pass
                }
            } else if delta < threshold {
                MetricStatus::Fail
            } else {
                MetricStatus::Pass
            };
            if status == MetricStatus::Fail {
                exit_code = 1;
            }
            samples.push(MetricSample {
                benchmark: bench_name.clone(),
                metric: metric.clone(),
                baseline: *base_value,
                current: *cur_value,
                delta,
                threshold,
                status,
            });
        }
    }

    (samples, exit_code)
}

fn render_compare_report(samples: &[MetricSample]) {
    let mut grouped: BTreeMap<&str, Vec<&MetricSample>> = BTreeMap::new();
    for sample in samples {
        grouped.entry(&sample.benchmark).or_default().push(sample);
    }

    for (bench, metrics) in grouped {
        println!("BENCH {bench}");
        for m in metrics {
            let status = match m.status {
                MetricStatus::Pass => "PASS",
                MetricStatus::Fail => "FAIL",
            };
            println!(
                "  metric {metric}: baseline={base:.4} current={cur:.4} delta={delta:.2}% \
                 threshold={thresh:.2}% {status}",
                metric = m.metric,
                base = m.baseline,
                cur = m.current,
                delta = m.delta * 100.0,
                thresh = m.threshold * 100.0,
                status = status,
            );
        }
    }
}

fn write_machine_report(path: &Path, samples: &[MetricSample]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("invalid report path: {}", path.display()))?;
    if !dir.as_os_str().is_empty() {
        fs::create_dir_all(dir)
            .with_context(|| format!("create report directory {}", dir.display()))?;
    }

    let mut grouped: BTreeMap<&str, Vec<&MetricSample>> = BTreeMap::new();
    for sample in samples {
        grouped.entry(&sample.benchmark).or_default().push(sample);
    }

    let mut out = Vec::new();
    for (bench, metrics) in grouped {
        let mut metric_reports = Vec::new();
        for m in metrics {
            metric_reports.push(serde_json::json!({
                "metric": m.metric,
                "baseline": m.baseline,
                "current": m.current,
                "delta": m.delta,
                "threshold": m.threshold,
                "status": format!("{:?}", m.status),
            }));
        }
        out.push(serde_json::json!({
            "benchmark": bench,
            "metrics": metric_reports,
        }));
    }

    let buf = serde_json::to_vec_pretty(&out)?;
    fs::write(path, buf).with_context(|| format!("write report {}", path.display()))?;
    Ok(())
}

fn extract_numeric_metrics(value: &Value) -> BTreeMap<String, f64> {
    let mut metrics = BTreeMap::new();
    let Some(obj) = value.as_object() else {
        return metrics;
    };

    for (k, v) in obj {
        if let Some(n) = v.as_f64() {
            metrics.insert(k.clone(), n);
        } else if let Some(n) = v.as_u64() {
            metrics.insert(k.clone(), n as f64);
        } else if let Some(n) = v.as_i64() {
            metrics.insert(k.clone(), n as f64);
        }
    }
    metrics
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    fn temp_config() -> PathBuf {
        let mut file = NamedTempFile::new().expect("temp file");
        // Touch the file so validation succeeds; contents are irrelevant for these tests.
        writeln!(file, "dummy: true").expect("write temp config");
        let path = file.path().to_path_buf();
        file.keep().expect("persist temp config");
        path
    }

    #[test]
    fn build_scenario_command_with_config() {
        let cfg = temp_config();
        let args = RunArgs {
            mode: Mode::Scenario,
            config: Some(cfg.clone()),
            scenario_config: None,
            component_config: None,
            profile: Profile::Local,
            dry_run: false,
            report_md: None,
            diagnostics_sample_ms: None,
            diagnostics_max_samples: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs, &[]).expect("commands");
        assert_eq!(cmds.len(), 1);
        let cmd = &cmds[0];
        assert!(cmd.args.contains(&"tonbo_scenarios".to_string()));
        assert!(cmd.args.contains(&"--features".to_string()));
        assert!(cmd.args.contains(&"test".to_string()));
        assert!(format_command(cmd).contains("cargo bench"));
    }

    #[test]
    fn build_all_runs_both() {
        let cfg = temp_config();
        let args = RunArgs {
            mode: Mode::All,
            config: Some(cfg.clone()),
            scenario_config: None,
            component_config: None,
            profile: Profile::Local,
            dry_run: true,
            report_md: None,
            diagnostics_sample_ms: None,
            diagnostics_max_samples: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs, &[]).expect("commands");
        assert_eq!(cmds.len(), 2);
        assert!(
            cmds.iter()
                .any(|c| c.args.contains(&"tonbo_scenarios".to_string()))
        );
        assert!(
            cmds.iter()
                .any(|c| c.args.contains(&"tonbo_components".to_string()))
        );
    }

    #[test]
    fn profile_ci_provides_default_config() {
        // Ensure the default path is validated; resolve relative to the repo root.
        let default = repo_root().join("benches/harness/configs/ci.yaml");
        assert!(
            default.exists(),
            "expected default CI config to exist at {}",
            default.display()
        );

        let args = RunArgs {
            mode: Mode::Scenario,
            config: None,
            scenario_config: None,
            component_config: None,
            profile: Profile::Ci,
            dry_run: true,
            report_md: None,
            diagnostics_sample_ms: None,
            diagnostics_max_samples: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs, &[]).expect("commands");
        assert_eq!(cmds.len(), 1);
        assert!(cmds[0].args.iter().any(|arg| arg.contains("ci.yaml")));
    }

    #[test]
    fn list_json_files_finds_outputs() {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_dir = temp_dir.path().join("run");
        fs::create_dir_all(run_dir.join("nested/sub")).unwrap();
        let f1 = run_dir.join("a.json");
        let f2 = run_dir.join("nested/sub/b.json");
        fs::write(&f1, "{}").unwrap();
        fs::write(&f2, "{}").unwrap();

        let mut found = list_json_files(&run_dir);
        found.sort();
        assert!(found.contains(&f1));
        assert!(found.contains(&f2));
    }

    #[test]
    fn mode_component_wal_sets_selector() {
        let cfg = temp_config();
        let args = RunArgs {
            mode: Mode::ComponentWal,
            config: Some(cfg.clone()),
            scenario_config: None,
            component_config: None,
            profile: Profile::Local,
            dry_run: true,
            report_md: None,
            diagnostics_sample_ms: None,
            diagnostics_max_samples: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs, &[]).expect("commands");
        assert_eq!(cmds.len(), 1);
        let joined = cmds[0].args.join(" ");
        assert!(joined.contains("--component wal"));
    }

    #[test]
    fn diagnostics_env_is_propagated() {
        let cfg = temp_config();
        let args = RunArgs {
            mode: Mode::Scenario,
            config: Some(cfg.clone()),
            scenario_config: None,
            component_config: None,
            profile: Profile::Local,
            dry_run: true,
            report_md: None,
            diagnostics_sample_ms: Some(500),
            diagnostics_max_samples: Some(10),
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs, &[]).expect("commands");
        assert_eq!(cmds.len(), 1);
        let envs = &cmds[0].env;
        assert!(
            envs.iter()
                .any(|(k, v)| k == "TONBO_BENCH_DIAG_SAMPLE_MS" && v == "500")
        );
        assert!(
            envs.iter()
                .any(|(k, v)| k == "TONBO_BENCH_DIAG_MAX_SAMPLES" && v == "10")
        );
    }
}
