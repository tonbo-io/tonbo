//! One-command runner for Tonbo benchmarks.
//!
//! Recon notes:
//! - Current benches run via `cargo bench --bench tonbo_scenarios -- --config <path>` and `cargo
//!   bench --bench tonbo_components -- --config <path>`.
//! - Bench-only diagnostics are gated behind `cfg(tonbo_bench)`, so we must set `RUSTFLAGS=--cfg
//!   tonbo_bench` for benchmark subprocesses; normal builds should remain unaffected.

use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "tonbo-bench-runner")]
#[command(about = "One-command runner for Tonbo scenario and component benchmarks")]
struct Args {
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
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum Mode {
    Scenario,
    Component,
    #[value(name = "memtable")]
    ComponentMemtable,
    #[value(name = "wal")]
    ComponentWal,
    #[value(name = "sst")]
    ComponentSst,
    #[value(name = "iterator")]
    ComponentIterator,
    All,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
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
    let configs = resolve_configs(&args)?;
    let commands = build_commands(&args, &configs)?;

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
        write_markdown_report(&run_summaries, path)?;
        println!("Wrote report to {}", path.display());
    }

    Ok(())
}

fn build_commands(args: &Args, configs: &ResolvedConfigs) -> Result<Vec<BenchCommand>> {
    let mut commands = Vec::new();

    let rustflags = resolve_rustflags();
    let components = mode_components(args.mode);

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
                    "--bench".into(),
                    "tonbo_scenarios".into(),
                    "--".into(),
                    "--config".into(),
                    config.display().to_string(),
                ],
                env: vec![("RUSTFLAGS".into(), rustflags.clone())],
                description: format!("scenario benches with config {}", config.display()),
            });
        }
        _ => {}
    }

    match args.mode {
        Mode::Component | Mode::All | Mode::ComponentMemtable | Mode::ComponentWal => {
            let config = configs
                .component
                .as_ref()
                .ok_or_else(|| RunnerError::MissingConfig("component requires --config".into()))?;
            let mut args_vec = vec![
                "bench".into(),
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
                env: vec![("RUSTFLAGS".into(), rustflags)],
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

fn resolve_configs(args: &Args) -> Result<ResolvedConfigs> {
    let default_ci = repo_root().join("benches/harness/configs/ci-write-only.yaml");
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
        ("scenario", Mode::Scenario | Mode::All) | ("component", Mode::Component | Mode::All)
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

fn resolve_rustflags() -> String {
    // Do not mutate the parent env; scope tonbo_bench to the subprocess.
    let flags = env::var("RUSTFLAGS").unwrap_or_default();
    if flags.is_empty() {
        "--cfg tonbo_bench".into()
    } else {
        format!("{flags} --cfg tonbo_bench")
    }
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
                 {metrics} ({path})",
                name = summary.benchmark_name,
                kind = summary.benchmark_type,
                target = summary.bench_target.as_deref().unwrap_or("unknown"),
                backend = summary.backend.as_deref().unwrap_or("unknown"),
                substr = summary.storage_substrate.as_deref().unwrap_or("unknown"),
                metrics = summary.metrics_summary,
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

fn write_markdown_report(runs: &[RunSummary], path: &Path) -> Result<()> {
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
    fs::write(path, out)?;
    Ok(())
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
        let args = Args {
            mode: Mode::Scenario,
            config: Some(cfg.clone()),
            scenario_config: None,
            component_config: None,
            profile: Profile::Local,
            dry_run: false,
            report_md: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs).expect("commands");
        assert_eq!(cmds.len(), 1);
        let cmd = &cmds[0];
        assert!(cmd.args.contains(&"tonbo_scenarios".to_string()));
        assert!(
            cmd.env
                .iter()
                .any(|(k, v)| k == "RUSTFLAGS" && v.contains("tonbo_bench"))
        );
        assert!(format_command(cmd).contains("cargo bench"));
    }

    #[test]
    fn build_all_runs_both() {
        let cfg = temp_config();
        let args = Args {
            mode: Mode::All,
            config: Some(cfg.clone()),
            scenario_config: None,
            component_config: None,
            profile: Profile::Local,
            dry_run: true,
            report_md: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs).expect("commands");
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
        let default = repo_root().join("benches/harness/configs/ci-write-only.yaml");
        assert!(
            default.exists(),
            "expected default CI config to exist at {}",
            default.display()
        );

        let args = Args {
            mode: Mode::Scenario,
            config: None,
            scenario_config: None,
            component_config: None,
            profile: Profile::Ci,
            dry_run: true,
            report_md: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs).expect("commands");
        assert_eq!(cmds.len(), 1);
        assert!(cmds[0].args.iter().any(|arg| arg.contains("ci-write-only")));
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
        let args = Args {
            mode: Mode::ComponentWal,
            config: Some(cfg.clone()),
            scenario_config: None,
            component_config: None,
            profile: Profile::Local,
            dry_run: true,
            report_md: None,
        };
        let configs = resolve_configs(&args).expect("configs");
        let cmds = build_commands(&args, &configs).expect("commands");
        assert_eq!(cmds.len(), 1);
        let joined = cmds[0].args.join(" ");
        assert!(joined.contains("--component wal"));
    }
}
