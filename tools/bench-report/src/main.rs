use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use clap::Parser;
use itertools::Itertools;
use serde::Deserialize;
use serde_json::Value;

const DEFAULT_SCHEMA_VERSION: &str = "1";

/// Report generator for benchmark performance history.
#[derive(Debug, Parser)]
struct Args {
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

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RawBenchResult {
    #[serde(default = "default_schema_version")]
    schema_version: String,
    benchmark_name: String,
    benchmark_type: Option<String>,
    backend: Option<String>,
    #[serde(default)]
    backend_details: Option<Value>,
    #[serde(default)]
    workload_type: Option<String>,
    parameters: Value,
    metrics: Value,
    #[serde(default)]
    git_commit: Option<String>,
}

#[derive(Debug)]
struct Run {
    run_id: String,
    timestamp: Option<DateTime<Utc>>,
    commit: Option<String>,
    benches: BTreeMap<String, BenchMetrics>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct BenchMetrics {
    metrics: BTreeMap<String, f64>,
    workload_type: Option<String>,
    backend: Option<String>,
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

fn default_schema_version() -> String {
    DEFAULT_SCHEMA_VERSION.to_string()
}

fn main() {
    match try_main() {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("error: {err:?}");
            std::process::exit(2);
        }
    }
}

fn try_main() -> anyhow::Result<i32> {
    let args = Args::parse();
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

    write_markdown_report(
        &args.output_md,
        current,
        baseline_id.as_deref(),
        &summary,
        &trend,
        &trend_ids,
    )?;
    if let Some(json_path) = &args.output_json {
        write_json_report(
            json_path,
            current,
            baseline_id.as_deref(),
            &summary,
            &trend,
            &trend_ids,
        )?;
    }

    Ok(0)
}

fn load_runs(results_dir: &Path) -> anyhow::Result<BTreeMap<String, Run>> {
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

    let mut runs: BTreeMap<String, Run> = BTreeMap::new();
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
        let bench_entry = BenchMetrics {
            metrics,
            workload_type: doc.workload_type.clone(),
            backend: doc.backend.clone(),
        };

        let run = runs.entry(run_id.to_string()).or_insert_with(|| Run {
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
    if run_id.len() != 14 {
        return None;
    }
    DateTime::parse_from_str(&(run_id.to_string() + " +0000"), "%Y%m%d%H%M%S %z")
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn extract_numeric_metrics(value: &Value) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    let Some(obj) = value.as_object() else {
        return out;
    };
    for (k, v) in obj {
        if let Some(n) = v.as_f64() {
            out.insert(k.clone(), n);
        }
    }
    out
}

fn select_current_run(
    sorted_ids: &BTreeSet<String>,
    explicit: Option<&str>,
) -> anyhow::Result<String> {
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

fn build_summary(baseline: &Run, current: &Run) -> Vec<SummaryRow> {
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
    runs: &BTreeMap<String, Run>,
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

fn write_markdown_report(
    path: &Path,
    current: &Run,
    baseline_id: Option<&str>,
    summary: &[SummaryRow],
    trend: &[TrendRow],
    trend_ids: &[String],
) -> anyhow::Result<()> {
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
        out.push_str("| Benchmark | Metric | Baseline | Current | Δ (%) |\n");
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
        out.push_str("| Benchmark | Metric | Min Δ (%) | Median Δ (%) | Max Δ (%) | Samples |\n");
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

fn write_json_report(
    path: &Path,
    current: &Run,
    baseline_id: Option<&str>,
    summary: &[SummaryRow],
    trend: &[TrendRow],
    trend_ids: &[String],
) -> anyhow::Result<()> {
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
        })).collect_vec(),
        "trends": trend.iter().map(|r| serde_json::json!({
            "benchmark": r.benchmark,
            "metric": r.metric,
            "min_delta": r.min_delta,
            "median_delta": r.median_delta,
            "max_delta": r.max_delta,
            "samples": r.samples,
        })).collect_vec(),
    });

    let buf = serde_json::to_vec_pretty(&json)?;
    fs::write(path, buf).with_context(|| format!("write json report {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn groups_runs_by_prefix() {
        let dir = TempDir::new().unwrap();
        let run_id = "20240101010101";
        let run_dir = dir.path().join(run_id);
        fs::create_dir_all(&run_dir).unwrap();
        fs::write(
            run_dir.join("mixed.json"),
            sample_result("mixed", 100.0, None),
        )
        .unwrap();
        fs::write(
            run_dir.join("write_only.json"),
            sample_result("write_only", 200.0, None),
        )
        .unwrap();

        let runs = load_runs(dir.path()).unwrap();
        assert_eq!(runs.len(), 1);
        let run = runs.get(run_id).unwrap();
        assert_eq!(run.benches.len(), 2);
        assert!(run.timestamp.is_some());
    }

    #[test]
    fn builds_summary_against_baseline() {
        let dir = TempDir::new().unwrap();
        let run1_dir = dir.path().join("20240101010101");
        let run2_dir = dir.path().join("20240101020101");
        fs::create_dir_all(&run1_dir).unwrap();
        fs::create_dir_all(&run2_dir).unwrap();
        fs::write(
            run1_dir.join("mixed.json"),
            sample_result("mixed", 100.0, None),
        )
        .unwrap();
        fs::write(
            run2_dir.join("mixed.json"),
            sample_result("mixed", 120.0, None),
        )
        .unwrap();
        let runs = load_runs(dir.path()).unwrap();
        let base = runs.get("20240101010101").unwrap();
        let cur = runs.get("20240101020101").unwrap();
        let summary = build_summary(base, cur);
        assert_eq!(summary.len(), 1);
        let row = &summary[0];
        assert_eq!(row.benchmark, "mixed");
        assert_eq!(row.metric, "ops_per_sec");
        assert!((row.delta - 0.2).abs() < 1e-6);
    }

    #[test]
    fn trends_respect_limit_and_include_current() {
        let dir = TempDir::new().unwrap();
        for i in 0..12 {
            let ts = format!("202401010101{:02}", i);
            let value = 100.0 + (i as f64);
            let run_dir = dir.path().join(&ts);
            fs::create_dir_all(&run_dir).unwrap();
            fs::write(
                run_dir.join("mixed.json"),
                sample_result("mixed", value, None),
            )
            .unwrap();
        }
        let runs = load_runs(dir.path()).unwrap();
        let ids: BTreeSet<String> = runs.keys().cloned().collect();
        let current = select_current_run(&ids, None).unwrap();
        assert_eq!(current, "20240101010111");
        let trend_ids = select_trend_ids(&ids, &current, 5);
        assert_eq!(trend_ids.len(), 5);
        assert_eq!(trend_ids.last().unwrap(), &current);
        let trend = build_trend(&runs, &current, &trend_ids);
        assert!(!trend.is_empty());
    }

    fn sample_result(name: &str, ops: f64, commit: Option<&str>) -> String {
        let commit_json = commit
            .map(|c| format!("\"git_commit\": \"{c}\","))
            .unwrap_or_default();
        format!(
            indoc!(
                r#"
            {{
                "schema_version": "1",
                "benchmark_name": "{name}",
                "metrics": {{
                    "ops_per_sec": {ops}
                }},
                {commit_json}
                "parameters": {{}}
            }}
        "#
            ),
            name = name,
            ops = ops,
            commit_json = commit_json,
        )
    }
}
