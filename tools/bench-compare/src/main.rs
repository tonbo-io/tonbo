use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow};
use clap::Parser;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Parser)]
struct Args {
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

#[derive(Debug, Deserialize)]
struct BenchResult {
    benchmark_name: String,
    #[serde(default)]
    metrics: Value,
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

#[derive(Debug)]
struct Run {
    benchmarks: BTreeMap<String, BTreeMap<String, f64>>,
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
    if args.missing_baseline != "warn" && args.missing_baseline != "fail" {
        anyhow::bail!("missing_baseline must be 'warn' or 'fail'");
    }

    let thresholds_file = load_thresholds(&args.thresholds)?;
    if !thresholds_file.regression.enabled {
        println!("regression checks disabled via thresholds file");
        return Ok(0);
    }

    let baseline = load_run(&args.baseline).context("load baseline results")?;
    let current = load_run(&args.current).context("load current results")?;

    let (samples, exit_code) =
        compare_runs(&baseline, &current, &thresholds_file.regression, &args);
    render_report(&samples);
    if let Some(report_path) = args.report
        && let Err(err) = write_machine_report(&report_path, &samples)
    {
        eprintln!("failed to write report to {}: {err}", report_path.display());
    }

    Ok(exit_code)
}

fn load_thresholds(path: &Path) -> anyhow::Result<ThresholdsFile> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read thresholds at {}", path.display()))?;
    let cfg: ThresholdsFile = serde_yaml::from_str(&content)
        .with_context(|| format!("parse YAML thresholds at {}", path.display()))?;
    Ok(cfg)
}

fn load_run(path: &Path) -> anyhow::Result<Run> {
    let mut files = Vec::new();
    let meta = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if meta.is_file() {
        files.push(path.to_path_buf());
    } else if meta.is_dir() {
        for entry in
            fs::read_dir(path).with_context(|| format!("list directory {}", path.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                files.push(path);
            }
        }
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
        let result: BenchResult = match serde_json::from_str(&content) {
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

    Ok(Run { benchmarks })
}

fn extract_numeric_metrics(value: &Value) -> BTreeMap<String, f64> {
    let mut metrics = BTreeMap::new();
    let Some(obj) = value.as_object() else {
        return metrics;
    };

    for (k, v) in obj {
        if let Some(n) = v.as_f64() {
            metrics.insert(k.clone(), n);
        }
    }
    metrics
}

fn compare_runs(
    baseline: &Run,
    current: &Run,
    thresholds: &RegressionThresholds,
    args: &Args,
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
                // Lower is better (e.g., latency); fail if delta exceeds allowed positive increase.
                if delta > threshold {
                    MetricStatus::Fail
                } else {
                    MetricStatus::Pass
                }
            } else if delta < threshold {
                // Higher is better (throughput); fail if drop exceeds allowed negative threshold.
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

fn render_report(samples: &[MetricSample]) {
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

fn write_machine_report(path: &Path, samples: &[MetricSample]) -> anyhow::Result<()> {
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
