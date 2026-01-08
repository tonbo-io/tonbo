use std::{
    fs,
    path::{Path, PathBuf},
};

use chrono::Utc;
use serde::Serialize;

use crate::harness::DiagnosticsOutput;

pub const DEFAULT_RESULTS_ROOT: &str = "target/bench-results";

pub fn default_results_root() -> PathBuf {
    PathBuf::from(DEFAULT_RESULTS_ROOT)
}

pub fn new_run_id() -> String {
    let now = Utc::now();
    let millis = now.timestamp_subsec_millis();
    format!("{}-{millis:03}", now.format("%Y%m%d%H%M%S"))
}

#[derive(Debug, Serialize)]
pub struct BenchResult {
    pub run_id: String,
    pub bench_target: String,
    pub storage_substrate: String,
    pub benchmark_name: String,
    pub benchmark_type: String,
    pub backend: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_type: Option<String>,
    pub parameters: serde_json::Value,
    pub metrics: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<DiagnosticsOutput>,
}

pub struct BenchResultWriter {
    base_dir: PathBuf,
}

impl BenchResultWriter {
    pub fn new(
        base_dir: impl AsRef<Path>,
        run_id: impl AsRef<str>,
        bench_target: impl AsRef<str>,
        storage_substrate: impl AsRef<str>,
    ) -> anyhow::Result<Self> {
        let dir = base_dir
            .as_ref()
            .join(run_id.as_ref())
            .join(normalize_component(bench_target.as_ref()))
            .join(normalize_component(storage_substrate.as_ref()));
        fs::create_dir_all(&dir)?;
        Ok(Self { base_dir: dir })
    }

    pub fn write(&self, name: &str, result: &BenchResult) -> anyhow::Result<PathBuf> {
        let path = self.base_dir.join(format!("{name}.json"));
        let buf = serde_json::to_vec_pretty(result)?;
        fs::write(&path, buf)?;
        Ok(path)
    }
}

fn normalize_component(component: &str) -> String {
    let trimmed = component.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }

    trimmed.replace(['/', '\\'], "_")
}
