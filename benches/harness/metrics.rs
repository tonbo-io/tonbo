use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;

use crate::harness::DiagnosticsOutput;

#[derive(Debug, Serialize)]
pub struct BenchResult {
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
    pub fn new(base_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let base = base_dir.as_ref();
        fs::create_dir_all(base)?;
        Ok(Self {
            base_dir: base.to_path_buf(),
        })
    }

    pub fn write(&self, name: &str, result: &BenchResult) -> anyhow::Result<PathBuf> {
        let path = self.base_dir.join(format!("{name}.json"));
        let buf = serde_json::to_vec_pretty(result)?;
        fs::write(&path, buf)?;
        Ok(path)
    }
}
