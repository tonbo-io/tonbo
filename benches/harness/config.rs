#![allow(dead_code)]

use std::{fs, path::Path};

use serde::Deserialize;

use crate::harness::diagnostics::DiagnosticsConfig;

#[derive(Debug, Deserialize)]
pub struct BenchConfig {
    pub backend: BackendConfig,
    pub workload: WorkloadConfig,
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub diagnostics: Option<DiagnosticsConfig>,
}

#[derive(Debug, Deserialize)]
pub struct BackendConfig {
    #[serde(alias = "kind")]
    pub r#type: String,
    pub path: Option<String>,
    pub s3: Option<S3BackendConfig>,
}

#[derive(Debug, Deserialize)]
pub struct S3BackendConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub sign_payload: Option<bool>,
    #[serde(default)]
    pub checksum: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct WorkloadConfig {
    #[serde(alias = "type")]
    pub name: String,
    pub num_records: u64,
    pub value_size_bytes: usize,
    #[serde(default)]
    pub read_ratio: Option<f64>,
    #[serde(default)]
    pub write_ratio: Option<f64>,
    #[serde(default)]
    pub warmup_records: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RuntimeConfig {
    pub concurrency: usize,
}

impl BenchConfig {
    pub fn from_yaml_file(path: &Path) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let cfg: BenchConfig = serde_yaml::from_str(&content)?;
        Ok(cfg)
    }
}
