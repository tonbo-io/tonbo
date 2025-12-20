use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, anyhow};
use arrow_array::RecordBatch;
use aws_config::BehaviorVersion;
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client as AwsS3, config::Region};
use fusio::{
    fs::Fs,
    impls::remotes::aws::{credential::AwsCredential, fs::AmazonS3Builder},
    path::Path as FusioPath,
};
use futures::StreamExt;
use tempfile::TempDir;
use tonbo::{
    db::{AwsCreds, DbBuilder, ObjectSpec, S3Spec},
    prelude::Predicate,
};

use crate::harness::{BenchConfig, S3BackendConfig, default_results_root, new_run_id};

// Recon: Phase 1 harness only supported disk via DbBuilder::on_disk().
// Tonbo exposes object-store through DbBuilder::object_store(S3Spec) using Fusio AmazonS3,
// which accepts a custom endpoint (LocalStack) and signs requests via Fusio's HTTP client.

type DiskDb = tonbo::db::DB<fusio::disk::LocalFs, fusio::executor::tokio::TokioExecutor>;
type S3Db =
    tonbo::db::DB<fusio::impls::remotes::aws::fs::AmazonS3, fusio::executor::tokio::TokioExecutor>;

#[allow(dead_code)]
pub struct BenchDb {
    inner: DbInner,
}

#[allow(dead_code)]
enum DbInner {
    Disk(DiskDb),
    S3(S3Db),
}

#[allow(dead_code)]
impl BenchDb {
    pub async fn ingest(&self, batch: RecordBatch) -> anyhow::Result<()> {
        match &self.inner {
            DbInner::Disk(db) => db.ingest(batch).await?,
            DbInner::S3(db) => db.ingest(batch).await?,
        }
        Ok(())
    }

    pub async fn scan_all(&self) -> anyhow::Result<Vec<RecordBatch>> {
        match &self.inner {
            DbInner::Disk(db) => Ok(db.scan().collect().await?),
            DbInner::S3(db) => Ok(db.scan().collect().await?),
        }
    }

    pub async fn scan_with_predicate(
        &self,
        predicate: Predicate,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        match &self.inner {
            DbInner::Disk(db) => Ok(db.scan().filter(predicate).collect().await?),
            DbInner::S3(db) => Ok(db.scan().filter(predicate).collect().await?),
        }
    }

    pub async fn metrics_snapshot(&self) -> tonbo::metrics::DbMetricsSnapshot {
        match &self.inner {
            DbInner::Disk(db) => db.metrics_snapshot().await,
            DbInner::S3(db) => db.metrics_snapshot().await,
        }
    }
}

/// Disk-backed benchmark storage.
struct DiskBackend {
    base: PathBuf,
    root: TempDir,
}

impl DiskBackend {
    fn new(base: PathBuf, root: TempDir) -> Self {
        Self { base, root }
    }

    fn data_root(&self) -> &Path {
        self.root.path()
    }

    fn label(&self) -> &Path {
        &self.base
    }

    fn cleanup(self) -> anyhow::Result<()> {
        drop(self);
        Ok(())
    }
}

struct S3Backend {
    fs: Arc<fusio::impls::remotes::aws::fs::AmazonS3>,
    spec: S3Spec,
    endpoint: String,
    bucket: String,
    region: String,
    prefix: String,
}

impl S3Backend {
    async fn new(config: &S3BackendConfig, run_id: &str) -> anyhow::Result<Self> {
        if config.endpoint.trim().is_empty() {
            return Err(anyhow!(
                "s3 backend requires a non-empty endpoint (LocalStack)"
            ));
        }
        if config.bucket.trim().is_empty() {
            return Err(anyhow!("s3 backend requires a bucket name"));
        }
        if config.region.trim().is_empty() {
            return Err(anyhow!("s3 backend requires a region"));
        }

        let credentials = resolve_credentials(config)?;
        let fusio_credential = AwsCredential {
            key_id: credentials.access_key.clone(),
            secret_key: credentials.secret_key.clone(),
            token: credentials.session_token.clone(),
        };

        let prefix_base = config.prefix.clone().unwrap_or_else(|| "bench".to_string());
        let trimmed_prefix = prefix_base.trim_matches('/').to_string();
        let resolved_prefix = if trimmed_prefix.is_empty() {
            run_id.to_string()
        } else {
            format!("{}/{}", trimmed_prefix, run_id)
        };

        ensure_bucket_exists(config, &credentials).await?;

        let mut fusio_builder =
            AmazonS3Builder::new(config.bucket.clone()).region(config.region.clone());
        fusio_builder = fusio_builder.endpoint(config.endpoint.clone());
        fusio_builder = fusio_builder.credential(fusio_credential.clone());
        if let Some(sign) = config.sign_payload {
            fusio_builder = fusio_builder.sign_payload(sign);
        }
        if let Some(checksum) = config.checksum {
            fusio_builder = fusio_builder.checksum(checksum);
        }

        let fs = Arc::new(fusio_builder.build());

        let mut spec = S3Spec::new(config.bucket.clone(), resolved_prefix.clone(), credentials);
        spec.endpoint = Some(config.endpoint.clone());
        spec.region = Some(config.region.clone());
        spec.sign_payload = Some(config.sign_payload.unwrap_or(true));
        spec.checksum = config.checksum;

        Ok(Self {
            fs,
            spec,
            endpoint: config.endpoint.clone(),
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            prefix: resolved_prefix,
        })
    }

    async fn cleanup(self) -> anyhow::Result<()> {
        let prefix_path = FusioPath::parse(&self.prefix).context("parse prefix for cleanup")?;
        let stream = self
            .fs
            .list(&prefix_path)
            .await
            .context("list s3 prefix for cleanup")?;
        let mut stream = Box::pin(stream);
        while let Some(entry) = stream.next().await {
            let meta = entry.context("list entry for s3 cleanup")?;
            self.fs
                .remove(&meta.path)
                .await
                .with_context(|| format!("remove {}", meta.path))?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub struct BackendRun {
    backend: Backend,
    pub result_root: PathBuf,
    run_id: String,
}

enum Backend {
    Disk(DiskBackend),
    S3(S3Backend),
}

#[allow(dead_code)]
impl BackendRun {
    pub async fn new(config: &BenchConfig) -> anyhow::Result<Self> {
        let run_id = new_run_id();
        let result_root = default_results_root();

        let backend = match config.backend.r#type.as_str() {
            "disk" => {
                let base = config.backend.path.as_deref().unwrap_or("target/bench-tmp");
                std::fs::create_dir_all(base)
                    .with_context(|| format!("create bench temp base at {base}"))?;
                let base_path = PathBuf::from(base);
                let tmp_root = tempfile::Builder::new()
                    .prefix("tonbo-bench-")
                    .tempdir_in(&base_path)
                    .with_context(|| format!("create tempdir under {base}"))?;
                Backend::Disk(DiskBackend::new(base_path, tmp_root))
            }
            "s3" => {
                let cfg = config
                    .backend
                    .s3
                    .as_ref()
                    .ok_or_else(|| anyhow!("backend.s3 must be configured for s3 backend"))?;
                Backend::S3(S3Backend::new(cfg, &run_id).await?)
            }
            other => return Err(anyhow!("unsupported backend kind: {other}")),
        };

        Ok(Self {
            backend,
            result_root,
            run_id,
        })
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn backend_kind(&self) -> &'static str {
        match &self.backend {
            Backend::Disk(_) => "disk",
            Backend::S3(_) => "s3",
        }
    }

    pub fn storage_substrate(&self) -> String {
        match &self.backend {
            Backend::Disk(_) => "disk".to_string(),
            Backend::S3(s3) => {
                let lower = s3.endpoint.to_ascii_lowercase();
                if lower.contains("localstack")
                    || lower.contains("localhost")
                    || lower.contains("127.0.0.1")
                {
                    "s3-localstack".to_string()
                } else {
                    "s3".to_string()
                }
            }
        }
    }

    pub fn backend_details(&self) -> serde_json::Value {
        match &self.backend {
            Backend::Disk(disk) => serde_json::json!({
                "type": "disk",
                "path": disk.label(),
            }),
            Backend::S3(s3) => serde_json::json!({
                "type": "s3",
                "endpoint": s3.endpoint,
                "bucket": s3.bucket,
                "prefix": s3.prefix,
                "region": s3.region,
            }),
        }
    }

    pub async fn open_db(
        &self,
        schema: arrow_schema::SchemaRef,
        key_name: &str,
    ) -> anyhow::Result<BenchDb> {
        self.open_db_with_builder(DbBuilder::from_schema_key_name(schema, key_name)?)
            .await
    }

    pub async fn open_db_with_builder(&self, builder: DbBuilder) -> anyhow::Result<BenchDb> {
        match &self.backend {
            Backend::Disk(disk) => {
                let db_path = disk.data_root().join("db");
                let db = builder
                    .on_disk(&db_path)?
                    .open()
                    .await
                    .map_err(anyhow::Error::from)?;
                Ok(BenchDb {
                    inner: DbInner::Disk(db),
                })
            }
            Backend::S3(s3) => {
                let db = builder
                    .object_store(ObjectSpec::s3(s3.spec.clone()))
                    .map_err(anyhow::Error::from)?
                    .open()
                    .await
                    .map_err(anyhow::Error::from)?;
                Ok(BenchDb {
                    inner: DbInner::S3(db),
                })
            }
        }
    }

    pub async fn open_db_with_wal_policy(
        &self,
        schema: arrow_schema::SchemaRef,
        key_name: &str,
        policy: tonbo::db::WalSyncPolicy,
    ) -> anyhow::Result<BenchDb> {
        match &self.backend {
            Backend::Disk(disk) => {
                let db_path = disk.data_root().join("db");
                let builder = DbBuilder::from_schema_key_name(schema, key_name)?;
                let db = builder
                    .on_disk(&db_path)?
                    .wal_sync_policy(policy)
                    .open()
                    .await
                    .map_err(anyhow::Error::from)?;
                Ok(BenchDb {
                    inner: DbInner::Disk(db),
                })
            }
            Backend::S3(s3) => {
                let builder = DbBuilder::from_schema_key_name(schema, key_name)?;
                let db = builder
                    .object_store(ObjectSpec::s3(s3.spec.clone()))
                    .map_err(anyhow::Error::from)?
                    .wal_sync_policy(policy)
                    .open()
                    .await
                    .map_err(anyhow::Error::from)?;
                Ok(BenchDb {
                    inner: DbInner::S3(db),
                })
            }
        }
    }

    pub async fn cleanup(self) -> anyhow::Result<()> {
        match self.backend {
            Backend::Disk(disk) => disk.cleanup(),
            Backend::S3(s3) => s3.cleanup().await,
        }
    }

    pub async fn physical_bytes(&self) -> anyhow::Result<u64> {
        match &self.backend {
            Backend::Disk(disk) => dir_size(disk.data_root()),
            Backend::S3(s3) => {
                let prefix_path =
                    FusioPath::parse(&s3.prefix).context("parse prefix for size accounting")?;
                let stream = s3
                    .fs
                    .list(&prefix_path)
                    .await
                    .context("list s3 prefix for size accounting")?;
                let mut stream = Box::pin(stream);
                let mut total: u64 = 0;
                while let Some(entry) = stream.next().await {
                    let meta = entry.context("list entry for s3 size accounting")?;
                    total = total.saturating_add(meta.size);
                }
                Ok(total)
            }
        }
    }
}

fn resolve_credentials(config: &S3BackendConfig) -> anyhow::Result<AwsCreds> {
    let access = config
        .access_key
        .clone()
        .or_else(|| std::env::var("TONBO_S3_ACCESS_KEY").ok())
        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
        .ok_or_else(|| {
            anyhow!("s3 backend requires access key (TONBO_S3_ACCESS_KEY or AWS_ACCESS_KEY_ID)")
        })?;
    let secret = config
        .secret_key
        .clone()
        .or_else(|| std::env::var("TONBO_S3_SECRET_KEY").ok())
        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
        .ok_or_else(|| {
            anyhow!("s3 backend requires secret key (TONBO_S3_SECRET_KEY or AWS_SECRET_ACCESS_KEY)")
        })?;
    let session_token = config
        .session_token
        .clone()
        .or_else(|| std::env::var("TONBO_S3_SESSION_TOKEN").ok())
        .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok());

    Ok(match session_token {
        Some(token) => AwsCreds::with_session_token(access, secret, token),
        None => AwsCreds::new(access, secret),
    })
}

async fn ensure_bucket_exists(config: &S3BackendConfig, creds: &AwsCreds) -> anyhow::Result<()> {
    // Control-plane call: fusio does not manage bucket lifecycle, so we rely on the AWS SDK
    // to provision the LocalStack bucket before data-plane traffic flows through fusio.
    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(config.region.clone()))
        .endpoint_url(config.endpoint.clone())
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            creds.access_key.clone(),
            creds.secret_key.clone(),
            creds.session_token.clone(),
            None,
            "bench-harness",
        )))
        .load()
        .await;

    // Use path-style addressing for LocalStack compatibility
    let s3_config = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(true)
        .build();
    let client = AwsS3::from_conf(s3_config);
    let head = client.head_bucket().bucket(&config.bucket).send().await;
    if head.is_ok() {
        return Ok(());
    }

    let create = client.create_bucket().bucket(&config.bucket).send().await;

    match create {
        Ok(_) => Ok(()),
        Err(err) => {
            let message = err.to_string();
            if message.contains("BucketAlreadyOwnedByYou")
                || message.contains("BucketAlreadyExists")
            {
                return Ok(());
            }
            Err(anyhow!(err))
        }
    }
}

fn dir_size(path: &Path) -> anyhow::Result<u64> {
    let mut total: u64 = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total = total.saturating_add(dir_size(&entry.path())?);
        } else {
            total = total.saturating_add(meta.len());
        }
    }
    Ok(total)
}
