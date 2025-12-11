#![cfg(test)]

use std::{
    env,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fusio::{DynFs, disk::LocalFs, path::Path as FusioPath};

use crate::{
    db::{AwsCreds, ObjectSpec, S3Spec, WalConfig},
    wal::{WalSyncPolicy, state::FsWalStateStore},
};

/// Local filesystem backend harness metadata.
pub struct LocalHarness {
    pub root: PathBuf,
    pub wal_dir: PathBuf,
    pub wal_config: WalConfig,
    pub cleanup: Option<Box<dyn FnOnce() + Send>>,
}

/// S3/object-store backend harness metadata.
pub struct S3Harness {
    pub object: ObjectSpec,
    pub wal_config: WalConfig,
}

/// Common WAL tuning for e2e forcing small segments and fast flush.
pub fn wal_tuning(policy: WalSyncPolicy) -> WalConfig {
    WalConfig::default()
        .segment_max_bytes(256)
        .flush_interval(Duration::from_millis(1))
        .sync_policy(policy)
}

fn workspace_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    let dir = base.join("target").join("tmp").join(format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create workspace temp dir");
    dir
}

/// Local filesystem backend harness.
pub fn local_harness(
    label: &str,
    wal_cfg: WalConfig,
) -> Result<LocalHarness, Box<dyn std::error::Error>> {
    let root = workspace_temp_dir(label);
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&wal_dir)?;

    let wal_path = FusioPath::from_filesystem_path(&wal_dir)?;
    let wal_fs = Arc::new(LocalFs {});
    let wal_backend: Arc<dyn DynFs> = wal_fs.clone();
    let wal_state = Arc::new(FsWalStateStore::new(wal_fs));
    let wal_config = wal_cfg
        .clone()
        .wal_dir(wal_path)
        .segment_backend(wal_backend)
        .state_store(Some(wal_state));

    Ok(LocalHarness {
        root: root.clone(),
        wal_dir,
        wal_config,
        cleanup: Some(Box::new(move || {
            if let Err(err) = std::fs::remove_dir_all(&root) {
                eprintln!("cleanup failed for {:?}: {err}", &root);
            }
        })),
    })
}

fn s3_env() -> Option<(String, String, String, String, String, Option<String>)> {
    let endpoint = env::var("TONBO_S3_ENDPOINT").ok()?;
    let bucket = env::var("TONBO_S3_BUCKET").ok()?;
    let region = env::var("TONBO_S3_REGION").ok()?;
    let access = env::var("TONBO_S3_ACCESS_KEY").ok()?;
    let secret = env::var("TONBO_S3_SECRET_KEY").ok()?;
    let session = env::var("TONBO_S3_SESSION_TOKEN").ok();
    Some((endpoint, bucket, region, access, secret, session))
}

fn unique_label(base: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos();
    format!("{base}-{nanos}")
}

/// S3/object-store backend harness. Returns None when env is not present.
pub fn maybe_s3_harness(
    label: &str,
    wal_cfg: WalConfig,
) -> Result<Option<S3Harness>, Box<dyn std::error::Error>> {
    let Some((endpoint, bucket, region, access, secret, session)) = s3_env() else {
        return Ok(None);
    };

    let credentials = match session {
        Some(token) => AwsCreds::with_session_token(access, secret, token),
        None => AwsCreds::new(access, secret),
    };

    let mut s3 = S3Spec::new(bucket.clone(), unique_label(label), credentials);
    s3.endpoint = Some(endpoint);
    s3.region = Some(region);
    s3.sign_payload = Some(true);

    let object = ObjectSpec::s3(s3);

    Ok(Some(S3Harness {
        object,
        wal_config: wal_cfg,
    }))
}
