#![cfg(feature = "tokio")]

use std::{error::Error, future::Future, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::executor::tokio::TokioExecutor;
use futures::StreamExt;

use crate::{
    db::{BatchesThreshold, CompactionOptions, DB, DbBuilder, Expr, NeverSeal, ScalarValue},
    tests_internal::backend::{S3Harness, local_harness, maybe_s3_harness, wal_tuning},
    wal::{WalExt, WalSyncPolicy},
};

fn build_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

fn extract_rows(batches: Vec<RecordBatch>) -> Vec<(String, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id col");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("v col");
        for (id, v) in ids.iter().zip(vals.iter()) {
            if let (Some(id), Some(v)) = (id, v) {
                rows.push((id.to_string(), v));
            }
        }
    }
    rows.sort();
    rows
}

async fn extract_rows_with_profile<FS>(
    db: &DB<FS, TokioExecutor>,
) -> Result<Vec<(String, i32)>, Box<dyn Error>>
where
    FS: crate::manifest::ManifestFs<TokioExecutor>,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    let (mut stream, _profile) = db.scan().stream_with_profile().await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(extract_rows(batches))
}

async fn latest_version<FS>(
    db: &DB<FS, TokioExecutor>,
) -> Result<crate::db::Version, Box<dyn Error>>
where
    FS: crate::manifest::ManifestFs<TokioExecutor>,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    db.list_versions(1)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| "list_versions returned no versions".into())
}

async fn wait_for_reopen_compaction_progress<FS>(
    db: &DB<FS, TokioExecutor>,
    before: &crate::db::Version,
) -> Result<crate::db::Version, Box<dyn Error>>
where
    FS: crate::manifest::ManifestFs<TokioExecutor>,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    let timeout = Duration::from_secs(180);
    let poll = Duration::from_millis(250);
    let started = std::time::Instant::now();
    loop {
        let current = latest_version(db).await?;
        let manifest_advanced = current.timestamp != before.timestamp;
        let compaction_progressed =
            current.level_count >= 2 && current.sst_count < before.sst_count;
        if manifest_advanced && compaction_progressed {
            return Ok(current);
        }
        if started.elapsed() >= timeout {
            return Err(format!(
                "timed out waiting for reopen compaction progress: before(ts={}, ssts={}, \
                 levels={}) current(ts={}, ssts={}, levels={})",
                before.timestamp.get(),
                before.sst_count,
                before.level_count,
                current.timestamp.get(),
                current.sst_count,
                current.level_count
            )
            .into());
        }
        tokio::time::sleep(poll).await;
    }
}

fn error_chain_contains(err: &dyn Error, needle: &str) -> bool {
    if err.to_string().contains(needle) {
        return true;
    }
    let mut current = err.source();
    while let Some(source) = current {
        if source.to_string().contains(needle) {
            return true;
        }
        current = source.source();
    }
    false
}

fn is_transient_remote_read_error(err: &dyn Error) -> bool {
    error_chain_contains(err, "IncompleteMessage")
        || error_chain_contains(err, "connection closed before message completed")
}

async fn retry_transient_remote_read<T, F, Fut>(label: &str, mut op: F) -> Result<T, Box<dyn Error>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn Error>>>,
{
    let max_attempts = 3usize;
    let mut delay = Duration::from_millis(250);
    for attempt in 1..=max_attempts {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < max_attempts && is_transient_remote_read_error(err.as_ref()) => {
                eprintln!(
                    "retrying transient remote read failure during {label} (attempt \
                     {attempt}/{max_attempts}): {err}"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            Err(err) => return Err(err),
        }
    }
    Err(format!("unreachable retry state for {label}").into())
}

async fn public_compaction_local(schema: Arc<Schema>) -> Result<(), Box<dyn Error>> {
    let mut harness = local_harness("public-compaction", wal_tuning(WalSyncPolicy::Always))?;
    let mut db = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(harness.root.to_string_lossy().into_owned())?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .open()
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    for idx in 0..3 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![format!("k{idx}")])) as _,
                Arc::new(Int32Array::from(vec![idx])) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    let versions = db.list_versions(8).await?;
    assert!(
        versions.len() >= 3,
        "each sealed batch should publish a manifest version"
    );

    db.disable_wal().await?;
    drop(db);

    let reopened: DB<_, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(harness.root.to_string_lossy().into_owned())?
        .wal_config(harness.wal_config.clone())
        .open()
        .await?;

    let predicate = Expr::is_not_null("id");
    let rows = extract_rows(reopened.scan().filter(predicate).collect().await?);
    assert_eq!(
        rows,
        vec![("k0".into(), 0), ("k1".into(), 1), ("k2".into(), 2)]
    );

    let versions_after_restart = reopened.list_versions(8).await?;
    assert!(
        !versions_after_restart.is_empty(),
        "manifest versions should survive restart"
    );

    let mut inner = reopened.into_inner();
    inner.disable_wal().await?;
    if let Some(cleanup) = harness.cleanup.take() {
        cleanup();
    }
    Ok(())
}

async fn public_compaction_s3(
    schema: Arc<Schema>,
    harness: S3Harness,
) -> Result<(), Box<dyn Error>> {
    let mut db = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .open()
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    for idx in 0..3 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![format!("k{idx}")])) as _,
                Arc::new(Int32Array::from(vec![idx])) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    let versions = db.list_versions(8).await?;
    assert!(
        versions.len() >= 3,
        "each sealed batch should publish a manifest version (s3)"
    );

    db.disable_wal().await?;
    drop(db);

    let reopened: DB<_, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .open()
        .await?;

    let predicate = Expr::is_not_null("id");
    let rows = extract_rows(reopened.scan().filter(predicate).collect().await?);
    assert_eq!(
        rows,
        vec![("k0".into(), 0), ("k1".into(), 1), ("k2".into(), 2)]
    );

    let mut inner = reopened.into_inner();
    inner.disable_wal().await?;
    Ok(())
}

async fn wal_rotation_local(schema: Arc<Schema>) -> Result<(), Box<dyn Error>> {
    let mut harness = local_harness("public-wal-rotation", wal_tuning(WalSyncPolicy::Always))?;
    let mut db = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(harness.root.to_string_lossy().into_owned())?
        .wal_config(harness.wal_config.clone())
        .open()
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    for idx in 0..3 {
        let ids: Vec<String> = (0..64).map(|n| format!("user-{idx}-{n:02}")).collect();
        let vals: Vec<i32> = (0..64).map(|n| idx * 10 + n).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(Int32Array::from(vals)) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    db.disable_wal().await?;

    let wal_files: Vec<_> = std::fs::read_dir(&harness.wal_dir)?
        .flatten()
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.starts_with("wal-") && name.ends_with(".tonwal")
        })
        .collect();
    assert!(
        wal_files.len() >= 2,
        "small segment size should trigger WAL rotation"
    );

    drop(db);

    let reopened: DB<_, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(harness.root.to_string_lossy().into_owned())?
        .wal_config(harness.wal_config.clone())
        .open()
        .await?;

    let predicate = Expr::is_not_null("id");
    let rows = extract_rows(reopened.scan().filter(predicate).collect().await?);
    assert!(
        rows.len() >= 3 * 64,
        "wal replay should restore all ingested rows"
    );

    let mut inner = reopened.into_inner();
    inner.disable_wal().await?;
    if let Some(cleanup) = harness.cleanup.take() {
        cleanup();
    }
    Ok(())
}

async fn wal_rotation_s3(schema: Arc<Schema>, harness: S3Harness) -> Result<(), Box<dyn Error>> {
    let mut db = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .open()
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    for idx in 0..3 {
        let ids: Vec<String> = (0..64).map(|n| format!("user-{idx}-{n:02}")).collect();
        let vals: Vec<i32> = (0..64).map(|n| idx * 10 + n).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(Int32Array::from(vals)) as _,
            ],
        )?;
        db.ingest(batch).await?;
    }

    db.disable_wal().await?;
    drop(db);

    let reopened: DB<_, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .open()
        .await?;

    let predicate = Expr::is_not_null("id");
    let rows = extract_rows(reopened.scan().filter(predicate).collect().await?);
    assert!(
        rows.len() >= 3 * 64,
        "wal replay should restore all ingested rows (s3)"
    );

    let mut inner = reopened.into_inner();
    inner.disable_wal().await?;
    Ok(())
}

async fn snapshot_and_merge_local(schema: Arc<Schema>) -> Result<(), Box<dyn Error>> {
    let mut harness = local_harness(
        "public-snapshot-merge",
        wal_tuning(WalSyncPolicy::IntervalBytes(1)),
    )?;
    let mut inner = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .on_disk(harness.root.to_string_lossy().into_owned())?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .open()
        .await?
        .into_inner();
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let immutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "z"])) as _,
            Arc::new(Int32Array::from(vec![1, 9])) as _,
        ],
    )?;
    inner
        .ingest_with_tombstones(immutable_batch, vec![false, false])
        .await?;

    let first_ts = inner
        .list_versions(4)
        .await?
        .last()
        .expect("first manifest version")
        .timestamp;

    let second_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b"])) as _,
            Arc::new(Int32Array::from(vec![5])) as _,
        ],
    )?;
    inner
        .ingest_with_tombstones(second_batch, vec![false])
        .await?;

    let db = DB::from_inner(Arc::new(inner));

    let snapshot = db.snapshot_at(first_ts).await?;
    let base_predicate = Expr::gt("v", ScalarValue::from(0i64));
    let snapshot_rows = extract_rows(
        snapshot
            .scan(&db)
            .filter(base_predicate.clone())
            .collect()
            .await?,
    );
    assert_eq!(
        snapshot_rows,
        vec![("a".into(), 1), ("z".into(), 9)],
        "snapshot should pin the pre-second-batch version"
    );

    let latest_rows = extract_rows(db.scan().filter(base_predicate.clone()).collect().await?);
    assert!(
        latest_rows.contains(&("b".into(), 5)),
        "latest view should include second sealed batch"
    );

    let mut inner = db.into_inner();
    inner.set_seal_policy(Arc::new(NeverSeal));
    let mutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["m-live", "m-drop", "z"])) as _,
            Arc::new(Int32Array::from(vec![3, 4, 0])) as _,
        ],
    )?;
    inner
        .ingest_with_tombstones(mutable_batch, vec![false, true, true])
        .await?;

    let db = DB::from_inner(Arc::new(inner));
    let merged_rows = extract_rows(db.scan().filter(base_predicate).limit(2).collect().await?);
    assert_eq!(merged_rows, vec![("a".into(), 1), ("b".into(), 5)]);

    let mut inner = db.into_inner();
    inner.disable_wal().await?;
    if let Some(cleanup) = harness.cleanup.take() {
        cleanup();
    }
    Ok(())
}

async fn snapshot_and_merge_s3(
    schema: Arc<Schema>,
    harness: S3Harness,
) -> Result<(), Box<dyn Error>> {
    let mut inner = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .open()
        .await?
        .into_inner();
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let immutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "z"])) as _,
            Arc::new(Int32Array::from(vec![1, 9])) as _,
        ],
    )?;
    inner
        .ingest_with_tombstones(immutable_batch, vec![false, false])
        .await?;

    let first_ts = inner
        .list_versions(4)
        .await?
        .last()
        .expect("first manifest version")
        .timestamp;

    let second_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b"])) as _,
            Arc::new(Int32Array::from(vec![5])) as _,
        ],
    )?;
    inner
        .ingest_with_tombstones(second_batch, vec![false])
        .await?;

    let db = DB::from_inner(Arc::new(inner));

    let snapshot = db.snapshot_at(first_ts).await?;
    let base_predicate = Expr::gt("v", ScalarValue::from(0i64));
    let snapshot_rows = extract_rows(
        snapshot
            .scan(&db)
            .filter(base_predicate.clone())
            .collect()
            .await?,
    );
    assert_eq!(
        snapshot_rows,
        vec![("a".into(), 1), ("z".into(), 9)],
        "snapshot should pin the pre-second-batch version (s3)"
    );

    let latest_rows = extract_rows(db.scan().filter(base_predicate.clone()).collect().await?);
    assert!(
        latest_rows.contains(&("b".into(), 5)),
        "latest view should include second sealed batch (s3)"
    );

    let mut inner = db.into_inner();
    inner.set_seal_policy(Arc::new(NeverSeal));
    let mutable_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["m-live", "m-drop", "z"])) as _,
            Arc::new(Int32Array::from(vec![3, 4, 0])) as _,
        ],
    )?;
    inner
        .ingest_with_tombstones(mutable_batch, vec![false, true, true])
        .await?;

    let db = DB::from_inner(Arc::new(inner));
    let merged_rows = extract_rows(db.scan().filter(base_predicate).limit(2).collect().await?);
    assert_eq!(merged_rows, vec![("a".into(), 1), ("b".into(), 5)]);

    let mut inner = db.into_inner();
    inner.disable_wal().await?;
    Ok(())
}

async fn compaction_reopen_scan_s3(
    schema: Arc<Schema>,
    harness: S3Harness,
) -> Result<(), Box<dyn Error>> {
    let mut inner = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .open()
        .await?
        .into_inner();
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let batch_count = 16usize;
    let rows_per_batch = 64usize;
    for batch_idx in 0..batch_count {
        let ids: Vec<String> = (0..rows_per_batch)
            .map(|row_idx| format!("bench-{batch_idx:02}-{row_idx:02}"))
            .collect();
        let vals: Vec<i32> = (0..rows_per_batch)
            .map(|row_idx| i32::try_from(batch_idx * rows_per_batch + row_idx).unwrap_or(i32::MAX))
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(Int32Array::from(vals)) as _,
            ],
        )?;
        inner.ingest(batch).await?;
    }

    let versions_before = inner.list_versions(4).await?;
    let before_latest = versions_before
        .first()
        .ok_or("expected manifest versions before reopen")?;
    assert!(
        before_latest.sst_count >= 4,
        "expected multiple SSTs before compaction, got {}",
        before_latest.sst_count
    );

    drop(inner);

    let reopened: DB<_, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .with_compaction_options(CompactionOptions::new().periodic_tick(Duration::from_millis(50)))
        .open()
        .await?;

    wait_for_reopen_compaction_progress(&reopened, before_latest).await?;

    let predicate = Expr::is_not_null("id");
    let rows = retry_transient_remote_read("reopened s3 visibility scan", || async {
        Ok(extract_rows(
            reopened.scan().filter(predicate.clone()).collect().await?,
        ))
    })
    .await?;
    assert_eq!(
        rows.len(),
        batch_count * rows_per_batch,
        "reopened S3 compaction path should preserve visibility after compaction"
    );

    let mut inner = reopened.into_inner();
    inner.disable_wal().await?;
    Ok(())
}

async fn compaction_reopen_scan_s3_duplicate_key_workload(
    schema: Arc<Schema>,
    harness: S3Harness,
    batch_count: usize,
    rows_per_batch: usize,
    key_space: usize,
) -> Result<(), Box<dyn Error>> {
    let mut inner = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .open()
        .await?
        .into_inner();
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let mut expected_latest = std::collections::BTreeMap::new();
    for batch_idx in 0..batch_count {
        let ids: Vec<String> = (0..rows_per_batch)
            .map(|row_idx| {
                let global_idx = batch_idx
                    .saturating_mul(rows_per_batch)
                    .saturating_add(row_idx);
                let key_idx = global_idx % key_space;
                format!("k{key_idx:08}")
            })
            .collect();
        let vals: Vec<i32> = (0..rows_per_batch)
            .map(|row_idx| {
                i32::try_from(
                    batch_idx
                        .saturating_mul(rows_per_batch)
                        .saturating_add(row_idx),
                )
                .unwrap_or(i32::MAX)
            })
            .collect();
        for (id, value) in ids.iter().zip(vals.iter()) {
            expected_latest.insert(id.clone(), *value);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(Int32Array::from(vals)) as _,
            ],
        )?;
        inner.ingest(batch).await?;
    }

    let versions_before = inner.list_versions(4).await?;
    let before_latest = versions_before
        .first()
        .ok_or("expected manifest versions before reopen")?;
    assert!(
        before_latest.sst_count >= 4,
        "expected multiple SSTs before compaction, got {}",
        before_latest.sst_count
    );

    drop(inner);

    let reopened: DB<_, TokioExecutor> = DbBuilder::from_schema_key_name(schema.clone(), "id")?
        .object_store(harness.object.clone())
        .map_err(|err| format!("object_store config: {err}"))?
        .wal_config(harness.wal_config.clone())
        .with_minor_compaction(1, 0)
        .with_compaction_options(CompactionOptions::new().periodic_tick(Duration::from_millis(50)))
        .open()
        .await?;

    wait_for_reopen_compaction_progress(&reopened, before_latest).await?;

    let expected_rows: Vec<(String, i32)> = expected_latest.into_iter().collect();
    let predicate = Expr::is_not_null("id");
    let verification = async {
        let rows =
            retry_transient_remote_read("reopened s3 duplicate-key unfiltered scan", || async {
                Ok(extract_rows(reopened.scan().collect().await?))
            })
            .await?;
        assert_eq!(
            rows, expected_rows,
            "reopened S3 duplicate-key compaction path should preserve latest-value visibility \
             for unfiltered scans"
        );

        let profiled_rows =
            retry_transient_remote_read("reopened s3 duplicate-key profiled scan", || async {
                extract_rows_with_profile(&reopened).await
            })
            .await?;
        assert_eq!(
            profiled_rows, expected_rows,
            "reopened S3 duplicate-key compaction path should preserve latest-value visibility \
             for profiled scans"
        );

        let filtered_rows =
            retry_transient_remote_read("reopened s3 duplicate-key filtered scan", || async {
                Ok(extract_rows(
                    reopened.scan().filter(predicate.clone()).collect().await?,
                ))
            })
            .await?;
        assert_eq!(
            filtered_rows, expected_rows,
            "reopened S3 duplicate-key compaction path should preserve latest-value visibility \
             for filtered scans"
        );

        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let mut inner = reopened.into_inner();
    inner.disable_wal().await?;
    verification
}

/// Public knobs only: force minor compaction + manifest updates, then restart and time-travel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_compaction_publishes_manifest_versions() -> Result<(), Box<dyn std::error::Error>> {
    let schema = build_schema();
    public_compaction_local(schema.clone()).await?;

    if let Some(h) = maybe_s3_harness("public-compaction-s3", wal_tuning(WalSyncPolicy::Always))? {
        public_compaction_s3(schema, h).await?;
    }
    Ok(())
}

/// WAL rotation + replay via public DB builder and WAL config knobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_wal_rotation_and_replay() -> Result<(), Box<dyn std::error::Error>> {
    let schema = build_schema();
    wal_rotation_local(schema.clone()).await?;

    if let Some(h) = maybe_s3_harness("public-wal-rotation-s3", wal_tuning(WalSyncPolicy::Always))?
    {
        wal_rotation_s3(schema, h).await?;
    }
    Ok(())
}

/// Snapshot + scan merge mutable/immutable layers using only DB public API.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_snapshot_and_merge_scan() -> Result<(), Box<dyn std::error::Error>> {
    let schema = build_schema();
    snapshot_and_merge_local(schema.clone()).await?;

    if let Some(h) = maybe_s3_harness(
        "public-snapshot-merge-s3",
        wal_tuning(WalSyncPolicy::IntervalBytes(1)),
    )? {
        snapshot_and_merge_s3(schema, h).await?;
    }
    Ok(())
}

/// Reopen an S3-backed DB with compaction enabled and verify scans stay visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_s3_reopen_with_compaction_preserves_visibility()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = build_schema();
    if let Some(h) = maybe_s3_harness(
        "public-s3-reopen-compaction",
        wal_tuning(WalSyncPolicy::Always),
    )? {
        compaction_reopen_scan_s3(schema, h).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_s3_reopen_with_compaction_preserves_duplicate_key_visibility()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = build_schema();
    if let Some(h) = maybe_s3_harness(
        "public-s3-reopen-compaction-duplicate-keys",
        wal_tuning(WalSyncPolicy::Always),
    )? {
        compaction_reopen_scan_s3_duplicate_key_workload(schema, h, 32, 64, 512).await?;
    }
    Ok(())
}
