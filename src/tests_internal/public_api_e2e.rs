#![cfg(feature = "tokio")]

use std::{error::Error, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::executor::tokio::TokioExecutor;

use crate::{
    db::{BatchesThreshold, ColumnRef, DB, DbBuilder, NeverSeal, Predicate, ScalarValue},
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
                Arc::new(Int32Array::from(vec![idx as i32])) as _,
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

    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
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
                Arc::new(Int32Array::from(vec![idx as i32])) as _,
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

    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
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
        let vals: Vec<i32> = (0..64).map(|n| idx as i32 * 10 + n as i32).collect();
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

    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
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
        let vals: Vec<i32> = (0..64).map(|n| idx as i32 * 10 + n as i32).collect();
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

    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
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
    let base_predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(0i64));
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
    inner.set_seal_policy(Arc::new(NeverSeal::default()));
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
    let base_predicate = Predicate::gt(ColumnRef::new("v"), ScalarValue::from(0i64));
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
    inner.set_seal_policy(Arc::new(NeverSeal::default()));
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
