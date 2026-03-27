#![cfg(feature = "tokio")]

use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor};
use tokio::time::sleep;

use crate::{
    db::{BatchesThreshold, DB, Expr},
    tests_internal::common::config_with_pk,
};

fn workspace_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    let dir = base.join("target").join("tmp").join(format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("create workspace temp dir");
    dir
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
        for (id, value) in ids.iter().zip(vals.iter()) {
            if let (Some(id), Some(value)) = (id, value) {
                rows.push((id.to_string(), value));
            }
        }
    }
    rows.sort();
    rows
}

fn build_batch(
    schema: &arrow_schema::SchemaRef,
    ids: Vec<String>,
    values: Vec<i32>,
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(ids)) as _,
            Arc::new(Int32Array::from(values)) as _,
        ],
    )
}

async fn scan_all(
    db: &DB<LocalFs, TokioExecutor>,
) -> Result<Vec<(String, i32)>, Box<dyn std::error::Error>> {
    Ok(extract_rows(
        db.scan().filter(Expr::is_not_null("id")).collect().await?,
    ))
}

async fn scan_snapshot(
    db: &DB<LocalFs, TokioExecutor>,
    ts: crate::mvcc::Timestamp,
) -> Result<Vec<(String, i32)>, Box<dyn std::error::Error>> {
    let snapshot = db.snapshot_at(ts).await?;
    Ok(extract_rows(
        snapshot
            .scan(db)
            .filter(Expr::is_not_null("id"))
            .collect()
            .await?,
    ))
}

async fn latest_version(
    db: &DB<LocalFs, TokioExecutor>,
) -> Result<crate::db::Version, Box<dyn std::error::Error>> {
    db.list_versions(1)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| "list_versions returned no versions".into())
}

async fn wait_for_manifest_change(
    db: &DB<LocalFs, TokioExecutor>,
    before: &crate::db::Version,
) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(10);
    let poll = Duration::from_millis(50);
    let started = std::time::Instant::now();
    loop {
        let current = latest_version(db).await?;
        let changed = current.timestamp != before.timestamp
            && (current.sst_count != before.sst_count || current.level_count != before.level_count);
        if changed {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(format!(
                "timed out waiting for manifest change: before(ts={}, ssts={}, levels={}) \
                 current(ts={}, ssts={}, levels={})",
                before.timestamp.get(),
                before.sst_count,
                before.level_count,
                current.timestamp.get(),
                current.sst_count,
                current.level_count
            )
            .into());
        }
        sleep(poll).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn swmr_snapshot_stability_survives_writes_and_compaction()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("swmr-snapshot-stability");
    let root_str = temp_root.to_string_lossy().into_owned();

    let config = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let schema = config.schema();
    let executor = Arc::new(TokioExecutor::default());

    let mut inner = DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(root_str.clone())?
        .with_minor_compaction(1, 0)
        .open_with_executor(Arc::clone(&executor))
        .await?
        .into_inner();
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
    let db = DB::from_inner(Arc::new(inner));

    for batch_idx in 0..4 {
        let ids: Vec<String> = (0..32)
            .map(|row| format!("base-{batch_idx:02}-{row:02}"))
            .collect();
        let values: Vec<i32> = (0..32).map(|row| batch_idx * 100 + row).collect();
        db.ingest(build_batch(&schema, ids, values)?).await?;
    }

    let pinned = latest_version(&db).await?;
    let pinned_ts = pinned.timestamp;
    let expected_snapshot_rows = scan_snapshot(&db, pinned_ts).await?;
    assert!(
        !expected_snapshot_rows.is_empty(),
        "snapshot baseline should not be empty"
    );

    let overwrite_ids: Vec<String> = (0..32).map(|row| format!("base-00-{row:02}")).collect();
    let overwrite_values: Vec<i32> = (0..32).map(|row| 10_000 + row).collect();
    db.ingest(build_batch(&schema, overwrite_ids, overwrite_values)?)
        .await?;

    let mixed_ids: Vec<String> = (0..32)
        .map(|row| {
            if row < 16 {
                format!("base-01-{row:02}")
            } else {
                format!("live-append-{row:02}")
            }
        })
        .collect();
    let mixed_values: Vec<i32> = (0..32).map(|row| 20_000 + row).collect();
    let mut tombstones = vec![true; 16];
    tombstones.extend(std::iter::repeat_n(false, 16));
    db.ingest_with_tombstones(build_batch(&schema, mixed_ids, mixed_values)?, tombstones)
        .await?;

    wait_for_manifest_change(&db, &pinned).await?;

    let pinned_rows_after = scan_snapshot(&db, pinned_ts).await?;
    assert_eq!(
        pinned_rows_after, expected_snapshot_rows,
        "pinned snapshot must remain stable while HEAD advances"
    );

    let latest_rows = scan_all(&db).await?;
    assert!(
        latest_rows.iter().any(|(id, _)| id == "live-append-31"),
        "head should include appended rows"
    );
    assert!(
        latest_rows.iter().all(|(id, _)| id != "base-01-00"),
        "head should hide deleted rows after compaction"
    );
    assert!(
        latest_rows
            .iter()
            .any(|(id, value)| id == "base-00-00" && *value == 10_000),
        "head should expose overwritten values"
    );

    drop(db);

    let reopened = DB::<LocalFs, TokioExecutor>::builder(config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    ))
    .on_disk(root_str.clone())?
    .open_with_executor(executor)
    .await?;

    let reopened_latest_rows = scan_all(&reopened).await?;
    assert_eq!(
        reopened_latest_rows, latest_rows,
        "reopen should preserve the current head-visible rows"
    );

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }
    Ok(())
}
