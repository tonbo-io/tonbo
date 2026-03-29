#![cfg(feature = "tokio")]

use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use fusio::{disk::LocalFs, executor::tokio::TokioExecutor};
use tokio::time::sleep;

use crate::{
    db::{BatchesThreshold, DB, Expr, ScalarValue},
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScanObservation {
    rows: usize,
    first_key: Option<String>,
    last_key: Option<String>,
    fingerprint: u64,
    all_keys_in_band: bool,
}

fn fingerprint_seed(label: &str) -> u64 {
    fingerprint_update(0xcbf2_9ce4_8422_2325, label.as_bytes())
}

fn fingerprint_update(mut fingerprint: u64, bytes: &[u8]) -> u64 {
    for &byte in bytes {
        fingerprint ^= u64::from(byte);
        fingerprint = fingerprint.wrapping_mul(0x0000_0100_0000_01b3);
    }
    fingerprint = fingerprint.wrapping_mul(0x0000_0100_0000_01b3);
    fingerprint
}

fn observe_rows(rows: &[(String, i32)], prefix: &str) -> ScanObservation {
    let mut fingerprint = fingerprint_seed(prefix);
    let mut first_key = None;
    let mut last_key = None;
    let mut all_keys_in_band = true;
    for (key, _) in rows {
        if first_key.is_none() {
            first_key = Some(key.clone());
        }
        last_key = Some(key.clone());
        all_keys_in_band &= key.starts_with(prefix);
        fingerprint = fingerprint_update(fingerprint, key.as_bytes());
    }
    ScanObservation {
        rows: rows.len(),
        first_key,
        last_key,
        fingerprint,
        all_keys_in_band,
    }
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

async fn scan_snapshot_range(
    db: &DB<LocalFs, TokioExecutor>,
    snapshot: &crate::db::Snapshot,
    lower: &'static str,
    upper: &'static str,
) -> Result<Vec<(String, i32)>, Box<dyn std::error::Error>> {
    Ok(extract_rows(
        snapshot
            .scan(db)
            .filter(Expr::and(vec![
                Expr::gt_eq("id", ScalarValue::from(lower)),
                Expr::lt("id", ScalarValue::from(upper)),
            ]))
            .collect()
            .await?,
    ))
}

async fn scan_head_range(
    db: &DB<LocalFs, TokioExecutor>,
    lower: &'static str,
    upper: &'static str,
    limit: usize,
) -> Result<Vec<(String, i32)>, Box<dyn std::error::Error>> {
    Ok(extract_rows(
        db.scan()
            .filter(Expr::and(vec![
                Expr::gt_eq("id", ScalarValue::from(lower)),
                Expr::lt("id", ScalarValue::from(upper)),
            ]))
            .limit(limit)
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

fn build_benchmark_like_batch(
    schema: &arrow_schema::SchemaRef,
    start_idx: usize,
    row_count: usize,
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let ids: Vec<String> = (start_idx..start_idx.saturating_add(row_count))
        .map(|idx| {
            if idx < 16_384 {
                format!("hot-{idx:08}")
            } else if idx < 49_152 {
                format!("warm-{:08}", idx - 16_384)
            } else {
                format!("cold-{:08}", idx - 49_152)
            }
        })
        .collect();
    let values: Vec<i32> = (start_idx..start_idx.saturating_add(row_count))
        .map(|idx| i32::try_from(idx).unwrap_or(i32::MAX))
        .collect();
    build_batch(schema, ids, values)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn swmr_manifest_version_timestamp_is_not_a_safe_pinned_snapshot_substitute()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("swmr-manifest-version-gap");
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
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 64 }));
    let db = DB::from_inner(Arc::new(inner));
    for batch_idx in 0..96usize {
        db.ingest(build_benchmark_like_batch(
            &schema,
            batch_idx.saturating_mul(256),
            256,
        )?)
        .await?;
    }

    let manifest_version = latest_version(&db).await?;
    let held_snapshot = db.begin_snapshot().await?;
    assert!(
        held_snapshot.read_timestamp() > manifest_version.timestamp,
        "held snapshot should advance beyond the latest manifest publish timestamp"
    );

    let held_rows = scan_snapshot_range(&db, &held_snapshot, "warm-00000000", "zzzzzzzz").await?;
    assert!(
        !held_rows.is_empty(),
        "held snapshot should include warm rows that arrived after the last manifest publish"
    );

    let reconstructed = db.snapshot_at(manifest_version.timestamp).await?;
    let reconstructed_rows =
        scan_snapshot_range(&db, &reconstructed, "warm-00000000", "zzzzzzzz").await?;
    assert!(
        reconstructed_rows.len() < held_rows.len(),
        "snapshot_at(manifest_version.timestamp) should under-read compared with the held snapshot"
    );

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn swmr_pinned_snapshot_shape_fingerprint_stays_stable()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("swmr-pinned-shape-stability");
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
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 64 }));
    let db = DB::from_inner(Arc::new(inner));

    for batch_idx in 0..96usize {
        db.ingest(build_benchmark_like_batch(
            &schema,
            batch_idx.saturating_mul(256),
            256,
        )?)
        .await?;
    }

    let pinned_snapshot = db.begin_snapshot().await?;
    let pinned_hot_before = observe_rows(
        &scan_snapshot_range(&db, &pinned_snapshot, "hot-00000000", "hot-99999999").await?,
        "hot-",
    );
    let pinned_warm_before = observe_rows(
        &scan_snapshot_range(&db, &pinned_snapshot, "warm-00000000", "zzzzzzzz").await?,
        "warm-",
    );

    let hot_overwrite_ids: Vec<String> = (0..64).map(|row| format!("hot-{row:08}")).collect();
    let hot_overwrite_values: Vec<i32> = (0..64).map(|row| 50_000 + row).collect();
    db.ingest(build_batch(
        &schema,
        hot_overwrite_ids,
        hot_overwrite_values,
    )?)
    .await?;

    let hot_delete_ids: Vec<String> = (64..128).map(|row| format!("hot-{row:08}")).collect();
    let hot_delete_values: Vec<i32> = (64..128).map(|row| 60_000 + row).collect();
    db.ingest_with_tombstones(
        build_batch(&schema, hot_delete_ids, hot_delete_values)?,
        std::iter::repeat_n(true, 64).collect(),
    )
    .await?;

    let append_ids: Vec<String> = (0..64).map(|row| format!("append-{row:08}")).collect();
    let append_values: Vec<i32> = (0..64).map(|row| 70_000 + row).collect();
    db.ingest(build_batch(&schema, append_ids, append_values)?)
        .await?;

    let pinned_hot_after = observe_rows(
        &scan_snapshot_range(&db, &pinned_snapshot, "hot-00000000", "hot-99999999").await?,
        "hot-",
    );
    let pinned_warm_after = observe_rows(
        &scan_snapshot_range(&db, &pinned_snapshot, "warm-00000000", "zzzzzzzz").await?,
        "warm-",
    );

    assert_eq!(pinned_hot_after, pinned_hot_before);
    assert_eq!(pinned_warm_after, pinned_warm_before);

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn swmr_head_reader_shape_checks_match_workload_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("swmr-head-shape-checks");
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
    inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 64 }));
    let db = DB::from_inner(Arc::new(inner));

    for batch_idx in 0..128usize {
        db.ingest(build_benchmark_like_batch(
            &schema,
            batch_idx.saturating_mul(256),
            256,
        )?)
        .await?;
    }

    let head_heavy_before = observe_rows(
        &scan_head_range(&db, "warm-00000000", "zzzzzzzz", 2_048).await?,
        "warm-",
    );
    assert_eq!(head_heavy_before.rows, 2_048);
    assert!(head_heavy_before.all_keys_in_band);

    let delete_ids: Vec<String> = (0..64).map(|row| format!("hot-{row:08}")).collect();
    let delete_values: Vec<i32> = (0..64).map(|row| 80_000 + row).collect();
    db.ingest_with_tombstones(
        build_batch(&schema, delete_ids, delete_values)?,
        std::iter::repeat_n(true, 64).collect(),
    )
    .await?;

    let overwrite_ids: Vec<String> = (128..192).map(|row| format!("warm-{row:08}")).collect();
    let overwrite_values: Vec<i32> = (0..64).map(|row| 90_000 + row).collect();
    db.ingest(build_batch(&schema, overwrite_ids, overwrite_values)?)
        .await?;

    let append_ids: Vec<String> = (0..64).map(|row| format!("append-live-{row:08}")).collect();
    let append_values: Vec<i32> = (0..64).map(|row| 100_000 + row).collect();
    db.ingest(build_batch(&schema, append_ids, append_values)?)
        .await?;

    let head_light_after = observe_rows(
        &scan_head_range(&db, "hot-00000000", "hot-99999999", 256).await?,
        "hot-",
    );
    assert_eq!(head_light_after.rows, 256);
    assert!(head_light_after.all_keys_in_band);

    let head_heavy_after = observe_rows(
        &scan_head_range(&db, "warm-00000000", "zzzzzzzz", 2_048).await?,
        "warm-",
    );
    assert_eq!(head_heavy_after, head_heavy_before);

    if let Err(err) = fs::remove_dir_all(&temp_root) {
        eprintln!("failed to clean temp dir {:?}: {err}", &temp_root);
    }
    Ok(())
}
