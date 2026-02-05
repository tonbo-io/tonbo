use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use fusio::{
    DynFs, IoBuf, IoBufMut, Read as FusioRead, Write as FusioWrite,
    durability::FileCommit,
    dynamic::{DynFile, MaybeSendFuture, MaybeSendStream},
    error::Error as FusioError,
    executor::NoopExecutor,
    fs::{FileMeta, FileSystemTag, OpenOptions},
    mem::fs::InMemoryFs,
    path::Path,
};
use fusio_parquet::writer::AsyncWriter;
use futures::TryStreamExt;
use parquet::{
    arrow::AsyncArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{
    db::{DB, DbInner, Expr, ScalarValue},
    extractor,
    inmem::policy::{BatchesThreshold, NeverSeal, SealPolicy},
    manifest::{SstEntry, VersionEdit},
    mode::DynModeConfig,
    mvcc::{MVCC_COMMIT_COL, Timestamp},
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableId, SsTableStats},
    query::scan::ScanSelection,
    test::build_batch,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_filters_immutable_segments() {
    let db = db_with_immutable_keys(&["k1", "z1"]).await;
    let predicate = Expr::eq("id", ScalarValue::from("k1"));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    // Expect key-bound pruning to skip the non-overlapping immutable.
    assert_eq!(plan.immutable_indexes, vec![0]);
    assert!(plan.pushdown_predicate.is_some());
    assert!(plan.residual_predicate.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_preserves_residual_predicate() {
    let db = db_with_immutable_keys(&["k1"]).await;
    let key_pred = Expr::eq("id", ScalarValue::from("k1"));
    let value_pred = Expr::gt("v", ScalarValue::from(5i64));
    let predicate = Expr::and(vec![key_pred, value_pred]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert!(plan.pushdown_predicate.is_some());
    assert!(plan.residual_predicate.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_marks_empty_range() {
    let db = db_with_immutable_keys(&["k1"]).await;
    let pred_a = Expr::eq("id", ScalarValue::from("k1"));
    let pred_b = Expr::eq("id", ScalarValue::from("k2"));
    let predicate = Expr::and(vec![pred_a, pred_b]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    // Contradictory predicates yield empty key bounds; skip all immutables.
    assert!(plan.immutable_indexes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_includes_predicate_columns_and_filters_before_projection() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let rows = vec![
        DynRow(vec![
            Some(DynCell::Str("keep".into())),
            Some(DynCell::I32(1)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("drop".into())),
            Some(DynCell::I32(-1)),
        ]),
    ];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let predicate = Expr::gt("v", ScalarValue::from(0i64));
    let projection = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&projection), None)
        .await
        .expect("plan");

    let scan_fields: Vec<&str> = plan
        .scan_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(scan_fields, vec!["id", "v"]);
    let projected = plan.projected_schema.as_ref().expect("projection");
    let projected_fields: Vec<&str> = projected
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(projected_fields, vec!["id"]);

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["keep".to_string()]);
    assert!(
        batches.iter().all(|batch| batch.num_columns() == 1),
        "projection should apply after filtering"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_missing_column_is_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;
    let predicate = Expr::eq("missing", ScalarValue::from(1i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let err = match snapshot.plan_scan(&db, &predicate, None, None).await {
        Ok(_) => panic!("missing column should fail at plan time"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("no such field in schema"),
        "unexpected error: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_bloom_filter_predicate_is_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;
    let predicate = Expr::BloomFilterEq {
        column: "id".to_string(),
        value: ScalarValue::from("k1"),
    };
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let err = match snapshot.plan_scan(&db, &predicate, None, None).await {
        Ok(_) => panic!("bloom filter predicate should be rejected at plan time"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("bloom filter predicates are not supported"),
        "unexpected error: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_missing_page_indexes_is_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let sst_root = Path::from("scan-missing-page-index");
    db.fs.create_dir_all(&sst_root).await.expect("create dir");
    let data_path = sst_root.child("000.parquet");
    let batch = rows_with_commit_ts(0, 2, Timestamp::MIN.get());
    write_parquet_data_missing_page_index(Arc::clone(&db.fs), data_path.clone(), batch).await;

    let sst_entry = SstEntry::new(SsTableId::new(9), None, None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let err = match snapshot.plan_scan(&db, &Expr::True, None, None).await {
        Ok(_) => panic!("missing page indexes should error"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("missing page indexes"),
        "unexpected error: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_prunes_sst_row_groups_and_pages() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let sst_root = Path::from("scan-prune");
    db.fs.create_dir_all(&sst_root).await.expect("create dir");
    let data_path = sst_root.child("000.parquet");
    write_parquet_data(
        Arc::clone(&db.fs),
        data_path.clone(),
        rows_with_commit_ts(0, 100, Timestamp::MIN.get()),
        50,
        10,
    )
    .await;
    let sst_entry = SstEntry::new(SsTableId::new(1), None, None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let predicate = Expr::gt_eq("v", ScalarValue::from(60i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    let selection = plan.sst_selections[0].selection.clone();
    let ScanSelection::Sst(selection) = selection else {
        panic!("expected sst selection");
    };
    assert_eq!(selection.row_groups.as_ref(), Some(&vec![1]));
    let row_selection = selection.row_set.to_row_selection().expect("row selection");
    let has_skip = row_selection.iter().any(|sel| sel.skip);
    let has_select = row_selection.iter().any(|sel| !sel.skip);
    assert!(has_skip && has_select, "expected page-level pruning");
    assert_eq!(row_selection.row_count(), 40);

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids.len(), 40);
    assert!(ids.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_pruning_preserves_pk_order_across_row_groups() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let sst_root = Path::from("scan-prune-order");
    db.fs.create_dir_all(&sst_root).await.expect("create dir");
    let data_path = sst_root.child("000.parquet");
    write_parquet_data(
        Arc::clone(&db.fs),
        data_path.clone(),
        rows_with_commit_ts(0, 120, Timestamp::MIN.get()),
        40,
        10,
    )
    .await;
    let sst_entry = SstEntry::new(SsTableId::new(6), None, None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let predicate = Expr::and(vec![
        Expr::gt_eq("v", ScalarValue::from(20i32)),
        Expr::lt_eq("v", ScalarValue::from(75i32)),
    ]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    let selection = plan.sst_selections[0].selection.clone();
    let ScanSelection::Sst(selection) = selection else {
        panic!("expected sst selection");
    };
    assert_eq!(selection.row_groups.as_ref(), Some(&vec![0, 1]));

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids.first().map(String::as_str), Some("k000020"));
    assert_eq!(ids.last().map(String::as_str), Some("k000075"));
    assert!(ids.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_skips_fully_pruned_sst() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let sst_root = Path::from("scan-prune-empty");
    db.fs.create_dir_all(&sst_root).await.expect("create dir");
    let data_path = sst_root.child("000.parquet");
    write_parquet_data(
        Arc::clone(&db.fs),
        data_path.clone(),
        rows_with_commit_ts(0, 100, Timestamp::MIN.get()),
        50,
        10,
    )
    .await;
    let sst_entry = SstEntry::new(SsTableId::new(13), None, None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let predicate = Expr::gt("v", ScalarValue::from(1000i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert!(
        plan.sst_selections.is_empty(),
        "expected fully pruned SST to be skipped"
    );

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert!(ids.is_empty(), "expected no rows");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_skips_ssts_after_read_ts() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let sst_root = Path::from("scan-skip-commit-ts");
    db.fs.create_dir_all(&sst_root).await.expect("create dir");
    let data_path = sst_root.child("000.parquet");
    write_parquet_data(
        Arc::clone(&db.fs),
        data_path.clone(),
        rows_with_commit_ts(0, 1, Timestamp::MIN.get()),
        1,
        1,
    )
    .await;
    let stats = SsTableStats {
        min_commit_ts: Some(Timestamp::MAX),
        max_commit_ts: Some(Timestamp::MAX),
        ..Default::default()
    };
    let sst_entry = SstEntry::new(SsTableId::new(10), Some(stats), None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let predicate = Expr::gt_eq("v", ScalarValue::from(0i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert!(
        plan.sst_selections.is_empty(),
        "expected SST skipped when min_commit_ts is after read_ts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_prunes_sst_commit_ts_at_plan_time() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let sst_root = Path::from("scan-commit-ts");
    db.fs.create_dir_all(&sst_root).await.expect("create dir");
    let data_path = sst_root.child("000.parquet");
    write_parquet_data(
        Arc::clone(&db.fs),
        data_path.clone(),
        rows_with_commit_ts_range(0, 100, 0),
        100,
        10,
    )
    .await;
    let sst_entry = SstEntry::new(SsTableId::new(11), None, None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let predicate = Expr::gt_eq("v", ScalarValue::from(0i32));
    let snapshot = db.snapshot_at(Timestamp::new(49)).await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert_eq!(plan.sst_selections.len(), 1);
    let selection = plan.sst_selections[0].selection.clone();
    let ScanSelection::Sst(selection) = selection else {
        panic!("expected sst selection");
    };
    let row_selection = selection.row_set.to_row_selection().expect("row selection");
    assert_eq!(row_selection.row_count(), 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_prunes_mutable_by_key_bounds() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema_and_policy(schema.clone(), Arc::new(NeverSeal))
        .await
        .into_inner();

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k1".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let predicate = Expr::eq("id", ScalarValue::from("z1"));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert!(plan.mutable_row_set.is_empty(), "expected mutable prune");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_prunes_mutable_commit_ts() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema_and_policy(schema.clone(), Arc::new(NeverSeal))
        .await
        .into_inner();

    db.next_commit_ts(); // advance clock so first ingest is > read_ts
    let snapshot = db.begin_snapshot().await.expect("snapshot");

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k1".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let plan = snapshot
        .plan_scan(&db, &Expr::True, None, None)
        .await
        .expect("plan");
    assert!(plan.mutable_row_set.is_empty(), "expected commit_ts prune");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_prunes_immutable_commit_ts() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;
    db.next_commit_ts(); // advance clock so first ingest is > read_ts

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k1".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let snapshot = db.snapshot_at(Timestamp::new(0)).await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &Expr::True, None, None)
        .await
        .expect("plan");
    assert!(
        plan.immutable_indexes.is_empty(),
        "expected immutable commit_ts prune"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn plan_scan_skips_sst_by_key_bounds_before_metadata() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let stats = SsTableStats {
        min_key: Some(crate::key::KeyOwned::from("a")),
        max_key: Some(crate::key::KeyOwned::from("b")),
        ..Default::default()
    };
    let data_path = Path::from("scan-pre-prune/000.parquet");
    let sst_entry = SstEntry::new(SsTableId::new(12), Some(stats), None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let predicate = Expr::eq("id", ScalarValue::from("z9"));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert!(
        plan.sst_selections.is_empty(),
        "expected pre-metadata key bound pruning"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_limit_waits_for_residual_predicate() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;
    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(-1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(-2))]),
        DynRow(vec![Some(DynCell::Str("c".into())), Some(DynCell::I32(7))]),
    ];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let predicate = Expr::gt("v", ScalarValue::from(0i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, Some(1))
        .await
        .expect("plan");
    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["c".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_limit_waits_for_pushdown_on_non_sst() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;
    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(-1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(-2))]),
        DynRow(vec![Some(DynCell::Str("c".into())), Some(DynCell::I32(7))]),
    ];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let predicate = Expr::gt("v", ScalarValue::from(0i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, Some(1))
        .await
        .expect("plan");
    assert!(plan.pushdown_predicate.is_some());
    assert!(plan.residual_predicate.is_none());

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["c".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_residual_predicate_filters_after_pushdown() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(7))]),
    ];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::clone(&db.fs),
        Path::from("scan-residual-after-pushdown"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(12), 0);
    db.flush_immutables_with_descriptor(sst_cfg, descriptor)
        .await
        .expect("flush");

    let predicate = Expr::and(vec![
        Expr::gt_eq("v", ScalarValue::from(0i32)),
        Expr::gt("v", ScalarValue::Float64(Some(5.5))),
    ]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    assert!(plan.pushdown_predicate.is_some());
    assert!(plan.residual_predicate.is_some());

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["b".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_schema_sst_only_pushdown_skips_predicate_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(-1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::clone(&db.fs),
        Path::from("scan-schema-sst-pushdown"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(20), 0);
    db.flush_immutables_with_descriptor(sst_cfg, descriptor)
        .await
        .expect("flush");

    let projection = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let predicate = Expr::gt("v", ScalarValue::from(0i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&projection), None)
        .await
        .expect("plan");

    let scan_fields: Vec<&str> = plan
        .scan_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(scan_fields, vec!["id"]);
    assert!(plan.residual_predicate.is_none());

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["b".to_string()]);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_schema_sst_only_residual_keeps_predicate_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(7))]),
    ];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::clone(&db.fs),
        Path::from("scan-schema-sst-residual"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(21), 0);
    db.flush_immutables_with_descriptor(sst_cfg, descriptor)
        .await
        .expect("flush");

    let projection = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let predicate = Expr::and(vec![
        Expr::gt_eq("v", ScalarValue::from(0i32)),
        Expr::gt("v", ScalarValue::Float64(Some(5.5))),
    ]);
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&projection), None)
        .await
        .expect("plan");

    let scan_fields: Vec<&str> = plan
        .scan_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(scan_fields, vec!["id", "v"]);
    assert!(plan.residual_predicate.is_some());

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["b".to_string()]);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_schema_mixed_sources_pushdown_keeps_predicate_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mut db = db_with_schema(schema.clone()).await;

    let sst_rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(-1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let sst_batch = build_batch(schema.clone(), sst_rows).expect("batch");
    db.ingest(sst_batch).await.expect("ingest");

    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::clone(&db.fs),
        Path::from("scan-schema-mixed"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(22), 0);
    db.flush_immutables_with_descriptor(sst_cfg, descriptor)
        .await
        .expect("flush");

    db.set_seal_policy(Arc::new(NeverSeal));
    let mem_rows = vec![
        DynRow(vec![Some(DynCell::Str("c".into())), Some(DynCell::I32(3))]),
        DynRow(vec![Some(DynCell::Str("d".into())), Some(DynCell::I32(-2))]),
    ];
    let mem_batch = build_batch(schema.clone(), mem_rows).expect("batch");
    db.ingest(mem_batch).await.expect("ingest");

    let projection = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let predicate = Expr::gt("v", ScalarValue::from(0i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&projection), None)
        .await
        .expect("plan");

    let scan_fields: Vec<&str> = plan
        .scan_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(scan_fields, vec!["id", "v"]);

    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert_eq!(ids, vec!["b".to_string(), "c".to_string()]);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_row_filter_respects_tombstones() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let upsert_rows = vec![DynRow(vec![
        Some(DynCell::Str("k000001".into())),
        Some(DynCell::I32(10)),
    ])];
    let upsert = build_batch(schema.clone(), upsert_rows).expect("upsert");
    db.ingest_with_tombstones(upsert, vec![false])
        .await
        .expect("upsert ingest");

    let delete_rows = vec![DynRow(vec![
        Some(DynCell::Str("k000001".into())),
        Some(DynCell::I32(10)),
    ])];
    let delete = build_batch(schema.clone(), delete_rows).expect("delete");
    db.ingest_with_tombstones(delete, vec![true])
        .await
        .expect("delete ingest");

    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::clone(&db.fs),
        Path::from("scan-mvcc"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(2), 0);
    db.flush_immutables_with_descriptor(sst_cfg, descriptor)
        .await
        .expect("flush");

    let predicate = Expr::gt_eq("v", ScalarValue::from(0i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, None, None)
        .await
        .expect("plan");
    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let ids = collect_ids(&batches);
    assert!(ids.is_empty(), "tombstoned row should not be visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_projection_respects_tombstones() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let upsert_rows = vec![DynRow(vec![
        Some(DynCell::Str("k000001".into())),
        Some(DynCell::I32(10)),
    ])];
    let upsert = build_batch(schema.clone(), upsert_rows).expect("upsert");
    db.ingest_with_tombstones(upsert, vec![false])
        .await
        .expect("upsert ingest");

    let delete_rows = vec![DynRow(vec![
        Some(DynCell::Str("k000001".into())),
        Some(DynCell::I32(10)),
    ])];
    let delete = build_batch(schema.clone(), delete_rows).expect("delete");
    db.ingest_with_tombstones(delete, vec![true])
        .await
        .expect("delete ingest");

    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::clone(&db.fs),
        Path::from("scan-projection-tombstones"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(4), 0);
    db.flush_immutables_with_descriptor(sst_cfg, descriptor)
        .await
        .expect("flush");

    let projection = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let predicate = Expr::gt_eq("v", ScalarValue::from(0i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&projection), None)
        .await
        .expect("plan");
    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let values = collect_i32s(&batches, 0);
    assert!(values.is_empty(), "tombstoned row should be filtered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_sst_non_prefix_projection_returns_correct_values() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;

    let rows = vec![
        DynRow(vec![
            Some(DynCell::Str("id1".into())),
            Some(DynCell::I32(1)),
            Some(DynCell::I32(10)),
        ]),
        DynRow(vec![
            Some(DynCell::Str("id2".into())),
            Some(DynCell::I32(2)),
            Some(DynCell::I32(20)),
        ]),
    ];
    let batch = build_batch(schema.clone(), rows).expect("batch");
    db.ingest(batch).await.expect("ingest");

    let sst_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        Arc::clone(&db.fs),
        Path::from("scan-projection"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(3), 0);
    db.flush_immutables_with_descriptor(sst_cfg, descriptor)
        .await
        .expect("flush");

    let projection = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
    let predicate = Expr::gt("a", ScalarValue::from(1i32));
    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &predicate, Some(&projection), None)
        .await
        .expect("plan");
    let stream = db.execute_scan(plan).await.expect("execute");
    let batches = stream.try_collect::<Vec<_>>().await.expect("collect");
    let values = collect_i32s(&batches, 0);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1, "projection should drop key columns");
        assert_eq!(batch.schema().field(0).name(), "b");
    }
    assert_eq!(values, vec![20]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn scan_plan_reuses_cached_sst_metadata() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mut db = db_with_schema(schema.clone()).await;

    let sst_root = Path::from("scan-metadata-reuse");
    let data_path = sst_root.child("000.parquet");
    let reads = Arc::new(AtomicUsize::new(0));
    let base_fs = Arc::clone(&db.fs);
    db.fs = Arc::new(CountingFs::new(
        base_fs,
        data_path.clone(),
        Arc::clone(&reads),
    ));

    db.fs.create_dir_all(&sst_root).await.expect("create dir");
    write_parquet_data(
        Arc::clone(&db.fs),
        data_path.clone(),
        rows_with_commit_ts_range(0, 8, Timestamp::MIN.get()),
        4,
        2,
    )
    .await;
    let sst_entry = SstEntry::new(SsTableId::new(5), None, None, data_path, None);
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![sst_entry],
            }],
        )
        .await
        .expect("add sst");

    let snapshot = db.begin_snapshot().await.expect("snapshot");
    let plan = snapshot
        .plan_scan(&db, &Expr::True, None, None)
        .await
        .expect("plan");
    let reads_after_plan = reads.load(Ordering::SeqCst);
    assert!(
        reads_after_plan > 0,
        "expected metadata read during planning"
    );

    let _streams = db
        .build_scan_streams(&plan, None)
        .await
        .expect("open streams");
    let reads_after_open = reads.load(Ordering::SeqCst);
    assert_eq!(
        reads_after_open, reads_after_plan,
        "stream open should reuse cached metadata"
    );
}

async fn db_with_immutable_keys(keys: &[&str]) -> DbInner<InMemoryFs, NoopExecutor> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let db = db_with_schema(schema.clone()).await;
    for (idx, key) in keys.iter().enumerate() {
        let rows = vec![DynRow(vec![
            Some(DynCell::Str((*key).into())),
            Some(DynCell::I32(idx as i32)),
        ])];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        db.ingest_with_tombstones(batch, vec![false])
            .await
            .expect("ingest");
    }
    db
}

async fn db_with_schema(schema: Arc<Schema>) -> DbInner<InMemoryFs, NoopExecutor> {
    let extractor = extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema, extractor).expect("config");
    let policy = Arc::new(BatchesThreshold { batches: 1 });
    DB::new_with_policy(config, Arc::clone(&executor), policy)
        .await
        .expect("db")
        .into_inner()
}

async fn db_with_schema_and_policy(
    schema: Arc<Schema>,
    policy: Arc<dyn SealPolicy + Send + Sync>,
) -> DB<InMemoryFs, NoopExecutor> {
    let extractor = extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema, extractor).expect("config");
    DB::new_with_policy(config, Arc::clone(&executor), policy)
        .await
        .expect("db")
}

fn rows_with_commit_ts(start: i32, count: usize, commit_ts: u64) -> RecordBatch {
    let mut ids = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    let mut commits = Vec::with_capacity(count);
    for offset in 0..count {
        let value = start + offset as i32;
        ids.push(format!("k{value:06}"));
        values.push(value);
        commits.push(commit_ts);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
        Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
    ]));
    let columns = vec![
        Arc::new(StringArray::from(ids)) as _,
        Arc::new(Int32Array::from(values)) as _,
        Arc::new(UInt64Array::from(commits)) as _,
    ];
    RecordBatch::try_new(schema, columns).expect("parquet batch")
}

fn rows_with_commit_ts_range(start: i32, count: usize, commit_ts_start: u64) -> RecordBatch {
    let mut ids = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    let mut commits = Vec::with_capacity(count);
    for offset in 0..count {
        let value = start + offset as i32;
        ids.push(format!("k{value:06}"));
        values.push(value);
        commits.push(commit_ts_start + offset as u64);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
        Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
    ]));
    let columns = vec![
        Arc::new(StringArray::from(ids)) as _,
        Arc::new(Int32Array::from(values)) as _,
        Arc::new(UInt64Array::from(commits)) as _,
    ];
    RecordBatch::try_new(schema, columns).expect("parquet batch")
}

async fn write_parquet_data(
    fs: Arc<dyn fusio::DynFs>,
    path: Path,
    batch: RecordBatch,
    row_group_size: usize,
    page_row_limit: usize,
) {
    let file = fs
        .open_options(&path, OpenOptions::default().create(true).write(true))
        .await
        .expect("open parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_size(row_group_size)
        .set_data_page_row_count_limit(page_row_limit)
        .set_write_batch_size(page_row_limit)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let writer = AsyncWriter::new(file, NoopExecutor);
    let mut arrow_writer =
        AsyncArrowWriter::try_new(writer, batch.schema(), Some(props)).expect("arrow writer");
    arrow_writer.write(&batch).await.expect("write batch");
    arrow_writer.close().await.expect("close parquet");
}

async fn write_parquet_data_missing_page_index(
    fs: Arc<dyn fusio::DynFs>,
    path: Path,
    batch: RecordBatch,
) {
    let file = fs
        .open_options(
            &path,
            OpenOptions::default()
                .create(true)
                .write(true)
                .truncate(true),
        )
        .await
        .expect("open parquet file");
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_offset_index_disabled(true)
        .build();
    let writer = AsyncWriter::new(file, NoopExecutor);
    let mut arrow_writer =
        AsyncArrowWriter::try_new(writer, batch.schema(), Some(props)).expect("arrow writer");
    arrow_writer.write(&batch).await.expect("write batch");
    arrow_writer.close().await.expect("close parquet");
}

fn collect_ids(batches: &[RecordBatch]) -> Vec<String> {
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        for idx in 0..batch.num_rows() {
            ids.push(col.value(idx).to_string());
        }
    }
    ids
}

fn collect_i32s(batches: &[RecordBatch], column_idx: usize) -> Vec<i32> {
    let mut values = Vec::new();
    for batch in batches {
        let col = batch
            .column(column_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        for idx in 0..batch.num_rows() {
            values.push(col.value(idx));
        }
    }
    values
}

struct CountingFs {
    inner: Arc<dyn DynFs>,
    target: Path,
    reads: Arc<AtomicUsize>,
}

impl CountingFs {
    fn new(inner: Arc<dyn DynFs>, target: Path, reads: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            target,
            reads,
        }
    }
}

struct CountingFile {
    inner: Box<dyn DynFile>,
    track_reads: bool,
    reads: Arc<AtomicUsize>,
}

impl FusioRead for CountingFile {
    async fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> (Result<(), FusioError>, B) {
        let (result, buf) = self.inner.read_exact_at(buf, pos).await;
        if self.track_reads {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        (result, buf)
    }

    async fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> (Result<(), FusioError>, Vec<u8>) {
        let (result, buf) = self.inner.read_to_end_at(buf, pos).await;
        if self.track_reads {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        (result, buf)
    }

    async fn size(&self) -> Result<u64, FusioError> {
        self.inner.size().await
    }
}

impl FusioWrite for CountingFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), FusioError>, B) {
        self.inner.write_all(buf).await
    }

    async fn flush(&mut self) -> Result<(), FusioError> {
        self.inner.flush().await
    }

    async fn close(&mut self) -> Result<(), FusioError> {
        self.inner.close().await
    }
}

impl FileCommit for CountingFile {
    async fn commit(&mut self) -> Result<(), FusioError> {
        self.inner.commit().await
    }
}

impl DynFs for CountingFs {
    fn file_system(&self) -> FileSystemTag {
        self.inner.file_system()
    }

    fn open_options<'s, 'path: 's>(
        &'s self,
        path: &'path Path,
        options: OpenOptions,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Box<dyn DynFile>, FusioError>> + 's>> {
        let inner = Arc::clone(&self.inner);
        let target = self.target.clone();
        let reads = Arc::clone(&self.reads);
        Box::pin(async move {
            let file = inner.open_options(path, options).await?;
            let track_reads = path == &target;
            Ok(Box::new(CountingFile {
                inner: file,
                track_reads,
                reads,
            }) as Box<dyn DynFile>)
        })
    }

    fn create_dir_all<'s, 'path: 's>(
        &'s self,
        path: &'path Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), FusioError>> + 's>> {
        self.inner.create_dir_all(path)
    }

    fn list<'s, 'path: 's>(
        &'s self,
        path: &'path Path,
    ) -> Pin<
        Box<
            dyn MaybeSendFuture<
                    Output = Result<
                        Pin<Box<dyn MaybeSendStream<Item = Result<FileMeta, FusioError>> + 's>>,
                        FusioError,
                    >,
                > + 's,
        >,
    > {
        self.inner.list(path)
    }

    fn remove<'s, 'path: 's>(
        &'s self,
        path: &'path Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), FusioError>> + 's>> {
        self.inner.remove(path)
    }

    fn copy<'s, 'path: 's>(
        &'s self,
        from: &'path Path,
        to: &'path Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), FusioError>> + 's>> {
        self.inner.copy(from, to)
    }

    fn link<'s, 'path: 's>(
        &'s self,
        from: &'path Path,
        to: &'path Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), FusioError>> + 's>> {
        self.inner.link(from, to)
    }
}
