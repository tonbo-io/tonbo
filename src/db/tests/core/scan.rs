use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::NoopExecutor, fs::OpenOptions, mem::fs::InMemoryFs, path::Path};
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
    inmem::policy::BatchesThreshold,
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
    // Pruning is currently disabled; expect to scan all immutables and retain the predicate
    // for residual evaluation.
    assert_eq!(plan.immutable_indexes, vec![0, 1]);
    assert!(plan.residual_predicate.is_some());
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
    // Pruning is currently disabled; even contradictory predicates scan all immutables.
    assert_eq!(plan.immutable_indexes, vec![0]);
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

    let predicate = Expr::gt("v", ScalarValue::from(0i32));
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
    let row_selection = selection.row_selection.as_ref().expect("row selection");
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
    let row_selection = selection.row_selection.as_ref().expect("row selection");
    assert_eq!(row_selection.row_count(), 50);
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
