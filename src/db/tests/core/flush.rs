use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use fusio::{DynFs, disk::LocalFs, executor::NoopExecutor, mem::fs::InMemoryFs, path::Path};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{
    db::{DB, DbInner},
    inmem::policy::BatchesThreshold,
    mode::DynModeConfig,
    mvcc::Timestamp,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
    test::build_batch,
};

#[tokio::test(flavor = "current_thread")]
async fn flush_without_immutables_errors() {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name config");
    let executor = Arc::new(NoopExecutor);
    let mut db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, executor)
        .await
        .expect("db init")
        .into_inner();

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sstable_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        fs,
        Path::from("/tmp/tonbo-flush-test"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(1), 0);

    let result = db
        .flush_immutables_with_descriptor(sstable_cfg, descriptor.clone())
        .await;
    assert!(matches!(result, Err(SsTableError::NoImmutableSegments)));
    assert_eq!(db.num_immutable_segments(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_publishes_manifest_version() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let executor = Arc::new(NoopExecutor);
    let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
    let mut db: DbInner<InMemoryFs, NoopExecutor> = DB::new(config, Arc::clone(&executor))
        .await
        .expect("db")
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let rows = vec![
        DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
        DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
    ];
    let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
    db.ingest(batch).await.expect("ingest triggers seal");
    assert_eq!(db.num_immutable_segments(), 1);

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sstable_cfg = Arc::new(SsTableConfig::new(
        schema.clone(),
        fs,
        Path::from("/tmp/tonbo-flush-ok"),
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(7), 0);

    let table = db
        .flush_immutables_with_descriptor(sstable_cfg, descriptor.clone())
        .await
        .expect("flush succeeds");
    assert_eq!(db.num_immutable_segments(), 0);

    let snapshot = db
        .manifest
        .snapshot_latest(db.manifest_table)
        .await
        .expect("manifest snapshot");
    assert_eq!(
        snapshot.head.last_manifest_txn,
        Some(Timestamp::new(1)),
        "first flush should publish manifest txn 1"
    );
    let latest = snapshot
        .latest_version
        .expect("latest version must exist after flush");
    assert_eq!(
        latest.commit_timestamp(),
        Timestamp::new(1),
        "latest version should reflect manifest txn 1"
    );
    assert_eq!(latest.ssts().len(), 1);
    assert_eq!(latest.ssts()[0].len(), 1);
    let recorded = &latest.ssts()[0][0];
    assert_eq!(recorded.sst_id(), descriptor.id());
    assert!(
        recorded.stats().is_some() || table.descriptor().stats().is_none(),
        "stats should propagate when available"
    );
    assert!(
        recorded.wal_segments().is_none(),
        "no WAL segments recorded since none were attached"
    );
}
