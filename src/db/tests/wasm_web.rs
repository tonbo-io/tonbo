//! Wasm/web integration tests for DB with S3 backend.

use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::web::WebExecutor, impls::remotes::aws::fs::AmazonS3, path::Path};
use futures::StreamExt;
use js_sys::Date;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use super::{AwsCreds, DB, ObjectSpec, S3Spec};
use crate::{
    inmem::policy::BatchesThreshold,
    mode::DynMode,
    mvcc::Timestamp,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableId, SsTableReader},
    schema::SchemaBuilder,
    wal::{WalSyncPolicy, frame::WalEvent, replay::Replayer},
};

wasm_bindgen_test_configure!(run_in_browser);

fn memory_s3_spec(prefix: String) -> S3Spec {
    let mut spec = S3Spec::new(
        "wasm-mock-bucket",
        prefix,
        AwsCreds::new("access", "secret"),
    );
    spec.endpoint = Some("memory://wasm-web".to_string());
    spec.region = Some("us-east-1".to_string());
    spec
}

#[wasm_bindgen_test]
async fn web_s3_roundtrip_wal_and_sstable() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let schema_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema config");

    let now_ms = Date::now() as u128;
    let prefix = format!("wasm-web-smoke-{now_ms}");
    let s3_spec = memory_s3_spec(prefix.clone());

    let exec = Arc::new(WebExecutor::new());
    let mut db: DB<DynMode, AmazonS3, WebExecutor> =
        DB::<DynMode, AmazonS3, WebExecutor>::builder(schema_cfg)
            .object_store(ObjectSpec::s3(s3_spec))
            .expect("object_store config")
            .wal_sync_policy(WalSyncPolicy::Always)
            .build_with_executor(Arc::clone(&exec))
            .await
            .expect("build web db");

    // Seal after every batch so immutables are flushed deterministically in tests.
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["alpha", "beta"])) as _,
            Arc::new(Int32Array::from(vec![1, 2])) as _,
        ],
    )
    .expect("batch");

    db.ingest(batch.clone()).await.expect("ingest");

    let wal_cfg = db.wal_config().cloned().expect("wal config present");
    let mut wal_events = Replayer::new(wal_cfg.clone())
        .scan()
        .await
        .expect("wal replay");
    assert!(
        wal_events
            .iter()
            .any(|event| matches!(event, WalEvent::DynAppend { .. })),
        "wal append should be visible"
    );

    // Flush immutables into an SST and read it back through the Parquet reader.
    let sst_root = Path::parse(format!("{}/sst", prefix)).expect("sst root path");
    let sst_cfg = Arc::new(SsTableConfig::new(
        Arc::clone(&schema),
        Arc::clone(&wal_cfg.segment_backend),
        sst_root,
    ));
    let descriptor = SsTableDescriptor::new(SsTableId::new(1), 0);
    let sstable = db
        .flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor)
        .await
        .expect("flush to sst");

    let reader = SsTableReader::<DynMode>::open(Arc::clone(&sst_cfg), sstable.descriptor().clone())
        .await
        .expect("open sstable reader");

    let mut stream = reader
        .into_stream(Timestamp::MAX, None)
        .await
        .expect("open stream");

    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("stream batch");
        assert!(batch.delete.is_none(), "no deletes expected");
        if batch.data.num_rows() == 0 {
            continue;
        }
        let ids = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string ids");
        let values = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int values");
        for idx in 0..ids.len() {
            rows.push((ids.value(idx).to_string(), values.value(idx)));
        }
    }

    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        rows,
        vec![("alpha".to_string(), 1), ("beta".to_string(), 2)]
    );
    assert!(
        wal_events
            .drain(..)
            .any(|event| matches!(event, WalEvent::TxnCommit { .. })),
        "wal should contain commit"
    );
}
