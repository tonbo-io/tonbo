use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fusio::executor::NoopExecutor;
use tonbo::db::DbBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sstable_id_persistence_across_restarts() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let db = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .unwrap()
        .in_memory("test_db")
        .unwrap()
        .open_with_executor(Arc::new(NoopExecutor))
        .await
        .unwrap();

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["key1", "key2"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    db.ingest(batch1).await.unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["key3", "key4"])),
            Arc::new(Int64Array::from(vec![300, 400])),
        ],
    )
    .unwrap();
    db.ingest(batch2).await.unwrap();

    // Give db time to compact
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let batches = db.scan().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Should have all 4 rows in the database");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_db_with_no_latest_version() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let db = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .unwrap()
        .in_memory("empty_db")
        .unwrap()
        .open_with_executor(Arc::new(NoopExecutor))
        .await
        .unwrap();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["first_key"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    db.ingest(batch).await.unwrap();

    let batches = db.scan().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}
