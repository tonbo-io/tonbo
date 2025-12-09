//! Composite Keys: multi-column primary keys for time-series and partitioned data
//!
//! Run: cargo run --example 06_composite_key

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use tonbo::{
    db::DbBuilder,
    query::{ColumnRef, Predicate, ScalarValue},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define schema with composite key: (device_id, timestamp)
    // This is common for time-series data
    let schema = Arc::new(Schema::new(vec![
        Field::new("device_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("temperature", DataType::Float64, true),
        Field::new("humidity", DataType::Float64, true),
    ]));

    // Create DB with composite key using column indices [0, 1]
    let db = DbBuilder::from_schema_key_indices(schema.clone(), vec![0, 1])?
        .on_disk("/tmp/tonbo_composite_key")?
        .create_dirs(true)
        .open()
        .await?;

    // Insert time-series data
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::StringArray::from(vec![
                "sensor-1", "sensor-1", "sensor-1", "sensor-2", "sensor-2",
            ])),
            Arc::new(arrow_array::Int64Array::from(vec![
                1000, 2000, 3000, // sensor-1 timestamps
                1500, 2500, // sensor-2 timestamps
            ])),
            Arc::new(arrow_array::Float64Array::from(vec![
                22.5, 23.0, 22.8, 25.0, 25.5,
            ])),
            Arc::new(arrow_array::Float64Array::from(vec![
                45.0, 46.0, 44.5, 50.0, 51.0,
            ])),
        ],
    )?;

    db.ingest(batch).await?;
    println!("Inserted 5 sensor readings with composite key (device_id, timestamp)");

    // Query all data - results are ordered by composite key
    println!("\nAll readings (ordered by device_id, then timestamp):");
    let batches = db.scan().collect().await?;
    print_readings(&batches)?;

    // Filter by first key component: device_id = 'sensor-1'
    println!("\nReadings for sensor-1:");
    let filter = Predicate::eq(ColumnRef::new("device_id"), ScalarValue::from("sensor-1"));
    let batches = db.scan().filter(filter).collect().await?;
    print_readings(&batches)?;

    // Filter by second key component: timestamp > 2000
    println!("\nReadings after timestamp 2000:");
    let filter = Predicate::gt(ColumnRef::new("timestamp"), ScalarValue::from(2000_i64));
    let batches = db.scan().filter(filter).collect().await?;
    print_readings(&batches)?;

    // Combined filter on both key components
    println!("\nSensor-1 readings after timestamp 1500:");
    let filter = Predicate::and(vec![
        Predicate::eq(ColumnRef::new("device_id"), ScalarValue::from("sensor-1")),
        Predicate::gt(ColumnRef::new("timestamp"), ScalarValue::from(1500_i64)),
    ]);
    let batches = db.scan().filter(filter).collect().await?;
    print_readings(&batches)?;

    // Upsert: update existing key, insert new key
    println!("\nUpserting (sensor-1, 2000) and new (sensor-3, 1000):");
    let mut tx = db.begin_transaction().await?;

    let update_batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::StringArray::from(vec!["sensor-1", "sensor-3"])),
            Arc::new(arrow_array::Int64Array::from(vec![2000, 1000])),
            Arc::new(arrow_array::Float64Array::from(vec![99.9, 20.0])),
            Arc::new(arrow_array::Float64Array::from(vec![99.9, 40.0])),
        ],
    )?;
    tx.upsert_batch(&update_batch)?;
    tx.commit().await?;

    let batches = db.scan().collect().await?;
    print_readings(&batches)?;

    Ok(())
}

fn print_readings(batches: &[arrow_array::RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    use arrow_array::Array;

    for batch in batches {
        let devices = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let temps = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        let humidity = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            println!(
                "  ({}, {}) -> temp={:.1}, humidity={:.1}",
                devices.value(i),
                timestamps.value(i),
                temps.value(i),
                humidity.value(i),
            );
        }
    }
    Ok(())
}
