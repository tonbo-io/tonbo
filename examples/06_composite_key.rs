//! Composite Keys: multi-column primary keys for time-series and partitioned data
//!
//! Run: cargo run --example 06_composite_key

use tonbo::{
    ColumnRef, Predicate, ScalarValue,
    db::DbBuilder,
    typed_arrow::{Record, prelude::*, schema::SchemaMeta},
};

// Define schema with composite key: (device_id, timestamp)
// Use ordinal values in metadata for composite key ordering
#[derive(Record)]
struct SensorReading {
    #[metadata(k = "tonbo.key", v = "0")]
    device_id: String,
    #[metadata(k = "tonbo.key", v = "1")]
    timestamp: i64,
    temperature: Option<f64>,
    humidity: Option<f64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create DB with composite key detected from schema metadata
    let db = DbBuilder::from_schema(SensorReading::schema())?
        .on_disk("/tmp/tonbo_composite_key")?
        .open()
        .await?;

    // Insert time-series data
    let readings = vec![
        SensorReading {
            device_id: "sensor-1".into(),
            timestamp: 1000,
            temperature: Some(22.5),
            humidity: Some(45.0),
        },
        SensorReading {
            device_id: "sensor-1".into(),
            timestamp: 2000,
            temperature: Some(23.0),
            humidity: Some(46.0),
        },
        SensorReading {
            device_id: "sensor-1".into(),
            timestamp: 3000,
            temperature: Some(22.8),
            humidity: Some(44.5),
        },
        SensorReading {
            device_id: "sensor-2".into(),
            timestamp: 1500,
            temperature: Some(25.0),
            humidity: Some(50.0),
        },
        SensorReading {
            device_id: "sensor-2".into(),
            timestamp: 2500,
            temperature: Some(25.5),
            humidity: Some(51.0),
        },
    ];

    let mut builders = SensorReading::new_builders(readings.len());
    builders.append_rows(readings);
    db.ingest(builders.finish().into_record_batch()).await?;
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

    let updates = vec![
        SensorReading {
            device_id: "sensor-1".into(),
            timestamp: 2000,
            temperature: Some(99.9),
            humidity: Some(99.9),
        },
        SensorReading {
            device_id: "sensor-3".into(),
            timestamp: 1000,
            temperature: Some(20.0),
            humidity: Some(40.0),
        },
    ];
    let mut builders = SensorReading::new_builders(updates.len());
    builders.append_rows(updates);
    tx.upsert_batch(&builders.finish().into_record_batch())?;
    tx.commit().await?;

    let batches = db.scan().collect().await?;
    print_readings(&batches)?;

    Ok(())
}

fn print_readings(batches: &[arrow_array::RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    for batch in batches {
        for r in batch.iter_views::<SensorReading>()?.try_flatten()? {
            let temp = r
                .temperature
                .map(|t| format!("{:.1}", t))
                .unwrap_or("N/A".into());
            let hum = r
                .humidity
                .map(|h| format!("{:.1}", h))
                .unwrap_or("N/A".into());
            println!(
                "  ({}, {}) -> temp={}, humidity={}",
                r.device_id, r.timestamp, temp, hum
            );
        }
    }
    Ok(())
}
