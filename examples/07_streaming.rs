//! Streaming: process large datasets without loading everything into memory
//!
//! Run: cargo run --example 07_streaming

use futures::StreamExt;
use tonbo::{ColumnRef, Predicate, ScalarValue, db::DbBuilder};
use typed_arrow::{Record, prelude::*, schema::SchemaMeta};

#[derive(Record)]
struct LogEntry {
    id: i64,
    level: String,
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbBuilder::from_schema_key_name(LogEntry::schema(), "id")?
        .on_disk("/tmp/tonbo_streaming")?
        .open()
        .await?;

    // Insert a batch of log entries
    let entries: Vec<LogEntry> = (0..1000)
        .map(|i| LogEntry {
            id: i,
            level: match i % 3 {
                0 => "INFO".into(),
                1 => "WARN".into(),
                _ => "ERROR".into(),
            },
            message: format!("Log message #{}", i),
        })
        .collect();

    let mut builders = LogEntry::new_builders(entries.len());
    builders.append_rows(entries);
    db.ingest(builders.finish().into_record_batch()).await?;

    println!("Inserted 1000 log entries\n");

    // Method 1: collect() - loads all matching rows into memory
    // Good for small result sets
    println!("=== Method 1: collect() ===");
    let filter = Predicate::eq(ColumnRef::new("level"), ScalarValue::from("ERROR"));
    let batches = db.scan().filter(filter).collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "Collected {} ERROR entries in {} batches\n",
        total_rows,
        batches.len()
    );

    // Method 2: stream() - process batches one at a time
    // Good for large result sets or when you want to stop early
    println!("=== Method 2: stream() ===");
    let filter = Predicate::eq(ColumnRef::new("level"), ScalarValue::from("WARN"));
    let mut stream = db.scan().filter(filter).stream().await?;

    let mut batch_count = 0;
    let mut row_count = 0;
    while let Some(result) = stream.next().await {
        let batch = result?;
        batch_count += 1;
        row_count += batch.num_rows();
        println!("  Batch {}: {} rows", batch_count, batch.num_rows());
    }
    println!(
        "Streamed {} WARN entries in {} batches\n",
        row_count, batch_count
    );

    // Method 3: stream() with early termination
    // Process until you find what you need
    println!("=== Method 3: stream() with early exit ===");
    let filter = Predicate::eq(ColumnRef::new("level"), ScalarValue::from("INFO"));
    let mut stream = db.scan().filter(filter).stream().await?;

    let mut found_count = 0;
    let target = 5;
    'outer: while let Some(result) = stream.next().await {
        let batch = result?;
        for entry in batch.iter_views::<LogEntry>()?.try_flatten()? {
            println!("  Found: id={}, message={}", entry.id, entry.message);
            found_count += 1;
            if found_count >= target {
                println!("  (stopping after {} entries)", target);
                break 'outer;
            }
        }
    }

    // Method 4: stream() for aggregation without storing all data
    println!("\n=== Method 4: stream() for aggregation ===");
    let mut stream = db.scan().stream().await?;

    let mut info_count = 0;
    let mut warn_count = 0;
    let mut error_count = 0;

    while let Some(result) = stream.next().await {
        let batch = result?;
        for entry in batch.iter_views::<LogEntry>()?.try_flatten()? {
            match entry.level.as_ref() {
                "INFO" => info_count += 1,
                "WARN" => warn_count += 1,
                "ERROR" => error_count += 1,
                _ => {}
            }
        }
    }
    println!(
        "Log level counts: INFO={}, WARN={}, ERROR={}",
        info_count, warn_count, error_count
    );

    Ok(())
}
