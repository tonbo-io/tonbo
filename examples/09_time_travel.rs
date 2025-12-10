//! Time Travel: list versions and create snapshots at specific timestamps
//!
//! This example demonstrates the time travel API:
//! - `db.list_versions(limit)` - enumerate committed versions
//! - `db.snapshot_at(timestamp)` - create a snapshot at a specific timestamp
//!
//! Tonbo supports two levels of time travel:
//! 1. **MVCC timestamps** - every commit gets a logical timestamp for visibility control
//! 2. **Manifest versions** - when data is flushed to SST files, a version snapshot is recorded in
//!    the manifest, enabling queries against historical file sets
//!
//! Run: cargo run --example 09_time_travel

use tonbo::prelude::*;

#[derive(Record)]
struct Product {
    #[metadata(k = "tonbo.key", v = "true")]
    id: i64,
    name: String,
    price: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbBuilder::from_schema(Product::schema())?
        .on_disk("/tmp/tonbo_time_travel")?
        .open()
        .await?;

    // === Insert data in multiple transactions ===
    // Each transaction gets a unique MVCC timestamp

    // Transaction 1: Initial products
    let products = vec![
        Product {
            id: 1,
            name: "Laptop".into(),
            price: 999,
        },
        Product {
            id: 2,
            name: "Mouse".into(),
            price: 29,
        },
    ];
    let mut builders = Product::new_builders(products.len());
    builders.append_rows(products);
    db.ingest(builders.finish().into_record_batch()).await?;
    println!("Tx1: Inserted Laptop ($999), Mouse ($29)");

    // Transaction 2: Price update
    let mut tx = db.begin_transaction().await?;
    let update = vec![Product {
        id: 1,
        name: "Laptop".into(),
        price: 899,
    }];
    let mut builders = Product::new_builders(update.len());
    builders.append_rows(update);
    tx.upsert_batch(&builders.finish().into_record_batch())?;
    tx.commit().await?;
    println!("Tx2: Laptop price reduced to $899");

    // Transaction 3: New product
    let mut tx = db.begin_transaction().await?;
    let update = vec![Product {
        id: 3,
        name: "Keyboard".into(),
        price: 79,
    }];
    let mut builders = Product::new_builders(update.len());
    builders.append_rows(update);
    tx.upsert_batch(&builders.finish().into_record_batch())?;
    tx.commit().await?;
    println!("Tx3: Added Keyboard ($79)");

    // === List persisted versions ===
    // Versions are created when data is flushed to SST files
    println!("\n=== Persisted Versions (from manifest) ===");
    let versions = db.list_versions(10).await?;
    if versions.is_empty() {
        println!("  (no SST versions yet - data is in memory)");
    } else {
        for (i, v) in versions.iter().enumerate() {
            println!(
                "  Version {}: timestamp={}, ssts={}, levels={}",
                versions.len() - i,
                v.timestamp.get(),
                v.sst_count,
                v.level_count
            );
        }
    }

    // === Query current state ===
    println!("\n=== Current State ===");
    let batches = db.scan().collect().await?;
    for batch in &batches {
        for product in batch.iter_views::<Product>()?.try_flatten()? {
            println!("  {} - {} (${})", product.id, product.name, product.price);
        }
    }

    // === Snapshot at specific MVCC timestamp ===
    // Query using timestamps from list_versions for reliable time travel
    if let Some(first_version) = versions.last() {
        println!(
            "\n=== Snapshot at timestamp={} (first version) ===",
            first_version.timestamp.get()
        );
        let snapshot = db.snapshot_at(first_version.timestamp).await?;
        let batches = snapshot.scan(&db).collect().await?;
        for batch in &batches {
            for product in batch.iter_views::<Product>()?.try_flatten()? {
                println!("  {} - {} (${})", product.id, product.name, product.price);
            }
        }
    }

    if let Some(latest_version) = versions.first() {
        println!(
            "\n=== Snapshot at timestamp={} (latest version) ===",
            latest_version.timestamp.get()
        );
        let snapshot = db.snapshot_at(latest_version.timestamp).await?;
        let batches = snapshot.scan(&db).collect().await?;
        for batch in &batches {
            for product in batch.iter_views::<Product>()?.try_flatten()? {
                println!("  {} - {} (${})", product.id, product.name, product.price);
            }
        }
    }

    // Current snapshot (includes in-memory data)
    println!("\n=== Current State (begin_snapshot) ===");
    let snapshot = db.begin_snapshot().await?;
    let batches = snapshot.scan(&db).collect().await?;
    for batch in &batches {
        for product in batch.iter_views::<Product>()?.try_flatten()? {
            println!("  {} - {} (${})", product.id, product.name, product.price);
        }
    }

    Ok(())
}
