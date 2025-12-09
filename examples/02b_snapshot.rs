//! Snapshots: read-only consistent view of the database
//!
//! Run: cargo run --example 02b_snapshot

use tonbo::{
    db::DbBuilder,
    query::{ColumnRef, Predicate},
};
use typed_arrow::{Record, prelude::*, schema::SchemaMeta};

#[derive(Record)]
struct User {
    id: String,
    name: String,
    score: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbBuilder::from_schema_key_name(User::schema(), "id")?
        .on_disk("/tmp/tonbo_snapshot_example")?
        .create_dirs(true)
        .open()
        .await?;

    // Insert initial data
    let users = vec![User {
        id: "u1".into(),
        name: "Alice".into(),
        score: Some(100),
    }];
    let mut builders = User::new_builders(users.len());
    builders.append_rows(users);
    db.ingest(builders.finish().into_record_batch()).await?;

    // Take a snapshot (read-only, consistent view)
    let snapshot = db.begin_snapshot().await?;

    // Insert more data after snapshot
    let more = vec![User {
        id: "u2".into(),
        name: "Bob".into(),
        score: Some(85),
    }];
    let mut builders = User::new_builders(more.len());
    builders.append_rows(more);
    db.ingest(builders.finish().into_record_batch()).await?;

    // Snapshot sees only data at snapshot time
    let filter = Predicate::is_not_null(ColumnRef::new("id"));
    let snapshot_data = snapshot.scan(&db).filter(filter.clone()).collect().await?;

    println!("Snapshot (frozen in time):");
    for batch in &snapshot_data {
        for user in batch.iter_views::<User>()?.try_flatten()? {
            println!("  {} - {}", user.id, user.name);
        }
    }

    // Current DB sees all data
    let current_data = db.scan().filter(filter).collect().await?;

    println!("\nCurrent DB:");
    for batch in &current_data {
        for user in batch.iter_views::<User>()?.try_flatten()? {
            println!("  {} - {}", user.id, user.name);
        }
    }

    Ok(())
}
