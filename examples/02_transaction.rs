//! Transactions: upsert, delete, read-your-writes, commit
//!
//! Run: cargo run --example 02_transaction

use tonbo::prelude::*;

#[derive(Record)]
struct User {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    name: String,
    score: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbBuilder::from_schema(User::schema())?
        .on_disk("/tmp/tonbo_tx_example")?
        .open()
        .await?;

    // Insert initial data
    let users = vec![
        User {
            id: "u1".into(),
            name: "Alice".into(),
            score: Some(100),
        },
        User {
            id: "u2".into(),
            name: "Bob".into(),
            score: Some(85),
        },
    ];
    let mut builders = User::new_builders(users.len());
    builders.append_rows(users);
    db.ingest(builders.finish().into_record_batch()).await?;

    // Begin transaction
    let mut tx = db.begin_transaction().await?;

    // Upsert: update Alice's score, add Carol
    let updates = vec![
        User {
            id: "u1".into(),
            name: "Alice".into(),
            score: Some(150),
        },
        User {
            id: "u3".into(),
            name: "Carol".into(),
            score: Some(90),
        },
    ];
    let mut builders = User::new_builders(updates.len());
    builders.append_rows(updates);
    tx.upsert_batch(&builders.finish().into_record_batch())?;

    // Delete Bob
    tx.delete("u2")?;

    // Read-your-writes: see uncommitted changes within the transaction
    let filter = Predicate::is_not_null(ColumnRef::new("id"));
    let preview = tx.scan().filter(filter).collect().await?;

    println!("Before commit (read-your-writes):");
    for batch in &preview {
        for user in batch.iter_views::<User>()?.try_flatten()? {
            println!("  {} - {} ({:?})", user.id, user.name, user.score);
        }
    }

    // Commit
    tx.commit().await?;

    // Verify after commit
    let filter = Predicate::is_not_null(ColumnRef::new("id"));
    let committed = db.scan().filter(filter).collect().await?;

    println!("\nAfter commit:");
    for batch in &committed {
        for user in batch.iter_views::<User>()?.try_flatten()? {
            println!("  {} - {} ({:?})", user.id, user.name, user.score);
        }
    }

    Ok(())
}
