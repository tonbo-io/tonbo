//! Basic Tonbo example: define schema, insert, query
//!
//! Run: cargo run --example 01_basic

use tonbo::{
    db::DbBuilder,
    query::{ColumnRef, Predicate, ScalarValue},
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
    // 1. Open database
    let db = DbBuilder::from_schema_key_name(User::schema(), "id")?
        .on_disk("/tmp/tonbo_example")?
        .create_dirs(true)
        .open()
        .await?;

    // 2. Insert data
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
        User {
            id: "u3".into(),
            name: "Carol".into(),
            score: None,
        },
    ];

    let mut builders = User::new_builders(users.len());
    builders.append_rows(users);
    db.ingest(builders.finish().into_record_batch()).await?;

    // 3. Query: score > 80
    let filter = Predicate::gt(ColumnRef::new("score"), ScalarValue::from(80_i64));
    let batches = db.scan().filter(filter).collect().await?;

    println!("Users with score > 80:");
    for batch in &batches {
        for user in batch.iter_views::<User>()?.try_flatten()? {
            println!("  {} - {} ({:?})", user.id, user.name, user.score);
        }
    }

    Ok(())
}
