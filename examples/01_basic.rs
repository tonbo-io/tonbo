//! Basic Tonbo example: define schema, insert, query
//!
//! Run: cargo run --example 01_basic

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
    // 1. Open database (key detected from schema metadata)
    let db = DbBuilder::from_schema(User::schema())?
        .on_disk("/tmp/tonbo_example")?
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
    let filter = Expr::gt("score", ScalarValue::from(80_i64));
    let batches = db.scan().filter(filter).collect().await?;

    println!("Users with score > 80:");
    for batch in &batches {
        for user in batch.iter_views::<User>()?.try_flatten()? {
            println!("  {} - {} ({:?})", user.id, user.name, user.score);
        }
    }

    Ok(())
}
