//! Query the database created by Tonbo's `01_basic` example through DataFusion.
//!
//! Run the following commands from the workspace root:
//!
//! ```text
//! cargo run --example 01_basic
//! cargo run -p tonbo-datafusion --example datafusion
//! ```

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use tonbo::prelude::*;
use tonbo_datafusion::tonbo_table::TonboTable;

#[derive(Record)]
struct User {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    name: String,
    score: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `01_basic` creates this database and inserts the sample users.
    let db = DbBuilder::from_schema(User::schema())?
        .on_disk("/tmp/tonbo_example")?
        .open()
        .await?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "users",
        Arc::new(TonboTable::from(Arc::new(db), User::schema())),
    )?;

    let df = ctx
        .sql("SELECT id, name, score FROM users WHERE score > 80 ORDER BY id")
        .await?;

    df.show().await?;

    // Output:
    // +----+-------+-------+
    // | id | name  | score |
    // +----+-------+-------+
    // | u1 | Alice | 100   |
    // | u2 | Bob   | 85    |
    // +----+-------+-------+

    Ok(())
}
