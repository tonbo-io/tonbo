use std::ops::Bound;

use fusio::path::Path;
use futures_util::StreamExt;
use tonbo::{executor::tokio::TokioExecutor, typed as t, DbOption, Projection, DB};

#[t::record]
#[derive(Debug, Clone, Default)]
pub struct Person {
    #[record(primary_key)]
    id: i64,
    name: String,
    age: Option<i16>,
}

#[tokio::main]
async fn main() {
    // Prepare a local directory for the DB
    let base = "/tmp/db_path/people";
    let _ = tokio::fs::create_dir_all(base).await;

    // Open the DB using the generated `PersonSchema`
    let schema: PersonSchema = Default::default();
    let options = DbOption::new(Path::from_filesystem_path(base).unwrap(), &schema);
    let db: DB<Person, TokioExecutor> = DB::new(options, TokioExecutor::default(), schema)
        .await
        .unwrap();

    // Insert a couple of rows
    db.insert(Person {
        id: 1,
        name: "Alice".into(),
        age: Some(30),
    })
    .await
    .unwrap();
    db.insert(Person {
        id: 2,
        name: "Bob".into(),
        age: None,
    })
    .await
    .unwrap();

    // Get by primary key
    {
        let txn = db.transaction().await;
        let got = txn
            .get(&PersonKey { id: 1 }, Projection::All)
            .await
            .unwrap();
        println!("get(1): {:?}", got.as_ref().map(|e| e.get()));
    }

    // Range scan with projection (only `name`)
    {
        let txn = db.transaction().await;
        let mut scan = txn
            .scan((Bound::Unbounded, Bound::Unbounded))
            .projection(&["name"])
            .take()
            .await
            .unwrap();
        while let Some(entry) = scan.next().await.transpose().unwrap() {
            println!("scan -> {:?}", entry.value());
        }
    }

    // Remove a row
    db.remove(PersonKey { id: 2 }).await.unwrap();
}
