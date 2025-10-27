use tonbo::{executor::tokio::TokioExecutor, typed as t, DbOption, Path, Projection, DB};

#[t::record(key(id, name))]
#[derive(Debug, Default)]
pub struct User {
    #[record(primary_key)]
    id: u64,
    #[record(primary_key)]
    name: String,
    age: u8,
}

#[tokio::main]
async fn main() {
    // Prepare a local directory for the DB
    let base = "/tmp/db_path/people";
    let _ = tokio::fs::create_dir_all(base).await;

    // Open the DB using the generated `UserSchema`
    let schema = UserSchema::default();
    let options = DbOption::new(Path::from_filesystem_path(base).unwrap(), &schema);
    let db: DB<User, TokioExecutor> = DB::new(options, TokioExecutor::default(), schema)
        .await
        .unwrap();

    db.insert(User {
        id: 1,
        name: "Alice".into(),
        age: 25,
    })
    .await
    .unwrap();
    // Get by primary key
    let txn = db.transaction().await;
    let key = UserKey {
        id: 1,
        name: "Alice".into(),
    };
    let user = txn.get(&key, Projection::All).await.unwrap();
    assert!(user.is_some());
    assert_eq!(
        user.unwrap().get(),
        UserRef {
            id: 1,
            name: "Alice",
            age: Some(25),
        }
    );
}
