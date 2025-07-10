use std::{fs, sync::Arc};

use fusio::path::Path;
use tonbo::{
    dyn_record, dyn_schema, executor::tokio::TokioExecutor, AsValue, DbOption, PrimaryKey, DB,
};

#[tokio::main]
async fn main() {
    fs::create_dir_all("./db_path/users").unwrap();

    let schema = dyn_schema!(("foo", String, false), ("bar", Int32, true), 0);

    let options = DbOption::new(
        Path::from_filesystem_path("./db_path/users").unwrap(),
        &schema,
    );
    let db = DB::new(options, TokioExecutor::current(), schema)
        .await
        .unwrap();

    {
        let mut txn = db.transaction().await;
        txn.insert(dyn_record!(
            ("foo", String, false, Arc::new("hello".to_owned())),
            ("bar", Int32, true, Arc::new(1)),
            0
        ));

        txn.commit().await.unwrap();
    }

    let key = PrimaryKey::new(vec![Arc::new("hello".to_owned())]);
    db.get(&key, |v| {
        let v = v.get();
        println!("{:?}", v.columns[0].as_string());
        Some(())
    })
    .await
    .unwrap();
}
