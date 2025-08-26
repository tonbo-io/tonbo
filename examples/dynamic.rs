use std::{fs, path::PathBuf};

use fusio::path::Path;
use tonbo::{
    dyn_schema,
    executor::tokio::TokioExecutor,
    record::{AsValue, DynRecord, Value},
    DbOption, DB,
};

#[tokio::main]
async fn main() {
    fs::create_dir_all("./db_path/users").unwrap();

    let schema = dyn_schema!(("foo", Utf8, false), ("bar", Int32, true), [0]);

    let options = DbOption::new(
        Path::from_filesystem_path(fs::canonicalize(PathBuf::from("./db_path/users")).unwrap())
            .unwrap(),
        &schema,
    );
    let db = DB::new(options, TokioExecutor::default(), schema)
        .await
        .unwrap();

    {
        let mut txn = db.transaction().await;
        let record = DynRecord::new(
            vec![Value::String("hello".to_owned()), Value::Int32(1)],
            vec![0],
        );
        txn.insert(record);

        txn.commit().await.unwrap();
    }

    db.get(&Value::String("hello".to_owned()), |v| {
        let v = v.get();
        println!("{:?}", v.columns[0].as_string());
        Some(())
    })
    .await
    .unwrap();
}
