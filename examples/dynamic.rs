use std::{fs, sync::Arc};

use arrow::datatypes::{DataType as ArrowDataType, Field};
use fusio::path::Path;
use tonbo::{
    dyn_record,
    executor::tokio::TokioExecutor,
    record::{DataType, Schema, Value},
    DbOption, DB,
};

#[tokio::main]
async fn main() {
    fs::create_dir_all("./db_path/users").unwrap();

    let schema = Schema::new(
        vec![
            Field::new("foo", ArrowDataType::Utf8, false),
            Field::new("bar", ArrowDataType::Int32, true),
        ],
        0,
    );

    let options = DbOption::new(Path::from_filesystem_path("./db_path/users").unwrap());

    let db = DB::new(options, TokioExecutor::current(), schema)
        .await
        .unwrap();

    {
        let mut txn = db.transaction().await;
        txn.insert(dyn_record!(
            ("foo", String, false, "hello".to_owned()),
            ("bar", Int32, true, 1),
            0
        ));

        txn.commit().await.unwrap();
    }

    db.get(
        &Value::new(
            DataType::String,
            "foo".into(),
            Arc::new("hello".to_owned()),
            false,
        ),
        |v| {
            let v = v.get();
            println!("{:?}", v.columns[0].value.downcast_ref::<String>());
            Some(())
        },
    )
    .await
    .unwrap();
}
