use std::{fs, sync::Arc};

use fusio::path::Path;
use tonbo::{
    executor::tokio::TokioExecutor,
    record::{DataType, DynRecord, DynSchema, Value, ValueDesc},
    DbOption, DB,
};

#[tokio::main]
async fn main() {
    fs::create_dir_all("./db_path/users").unwrap();

    let schema = DynSchema::new(
        vec![
            ValueDesc::new("foo".into(), DataType::String, false),
            ValueDesc::new("bar".into(), DataType::Int32, true),
        ],
        0,
    );

    let options = DbOption::new(
        Path::from_filesystem_path("./db_path/users").unwrap(),
        &schema,
    );
    let db = DB::new(options, TokioExecutor::current(), schema)
        .await
        .unwrap();

    {
        let mut txn = db.transaction().await;
        txn.insert(DynRecord::new(
            vec![
                Value::new(
                    DataType::String,
                    "foo".into(),
                    Arc::new("hello".to_owned()),
                    false,
                ),
                Value::new(DataType::Int32, "bar".into(), Arc::new(1), true),
            ],
            0,
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
