use std::ops::Bound;

use bytes::Bytes;
use fusio::path::Path;
use futures_util::stream::StreamExt;
use tokio::fs;
use tonbo::{executor::tokio::TokioExecutor, DbOption, Projection, Record, DB};

/// Use macro to define schema of column family just like ORM
/// It provides type-safe read & write API
#[derive(Record, Debug)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
    bytes: Bytes,
}

#[tokio::main]
async fn main() {
    // make sure the path exists
    let _ = fs::create_dir_all("./db_path/users").await;

    let options = DbOption::from((
        Path::from_filesystem_path("./db_path/users").unwrap(),
        &UserSchema,
    ));
    // pluggable async runtime and I/O
    let db = DB::new(options, TokioExecutor::default(), UserSchema)
        .await
        .unwrap();

    // insert with owned value
    db.insert(User {
        name: "Alice".into(),
        email: Some("alice@gmail.com".into()),
        age: 22,
        bytes: Bytes::from(vec![0, 1, 2]),
    })
    .await
    .unwrap();

    {
        // tonbo supports transaction
        let txn = db.transaction().await;

        // get from primary key
        let name = "Alice".into();

        // get the zero-copy reference of record without any allocations.
        let user = txn
            .get(
                &name,
                // tonbo supports pushing down projection
                Projection::All,
            )
            .await
            .unwrap();
        assert!(user.is_some());
        assert_eq!(user.unwrap().get().age, Some(22));

        {
            let upper = "Blob".into();
            // range scan of user
            let mut scan = txn
                .scan((Bound::Included(&name), Bound::Excluded(&upper)))
                // tonbo supports pushing down projection
                .projection(vec![1, 3])
                // push down limitation
                .limit(1)
                .take()
                .await
                .unwrap();
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                assert_eq!(
                    entry.value(),
                    Some(UserRef {
                        name: "Alice",
                        email: Some("alice@gmail.com"),
                        age: None,
                        bytes: Some(&[0, 1, 2]),
                    })
                );
            }
        }

        // commit transaction
        txn.commit().await.unwrap();
    }
}
