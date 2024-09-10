use std::ops::Bound;

use futures_util::stream::StreamExt;
use tonbo::{executor::tokio::TokioExecutor, Projection, Record, DB};

/// Use macro to define schema of column family just like ORM
/// It provides type-safe read & write API
#[derive(Record, Debug)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
}

#[tokio::main]
async fn main() {
    // pluggable async runtime and I/O
    let db = DB::new("./db_path/users".into(), TokioExecutor::default())
        .await
        .unwrap();

    // insert with owned value
    db.insert(User {
        name: "Alice".into(),
        email: Some("alice@gmail.com".into()),
        age: 22,
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
                .projection(vec![1])
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
                        age: Some(22),
                    })
                );
            }
        }

        // commit transaction
        txn.commit().await.unwrap();
    }
}
