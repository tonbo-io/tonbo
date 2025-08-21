use std::{ops::Bound, path::PathBuf};

use fusio::path::Path;
use futures_util::stream::StreamExt;
use tokio::fs;
use tonbo::{executor::tokio::TokioExecutor, typed as t, DbOption, Projection, DB};

/// Use macro to define schema of column family just like ORM
/// It provides type-safe read & write API
#[t::record]
#[derive(Debug, Default)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
    bytes: Vec<u8>,
    grade: f32,
}

#[tokio::main]
async fn main() {
    // make sure the path exists
    let _ = fs::create_dir_all("./db_path/users").await;

    let schema: UserSchema = Default::default();
    let options = DbOption::new(
        Path::from_filesystem_path(
            fs::canonicalize(PathBuf::from("./db_path/users"))
                .await
                .unwrap(),
        )
        .unwrap(),
        &schema,
    );
    // pluggable async runtime and I/O
    let db = DB::new(options, TokioExecutor::default(), schema)
        .await
        .unwrap();

    // insert with owned value
    db.insert(User {
        name: "Alice".into(),
        email: Some("alice@gmail.com".into()),
        age: 22,
        bytes: vec![0, 1, 2],
        grade: 96.5,
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
                .projection(&["email", "bytes", "grade"])
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
                        grade: Some(96.5),
                    })
                );
            }
        }

        {
            let upper = "Blob".into();
            // reverse scan of user (descending order)
            let mut reverse_scan = txn
                .scan((Bound::Included(&name), Bound::Excluded(&upper)))
                .reverse() // scan in descending order
                .projection(&["name", "grade"]) // tonbo supports combining reverse with projection
                .limit(10) // and with limits
                .take()
                .await
                .unwrap();

            println!("Users in reverse order:");
            while let Some(entry) = reverse_scan.next().await.transpose().unwrap() {
                if let Some(user) = entry.value() {
                    println!("- {}: {:?}", user.name, user.grade.unwrap_or(0.0.into()));
                }
            }
        }

        // commit transaction
        txn.commit().await.unwrap();
    }
}
