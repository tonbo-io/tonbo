use std::ops::Bound;

use futures_util::stream::StreamExt;
use tonbo::{executor::tokio::TokioExecutor, tonbo_record, Projection, DB};

/// Use macro to define schema of column family just like ORM
/// It provides type-safe read & write API
#[tonbo_record]
pub struct User {
    #[primary_key]
    name: String,
    email: Option<String>,
    age: u8,
}

#[tokio::test]
async fn test_insert_and_get_user() {
    let db = DB::new("./db_path/test_users".into(), TokioExecutor::default())
        .await
        .unwrap();

    let user = User {
        name: "Bob".into(),
        email: Some("bob@gmail.com".into()),
        age: 30,
    };

    db.insert(user.clone()).await.unwrap();

    let txn = db.transaction().await;
    let fetched_user = txn.get(&user.name, Projection::All).await.unwrap().unwrap();

    assert_eq!(fetched_user.get().name, user.name);
    assert_eq!(fetched_user.get().email, user.email.as_deref());
    assert_eq!(fetched_user.get().age, Some(user.age));
}

#[tokio::test]
async fn test_range_scan() {
    let db = DB::new("./db_path/test_users".into(), TokioExecutor::default())
        .await
        .unwrap();

    let user1 = User {
        name: "Alice".into(),
        email: Some("alice@gmail.com".into()),
        age: 22,
    };

    let user2 = User {
        name: "Bob".into(),
        email: Some("bob@gmail.com".into()),
        age: 30,
    };

    db.insert(user1.clone()).await.unwrap();

    let txn = db.transaction().await;
    let mut scan = txn
        .scan((Bound::Included(&user1.name), Bound::Excluded(&user2.name)))
        .await
        .projection(vec![1])
        .limit(1)
        .take()
        .await
        .unwrap();

        while let Some(entry) = scan.next().await.transpose().unwrap() {
            let user_ref = entry.value();
            if user_ref.unwrap().name == "Alice" {
                assert_eq!(
                    user_ref,
                    Some(UserRef {
                        name: "Alice",
                        email: Some("alice@gmail.com"),
                        age: Some(22),
                    })
                );
            } else if user_ref.unwrap().name == "Bob" {
                assert_eq!(
                    user_ref,
                    Some(UserRef {
                        name: "Bob",
                        email: Some("bob@gmail.com"),
                        age: Some(30),
                    })
                );
            } else {
                panic!("Unexpected user: {:?}", user_ref);
            }
        }
}

#[tokio::test]
async fn test_update_user() {
    let db = DB::new("./db_path/test_users".into(), TokioExecutor::default())
        .await
        .unwrap();

    let user = User {
        name: "Charlie".into(),
        email: Some("charlie@gmail.com".into()),
        age: 25,
    };

    db.insert(user.clone()).await.unwrap();

    let updated_user = User {
        name: "Charlie".into(),
        email: Some("charlie_updated@gmail.com".into()),
        age: 26,
    };

    db.insert(updated_user.clone()).await.unwrap();

    let txn = db.transaction().await;
    let fetched_user = txn
        .get(&updated_user.name, Projection::All)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(fetched_user.get().name, updated_user.name);
    assert_eq!(fetched_user.get().email, updated_user.email.as_deref());
    assert_eq!(fetched_user.get().age, Some(updated_user.age));
}