use tonbo_macros::Record;

#[derive(Record, Debug, PartialEq)]
pub struct User {
    email: Option<String>,
    age: u8,
    #[record(primary_key)]
    name: String,
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow::array::{BooleanArray, RecordBatch, StringArray, UInt32Array, UInt8Array};
    use fusio::Seek;
    use parquet::{
        arrow::{arrow_to_parquet_schema, ProjectionMask},
        format::SortingColumn,
        schema::types::ColumnPath,
    };
    use tonbo::{
        inmem::immutable::{ArrowArrays, Builder},
        record::{Record, RecordRef},
        serdes::{Decode, Encode},
        timestamp::timestamped::Timestamped,
    };

    use crate::{User, UserImmutableArrays, UserRef};

    #[tokio::test]
    async fn test_record_info() {
        let user = User {
            name: "cat".to_string(),
            email: Some("test@example.com".to_string()),
            age: 32,
        };

        assert_eq!(user.key(), "cat");
        assert_eq!(user.size(), 20);
        assert_eq!(User::primary_key_index(), 4);
        assert_eq!(
            User::primary_key_path(),
            (
                ColumnPath::new(vec!["_ts".to_string(), "name".to_string()]),
                vec![
                    SortingColumn::new(1, true, true),
                    SortingColumn::new(4, false, true),
                ],
            )
        );
    }

    #[tokio::test]
    async fn test_record_projection() {
        let user = User {
            name: "cat".to_string(),
            email: Some("test@example.com".to_string()),
            age: 32,
        };
        {
            let mut user_ref = user.as_record_ref();

            user_ref.projection(&ProjectionMask::roots(
                &arrow_to_parquet_schema(User::arrow_schema()).unwrap(),
                vec![2, 3],
            ));

            assert_eq!(user_ref.name, "cat");
            assert_eq!(user_ref.email, Some("test@example.com"));
            assert_eq!(user_ref.age, Some(32));
        }
        {
            let mut user_ref = user.as_record_ref();

            user_ref.projection(&ProjectionMask::roots(
                &arrow_to_parquet_schema(User::arrow_schema()).unwrap(),
                vec![],
            ));

            assert_eq!(user_ref.name, "cat");
            assert_eq!(user_ref.email, None);
            assert_eq!(user_ref.age, None);
        }
        {
            let mut user_ref = user.as_record_ref();

            user_ref.projection(&ProjectionMask::roots(
                &arrow_to_parquet_schema(User::arrow_schema()).unwrap(),
                vec![2],
            ));

            assert_eq!(user_ref.name, "cat");
            assert_eq!(user_ref.email, Some("test@example.com"));
            assert_eq!(user_ref.age, None);
        }
        {
            let mut user_ref = user.as_record_ref();

            user_ref.projection(&ProjectionMask::roots(
                &arrow_to_parquet_schema(User::arrow_schema()).unwrap(),
                vec![3],
            ));

            assert_eq!(user_ref.name, "cat");
            assert_eq!(user_ref.email, None);
            assert_eq!(user_ref.age, Some(32));
        }
    }

    #[tokio::test]
    async fn test_record_from_record_batch() {
        {
            let record_batch = RecordBatch::try_new(
                Arc::new(User::arrow_schema().project(&vec![0, 1, 2, 3, 4]).unwrap()),
                vec![
                    Arc::new(BooleanArray::from(vec![false])),
                    Arc::new(UInt32Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["test@example.com"])),
                    Arc::new(UInt8Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["cat"])),
                ],
            )
            .unwrap();

            let project_mask = ProjectionMask::roots(
                &arrow_to_parquet_schema(User::arrow_schema()).unwrap(),
                vec![0, 1, 2, 3, 4],
            );
            let record_ref = UserRef::from_record_batch(&record_batch, 0, &project_mask);
            assert_eq!(
                record_ref.value(),
                Timestamped {
                    ts: 9.into(),
                    value: "cat",
                }
            );
            if let Some(user_ref) = record_ref.get() {
                assert_eq!(user_ref.email, Some("test@example.com"));
                assert_eq!(user_ref.age, Some(9));
                assert_eq!(user_ref.name, "cat");
            } else {
                unreachable!();
            }
        }
        {
            let record_batch = RecordBatch::try_new(
                Arc::new(User::arrow_schema().project(&vec![0, 1, 3, 4]).unwrap()),
                vec![
                    Arc::new(BooleanArray::from(vec![false])),
                    Arc::new(UInt32Array::from(vec![9])),
                    Arc::new(UInt8Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["cat"])),
                ],
            )
            .unwrap();

            let project_mask = ProjectionMask::roots(
                &arrow_to_parquet_schema(User::arrow_schema()).unwrap(),
                vec![0, 1, 3, 4],
            );
            let record_ref = UserRef::from_record_batch(&record_batch, 0, &project_mask);
            assert_eq!(
                record_ref.value(),
                Timestamped {
                    ts: 9.into(),
                    value: "cat",
                }
            );
            if let Some(user_ref) = record_ref.get() {
                assert_eq!(user_ref.email, None);
                assert_eq!(user_ref.age, Some(9));
                assert_eq!(user_ref.name, "cat");
            } else {
                unreachable!();
            }
        }
    }

    #[tokio::test]
    async fn test_encode_and_decode() {
        let original = User {
            name: "cat".to_string(),
            email: Some("test@example.com".to_string()),
            age: 32,
        };
        let original_ref = original.as_record_ref();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        assert_eq!(original_ref.size(), 26);
        original_ref.encode(&mut cursor).await.unwrap();

        cursor.seek(0).await.unwrap();
        let decoded = User::decode(&mut cursor).await.unwrap();
        assert_eq!(original, decoded);
    }

    #[tokio::test]
    async fn test_record_arrays() {
        let mut builder = UserImmutableArrays::builder(User::arrow_schema(), 10);

        let cat = User {
            email: Some("cat@example.com".to_string()),
            age: 0,
            name: "cat".to_string(),
        };
        let dog = User {
            email: Some("dog@example.com".to_string()),
            age: 1,
            name: "dog".to_string(),
        };

        builder.push(
            Timestamped {
                ts: 0.into(),
                value: "cat",
            },
            Some(cat.as_record_ref()),
        );
        builder.push(
            Timestamped {
                ts: 1.into(),
                value: "dog",
            },
            Some(dog.as_record_ref()),
        );
        builder.push(
            Timestamped {
                ts: 2.into(),
                value: "human",
            },
            None,
        );

        assert_eq!(builder.written_size(), 57);

        let arrays = builder.finish(Some(&vec![0, 1, 2, 3, 4]));

        assert_eq!(
            arrays.as_record_batch(),
            &RecordBatch::try_new(
                Arc::new(User::arrow_schema().project(&vec![0, 1, 2, 3, 4]).unwrap(),),
                vec![
                    Arc::new(BooleanArray::from(vec![false, false, true])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2])),
                    Arc::new(StringArray::from(vec![
                        Some("cat@example.com"),
                        Some("dog@example.com"),
                        None
                    ])),
                    Arc::new(UInt8Array::from(vec![0, 1, 0])),
                    Arc::new(StringArray::from(vec!["cat", "dog", "human"])),
                ],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_record_arrays_projection() {
        let mut builder = UserImmutableArrays::builder(User::arrow_schema(), 10);

        let cat = User {
            email: Some("cat@example.com".to_string()),
            age: 0,
            name: "cat".to_string(),
        };
        let dog = User {
            email: Some("dog@example.com".to_string()),
            age: 1,
            name: "dog".to_string(),
        };

        builder.push(
            Timestamped {
                ts: 0.into(),
                value: "cat",
            },
            Some(cat.as_record_ref()),
        );
        builder.push(
            Timestamped {
                ts: 1.into(),
                value: "dog",
            },
            Some(dog.as_record_ref()),
        );
        builder.push(
            Timestamped {
                ts: 2.into(),
                value: "human",
            },
            None,
        );

        assert_eq!(builder.written_size(), 57);

        let arrays = builder.finish(Some(&vec![0, 1, 3, 4]));

        assert_eq!(
            arrays.as_record_batch(),
            &RecordBatch::try_new(
                Arc::new(User::arrow_schema().project(&vec![0, 1, 3, 4]).unwrap(),),
                vec![
                    Arc::new(BooleanArray::from(vec![false, false, true])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2])),
                    Arc::new(UInt8Array::from(vec![0, 1, 0])),
                    Arc::new(StringArray::from(vec!["cat", "dog", "human"])),
                ],
            )
            .unwrap()
        );
    }
}
