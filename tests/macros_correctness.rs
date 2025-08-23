use tonbo::typed as t;

#[t::record]
#[derive(Debug, PartialEq, Default)]
pub struct User {
    email: Option<String>,
    age: u8,
    #[record(primary_key)]
    name: String,
    grade: f32,
}

#[t::record]
#[derive(Debug, PartialEq, Default)]
pub struct Point {
    #[record(primary_key)]
    id: u64,
    x: i32,
    y: i32,
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow::array::{
        BooleanArray, Float32Array, RecordBatch, StringArray, UInt32Array, UInt8Array,
    };
    use fusio_log::{Decode, Encode};
    use parquet::{
        arrow::{ArrowSchemaConverter, ProjectionMask},
        format::SortingColumn,
        schema::types::ColumnPath,
    };
    use tokio::io::AsyncSeekExt;
    use tonbo::{
        record::{Record, RecordRef, Schema},
        ArrowArrays, ArrowArraysBuilder, Ts, TS,
    };

    use crate::{Point, User, UserImmutableArrays, UserKeyRef, UserRef, UserSchema};

    #[tokio::test]
    async fn test_record_info() {
        let user = User {
            name: "cat".to_string(),
            email: Some("test@example.com".to_string()),
            age: 32,
            grade: 92.9,
        };

        assert_eq!(user.key(), UserKeyRef { name: "cat" });
        assert_eq!(user.size(), 24);
        let schema: UserSchema = Default::default();
        assert_eq!(schema.primary_key_indices()[0], 4);
        let (paths, sorting) = schema.primary_key_paths_and_sorting();
        assert_eq!(
            paths,
            &[ColumnPath::new(vec![TS.to_string(), "name".to_string()])]
        );
        assert_eq!(
            sorting,
            &[
                SortingColumn::new(1, true, true),
                SortingColumn::new(4, false, true)
            ]
        );
    }

    #[tokio::test]
    async fn test_record_projection() {
        let user = User {
            name: "cat".to_string(),
            email: Some("test@example.com".to_string()),
            age: 32,
            grade: 92.9,
        };
        {
            let mut user_ref = user.as_record_ref();

            let schema: UserSchema = Default::default();
            user_ref.projection(&ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![2, 3],
            ));

            assert_eq!(user_ref.name, "cat");
            assert_eq!(user_ref.email, Some("test@example.com"));
            assert_eq!(user_ref.age, Some(32));
        }
        {
            let mut user_ref = user.as_record_ref();

            let schema: UserSchema = Default::default();
            user_ref.projection(&ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![],
            ));

            assert_eq!(user_ref.name, "cat");
            assert_eq!(user_ref.email, None);
            assert_eq!(user_ref.age, None);
        }
        {
            let mut user_ref = user.as_record_ref();

            let schema: UserSchema = Default::default();
            user_ref.projection(&ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![2],
            ));

            assert_eq!(user_ref.name, "cat");
            assert_eq!(user_ref.email, Some("test@example.com"));
            assert_eq!(user_ref.age, None);
        }
        {
            let mut user_ref = user.as_record_ref();

            let schema: UserSchema = Default::default();
            user_ref.projection(&ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
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
            let schema: UserSchema = Default::default();
            let record_batch = RecordBatch::try_new(
                Arc::new(schema.arrow_schema().project(&[0, 1, 2, 3, 4, 5]).unwrap()),
                vec![
                    Arc::new(BooleanArray::from(vec![false])),
                    Arc::new(UInt32Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["test@example.com"])),
                    Arc::new(UInt8Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["cat"])),
                    Arc::new(Float32Array::from(vec![1.2])),
                ],
            )
            .unwrap();

            let project_mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![0, 1, 2, 3, 4, 5],
            );
            let record_ref =
                UserRef::from_record_batch(&record_batch, 0, &project_mask, schema.arrow_schema());
            assert_eq!(
                record_ref.key(),
                Ts {
                    ts: 9.into(),
                    value: UserKeyRef { name: "cat" },
                }
            );
            if let Some(user_ref) = record_ref.get() {
                assert_eq!(user_ref.email, Some("test@example.com"));
                assert_eq!(user_ref.age, Some(9));
                assert_eq!(user_ref.name, "cat");
                assert_eq!(user_ref.grade, Some(1.2));
            } else {
                unreachable!();
            }
        }
        {
            let schema: UserSchema = Default::default();
            let record_batch = RecordBatch::try_new(
                Arc::new(schema.arrow_schema().project(&[0, 1, 3, 4, 5]).unwrap()),
                vec![
                    Arc::new(BooleanArray::from(vec![false])),
                    Arc::new(UInt32Array::from(vec![9])),
                    Arc::new(UInt8Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["cat"])),
                    Arc::new(Float32Array::from(vec![1.2_f32])),
                ],
            )
            .unwrap();

            let project_mask = ProjectionMask::roots(
                &ArrowSchemaConverter::new()
                    .convert(schema.arrow_schema())
                    .unwrap(),
                vec![0, 1, 3, 4, 5],
            );
            let record_ref =
                UserRef::from_record_batch(&record_batch, 0, &project_mask, schema.arrow_schema());
            assert_eq!(
                record_ref.key(),
                Ts {
                    ts: 9.into(),
                    value: UserKeyRef { name: "cat" },
                }
            );
            if let Some(user_ref) = record_ref.get() {
                assert_eq!(user_ref.email, None);
                assert_eq!(user_ref.age, Some(9));
                assert_eq!(user_ref.name, "cat");
                assert_eq!(user_ref.grade, Some(1.2));
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
            grade: 92.9.into(),
        };
        let original_ref = original.as_record_ref();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        assert_eq!(original_ref.size(), 31);
        original_ref.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = User::decode(&mut cursor).await.unwrap();
        assert_eq!(original, decoded);
    }

    #[tokio::test]
    async fn test_record_arrays() {
        let schema: UserSchema = Default::default();
        let mut builder = UserImmutableArrays::builder(schema.arrow_schema().clone(), 10);

        let cat = User {
            email: Some("cat@example.com".to_string()),
            age: 0,
            name: "cat".to_string(),
            grade: 92.9.into(),
        };
        let dog = User {
            email: Some("dog@example.com".to_string()),
            age: 1,
            name: "dog".to_string(),
            grade: f32::NAN.into(),
        };

        builder.push(
            Ts {
                ts: 0.into(),
                value: UserKeyRef { name: "cat" },
            },
            Some(cat.as_record_ref()),
        );
        builder.push(
            Ts {
                ts: 1.into(),
                value: UserKeyRef { name: "dog" },
            },
            Some(dog.as_record_ref()),
        );
        builder.push(
            Ts {
                ts: 2.into(),
                value: UserKeyRef { name: "human" },
            },
            None,
        );

        assert_eq!(builder.written_size(), 64);

        let arrays = builder.finish(Some(&[0, 1, 2, 3, 4, 5]));

        assert_eq!(
            arrays.as_record_batch(),
            &RecordBatch::try_new(
                Arc::new(schema.arrow_schema().project(&[0, 1, 2, 3, 4, 5]).unwrap()),
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
                    Arc::new(Float32Array::from(vec![92.9_f32, f32::NAN, 0.0_f32])),
                ],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_record_arrays_projection() {
        let schema: UserSchema = Default::default();
        let mut builder = UserImmutableArrays::builder(schema.arrow_schema().clone(), 10);

        let cat = User {
            email: Some("cat@example.com".to_string()),
            age: 0,
            name: "cat".to_string(),
            grade: 92.9.into(),
        };
        let dog = User {
            email: Some("dog@example.com".to_string()),
            age: 1,
            name: "dog".to_string(),
            grade: 93.1.into(),
        };

        builder.push(
            Ts {
                ts: 0.into(),
                value: UserKeyRef { name: "cat" },
            },
            Some(cat.as_record_ref()),
        );
        builder.push(
            Ts {
                ts: 1.into(),
                value: UserKeyRef { name: "dog" },
            },
            Some(dog.as_record_ref()),
        );
        builder.push(
            Ts {
                ts: 2.into(),
                value: UserKeyRef { name: "human" },
            },
            None,
        );

        assert_eq!(builder.written_size(), 64);

        let arrays = builder.finish(Some(&[0, 1, 3, 4, 5]));

        assert_eq!(
            arrays.as_record_batch(),
            &RecordBatch::try_new(
                Arc::new(schema.arrow_schema().project(&[0, 1, 3, 4, 5]).unwrap()),
                vec![
                    Arc::new(BooleanArray::from(vec![false, false, true])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2])),
                    Arc::new(UInt8Array::from(vec![0, 1, 0])),
                    Arc::new(StringArray::from(vec!["cat", "dog", "human"])),
                    Arc::new(Float32Array::from(vec![92.9_f32, 93.1_f32, 0.0_f32])),
                ],
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_encode_and_decode_without_ref() {
        let original = Point {
            id: 1243,
            x: 124,
            y: -124,
        };
        let original_ref = original.as_record_ref();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        assert_eq!(original_ref.size(), 18);
        original_ref.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = Point::decode(&mut cursor).await.unwrap();
        assert_eq!(original, decoded);
    }
}
