#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use futures::StreamExt;
    use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};
    use tempfile::TempDir;
    use tonbo::{
        executor::tokio::TokioExecutor,
        record::{DataType, DynRecord, DynSchema, Record, RecordRef, Schema, Value, ValueDesc},
        DbOption, Path, Projection, DB,
    };

    fn test_dyn_item_schema() -> DynSchema {
        DynSchema::new(
            vec![
                ValueDesc::new("id".into(), DataType::UInt32, false),
                ValueDesc::new(
                    "bools".into(),
                    DataType::List(Arc::new(ValueDesc::new(
                        "".into(),
                        DataType::Boolean,
                        false,
                    ))),
                    true,
                ),
                ValueDesc::new(
                    "bytes".into(),
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Bytes, false))),
                    true,
                ),
                ValueDesc::new(
                    "i64s".into(),
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Int64, false))),
                    true,
                ),
                ValueDesc::new(
                    "u16s".into(),
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::UInt16, false))),
                    false,
                ),
                ValueDesc::new(
                    "strs".into(),
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::String, false))),
                    false,
                ),
            ],
            0,
        )
    }

    fn generate_record(i: usize) -> DynRecord {
        DynRecord::new(
            vec![
                Value::new(DataType::UInt32, "id".into(), Arc::new(i as u32), false),
                Value::new(
                    DataType::List(Arc::new(ValueDesc::new(
                        "".into(),
                        DataType::Boolean,
                        false,
                    ))),
                    "bools".to_string(),
                    Arc::new(Some(vec![true, i % 3 == 0, i % 4 == 0, i % 5 == 0, false])),
                    true,
                ),
                Value::new(
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Bytes, false))),
                    "bytes".to_string(),
                    Arc::new(Some(vec![
                        format!("{i}@tonbo").as_bytes().to_vec(),
                        format!("{}@tonbo", i).as_bytes().to_vec(),
                        format!("{}@tonbo", i * 2).as_bytes().to_vec(),
                        format!("{}@tonbo", i * 3).as_bytes().to_vec(),
                    ])),
                    true,
                ),
                Value::new(
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Int64, false))),
                    "i64s".to_string(),
                    Arc::new(Some(vec![
                        1231_i64,
                        201 * i as i64,
                        379 * i as i64,
                        493 * i as i64,
                    ])),
                    true,
                ),
                Value::new(
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::UInt16, false))),
                    "u16s".to_string(),
                    Arc::new(vec![1_u16, 2, 3, 4]),
                    false,
                ),
                Value::new(
                    DataType::List(Arc::new(ValueDesc::new("".into(), DataType::String, false))),
                    "strs".to_string(),
                    Arc::new(vec![
                        format!("{i}@tonbo"),
                        format!("{}@tonbo", i * 3),
                        format!("{}@tonbo", i * 4),
                        format!("{}@tonbo", i * 7),
                    ]),
                    false,
                ),
            ],
            0,
        )
    }

    fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = Vec::with_capacity(50);
        for i in 0..50 {
            items.push(generate_record(i));
        }
        items
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_write_dyn_record_mem() {
        let temp_dir = TempDir::new().unwrap();

        let dyn_schema = test_dyn_item_schema();
        let arrow_schema = dyn_schema.arrow_schema().clone();
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &dyn_schema,
        );

        let db: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::current(), dyn_schema)
                .await
                .unwrap();

        for (i, item) in test_dyn_items().into_iter().enumerate() {
            if i == 28 {
                db.remove(item.key()).await.unwrap();
            } else {
                db.insert(item).await.unwrap();
            }
        }

        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = Value::new(
                    DataType::UInt32,
                    "id".to_string(),
                    Arc::new(i as u32),
                    false,
                );
                let option1 = tx.get(&key, Projection::All).await.unwrap();
                if i == 28 {
                    assert!(option1.is_none());
                    continue;
                }
                let entry = option1.unwrap();
                let record_ref = entry.get();

                let expected = generate_record(i);
                assert_eq!(record_ref.columns, expected.as_record_ref().columns);
            }
        }

        // test scan
        {
            let tx = db.transaction().await;
            let lower = Value::new(DataType::UInt32, "id".to_owned(), Arc::new(0_u32), false);
            let upper = Value::new(DataType::UInt32, "id".to_owned(), Arc::new(49_u32), false);
            let mut scan = tx
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(&["id", "bools", "bytes", "strs"])
                .take()
                .await
                .unwrap();

            let mut i = 0_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                if i == 28 {
                    assert!(entry.value().is_none());
                    i += 1;
                    continue;
                }
                let record_ref = entry.value().unwrap();
                let expected = generate_record(i as usize);
                let mut expected_ref = expected.as_record_ref();
                let mask = ProjectionMask::roots(
                    &ArrowSchemaConverter::new().convert(&arrow_schema).unwrap(),
                    [0, 1, 2, 3, 4, 7],
                );
                expected_ref.projection(&mask);

                assert_eq!(record_ref.columns, expected_ref.columns);
                i += 1;
            }
            assert_eq!(i, 50);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_write_dyn_record_from_disk() {
        let temp_dir = TempDir::new().unwrap();

        let dyn_schema = test_dyn_item_schema();
        let arrow_schema = dyn_schema.arrow_schema().clone();
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &dyn_schema,
        )
        .immutable_chunk_num(1)
        .major_threshold_with_sst_size(2)
        .level_sst_magnification(1)
        .max_sst_file_size(1024)
        .major_default_oldest_table_num(1);

        let db: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::current(), dyn_schema)
                .await
                .unwrap();

        for (i, item) in test_dyn_items().into_iter().enumerate() {
            if i == 28 {
                db.remove(item.key()).await.unwrap();
            } else {
                db.insert(item).await.unwrap();
            }
            if i == 5 {
                db.flush().await.unwrap();
            }
        }
        db.flush().await.unwrap();

        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = Value::new(
                    DataType::UInt32,
                    "id".to_string(),
                    Arc::new(i as u32),
                    false,
                );
                let option1 = tx.get(&key, Projection::All).await.unwrap();
                if i == 28 {
                    assert!(option1.is_none());
                    continue;
                }
                let entry = option1.unwrap();
                let record_ref = entry.get();
                // dbg!(record_ref.columns.clone());

                let expected = generate_record(i);
                assert_eq!(record_ref.columns, expected.as_record_ref().columns);
            }
        }
        // test scan
        {
            let tx = db.transaction().await;
            let lower = Value::new(DataType::UInt32, "id".to_owned(), Arc::new(0_u32), false);
            let upper = Value::new(DataType::UInt32, "id".to_owned(), Arc::new(49_u32), false);
            let mut scan = tx
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(&["id", "bools", "bytes", "strs"])
                .take()
                .await
                .unwrap();

            let mut i = 0_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                if i == 28 {
                    assert!(entry.value().is_none());
                    i += 1;
                    continue;
                }
                let record_ref = entry.value().unwrap();
                let expected = generate_record(i as usize);
                let mut expected_ref = expected.as_record_ref();
                // dbg!(record_ref.columns.clone());
                let mask = ProjectionMask::roots(
                    &ArrowSchemaConverter::new().convert(&arrow_schema).unwrap(),
                    [0, 1, 2, 3, 4, 7],
                );
                expected_ref.projection(&mask);

                assert_eq!(record_ref.columns, expected_ref.columns);
                i += 1;
            }
            assert_eq!(i, 50);
        }
    }
}
