#[cfg(all(test, feature = "opfs", target_arch = "wasm32"))]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
    use std::{collections::BTreeMap, ops::Bound, sync::Arc};

    use fusio::{path::Path, DynFs};
    use futures::StreamExt;
    use tonbo::{
        executor::opfs::OpfsExecutor,
        record::{DataType, DynRecord, DynSchema, Record, RecordRef, Schema, Value, ValueDesc},
        DbOption, Projection, DB,
    };
    use wasm_bindgen_test::wasm_bindgen_test;

    fn test_dyn_item_schema() -> DynSchema {
        let descs = vec![
            ValueDesc::new("id".to_string(), DataType::Int64, false),
            ValueDesc::new("age".to_string(), DataType::Int8, true),
            ValueDesc::new("name".to_string(), DataType::String, false),
            ValueDesc::new("email".to_string(), DataType::String, true),
            ValueDesc::new("bytes".to_string(), DataType::Bytes, true),
        ];
        DynSchema::new(descs, 0)
    }

    fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let columns = vec![
                Value::new(DataType::Int64, "id".to_string(), Arc::new(i as i64), false),
                Value::new(
                    DataType::Int8,
                    "age".to_string(),
                    Arc::new(Some(i as i8)),
                    true,
                ),
                Value::new(
                    DataType::String,
                    "name".to_string(),
                    Arc::new(i.to_string()),
                    false,
                ),
                Value::new(
                    DataType::String,
                    "email".to_string(),
                    Arc::new(Some(format!("{}@tonbo.io", i))),
                    true,
                ),
                Value::new(
                    DataType::Bytes,
                    "bytes".to_string(),
                    Arc::new(Some((i as i32).to_le_bytes().to_vec())),
                    true,
                ),
            ];

            items.push(DynRecord::new(columns, 0));
        }
        items
    }

    async fn remove(path: &str) {
        let path = Path::from_opfs_path(path).unwrap();
        let fs = fusio::disk::LocalFs {};

        fs.remove(&path).await.unwrap();
    }

    #[wasm_bindgen_test]
    async fn test_wasm_read_write() {
        let schema = test_dyn_item_schema();
        let path = Path::from_opfs_path("opfs_dir_rw").unwrap();
        let fs = fusio::disk::LocalFs {};
        fs.create_dir_all(&path).await.unwrap();

        let option = DbOption::new(Path::from_opfs_path("opfs_dir_rw").unwrap(), &schema);

        let db: DB<DynRecord, OpfsExecutor> =
            DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

        for item in test_dyn_items().into_iter() {
            db.insert(item).await.unwrap();
        }

        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = Value::new(DataType::Int64, "id".to_string(), Arc::new(i as i64), false);
                let option1 = tx.get(&key, Projection::All).await.unwrap();
                let entry = option1.unwrap();
                let record_ref = entry.get();

                assert_eq!(
                    *record_ref
                        .columns
                        .first()
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<i64>()
                        .unwrap(),
                    i as i64
                );
                assert_eq!(
                    *record_ref
                        .columns
                        .get(2)
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<Option<String>>()
                        .unwrap(),
                    Some(i.to_string()),
                );
                assert_eq!(
                    *record_ref
                        .columns
                        .get(3)
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<Option<String>>()
                        .unwrap(),
                    Some(format!("{}@tonbo.io", i)),
                );
                assert_eq!(
                    *record_ref
                        .columns
                        .get(4)
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<Option<Vec<u8>>>()
                        .unwrap(),
                    Some((i as i32).to_le_bytes().to_vec()),
                );
            }
            tx.commit().await.unwrap();
        }
        db.flush_wal().await.unwrap();
        drop(db);

        remove("opfs_dir_rw").await;
    }

    #[wasm_bindgen_test]
    async fn test_wasm_transaction() {
        let schema = test_dyn_item_schema();

        let fs = fusio::disk::LocalFs {};
        let path = Path::from_opfs_path("opfs_dir_txn").unwrap();
        fs.create_dir_all(&path).await.unwrap();

        let option = DbOption::new(Path::from_opfs_path("opfs_dir_txn").unwrap(), &schema);

        let db: DB<DynRecord, OpfsExecutor> =
            DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

        {
            let mut txn = db.transaction().await;
            for item in test_dyn_items().into_iter() {
                txn.insert(item);
            }
            txn.commit().await.unwrap();
        }

        // test scan
        {
            let txn = db.transaction().await;
            let lower = Value::new(DataType::Int64, "id".to_owned(), Arc::new(5_i64), false);
            let upper = Value::new(DataType::Int64, "id".to_owned(), Arc::new(47_i64), false);
            let mut scan = txn
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(&["id", "name", "bytes"])
                .take()
                .await
                .unwrap();

            let mut i = 5_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype(), DataType::Int64);
                assert_eq!(primary_key_col.desc.name, "id".to_string());
                assert_eq!(
                    *primary_key_col
                        .value
                        .as_ref()
                        .downcast_ref::<i64>()
                        .unwrap(),
                    i
                );

                let col = columns.get(1).unwrap();
                assert_eq!(col.datatype(), DataType::Int8);
                assert_eq!(col.desc.name, "age".to_string());
                let age = col.value.as_ref().downcast_ref::<Option<i8>>();
                assert!(age.is_some());
                assert_eq!(age.unwrap(), &None);

                let col = columns.get(2).unwrap();
                assert_eq!(col.datatype(), DataType::String);
                assert_eq!(col.desc.name, "name".to_string());
                let name = col.value.as_ref().downcast_ref::<Option<String>>();
                assert!(name.is_some());
                assert_eq!(name.unwrap(), &Some(i.to_string()));

                let col = columns.get(4).unwrap();
                assert_eq!(col.datatype(), DataType::Bytes);
                assert_eq!(col.desc.name, "bytes".to_string());
                let bytes = col.value.as_ref().downcast_ref::<Option<Vec<u8>>>();
                assert!(bytes.is_some());
                assert_eq!(bytes.unwrap(), &Some((i as i32).to_le_bytes().to_vec()));
                i += 1
            }
            assert_eq!(i, 48);
        }
        db.flush_wal().await.unwrap();
        drop(db);

        remove(&"opfs_dir_txn").await;
    }

    #[wasm_bindgen_test]
    async fn test_wasm_schema_recover() {
        let schema = test_dyn_item_schema();
        let path = Path::from_opfs_path("opfs_dir").unwrap();
        let fs = fusio::disk::LocalFs {};
        fs.create_dir_all(&path).await.unwrap();

        let option = DbOption::new(Path::from_opfs_path("opfs_dir").unwrap(), &schema);

        {
            let db: DB<DynRecord, OpfsExecutor> =
                DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

            for item in test_dyn_items().into_iter() {
                db.insert(item).await.unwrap();
            }

            db.flush_wal().await.unwrap();
        }

        let schema = test_dyn_item_schema();
        let option = DbOption::new(Path::from_opfs_path("opfs_dir").unwrap(), &schema);
        let db: DB<DynRecord, OpfsExecutor> =
            DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

        let mut sort_items = BTreeMap::new();
        for item in test_dyn_items() {
            sort_items.insert(item.key(), item);
        }

        {
            let tx = db.transaction().await;
            let mut scan = tx
                .scan((Bound::Unbounded, Bound::Unbounded))
                .take()
                .await
                .unwrap();

            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns1 = entry.value().unwrap().columns;
                let (_, record) = sort_items.pop_first().unwrap();
                let columns2 = record.as_record_ref().columns;

                assert_eq!(columns1.len(), columns2.len());
                for i in 0..columns1.len() {
                    assert_eq!(columns1.get(i), columns2.get(i));
                }
            }
        }
        {
            use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};
            // test projection

            let mut sort_items = BTreeMap::new();
            for item in test_dyn_items() {
                sort_items.insert(item.key(), item);
            }

            let tx = db.transaction().await;
            let mut scan = tx
                .scan((Bound::Unbounded, Bound::Unbounded))
                .projection(&["id", "age", "name"])
                .take()
                .await
                .unwrap();

            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns1 = entry.value().unwrap().columns;
                let (_, record) = sort_items.pop_first().unwrap();

                let schema = test_dyn_item_schema();
                let mask = ProjectionMask::roots(
                    &ArrowSchemaConverter::new()
                        .convert(schema.arrow_schema())
                        .unwrap(),
                    [2, 3, 4],
                );
                let mut record_ref = record.as_record_ref();
                record_ref.projection(&mask);
                let columns2 = record_ref.columns;

                assert_eq!(columns1.len(), columns2.len());
                for i in 0..columns1.len() {
                    assert_eq!(columns1.get(i), columns2.get(i));
                }
            }
        }
        db.flush_wal().await.unwrap();
        drop(db);
        remove("opfs_dir").await;
    }

    #[cfg(all(feature = "aws", feature = "wasm-http"))]
    #[wasm_bindgen_test]
    async fn test_s3_read_write() {
        use fusio::remotes::aws::AwsCredential;
        use fusio_dispatch::FsOptions;

        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = option_env!("AWS_SECRET_ACCESS_KEY").unwrap().to_string();

        let bucket = option_env!("BUCKET_NAME")
            .expect("expected s3 bucket not to be empty")
            .to_string();
        let region = Some(
            option_env!("AWS_REGION")
                .expect("expected s3 region not to be empty")
                .to_string(),
        );
        let token = option_env!("AWS_SESSION_TOKEN").map(|v| v.to_string());

        let schema = test_dyn_item_schema();

        let fs_option = FsOptions::S3 {
            bucket,
            credential: Some(AwsCredential {
                key_id,
                secret_key,
                token,
            }),
            endpoint: None,
            sign_payload: None,
            checksum: None,
            region,
        };

        let option = DbOption::new(Path::from_opfs_path("s3_rw").unwrap(), &schema)
            .level_path(
                0,
                Path::from_url_path("tonbo/l0").unwrap(),
                fs_option.clone(),
                true,
            )
            .unwrap()
            .level_path(
                1,
                Path::from_url_path("tonbo/l1").unwrap(),
                fs_option.clone(),
                false,
            )
            .unwrap()
            .level_path(2, Path::from_url_path("tonbo/l2").unwrap(), fs_option, true)
            .unwrap()
            .major_threshold_with_sst_size(3)
            .level_sst_magnification(1)
            .max_sst_file_size(1 * 1024);

        let db: DB<DynRecord, OpfsExecutor> =
            DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

        for (i, item) in test_dyn_items().into_iter().enumerate() {
            db.insert(item).await.unwrap();
            if i % 5 == 0 {
                db.flush().await.unwrap();
            }
        }

        {
            let tx = db.transaction().await;

            let mut scan = tx
                .scan((Bound::Unbounded, Bound::Unbounded))
                .projection(&["id", "age", "name"])
                .take()
                .await
                .unwrap();

            let mut i = 0;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype(), DataType::Int64);
                assert_eq!(primary_key_col.desc.name, "id".to_string());
                assert_eq!(
                    *primary_key_col
                        .value
                        .as_ref()
                        .downcast_ref::<i64>()
                        .unwrap(),
                    i
                );

                let col = columns.get(1).unwrap();
                assert_eq!(col.datatype(), DataType::Int8);
                assert_eq!(col.desc.name, "age".to_string());
                let age = col.value.as_ref().downcast_ref::<Option<i8>>();
                assert!(age.is_some());
                assert_eq!(age.unwrap(), &Some(i as i8));

                let col = columns.get(2).unwrap();
                assert_eq!(col.datatype(), DataType::String);
                assert_eq!(col.desc.name, "name".to_string());
                let name = col.value.as_ref().downcast_ref::<Option<String>>();
                assert!(name.is_some());
                assert_eq!(name.unwrap(), &Some(i.to_string()));

                let col = columns.get(4).unwrap();
                assert_eq!(col.datatype(), DataType::Bytes);
                assert_eq!(col.desc.name, "bytes".to_string());
                let bytes = col.value.as_ref().downcast_ref::<Option<Vec<u8>>>();
                assert!(bytes.unwrap().is_none());
                i += 1
            }
        }
        let _ = db.flush_wal().await;
        drop(db);

        remove("s3_rw").await;
    }
}
