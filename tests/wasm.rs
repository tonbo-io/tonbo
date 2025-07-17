#[cfg(all(test, feature = "opfs", target_arch = "wasm32"))]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
    use std::{collections::BTreeMap, ops::Bound, sync::Arc};

    use fusio::{path::Path, DynFs};
    use futures::StreamExt;
    use tonbo::{
        arrow::datatypes::{DataType as ArrowDataType, Field},
        datatype::DataType,
        executor::opfs::OpfsExecutor,
        record::{DynRecord, Record, RecordRef, Schema},
        AsValue, DbOption, PrimaryKey, Projection, Value, DB,
    };
    use wasm_bindgen_test::wasm_bindgen_test;

    fn test_dyn_item_schema() -> Schema {
        Schema::new(
            vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("age", ArrowDataType::Int8, true),
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("email", ArrowDataType::Utf8, true),
                Field::new("bytes", ArrowDataType::Binary, true),
            ],
            0,
        )
    }

    fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let columns: Vec<Arc<dyn Value>> = vec![
                Arc::new(i as i64),
                Arc::new(Some(i as i8)),
                Arc::new(i.to_string()),
                Arc::new(Some(format!("{}@tonbo.io", i))),
                Arc::new(Some((i as i32).to_le_bytes().to_vec())),
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

        let option = DbOption::new(Path::from_opfs_path("opfs_dir_rw").unwrap());

        let db: DB<DynRecord, OpfsExecutor> =
            DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

        for item in test_dyn_items().into_iter() {
            db.insert(item).await.unwrap();
        }

        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = PrimaryKey::new(vec![Arc::new(i as i64)]);
                let option1 = tx.get(&key, Projection::All).await.unwrap();
                let entry = option1.unwrap();
                let record_ref = entry.get();

                assert_eq!(*record_ref.columns.first().unwrap().as_i64(), i as i64);
                assert_eq!(
                    *record_ref.columns.get(2).unwrap().as_string_opt(),
                    Some(i.to_string()),
                );
                assert_eq!(
                    *record_ref.columns.get(3).unwrap().as_string_opt(),
                    Some(format!("{}@tonbo.io", i)),
                );
                assert_eq!(
                    *record_ref.columns.get(4).unwrap().as_bytes_opt(),
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

        let option = DbOption::new(Path::from_opfs_path("opfs_dir_txn").unwrap());

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
            let lower = PrimaryKey::new(vec![Arc::new(5_i64)]);
            let upper = PrimaryKey::new(vec![Arc::new(47_i64)]);
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
                assert_eq!(primary_key_col.data_type(), DataType::Int64);
                assert_eq!(*primary_key_col.as_i64(), i);

                let col = columns.get(1).unwrap();
                assert_eq!(col.data_type(), DataType::Int8);
                let age = col.as_i8_opt();
                assert_eq!(age, &None);

                let col = columns.get(2).unwrap();
                assert_eq!(col.data_type(), DataType::String);
                let name = col.as_string_opt();
                assert_eq!(name, &Some(i.to_string()));

                let col = columns.get(4).unwrap();
                assert_eq!(col.data_type(), DataType::Bytes);
                let bytes = col.as_bytes_opt();
                assert_eq!(bytes, &Some((i as i32).to_le_bytes().to_vec()));
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

        let option = DbOption::new(Path::from_opfs_path("opfs_dir").unwrap());

        {
            let db: DB<DynRecord, OpfsExecutor> =
                DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

            for item in test_dyn_items().into_iter() {
                db.insert(item).await.unwrap();
            }

            db.flush_wal().await.unwrap();
        }

        let schema = test_dyn_item_schema();
        let option = DbOption::new(Path::from_opfs_path("opfs_dir").unwrap());
        let db: DB<DynRecord, OpfsExecutor> =
            DB::new(option, OpfsExecutor::new(), schema).await.unwrap();

        let mut sort_items = BTreeMap::new();
        for item in test_dyn_items() {
            sort_items.insert(item.as_record_ref().key(), item);
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
                sort_items.insert(item.as_record_ref().key(), item);
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

        let option = DbOption::new(Path::from_opfs_path("s3_rw").unwrap())
            .level_path(
                0,
                Path::from_url_path("tonbo/l0").unwrap(),
                fs_option.clone(),
            )
            .unwrap()
            .level_path(
                1,
                Path::from_url_path("tonbo/l1").unwrap(),
                fs_option.clone(),
            )
            .unwrap()
            .level_path(2, Path::from_url_path("tonbo/l2").unwrap(), fs_option)
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
                assert_eq!(primary_key_col.data_type(), DataType::Int64);
                assert_eq!(*primary_key_col.as_i64(), i);

                let col = columns.get(1).unwrap();
                assert_eq!(col.data_type(), DataType::Int8);
                let age = col.as_i8_opt();
                assert_eq!(age, &Some(i as i8));

                let col = columns.get(2).unwrap();
                assert_eq!(col.data_type(), DataType::String);
                let name = col.as_string_opt();
                assert_eq!(name, &Some(i.to_string()));

                let col = columns.get(4).unwrap();
                assert_eq!(col.data_type(), DataType::Bytes);
                let bytes = col.as_bytes_opt();
                assert!(bytes.is_none());
                i += 1
            }
        }
        let _ = db.flush_wal().await;
        drop(db);

        remove("s3_rw").await;
    }
}
