#[cfg(all(test, feature = "opfs", target_arch = "wasm32"))]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
    use std::{collections::BTreeMap, ops::Bound, sync::Arc};

    use fusio::{path::Path, DynFs};
    use futures::StreamExt;
    use tonbo::{
        executor::opfs::OpfsExecutor,
        record::{Column, ColumnDesc, Datatype, DynRecord, Record},
        DbOption, Projection, DB,
    };
    use wasm_bindgen_test::wasm_bindgen_test;

    fn test_dyn_item_schema() -> (Vec<ColumnDesc>, usize) {
        let descs = vec![
            ColumnDesc::new("id".to_string(), Datatype::Int64, false),
            ColumnDesc::new("age".to_string(), Datatype::Int8, true),
            ColumnDesc::new("name".to_string(), Datatype::String, false),
            ColumnDesc::new("email".to_string(), Datatype::String, true),
            ColumnDesc::new("bytes".to_string(), Datatype::Bytes, true),
        ];
        (descs, 0)
    }

    fn test_dyn_items() -> Vec<DynRecord> {
        let mut items = vec![];
        for i in 0..50 {
            let columns = vec![
                Column::new(Datatype::Int64, "id".to_string(), Arc::new(i as i64), false),
                Column::new(
                    Datatype::Int8,
                    "age".to_string(),
                    Arc::new(Some(i as i8)),
                    true,
                ),
                Column::new(
                    Datatype::String,
                    "name".to_string(),
                    Arc::new(i.to_string()),
                    false,
                ),
                Column::new(
                    Datatype::String,
                    "email".to_string(),
                    Arc::new(Some(format!("{}@tonbo.io", i))),
                    true,
                ),
                Column::new(
                    Datatype::Bytes,
                    "bytes".to_string(),
                    Arc::new(Some((i as i32).to_le_bytes().to_vec())),
                    true,
                ),
            ];

            let record = DynRecord::new(columns, 0);
            items.push(record);
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
        let (cols_desc, primary_key_index) = test_dyn_item_schema();
        let path = Path::from_opfs_path("opfs_dir_rw").unwrap();
        let fs = fusio::disk::LocalFs {};
        fs.create_dir_all(&path).await.unwrap();

        let option = DbOption::with_path(
            Path::from_opfs_path("opfs_dir_rw").unwrap(),
            "id".to_string(),
            primary_key_index,
        );

        let db: DB<DynRecord, OpfsExecutor> =
            DB::with_schema(option, OpfsExecutor::new(), cols_desc, primary_key_index)
                .await
                .unwrap();

        for item in test_dyn_items().into_iter() {
            db.insert(item).await.unwrap();
        }

        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = Column::new(Datatype::Int64, "id".to_string(), Arc::new(i as i64), false);
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
        let (cols_desc, primary_key_index) = test_dyn_item_schema();

        let fs = fusio::disk::LocalFs {};
        let path = Path::from_opfs_path("opfs_dir_txn").unwrap();
        fs.create_dir_all(&path).await.unwrap();

        let option = DbOption::with_path(
            Path::from_opfs_path("opfs_dir_txn").unwrap(),
            "id".to_string(),
            primary_key_index,
        );

        let db: DB<DynRecord, OpfsExecutor> =
            DB::with_schema(option, OpfsExecutor::new(), cols_desc, primary_key_index)
                .await
                .unwrap();

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
            let lower = Column::new(Datatype::Int64, "id".to_owned(), Arc::new(5_i64), false);
            let upper = Column::new(Datatype::Int64, "id".to_owned(), Arc::new(47_i64), false);
            let mut scan = txn
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(vec![0, 2, 4])
                .take()
                .await
                .unwrap();

            let mut i = 5_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype, Datatype::Int64);
                assert_eq!(primary_key_col.name, "id".to_string());
                assert_eq!(
                    *primary_key_col
                        .value
                        .as_ref()
                        .downcast_ref::<i64>()
                        .unwrap(),
                    i
                );

                let col = columns.get(1).unwrap();
                assert_eq!(col.datatype, Datatype::Int8);
                assert_eq!(col.name, "age".to_string());
                let age = col.value.as_ref().downcast_ref::<Option<i8>>();
                assert!(age.is_some());
                assert_eq!(age.unwrap(), &None);

                let col = columns.get(2).unwrap();
                assert_eq!(col.datatype, Datatype::String);
                assert_eq!(col.name, "name".to_string());
                let name = col.value.as_ref().downcast_ref::<Option<String>>();
                assert!(name.is_some());
                assert_eq!(name.unwrap(), &Some(i.to_string()));

                let col = columns.get(4).unwrap();
                assert_eq!(col.datatype, Datatype::Bytes);
                assert_eq!(col.name, "bytes".to_string());
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
        let (cols_desc, primary_key_index) = test_dyn_item_schema();
        let path = Path::from_opfs_path("opfs_dir").unwrap();
        let fs = fusio::disk::LocalFs {};
        fs.create_dir_all(&path).await.unwrap();

        let option = DbOption::with_path(
            Path::from_opfs_path("opfs_dir").unwrap(),
            "id".to_string(),
            primary_key_index,
        );

        {
            let db: DB<DynRecord, OpfsExecutor> =
                DB::with_schema(option, OpfsExecutor::new(), cols_desc, primary_key_index)
                    .await
                    .unwrap();

            for item in test_dyn_items().into_iter() {
                db.insert(item).await.unwrap();
            }

            db.flush_wal().await.unwrap();
        }

        let (cols_desc, primary_key_index) = test_dyn_item_schema();
        let option = DbOption::with_path(
            Path::from_opfs_path("opfs_dir").unwrap(),
            "id".to_string(),
            primary_key_index,
        );
        let db: DB<DynRecord, OpfsExecutor> =
            DB::with_schema(option, OpfsExecutor::new(), cols_desc, primary_key_index)
                .await
                .unwrap();

        let mut sort_items = BTreeMap::new();
        for item in test_dyn_items() {
            sort_items.insert(item.key(), item);
        }

        {
            let tx = db.transaction().await;
            let mut scan = tx
                .scan((Bound::Unbounded, Bound::Unbounded))
                .projection(vec![0, 1, 2])
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
        drop(db);
        remove("opfs_dir").await;
    }
}
