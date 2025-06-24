use std::{mem::transmute, sync::Arc};

use futures::TryStreamExt;
use js_sys::{Array, Function, JsString, Object, Reflect};
use tonbo::{
    arrow::datatypes::Field,
    executor::opfs::OpfsExecutor,
    record::{DynRecord, Schema, ValueDesc},
    DB,
};
use wasm_bindgen::prelude::*;

use crate::{
    datatype::to_datatype,
    options::DbOption,
    transaction::Transaction,
    utils::{parse_key, parse_record, to_record},
    Bound,
};

type JsExecutor = OpfsExecutor;

#[wasm_bindgen]
pub struct TonboDB {
    desc: Arc<Vec<ValueDesc>>,
    primary_key_index: usize,
    db: Arc<DB<DynRecord, JsExecutor>>,
}

impl TonboDB {
    fn parse_schema(schema: Object) -> (Vec<ValueDesc>, usize) {
        let mut desc = vec![];
        let mut primary_index = None;

        for (i, entry) in Object::entries(&schema).iter().enumerate() {
            let pair = entry
                .dyn_ref::<Array>()
                .expect_throw("unexpected entry")
                .to_vec();
            let name = pair[0]
                .dyn_ref::<JsString>()
                .expect_throw("expected key to be string");
            let col = pair[1]
                .dyn_ref::<Object>()
                .expect_throw("convert to object failed");
            let datatype = Reflect::get(&col, &"type".into())
                .expect("type must be specified")
                .as_string()
                .unwrap();
            let primary = Reflect::get(&col, &"primary".into())
                .unwrap()
                .as_bool()
                .unwrap_or(false);
            let nullable = Reflect::get(&col, &"nullable".into())
                .unwrap()
                .as_bool()
                .unwrap_or(false);

            if primary {
                if primary_index.is_some() {
                    panic!("multiply primary keys are not supported!");
                }
                primary_index = Some(i);
            }
            desc.push(ValueDesc::new(
                name.into(),
                to_datatype(datatype.as_str()),
                nullable,
            ));
        }
        let primary_key_index = primary_index.expect_throw("expected to have one primary key");

        (desc, primary_key_index)
    }
}

#[wasm_bindgen]
impl TonboDB {
    #[wasm_bindgen(constructor)]
    pub async fn new(option: DbOption, schema: Object) -> Self {
        let (desc, primary_key_index) = Self::parse_schema(schema);
        let schema = Schema::new(
            desc.iter()
                .map(|v| Field::new(v.name.as_str(), (&v.datatype).into(), v.is_nullable)).collect(),
            primary_key_index,
        );

        let db = DB::new(option.into_option(), JsExecutor::new(), schema)
            .await
            .unwrap();

        Self {
            desc: Arc::new(desc),
            primary_key_index,
            db: Arc::new(db),
        }
    }

    /// get the record with `key` as the primary key and process it using closure `cb`
    pub async fn get(&self, key: JsValue, cb: Function) -> Result<JsValue, JsValue> {
        let key = parse_key(self.desc.get(self.primary_key_index).unwrap(), key, true)?;
        let this = JsValue::null();

        let record = self
            .db
            .get(&key, |entry| Some(entry.get().columns))
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;

        match record {
            Some(record) => cb.call1(&this, &to_record(&record, self.primary_key_index).into()),
            None => Ok(JsValue::null()),
        }
    }

    /// insert a single tonbo record
    pub async fn insert(&self, record: Object) -> Result<(), JsValue> {
        let record = parse_record(&record, &self.desc, self.primary_key_index)?;
        self.db
            .insert(record)
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;
        Ok(())
    }

    /// insert a sequence of data as a single batch
    #[wasm_bindgen(js_name = "insertBatch")]
    pub async fn insert_batch(&self, records: Vec<Object>) -> Result<(), JsValue> {
        let records = records
            .iter()
            .map(|record| parse_record(&record, &self.desc, self.primary_key_index).unwrap());

        self.db
            .insert_batch(records.into_iter())
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;
        Ok(())
    }

    /// delete the record with the primary key as the `key`
    pub async fn remove(&self, key: JsValue) -> Result<(), JsValue> {
        let key = parse_key(self.desc.get(self.primary_key_index).unwrap(), key, true)?;
        self.db
            .remove(key)
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;
        Ok(())
    }

    pub async fn scan(
        &self,
        lower: Bound,
        high: Bound,
    ) -> Result<wasm_streams::readable::sys::ReadableStream, JsValue> {
        let desc = self.desc.get(self.primary_key_index).unwrap();
        let lower = lower.into_bound(desc)?;
        let high = high.into_bound(desc)?;

        // FIXME: lifetime
        let db =
            unsafe { transmute::<&DB<_, _>, &'static DB<DynRecord, JsExecutor>>(self.db.as_ref()) };
        let stream = db
            .scan(
                (
                    unsafe {
                        transmute::<
                            std::ops::Bound<&tonbo::record::Value>,
                            std::ops::Bound<&'static tonbo::record::Value>,
                        >(lower.as_ref())
                    },
                    unsafe {
                        transmute::<
                            std::ops::Bound<&tonbo::record::Value>,
                            std::ops::Bound<&'static tonbo::record::Value>,
                        >(high.as_ref())
                    },
                ),
                |entry| {
                    let record = entry.get();
                    // to_record(&record.columns, record.primary_index).into()
                    to_record(&record.columns, record.primary_index)
                },
            )
            .await
            .map_err(|err| JsValue::from(err.to_string()));

        Ok(wasm_streams::ReadableStream::from_stream(stream).into_raw())
    }

    /// open an optimistic ACID transaction
    pub async fn transaction(&self, cb: Function) -> Result<(), JsValue> {
        let txn = self.db.transaction().await;

        let this = JsValue::null();
        let txn = Transaction::new(txn, self.desc.clone(), self.primary_key_index);

        {
            let js_txn = JsValue::from(txn);
            cb.call1(&this, &js_txn)?;
        }

        Ok(())
    }

    pub async fn flush(&self) -> Result<(), JsValue> {
        self.db
            .flush()
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;
        Ok(())
    }

    /// flush wal to disk
    #[wasm_bindgen(js_name = "flushWal")]
    pub async fn flush_wal(&self) -> Result<(), JsValue> {
        self.db
            .flush_wal()
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use fusio::{path::Path, DynFs};
    use js_sys::Object;
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::*;

    use crate::{options::DbOption, AwsCredential, S3Builder, TonboDB};

    wasm_bindgen_test_configure!(run_in_browser);

    async fn remove(path: &str) {
        let path = Path::from_opfs_path(path).unwrap();
        let fs = fusio::disk::LocalFs {};

        fs.remove(&path).await.unwrap();
    }

    fn schema() -> Object {
        let schema = Object::new();
        let id = Object::new();
        let name = Object::new();
        let price = Object::new();
        js_sys::Reflect::set(
            &id,
            &JsValue::from_str("primary"),
            &JsValue::from_bool(true),
        )
        .unwrap();
        js_sys::Reflect::set(&id, &JsValue::from_str("type"), &JsValue::from_str("UInt8")).unwrap();
        js_sys::Reflect::set(
            &id,
            &JsValue::from_str("nullable"),
            &JsValue::from_bool(false),
        )
        .unwrap();

        js_sys::Reflect::set(
            &name,
            &JsValue::from_str("type"),
            &JsValue::from_str("String"),
        )
        .unwrap();
        js_sys::Reflect::set(
            &name,
            &JsValue::from_str("nullable"),
            &JsValue::from_bool(true),
        )
        .unwrap();

        js_sys::Reflect::set(
            &price,
            &JsValue::from_str("type"),
            &JsValue::from_str("Float64"),
        )
        .unwrap();
        js_sys::Reflect::set(
            &price,
            &JsValue::from_str("nullable"),
            &JsValue::from_bool(true),
        )
        .unwrap();

        js_sys::Reflect::set(&schema, &JsValue::from_str("id"), &JsValue::from(id)).unwrap();
        js_sys::Reflect::set(&schema, &JsValue::from_str("name"), &JsValue::from(name)).unwrap();
        js_sys::Reflect::set(&schema, &JsValue::from_str("price"), &JsValue::from(price)).unwrap();

        schema
    }

    fn test_items() -> Vec<Object> {
        let mut items = vec![];
        for i in 0..50 {
            let item = Object::new();
            js_sys::Reflect::set(&item, &JsValue::from_str("id"), &JsValue::from(i)).unwrap();
            js_sys::Reflect::set(
                &item,
                &JsValue::from_str("name"),
                &JsValue::from_str(i.to_string().as_str()),
            )
            .unwrap();
            js_sys::Reflect::set(&item, &JsValue::from_str("price"), &JsValue::from(i as f64))
                .unwrap();

            items.push(item);
        }

        items
    }

    #[wasm_bindgen_test]
    pub async fn test_open() {
        let option = DbOption::new("open".to_string()).expect("cannot open DB");

        let schema = schema();
        let db = TonboDB::new(option, schema).await;

        db.flush_wal().await.unwrap();
        drop(db);
        remove("open").await;
    }

    #[wasm_bindgen_test]
    pub async fn test_write() {
        let option = DbOption::new("write".to_string()).expect("cannot open DB");

        let schema = schema();
        let db = TonboDB::new(option, schema).await;

        for item in test_items() {
            db.insert(item).await.unwrap();
        }
        db.flush_wal().await.unwrap();

        drop(db);
        remove("write").await;
    }

    #[ignore]
    #[wasm_bindgen_test]
    pub async fn test_write_s3() {
        if option_env!("AWS_ACCESS_KEY_ID").is_none() {
            return;
        }
        let key_id = option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = option_env!("AWS_SECRET_ACCESS_KEY").unwrap().to_string();

        let fs_option = S3Builder::new("wasm-data".to_string())
            .region("ap-southeast-2".to_string())
            .credential(AwsCredential::new(key_id, secret_key, None))
            .build();

        let option = DbOption::new("write_s3".to_string())
            .expect("cannot open DB")
            .level_path(0, "js/l0".to_string(), fs_option.clone())
            .unwrap()
            .level_path(1, "js/l1".to_string(), fs_option.clone())
            .unwrap()
            .level_path(2, "js/l2".to_string(), fs_option.clone())
            .unwrap();

        let schema = schema();
        let db = TonboDB::new(option, schema).await;

        for (i, item) in test_items().into_iter().enumerate() {
            if i % 5 == 0 {
                db.flush().await.unwrap();
            }
            db.insert(item).await.unwrap();
        }

        drop(db);
        remove("write_s3").await;
    }
}
