use std::{mem::transmute, sync::Arc};

use futures::StreamExt;
use js_sys::Object;
use tonbo::{
    executor::opfs::OpfsExecutor,
    record::{DynRecord, DynamicField},
    transaction, Projection,
};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

use crate::{
    range::Bound,
    utils::{parse_key, parse_record, to_record_ref},
};

#[wasm_bindgen]
pub struct Transaction {
    txn: Option<transaction::Transaction<'static, DynRecord, OpfsExecutor>>,
    desc: Arc<Vec<DynamicField>>,
    primary_key_index: usize,
}

impl Transaction {
    pub(crate) fn new<'txn>(
        txn: transaction::Transaction<'txn, DynRecord, OpfsExecutor>,
        desc: Arc<Vec<DynamicField>>,
        primary_key_index: usize,
    ) -> Self {
        Transaction {
            txn: Some(unsafe {
                transmute::<
                    transaction::Transaction<'txn, DynRecord, OpfsExecutor>,
                    transaction::Transaction<'static, DynRecord, OpfsExecutor>,
                >(txn)
            }),
            desc,
            primary_key_index,
        }
    }

    fn projection(&self, projection: Vec<String>) -> Vec<usize> {
        match projection.contains(&"*".to_string()) {
            true => (0..self.desc.len()).collect(),
            false => self
                .desc
                .iter()
                .enumerate()
                .filter(|(_idx, col)| projection.contains(&col.name))
                .map(|(idx, _col)| idx)
                .collect(),
        }
    }
}

#[wasm_bindgen]
impl Transaction {
    pub async fn get(&mut self, key: JsValue, projection: Vec<String>) -> Result<JsValue, JsValue> {
        if self.txn.is_none() {
            return Err("Can not operate a committed transaction".into());
        }

        let key = parse_key(self.desc.get(self.primary_key_index).unwrap(), key, true)?;
        let projection = if projection.contains(&"*".to_string()) {
            Projection::All
        } else {
            let mut projections = Vec::<&str>::with_capacity(projection.len());
            for name in projection.iter() {
                projections.push(name);
            }
            Projection::Parts(projections)
        };
        let entry = self
            .txn
            .as_ref()
            .unwrap()
            .get(&key, projection)
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;

        match entry {
            Some(entry) => Ok(to_record_ref(&self.desc, &entry.get().columns).into()),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn insert(&mut self, record: Object) -> Result<(), JsValue> {
        if self.txn.is_none() {
            return Err("Can not operate a committed transaction".into());
        }

        let record = parse_record(&record, &self.desc, self.primary_key_index)?;
        self.txn.as_mut().unwrap().insert(record);
        Ok(())
    }

    pub fn remove(&mut self, key: JsValue) -> Result<(), JsValue> {
        if self.txn.is_none() {
            return Err("Can not operate a committed transaction".into());
        }

        let key = parse_key(self.desc.get(self.primary_key_index).unwrap(), key, true)?;
        self.txn.as_mut().unwrap().remove(key);
        Ok(())
    }

    pub async fn scan(
        &self,
        lower: Bound,
        high: Bound,
        limit: Option<usize>,
        projection: Vec<String>,
    ) -> Result<wasm_streams::readable::sys::ReadableStream, JsValue> {
        if self.txn.is_none() {
            return Err("Can not operate a committed transaction".into());
        }

        let projection = self.projection(projection);
        let desc = self.desc.get(self.primary_key_index).unwrap();
        let lower = lower.into_bound(desc)?;
        let high = high.into_bound(desc)?;

        // FIXME: lifetime
        let txn = self.txn.as_ref().unwrap();
        let txn = unsafe {
            transmute::<
                &transaction::Transaction<'_, DynRecord, OpfsExecutor>,
                &'static transaction::Transaction<'_, DynRecord, OpfsExecutor>,
            >(txn)
        };
        let mut scan = txn
            .scan((
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
            ))
            .projection_with_index(projection);

        if let Some(limit) = limit {
            scan = scan.limit(limit);
        }

        let stream = scan
            .take()
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;
        let schema = self.desc.clone();

        let stream = stream.map(move |res| {
            res.map(|entry| match entry.value() {
                Some(record) => to_record_ref(&schema, &record.columns).into(),
                None => JsValue::NULL,
            })
            .map_err(|err| JsValue::from(err.to_string()))
        });
        Ok(wasm_streams::ReadableStream::from_stream(stream).into_raw())
    }

    pub async fn commit(&mut self) -> Result<(), JsValue> {
        if self.txn.is_none() {
            return Err("Can not operate a committed transaction".into());
        }

        let txn = self.txn.take();
        txn.unwrap()
            .commit()
            .await
            .map_err(|err| JsValue::from(err.to_string()))?;

        Ok(())
    }
}
