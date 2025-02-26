use std::{mem::transmute, sync::Arc};

use pyo3::{
    pyclass, pymethods,
    types::{PyAnyMethods, PyMapping, PyMappingMethods, PySequenceMethods, PyTuple},
    Bound, IntoPy, Py, PyAny, PyResult, Python,
};
use pyo3_asyncio::tokio::future_into_py;
use tonbo::{
    record::{DynRecord, Value},
    transaction, Projection,
};

use crate::{
    column::Column,
    error::{repeated_commit_err, CommitError, DbError},
    range,
    stream::ScanStream,
    utils::{to_bound, to_col, to_dict},
};

#[pyclass]
pub struct Transaction {
    txn: Option<transaction::Transaction<'static, DynRecord>>,
    desc: Arc<Vec<Column>>,
    primary_key_index: usize,
}

impl Transaction {
    pub(crate) fn new<'txn>(
        txn: transaction::Transaction<'txn, DynRecord>,
        desc: Arc<Vec<Column>>,
    ) -> Self {
        let primary_key_index = desc
            .iter()
            .enumerate()
            .find(|(_idx, col)| col.primary_key)
            .unwrap()
            .0;
        Transaction {
            txn: Some(unsafe {
                transmute::<
                    transaction::Transaction<'txn, DynRecord>,
                    transaction::Transaction<'static, DynRecord>,
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

#[pymethods]
impl Transaction {
    /// Get record from `TonboDB`.
    ///
    /// * `key`: Primary key of record
    /// * `projection`: Fields to projection, all fields by default.
    #[pyo3(signature= (key, projection=vec!["*".to_owned()]))]
    fn get<'py>(
        &'py self,
        py: Python<'py>,
        key: Py<PyAny>,
        projection: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.txn.is_none() {
            return Err(repeated_commit_err());
        }

        let col_desc = self.desc.get(self.primary_key_index).unwrap();
        let col = to_col(py, col_desc, key);
        let projection = self.projection(projection);

        let txn = self.txn.as_ref().unwrap();
        let txn = unsafe {
            transmute::<
                &transaction::Transaction<'_, DynRecord>,
                &'static transaction::Transaction<'_, DynRecord>,
            >(txn)
        };

        let primary_key_index = self.primary_key_index;

        future_into_py(py, async move {
            let entry = txn
                .get(&col, Projection::Parts(projection))
                .await
                .map_err(DbError::from)?;

            Python::with_gil(|py| {
                Ok(match entry {
                    Some(entry) => to_dict(py, primary_key_index, entry.get().columns).into_py(py),
                    None => py.None(),
                })
            })
        })
    }

    /// Insert record to `TonboDB`.
    ///
    /// * `record`: Primary key of record that is to be removed
    fn insert(&mut self, py: Python, record: Py<PyAny>) -> PyResult<()> {
        if self.txn.is_none() {
            return Err(repeated_commit_err());
        }
        let mut cols = vec![];
        let dict = record.getattr(py, "__dict__")?;
        let mapping_proxy = dict.downcast_bound::<PyMapping>(py)?;

        for x in mapping_proxy.items().iter() {
            let list = x.to_tuple()?;
            for x in list {
                let tuple = x.downcast::<PyTuple>()?;
                let col = tuple.get_item(1)?;
                if let Ok(bound_col) = col.downcast::<Column>() {
                    let col = Value::from(bound_col.extract::<Column>()?);
                    cols.push(col);
                }
            }
        }
        let record = DynRecord::new(cols, self.primary_key_index);
        self.txn.as_mut().unwrap().insert(record);
        Ok(())
    }

    /// Remove record from `TonboDB`.
    ///
    /// * `key`: Primary key of record that is to be removed
    fn remove(&mut self, py: Python, key: Py<PyAny>) -> PyResult<()> {
        if self.txn.is_none() {
            return Err(repeated_commit_err());
        }

        let col_desc = self.desc.get(self.primary_key_index).unwrap();
        let col = to_col(py, col_desc, key);

        self.txn.as_mut().unwrap().remove(col);
        Ok(())
    }

    /// Create an async stream for scanning.
    ///
    /// * `lower`: - Lower bound of range. Use None represent unbounded.
    /// * `high`: - High bound of range. Use None represent unbounded.
    /// * `limit`: - Max number records to scan.
    /// * `projection`: - Fields to projection in the record. Projection all by default.
    #[pyo3(signature= (lower, high, limit=None,  projection=vec!["*".to_string()]))]
    fn scan<'py>(
        &'py mut self,
        py: Python<'py>,
        lower: Option<Py<range::Bound>>,
        high: Option<Py<range::Bound>>,
        limit: Option<usize>,
        projection: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.txn.is_none() {
            return Err(repeated_commit_err());
        }
        let txn = self.txn.as_ref().unwrap();
        let txn = unsafe {
            transmute::<
                &transaction::Transaction<'_, DynRecord>,
                &'static transaction::Transaction<'_, DynRecord>,
            >(txn)
        };
        let col_desc = self.desc.get(self.primary_key_index).unwrap();
        let projection = self.projection(projection);

        let (lower, high) = to_bound(py, col_desc, lower, high);

        future_into_py(py, async move {
            let mut scan = txn.scan((
                unsafe {
                    transmute::<std::ops::Bound<&Value>, std::ops::Bound<&'static Value>>(
                        lower.as_ref(),
                    )
                },
                unsafe {
                    transmute::<std::ops::Bound<&Value>, std::ops::Bound<&'static Value>>(
                        high.as_ref(),
                    )
                },
            ));

            if let Some(limit) = limit {
                scan = scan.limit(limit);
            }
            scan = scan.projection_with_index(projection);
            let stream = scan.take().await.map_err(DbError::from)?;

            let stream = ScanStream::new(stream);

            Ok(Python::with_gil(|py| stream.into_py(py)))
        })
    }

    /// Commit `Transaction`
    fn commit<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.txn.is_none() {
            return Err(repeated_commit_err());
        }
        let txn = self.txn.take();
        future_into_py(py, async move {
            txn.unwrap().commit().await.map_err(CommitError::from)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}
