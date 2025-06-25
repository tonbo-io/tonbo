use std::sync::Arc;

use pyo3::{
    prelude::*,
    pyclass, pymethods,
    types::{PyDict, PyMapping},
    IntoPyObjectExt, Py, PyAny, PyResult, Python,
};
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};
use tonbo::{
    arrow::datatypes::Field,
    executor::tokio::TokioExecutor,
    record::{DynRecord, Schema, Value},
    DB,
};

use crate::{
    column::Column,
    error::{CommitError, DbError},
    options::DbOption,
    record_batch::RecordBatch,
    transaction::Transaction,
    utils::{to_col, to_dict},
};

type PyExecutor = TokioExecutor;

#[pyclass]
pub struct TonboDB {
    desc: Arc<Vec<Column>>,
    primary_key_index: usize,
    db: Arc<DB<DynRecord, PyExecutor>>,
}

#[pymethods]
impl TonboDB {
    #[new]
    fn new(py: Python<'_>, option: DbOption, record: Py<PyAny>) -> PyResult<Self> {
        let dict = record.getattr(py, "__dict__")?;
        let values = dict.downcast_bound::<PyMapping>(py)?.values()?;
        let mut desc = vec![];
        let mut cols = vec![];
        let mut primary_key_index = None;

        for i in 0..values.len() {
            let value = values.get_item(i)?;
            if let Ok(bound_col) = value.downcast::<Column>() {
                let col = bound_col.extract::<Column>()?;
                if col.primary_key {
                    if primary_key_index.is_some() {
                        panic!("Multiple primary keys is not allowed!")
                    }
                    primary_key_index = Some(desc.len());
                }
                cols.push(col.clone());
                desc.push(Field::from(col));
            }
        }
        let schema = Schema::new(desc, primary_key_index.unwrap());
        let option = option.into_option();
        let db = get_runtime()
            .block_on(async { DB::new(option, TokioExecutor::current(), schema).await })
            .unwrap();
        Ok(Self {
            db: Arc::new(db),
            desc: Arc::new(cols),
            primary_key_index: primary_key_index.expect("Primary key not found"),
        })
    }

    /// Insert record to `TonboDB`.
    ///
    /// * `record`: Primary key of record that is to be removed
    fn insert<'py>(&'py self, py: Python<'py>, record: Py<PyAny>) -> PyResult<Bound<PyAny>> {
        let mut cols = vec![];
        let dict = record.getattr(py, "__dict__")?;
        let values = dict.downcast_bound::<PyMapping>(py)?.values()?;

        for i in 0..values.len() {
            let value = values.get_item(i)?;
            if let Ok(bound_col) = value.downcast::<Column>() {
                let col = Value::from(bound_col.extract::<Column>()?);
                cols.push(col);
            }
        }

        let db = self.db.clone();
        let primary_key_index = self.primary_key_index;

        future_into_py(py, async move {
            db.insert(DynRecord::new(cols, primary_key_index))
                .await
                .map_err(CommitError::from)?;
            Python::with_gil(|py| PyDict::new(py).into_py_any(py))
        })
    }

    fn insert_batch<'py>(&'py self, py: Python<'py>, batch: RecordBatch) -> PyResult<Bound<PyAny>> {
        let record_batch = batch.into_record_batch();
        let db = self.db.clone();

        future_into_py(py, async move {
            db.insert_batch(record_batch.into_iter())
                .await
                .map_err(CommitError::from)?;
            Python::with_gil(|py| PyDict::new(py).into_py_any(py))
        })
    }

    /// Get record from `TonboDB`.
    ///
    /// * `key`: Primary key of record
    fn get<'py>(&'py self, py: Python<'py>, key: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let col_desc = self.desc.get(self.primary_key_index).unwrap();
        let col = to_col(py, col_desc, key);
        let db = self.db.clone();
        let primary_key_index = self.primary_key_index;
        future_into_py(py, async move {
            let record = db
                .get(&col, |e| Some(e.get().columns))
                .await
                .map_err(CommitError::from)?;
            Python::with_gil(|py| match record {
                Some(record) => to_dict(py, primary_key_index, record).into_py_any(py),
                None => Ok(py.None()),
            })
        })
    }

    /// Remove record from `TonboDB`.
    ///
    /// * `key`: Primary key of record that is to be removed
    fn remove<'py>(&'py self, py: Python<'py>, key: Py<PyAny>) -> PyResult<Bound<PyAny>> {
        let col_desc = self.desc.get(self.primary_key_index).unwrap();
        let col = to_col(py, col_desc, key);
        let db = self.db.clone();
        future_into_py(py, async move {
            let ret = db.remove(col).await.map_err(CommitError::from)?;
            Python::with_gil(|py| ret.into_py_any(py))
        })
    }

    /// Open an optimistic ACID `Transaction`.
    fn transaction<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let db = self.db.clone();
        let desc = self.desc.clone();
        future_into_py(py, async move {
            let txn = db.transaction().await;
            Ok(Transaction::new(txn, desc.clone()))
        })
    }

    fn flush<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let db = self.db.clone();

        future_into_py(py, async move {
            db.flush().await.map_err(CommitError::from)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// Flush wal manually
    fn flush_wal<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let db = self.db.clone();

        future_into_py(py, async move {
            db.flush_wal().await.map_err(DbError::from)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}
