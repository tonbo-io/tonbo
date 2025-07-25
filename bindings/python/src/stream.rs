use std::{pin::Pin, sync::Arc};

use futures::{Stream, TryStreamExt};
use pyo3::{
    exceptions::PyStopAsyncIteration, prelude::*, pyclass, pymethods, IntoPyObjectExt, PyRef,
    PyRefMut, PyResult, Python,
};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;
use tonbo::{parquet::errors::ParquetError, record::DynRecord, Entry};

use crate::{utils::to_dict_ref, Column};

type AsyncStream =
    Pin<Box<dyn Stream<Item = Result<Entry<'static, DynRecord>, ParquetError>> + Send>>;

#[pyclass]
pub struct ScanStream {
    stream: Arc<Mutex<AsyncStream>>,
    schema: Arc<Vec<Column>>,
}

impl ScanStream {
    pub fn new(
        schema: Arc<Vec<Column>>,
        stream: impl Stream<Item = Result<Entry<'static, DynRecord>, ParquetError>>
            + 'static
            + Sized
            + Send,
    ) -> Self {
        Self {
            schema,
            stream: Arc::new(Mutex::new(Box::pin(stream))),
        }
    }
}

#[pymethods]
impl ScanStream {
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let stream: Arc<Mutex<AsyncStream>> = Arc::clone(&slf.stream);
        let schema = slf.schema.clone();
        let fut = future_into_py(py, async move {
            let mut locked_stream = stream.lock().await;
            let entry = locked_stream.try_next().await.unwrap();
            match entry {
                Some(entry) => Python::with_gil(|py| match entry.value() {
                    Some(record) => to_dict_ref(py, schema, record.columns)?.into_py_any(py),
                    None => Ok(py.None()),
                }),
                None => Err(PyStopAsyncIteration::new_err("stream exhausted")),
            }
        })?;
        Ok(Some(fut.into()))
    }
}
