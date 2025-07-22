use std::{pin::Pin, sync::Arc};

use futures::{Stream, TryStreamExt};
use pyo3::{
    exceptions::PyStopAsyncIteration, prelude::*, pyclass, pymethods, IntoPyObjectExt, PyRef,
    PyRefMut, PyResult, Python,
};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;
use tonbo::{parquet::errors::ParquetError, record::DynRecord, Entry};

use crate::utils::to_dict;

type AsyncStream =
    Pin<Box<dyn Stream<Item = Result<Entry<'static, DynRecord>, ParquetError>> + Send>>;

#[pyclass]
pub struct ScanStream(Arc<Mutex<AsyncStream>>);

impl ScanStream {
    pub fn new(
        stream: impl Stream<Item = Result<Entry<'static, DynRecord>, ParquetError>>
            + 'static
            + Sized
            + Send,
    ) -> Self {
        Self(Arc::new(Mutex::new(Box::pin(stream))))
    }
}

#[pymethods]
impl ScanStream {
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let stream: Arc<Mutex<AsyncStream>> = Arc::clone(&slf.0);
        let fut = future_into_py(py, async move {
            let mut locked_stream = stream.lock().await;
            let entry = locked_stream.try_next().await.unwrap();
            match entry {
                Some(entry) => Python::with_gil(|py| match entry.value() {
                    Some(record) => {
                        to_dict(py, record.primary_index, record.columns).into_py_any(py)
                    }
                    None => Ok(py.None()),
                }),
                None => Err(PyStopAsyncIteration::new_err("stream exhausted")),
            }
        })?;
        Ok(Some(fut.into()))
    }
}
