use std::ops;

use pyo3::{pyclass, FromPyObject, Py, PyAny, Python};
use tonbo::record::Value;
use crate::{utils::to_col, Column};

#[pyclass]
#[derive(FromPyObject)]
pub enum Bound {
    Included { key: Py<PyAny> },
    Excluded { key: Py<PyAny> },
}

impl Bound {
    pub(crate) fn to_bound(&self, py: Python, col: &Column) -> ops::Bound<Value> {
        match self {
            Bound::Included { key } => ops::Bound::Included(to_col(py, col, key.clone_ref(py))),
            Bound::Excluded { key } => ops::Bound::Excluded(to_col(py, col, key.clone_ref(py))),
        }
    }
}
