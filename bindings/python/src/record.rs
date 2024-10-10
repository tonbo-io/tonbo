use std::sync::Arc;

use pyo3::{
    prelude::*,
    pyclass, pymethods,
    types::{PyDict, PyMapping, PyString},
    Bound,
};

use crate::{column::Column, datatype::DataType};

#[pyclass(subclass)]
pub struct Record {
    wraps: Py<PyAny>,
}

#[pymethods]
impl Record {
    #[new]
    fn new(wraps: Py<PyAny>) -> Self {
        Self { wraps }
    }

    #[pyo3(signature = ( **kwargs))]
    fn __call__(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Py<PyAny>> {
        let dict = self.wraps.getattr(py, "__dict__")?;
        let mapping_proxy = dict.downcast_bound::<PyMapping>(py).unwrap();
        if let Some(kwargs) = kwargs {
            for (key, v) in kwargs.iter() {
                let attr = key.downcast::<PyString>().unwrap();
                let col_bound = mapping_proxy.get_item(attr).expect("Unknown attr {attr}");
                let mut col = col_bound.extract::<Column>().unwrap();
                match col.datatype {
                    DataType::Int8 => {
                        let value = v.extract::<i8>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::Int16 => {
                        let value = v.extract::<i16>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::Int32 => {
                        let value = v.extract::<i32>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                };
                self.wraps.setattr(py, attr, col).unwrap();
            }
        }

        let ret = self.wraps.clone_ref(py).into_any();

        Ok(ret)
    }
}
