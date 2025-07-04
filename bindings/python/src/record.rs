use std::{i16, i8, sync::Arc};

use pyo3::{
    prelude::*,
    pyclass, pymethods,
    types::{PyDict, PyMapping, PyString, PyTuple},
    Bound,
};
use tonbo::F64;

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
        let record = self.wraps.call0(py)?;

        let dict = self.wraps.getattr(py, "__dict__")?;
        let mapping_proxy = dict.downcast_bound::<PyMapping>(py).unwrap();

        let record_dict = PyDict::new(py);

        for entry in mapping_proxy.items().iter() {
            for item in entry.iter() {
                let tuple = item.downcast_exact::<PyTuple>()?;
                let key = tuple.get_item(0)?;
                let value = tuple.get_item(1)?;
                record_dict.set_item(key.downcast::<PyString>()?, value)?;
            }
        }
        record.setattr(py, "__dict__", record_dict)?;

        if let Some(kwargs) = kwargs {
            for (key, v) in kwargs.iter() {
                let attr = key.downcast::<PyString>().unwrap();
                let col_bound = mapping_proxy.get_item(attr).expect("Unknown attr {attr}");
                let mut col = col_bound.extract::<Column>().unwrap();
                match col.datatype {
                    DataType::UInt8 => {
                        let value = v.extract::<u8>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::UInt16 => {
                        let value = v.extract::<u16>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::UInt32 => {
                        let value = v.extract::<u32>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::UInt64 => {
                        let value = v.extract::<u64>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
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
                    DataType::Int64 => {
                        let value = v.extract::<i64>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::String => {
                        let value = v.extract::<String>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::Boolean => {
                        let value = v.extract::<bool>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::Bytes => {
                        let value = v.extract::<Vec<u8>>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(value)),
                            false => col.value = Arc::new(value),
                        }
                    }
                    DataType::Float => {
                        let value = v.extract::<f64>()?;
                        match col.nullable {
                            true => col.value = Arc::new(Some(F64::from(value))),
                            false => col.value = Arc::new(F64::from(value)),
                        }
                    }
                };
                record.setattr(py, attr, col)?;
            }
        }

        Ok(record)
    }
}
