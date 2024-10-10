use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use pyo3::{pyclass, PyObject, Python, ToPyObject};
use tonbo::record::Datatype;

#[pyclass]
#[derive(PartialEq, Clone)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
}

impl Debug for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int8 => f.write_str("i8"),
            DataType::Int16 => f.write_str("i16"),
            DataType::Int32 => f.write_str("i32"),
        }
    }
}

impl DataType {
    #[allow(unused)]
    pub(crate) fn default_value(&self, py: Python<'_>) -> PyObject {
        match self {
            DataType::Int8 => i8::default().to_object(py),
            DataType::Int16 => i16::default().to_object(py),
            DataType::Int32 => i32::default().to_object(py),
        }
    }
    pub(crate) fn none_value(&self) -> Arc<dyn Any> {
        match self {
            DataType::Int8 => Arc::new(Option::<i8>::None),
            DataType::Int16 => Arc::new(Option::<i16>::None),
            DataType::Int32 => Arc::new(Option::<i32>::None),
        }
    }
}

impl From<DataType> for Datatype {
    fn from(datatype: DataType) -> Self {
        match datatype {
            DataType::Int8 => Datatype::Int8,
            DataType::Int16 => Datatype::Int16,
            DataType::Int32 => Datatype::Int32,
        }
    }
}

impl From<&DataType> for Datatype {
    fn from(datatype: &DataType) -> Self {
        match datatype {
            DataType::Int8 => Datatype::Int8,
            DataType::Int16 => Datatype::Int16,
            DataType::Int32 => Datatype::Int32,
        }
    }
}
