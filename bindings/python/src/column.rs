use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

use pyo3::{pyclass, pymethods};
use tonbo::{
    datatype::DataType as TonboDataType,
    arrow::datatypes::{DataType as ArrowDataType, Field},
    record::{Value, ValueDesc},
};

use crate::datatype::DataType;

#[pyclass]
#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub(crate) value: Arc<dyn Any + Send + Sync>,
}

unsafe impl Send for Column {}
unsafe impl Sync for Column {}

impl Column {}

#[pymethods]
impl Column {
    #[new]
    #[pyo3(signature= (datatype, name, nullable=false, primary_key=false))]
    pub fn new(datatype: DataType, name: String, nullable: bool, primary_key: bool) -> Self {
        if primary_key && nullable {
            panic!("Primary key should not be nullable!")
        }
        let value = datatype.none_value();
        Self {
            name,
            datatype,
            nullable,
            primary_key,
            value,
        }
    }

    fn __str__(&self) -> String {
        format!("{}", &self)
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Column")
            .field("name", &self.name)
            .field("type", &self.datatype)
            .field("nullable", &self.nullable)
            .field("primary_key", &self.primary_key)
            .finish()
    }
}

impl From<Column> for Field {
    fn from(col: Column) -> Self {
        let datatype = ArrowDataType::from(col.datatype);
        Field::new(col.name, datatype, col.nullable)
    }
}

impl From<Column> for Value {
    fn from(col: Column) -> Self {
        let datatype = TonboDataType::from(col.datatype);
        Value::new(datatype, col.name, col.value, col.nullable)
    }
}
