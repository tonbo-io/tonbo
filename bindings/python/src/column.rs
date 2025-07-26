use std::fmt::{Display, Formatter};

use pyo3::{pyclass, pymethods};
use tonbo::{
    arrow::datatypes::DataType as ArrowDataType,
    record::{DynamicField, Value},
};

use crate::datatype::DataType;

#[pyclass]
#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub(crate) value: Value,
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
        let value = Value::Null;
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

impl From<Column> for DynamicField {
    fn from(col: Column) -> Self {
        let datatype = ArrowDataType::from(col.datatype);
        DynamicField::new(col.name, datatype, col.nullable)
    }
}
