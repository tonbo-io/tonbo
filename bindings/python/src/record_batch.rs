use pyo3::{
    pyclass, pymethods,
    types::{PyAnyMethods, PyMapping, PyMappingMethods},
    Py, PyAny, PyResult, Python,
};
use tonbo::record::{DynRecord, Value};

use crate::Column;

#[derive(Clone)]
struct Record {
    columns: Vec<Value>,
    primary_key_index: usize,
}

impl Record {
    fn new(columns: Vec<Value>, primary_key_index: usize) -> Self {
        Self {
            columns,
            primary_key_index,
        }
    }
}

impl From<Record> for DynRecord {
    fn from(value: Record) -> Self {
        tonbo::record::DynRecord::new(value.columns, value.primary_key_index)
    }
}

#[pyclass]
#[derive(Clone)]
// #[derive(FromPyObject)]
pub struct RecordBatch {
    batch_data: Vec<Record>,
}

#[pymethods]
impl RecordBatch {
    #[new]
    fn new() -> Self {
        Self {
            batch_data: Vec::new(),
        }
    }

    fn append(&mut self, py: Python, record: Py<PyAny>) -> PyResult<()> {
        let mut cols = vec![];
        let dict = record.getattr(py, "__dict__")?;
        let values = dict.downcast_bound::<PyMapping>(py)?.values()?;
        let mut primary_key_index = 0;
        let mut col_idx = 0;

        for i in 0..values.len()? {
            let value = values.get_item(i)?;
            if let Ok(bound_col) = value.downcast::<Column>() {
                let col = bound_col.extract::<Column>()?;
                if col.primary_key {
                    primary_key_index = col_idx;
                }
                let col = Value::from(col);
                cols.push(col);
                col_idx += 1;
            }
        }

        self.batch_data
            .push(Record::new(cols.clone(), primary_key_index));
        Ok(())
    }
}

impl RecordBatch {
    pub(crate) fn into_record_batch(self) -> Vec<DynRecord> {
        let mut batch = vec![];
        for record in self.batch_data.into_iter() {
            batch.push(tonbo::record::DynRecord::from(record))
        }
        batch
    }
}
