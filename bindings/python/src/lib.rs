use error::{CommitError, DbError, RepeatedCommitError};
use pyo3::prelude::*;

use crate::record::Record;

mod column;
mod datatype;
mod db;
mod error;
mod options;
mod record;
mod stream;
mod transaction;
mod utils;

pub use column::*;
pub use datatype::*;
pub use db::*;
pub use options::*;
pub use stream::*;
pub use transaction::*;

#[pymodule]
fn _tonbo(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TonboDB>()?;
    m.add_class::<DbOption>()?;
    m.add_class::<DataType>()?;
    m.add_class::<Column>()?;
    m.add_class::<Record>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<ScanStream>()?;

    let error_module = PyModule::new_bound(py, "error")?;
    error_module.add_class::<DbError>()?;
    error_module.add_class::<CommitError>()?;
    error_module.add(
        "RepeatedCommitError",
        py.get_type_bound::<RepeatedCommitError>(),
    )?;

    Ok(())
}
