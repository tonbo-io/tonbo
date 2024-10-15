use error::*;
use pyo3::prelude::*;
use record_batch::RecordBatch;

use crate::record::Record;

mod column;
mod datatype;
mod db;
mod error;
mod fs;
mod options;
mod range;
mod record;
mod record_batch;
mod stream;
mod transaction;
mod utils;

pub use column::*;
pub use datatype::*;
pub use db::*;
pub use fs::*;
pub use options::*;
pub use stream::*;
pub use transaction::*;

use crate::error::{DecodeError, WriteConflictError};

#[pymodule]
fn _tonbo(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TonboDB>()?;
    m.add_class::<DbOption>()?;
    m.add_class::<DataType>()?;
    m.add_class::<Column>()?;
    m.add_class::<Record>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<ScanStream>()?;
    m.add_class::<range::Bound>()?;
    m.add_class::<RecordBatch>()?;

    let fs_module = PyModule::new_bound(py, "fs")?;
    fs_module.add_class::<FsOptions>()?;
    fs_module.add_class::<AwsCredential>()?;
    fs_module.add_function(wrap_pyfunction!(parse, &fs_module)?)?;
    fs_module.add_function(wrap_pyfunction!(from_filesystem_path, &fs_module)?)?;
    fs_module.add_function(wrap_pyfunction!(from_absolute_path, &fs_module)?)?;
    fs_module.add_function(wrap_pyfunction!(from_url_path, &fs_module)?)?;

    fs_module.add_submodule(&fs_module)?;
    py.import_bound("sys")?
        .getattr("modules")?
        .set_item("tonbo.fs", fs_module)?;

    let error_module = PyModule::new_bound(py, "error")?;
    error_module.add_class::<DbError>()?;
    error_module.add_class::<CommitError>()?;

    error_module.add("DecodeError", py.get_type_bound::<DecodeError>())?;
    error_module.add("RecoverError", py.get_type_bound::<RecoverError>())?;
    error_module.add(
        "ExceedsMaxLevelError",
        py.get_type_bound::<ExceedsMaxLevelError>(),
    )?;
    error_module.add(
        "WriteConflictError",
        py.get_type_bound::<WriteConflictError>(),
    )?;
    error_module.add("InnerError", py.get_type_bound::<InnerError>())?;
    error_module.add(
        "RepeatedCommitError",
        py.get_type_bound::<RepeatedCommitError>(),
    )?;
    error_module.add("PathParseError", py.get_type_bound::<PathParseError>())?;

    m.add_submodule(&error_module)?;
    py.import_bound("sys")?
        .getattr("modules")?
        .set_item("tonbo.error", error_module)?;

    Ok(())
}
