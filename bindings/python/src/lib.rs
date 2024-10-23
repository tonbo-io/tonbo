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

/// Tonbo Python binding
///
/// ## Usage
/// ```python
/// from tonbo import DbOption, Column, DataType, Record, TonboDB, Bound
/// from tonbo.fs import from_filesystem_path
/// import asyncio
///
/// @Record
/// class User:
///    id = Column(DataType.Int64, name="id", primary_key=True)
///    age = Column(DataType.Int16, name="age", nullable=True)
///    name = Column(DataType.String, name="name", nullable=False)
///
/// async def main():
///     db = TonboDB(DbOption(from_filesystem_path("db_path/user")), User())
///     await db.insert(User(id=18, age=175, name="Alice"))
///     record = await db.get(18)
///     print(record)
///
///     # use transcaction
///     txn = await db.transaction()
///     result = await txn.get(18)
///     scan = await txn.scan(Bound.Included(18), None, limit=10, projection=["id", "name"])
///
///     async for record in scan:
///         print(record)
///
/// asyncio.run(main())
/// ````
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

    m.add_submodule(&fs_module)?;
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
