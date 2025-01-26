use pyo3::{
    create_exception,
    exceptions::{PyException, PyIOError, PyValueError},
    pyclass, PyErr,
};
use tonbo::record::DynRecord;
create_exception!(tonbo, DecodeError, PyException, "Decode exception");
create_exception!(tonbo, RecoverError, PyException, "Recover exception");
create_exception!(
    tonbo,
    ExceedsMaxLevelError,
    PyException,
    "Exceeds max level exception"
);

create_exception!(
    tonbo,
    WriteConflictError,
    PyException,
    "Write conflict exception"
);
create_exception!(tonbo, InnerError, PyException, "Inner exception");

create_exception!(
    tonbo,
    RepeatedCommitError,
    PyException,
    "Repeated commit exception"
);

create_exception!(tonbo, PathParseError, PyException, "Path parse exception");

pub(crate) fn repeated_commit_err() -> PyErr {
    RepeatedCommitError::new_err("Transaction has been committed!")
}

#[pyclass]
pub(crate) struct DbError(tonbo::DbError<DynRecord>);

#[pyclass]
pub(crate) struct CommitError(tonbo::transaction::CommitError<DynRecord>);

impl From<DbError> for PyErr {
    fn from(err: DbError) -> Self {
        match err.0 {
            tonbo::DbError::Io(err) => PyIOError::new_err(err.to_string()),
            tonbo::DbError::Version(err) => PyValueError::new_err(err.to_string()),
            tonbo::DbError::Parquet(err) => InnerError::new_err(err.to_string()),
            tonbo::DbError::UlidDecode(err) => DecodeError::new_err(err.to_string()),
            tonbo::DbError::Fusio(err) => InnerError::new_err(err.to_string()),
            tonbo::DbError::Recover(err) => RecoverError::new_err(err.to_string()),
            tonbo::DbError::WalWrite(err) => PyIOError::new_err(err.to_string()),
            tonbo::DbError::ExceedsMaxLevel => ExceedsMaxLevelError::new_err("Exceeds max level"),
            tonbo::DbError::Logger(err) => PyIOError::new_err(err.to_string()),
        }
    }
}

impl From<CommitError> for PyErr {
    fn from(err: CommitError) -> Self {
        match err.0 {
            tonbo::transaction::CommitError::Io(err) => PyIOError::new_err(err.to_string()),
            tonbo::transaction::CommitError::Parquet(err) => InnerError::new_err(err.to_string()),
            tonbo::transaction::CommitError::Database(err) => DbError::from(err).into(),
            tonbo::transaction::CommitError::WriteConflict(key) => {
                WriteConflictError::new_err(key.name)
            }
            tonbo::transaction::CommitError::SendCompactTaskError(err) => {
                InnerError::new_err(err.to_string())
            }
            tonbo::transaction::CommitError::ChannelClose => InnerError::new_err("channel close"),
        }
    }
}

impl From<tonbo::DbError<DynRecord>> for DbError {
    fn from(err: tonbo::DbError<DynRecord>) -> Self {
        DbError(err)
    }
}

impl From<tonbo::transaction::CommitError<DynRecord>> for CommitError {
    fn from(err: tonbo::transaction::CommitError<DynRecord>) -> Self {
        CommitError(err)
    }
}
