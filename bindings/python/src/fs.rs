use fusio::path::Path;
use pyo3::{pyclass, pyfunction, pymethods, types::PyString, Bound, PyResult, Python};

use crate::PathParseError;

#[pyclass(get_all, set_all)]
#[derive(Debug, Clone)]
pub struct AwsCredential {
    pub key_id: String,
    pub secret_key: String,
    pub token: Option<String>,
}

impl From<AwsCredential> for fusio::remotes::aws::AwsCredential {
    fn from(cred: AwsCredential) -> Self {
        fusio::remotes::aws::AwsCredential {
            key_id: cred.key_id,
            secret_key: cred.secret_key,
            token: cred.token,
        }
    }
}

#[pymethods]
impl AwsCredential {
    #[new]
    fn new(key_id: String, secret_key: String, token: Option<String>) -> Self {
        Self {
            key_id,
            secret_key,
            token,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub enum FsOptions {
    Local {},
    S3 {
        bucket: String,
        credential: Option<AwsCredential>,
        region: Option<String>,
        sign_payload: Option<bool>,
        checksum: Option<bool>,
        endpoint: Option<String>,
    },
}

impl From<FsOptions> for fusio_dispatch::FsOptions {
    fn from(opt: FsOptions) -> Self {
        match opt {
            FsOptions::Local {} => fusio_dispatch::FsOptions::Local,
            FsOptions::S3 {
                bucket,
                credential,
                region,
                sign_payload,
                checksum,
                endpoint,
            } => fusio_dispatch::FsOptions::S3 {
                bucket,
                credential: credential.map(fusio::remotes::aws::AwsCredential::from),
                region,
                sign_payload,
                checksum,
                endpoint,
            },
        }
    }
}

#[pyfunction]
pub fn parse(path: String, py: Python) -> PyResult<Bound<PyString>> {
    let path = Path::parse(path).map_err(|e| PathParseError::new_err(e.to_string()))?;
    Ok(PyString::new_bound(py, path.as_ref()))
}

#[pyfunction]
pub fn from_filesystem_path(path: String, py: Python) -> PyResult<Bound<PyString>> {
    let path =
        Path::from_filesystem_path(path).map_err(|e| PathParseError::new_err(e.to_string()))?;
    Ok(PyString::new_bound(py, path.as_ref()))
}

#[pyfunction]
pub fn from_absolute_path(path: String, py: Python) -> PyResult<Bound<PyString>> {
    let path =
        Path::from_absolute_path(path).map_err(|e| PathParseError::new_err(e.to_string()))?;
    Ok(PyString::new_bound(py, path.as_ref()))
}

#[pyfunction]
pub fn from_url_path(path: String, py: Python) -> PyResult<Bound<PyString>> {
    let path = Path::from_url_path(path).map_err(|e| PathParseError::new_err(e.to_string()))?;
    Ok(PyString::new_bound(py, path.as_ref()))
}
