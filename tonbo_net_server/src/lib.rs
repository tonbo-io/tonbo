use thiserror::Error;
use tonbo::record::{RecordDecodeError, RecordEncodeError};

pub mod proto;
pub mod server;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("Failed to parse addr: {0}")]
    AddrParseError(#[from] std::net::AddrParseError),

    #[error("Failed to connect to server: {0}")]
    TonicTransportErr(#[from] tonic::transport::Error),

    #[error("Failed to call server: {0}")]
    TonicFailureStatus(#[from] tonic::Status),

    #[error("Failed to encode record: {0}")]
    RecordEncode(#[from] RecordEncodeError),

    #[error("Failed to decode record: {0}")]
    RecordDecode(#[from] RecordDecodeError),

    #[error("fusio error: {0}")]
    Fusio(#[from] fusio::Error),
}
