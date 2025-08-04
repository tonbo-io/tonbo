use thiserror::Error;

#[derive(Error, Debug)]
pub enum CloudError {
    #[error("Table not found: {0}")]
    NotFound(String),
    #[error("Cloud error: {0}")]
    Cloud(String),
    #[error("Unexpected: {0}")]
    Other(String),
}