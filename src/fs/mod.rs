#[cfg(any(feature = "tokio", test))]
pub mod tokio;

use std::{
    fmt::{Display, Formatter},
    future::Future,
    io,
    path::Path,
};

use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use ulid::Ulid;

pub(crate) type FileId = Ulid;

pub(crate) enum FileType {
    Wal,
    Parquet,
    Log,
}

pub trait AsyncFile: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static {}

impl<T> AsyncFile for T where T: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static {}

pub trait FileProvider {
    type File: AsyncFile;

    fn open(path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::File>>;

    fn remove(path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>>;
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Wal => write!(f, "wal"),
            FileType::Parquet => write!(f, "parquet"),
            FileType::Log => write!(f, "log"),
        }
    }
}
