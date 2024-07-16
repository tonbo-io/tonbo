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

pub enum FileType {
    WAL,
    PARQUET,
    LOG,
}

pub trait AsyncFile: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static {
    fn to_file(self) -> Box<dyn AsyncFile>
    where
        Self: Sized,
    {
        Box::new(self) as Box<dyn AsyncFile>
    }
}

impl<T> AsyncFile for T where T: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static {}

pub trait Fs {
    type File: AsyncFile;

    fn open(path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::File>>;

    fn remove(path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>>;
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::WAL => write!(f, "wal"),
            FileType::PARQUET => write!(f, "parquet"),
            FileType::LOG => write!(f, "log"),
        }
    }
}
