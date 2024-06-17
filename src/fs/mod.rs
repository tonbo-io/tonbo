#[cfg(any(feature = "tokio", test))]
pub mod tokio;

use std::{future::Future, io, path::Path};

use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};

pub trait AsyncFile: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin + 'static {}

impl<T> AsyncFile for T where T: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin + 'static {}

pub trait Fs {
    type File: AsyncFile;

    fn open(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::File>>;
}
