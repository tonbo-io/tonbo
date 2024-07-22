use std::{io, path::Path};

use tokio::fs::{create_dir_all, remove_file, File, OpenOptions};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use super::FileProvider;
use crate::executor::tokio::TokioExecutor;

impl FileProvider for TokioExecutor {
    type File = Compat<File>;

    async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
        create_dir_all(path).await
    }

    async fn open(path: impl AsRef<Path>) -> io::Result<Self::File> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await
            .map(TokioAsyncReadCompatExt::compat)
    }

    async fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        remove_file(path).await
    }
}
