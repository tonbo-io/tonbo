use std::{io, path::Path};

use tokio::fs::{remove_file, File};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use super::Fs;
use crate::executor::tokio::TokioExecutor;

impl Fs for TokioExecutor {
    type File = Compat<File>;

    async fn open(path: impl AsRef<Path>) -> io::Result<Self::File> {
        File::create_new(path)
            .await
            .map(TokioAsyncReadCompatExt::compat)
    }

    async fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        remove_file(path).await
    }
}
