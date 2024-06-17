use std::{io, path::Path};

use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use super::Fs;

pub struct TokioFs;

impl Fs for TokioFs {
    type File = Compat<tokio::fs::File>;

    async fn open(&self, path: impl AsRef<Path>) -> io::Result<Self::File> {
        tokio::fs::File::create_new(path)
            .await
            .map(TokioAsyncReadCompatExt::compat)
    }
}
