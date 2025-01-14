pub(crate) mod log;

use std::sync::Arc;

use async_stream::stream;
use fusio::DynFs;
use fusio_log::{error::LogError, Decode, FsOptions, Logger, Options, Path};
use futures_core::Stream;
use futures_util::TryStreamExt;
use thiserror::Error;

use crate::{fs::FileId, record::Record, wal::log::Log};

pub(crate) struct WalFile<R>
where
    R: Record,
{
    file: Logger<Log<R>>,
    file_id: FileId,
}

impl<R> WalFile<R>
where
    R: Record,
{
    pub(crate) async fn new(
        fs: Arc<dyn DynFs>,
        path: Path,
        wal_buffer_size: usize,
        file_id: FileId,
    ) -> Self {
        let file = Options::new(path)
            .buf_size(wal_buffer_size)
            .build_with_fs::<Log<R>>(fs)
            .await
            .unwrap();

        Self { file, file_id }
    }

    pub(crate) fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl<R> WalFile<R>
where
    R: Record,
{
    pub(crate) async fn write<'r>(&mut self, data: &Log<R>) -> Result<(), LogError> {
        self.file.write(data).await
    }

    pub(crate) async fn flush(&mut self) -> Result<(), LogError> {
        self.file.close().await
    }
}

impl<R> WalFile<R>
where
    R: Record,
{
    pub(crate) async fn recover(
        fs_option: FsOptions,
        path: Path,
    ) -> impl Stream<Item = Result<Vec<Log<R>>, RecoverError<<R as Decode>::Error>>> {
        stream! {
            let mut stream = Options::new(path)
                .fs(fs_option)
                .recover::<Log<R>>()
                .await
                .unwrap();
                while let Ok(batch) = stream.try_next().await {
                    match batch {
                        Some(batch) => yield Ok(batch),
                        None => break,
                    }
                }
        }
    }
}

#[derive(Debug, Error)]
pub enum RecoverError<E: std::error::Error> {
    #[error("wal recover decode error: {0}")]
    Decode(E),
    #[error("wal recover checksum error")]
    Checksum,
    #[error("wal recover io error")]
    Io(#[from] std::io::Error),
    #[error("wal recover fusio error")]
    Fusio(#[from] fusio::Error),
    #[error("wal recover log error")]
    Logger(#[from] LogError),
}

#[cfg(test)]
mod tests {
    use std::{pin::pin, sync::Arc};

    use fusio::disk::TokioFs;
    use fusio_log::Path;
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use super::{log::LogType, WalFile};
    use crate::{
        fs::{generate_file_id, FileType},
        timestamp::Timestamped,
        wal::log::Log,
    };

    #[tokio::test]
    async fn write_and_recover() {
        let temp_dir = TempDir::new().unwrap();

        let wal_id = generate_file_id();
        let wal_path = Path::from_filesystem_path(temp_dir.path())
            .unwrap()
            .child(format!("{}.{}", wal_id, FileType::Wal));
        let fs_option = fusio_log::FsOptions::Local;
        let mut wal = WalFile::<String>::new(Arc::new(TokioFs), wal_path.clone(), 0, wal_id).await;
        {
            wal.write(&Log::new(
                Timestamped::new("hello".into(), 0.into()),
                Some("hello".into()),
                Some(LogType::Full),
            ))
            .await
            .unwrap();
            wal.flush().await.unwrap();
        }
        {
            {
                let mut stream =
                    pin!(WalFile::<String>::recover(fs_option.clone(), wal_path.clone()).await);
                for log in stream.next().await.unwrap().unwrap() {
                    assert_eq!(log.key.ts, 0.into());
                    assert_eq!(log.value, Some("hello".to_string()));
                }
            }

            wal.write(&Log::new(
                Timestamped::new("world".into(), 1.into()),
                Some("world".into()),
                Some(LogType::Full),
            ))
            .await
            .unwrap();
        }

        {
            {
                let mut stream = pin!(WalFile::<String>::recover(fs_option, wal_path).await);
                for log in stream.next().await.unwrap().unwrap() {
                    assert_eq!(log.key.ts, 0.into());
                    assert_eq!(log.value, Some("hello".to_string()));
                }
                for log in stream.next().await.unwrap().unwrap() {
                    assert_eq!(log.key.ts, 1.into());
                    assert_eq!(log.value, Some("world".to_string()));
                }
            }
        }
    }
}
