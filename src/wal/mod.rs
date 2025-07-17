pub(crate) mod log;

use std::{pin::pin, sync::Arc};

use async_stream::stream;
use fusio::{disk::LocalFs, DynFs};
use fusio_log::{error::LogError, FsOptions, Logger, Options, Path};
use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};
use thiserror::Error;

use crate::{fs::FileId, record::Record, wal::log::Log};

pub(crate) struct WalFile<R>
where
    R: Record,
{
    file: Option<Logger<Log<R>>>,
    file_id: FileId,
    path: Path,
    wal_buffer_size: usize,
    fs: Arc<dyn DynFs>,
    local_fs: Arc<dyn DynFs>,
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
        let local_fs = Arc::new(LocalFs {});
        let file = Options::new(path.clone())
            .buf_size(wal_buffer_size)
            .truncate(true)
            .build_with_fs::<Log<R>>(local_fs.clone())
            .await
            .unwrap();

        Self {
            file: Some(file),
            file_id,
            path,
            wal_buffer_size,
            fs,
            local_fs,
        }
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
        if self.file.is_none() {
            self.file = Some(
                Options::new(self.path.clone())
                    .buf_size(self.wal_buffer_size)
                    .build_with_fs::<Log<R>>(self.local_fs.clone())
                    .await?,
            );
        }

        self.file.as_mut().unwrap().write(data).await
    }

    pub(crate) async fn flush(&mut self) -> Result<(), LogError> {
        match self.file.take() {
            Some(mut file) => {
                file.close().await?;
                if self.fs.file_system() != self.local_fs.file_system() {
                    let mut log = Options::new(self.path.clone())
                        .buf_size(self.wal_buffer_size)
                        .truncate(true)
                        .build_with_fs::<Log<R>>(self.fs.clone())
                        .await
                        .unwrap();

                    let mut log_stream =
                        pin!(Self::recover(FsOptions::Local, self.path.clone()).await);
                    while let Some(record) = log_stream.next().await {
                        let record_batch = record.unwrap();
                        log.write_batch(record_batch.iter()).await?;
                    }

                    log.close().await?;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub(crate) async fn remove(mut self) -> Result<(), LogError> {
        if let Some(mut file) = self.file.take() {
            file.close().await?;
        }
        self.fs.remove(&self.path).await?;
        Ok(())
    }
}

impl<R> WalFile<R>
where
    R: Record,
{
    pub(crate) async fn recover(
        fs_option: FsOptions,
        path: Path,
    ) -> impl Stream<Item = Result<Vec<Log<R>>, RecoverError<fusio::Error>>> {
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

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{pin::pin, sync::Arc};

    use common::PrimaryKey;
    use fusio_log::{FsOptions, Path};
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use super::{log::LogType, WalFile};
    use crate::{
        fs::{generate_file_id, FileType},
        timestamp::Ts,
        wal::log::Log,
    };

    async fn write_and_recover(fs_option: FsOptions) {
        let temp_dir = TempDir::new().unwrap();

        let wal_id = generate_file_id();
        let fs = fs_option.clone().parse().unwrap();
        let wal_path = Path::from_filesystem_path(temp_dir.path())
            .unwrap()
            .child(format!("{}.{}", wal_id, FileType::Wal));
        let mut wal = WalFile::<String>::new(fs.clone(), wal_path.clone(), 0, wal_id).await;

        {
            wal.write(&Log::new(
                Ts::new(
                    PrimaryKey::new(vec![Arc::new("hello".to_string())]),
                    0.into(),
                ),
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
                Ts::new(
                    PrimaryKey::new(vec![Arc::new("world".to_string())]),
                    1.into(),
                ),
                Some("world".into()),
                Some(LogType::Full),
            ))
            .await
            .unwrap();
            wal.flush().await.unwrap();
        }

        {
            {
                let path = Path::from_filesystem_path(temp_dir.path()).unwrap();
                let file_stream = fs.list(&path).await.unwrap();
                let file_number = file_stream.count().await;
                assert_eq!(file_number, 1);

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_write_and_recover() {
        write_and_recover(FsOptions::Local).await
    }

    #[cfg(all(feature = "aws", feature = "tokio-http"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_s3_write_and_recover() {
        use fusio::remotes::aws::AwsCredential;

        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = std::option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = std::option_env!("AWS_SECRET_ACCESS_KEY")
            .unwrap()
            .to_string();
        let token = Some(std::option_env!("AWS_SESSION_TOKEN").unwrap().to_string());
        let bucket = std::env::var("BUCKET_NAME").expect("expected s3 bucket not to be empty");
        let region = Some(std::env::var("AWS_REGION").expect("expected s3 region not to be empty"));

        let fs_option = fusio_log::FsOptions::S3 {
            bucket,
            credential: Some(AwsCredential {
                key_id,
                secret_key,
                token,
            }),
            endpoint: None,
            sign_payload: None,
            checksum: None,
            region,
        };

        write_and_recover(fs_option).await
    }
}
