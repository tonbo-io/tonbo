use std::sync::Arc;

use fusio::{disk::LocalFs, fs::FileSystemTag, path::Path, DynFs};
use fusio_log::{error::LogError, Decode, Encode, FsOptions, Logger as FusioLogger, Options};
use futures_util::{TryStream, TryStreamExt};

#[derive(Clone)]
pub struct LoggerOptions {
    options: Options,
    fs: Arc<dyn DynFs>,
    path: Path,
}

impl LoggerOptions {
    pub fn new(fs: Arc<dyn DynFs>, path: Path) -> Self {
        Self {
            options: Options::new(path.clone()),
            fs,
            path,
        }
    }

    pub fn buf_size(self, buf_size: usize) -> Self {
        Self {
            options: self.options.buf_size(buf_size),
            ..self
        }
    }

    pub fn truncate(self, truncate: bool) -> Self {
        Self {
            options: self.options.truncate(truncate),
            ..self
        }
    }

    pub(crate) async fn open_logger<R>(&self) -> Result<FusioLogger<R>, LogError>
    where
        R: Encode,
    {
        self.options.clone().build_with_fs(self.fs.clone()).await
    }
}

pub struct Logger<R> {
    local_options: LoggerOptions,
    remote_options: Option<LoggerOptions>,
    local: Option<FusioLogger<R>>,
}

impl<R> Logger<R>
where
    R: Encode + Decode,
{
    pub async fn open(
        dir: Path,
        base_fs: Arc<dyn DynFs>,
        filename: &str,
        buf_size: usize,
    ) -> Result<Self, LogError> {
        if base_fs.file_system() == FileSystemTag::S3 {
            let tmp_path = Path::new(std::env::temp_dir()).unwrap().child(filename);
            let remote_options = Some(
                LoggerOptions::new(base_fs, dir.child(filename))
                    .buf_size(buf_size)
                    .truncate(true),
            );
            let local_options =
                LoggerOptions::new(Arc::new(LocalFs {}), tmp_path).buf_size(buf_size);

            Ok(Self {
                local: Some(local_options.open_logger().await?),
                local_options,
                remote_options,
            })
        } else {
            let local_options =
                LoggerOptions::new(base_fs.clone(), dir.child(filename)).buf_size(buf_size);

            Ok(Self {
                local: Some(local_options.open_logger().await?),
                local_options,
                remote_options: None,
            })
        }
    }

    pub async fn write_batch<'r>(
        &mut self,
        data: impl ExactSizeIterator<Item = &'r R>,
    ) -> Result<(), LogError>
    where
        R: 'r,
    {
        if self.local.is_none() {
            self.local = Some(self.local_options.open_logger().await?);
        }
        self.local.as_mut().unwrap().write_batch(data).await
    }

    pub async fn write(&mut self, data: &R) -> Result<(), LogError> {
        if self.local.is_none() {
            self.local = Some(self.local_options.open_logger().await?);
        }
        self.local.as_mut().unwrap().write(data).await
    }

    /// Flush the cached data to the log file.
    #[cfg(all(feature = "opfs", not(feature = "sync")))]
    pub async fn flush(&mut self) -> Result<(), LogError> {
        if let Some(mut local_file) = self.local.take() {
            local_file.close().await?;

            if let Some(remote_options) = self.remote_options.as_ref() {
                Self::copy(self.local_options.clone(), remote_options.clone()).await?;
            }
        }
        Ok(())
    }

    #[cfg(not(all(feature = "opfs", not(feature = "sync"))))]
    pub async fn flush(&mut self) -> Result<(), LogError> {
        if let Some(local_file) = self.local.as_mut() {
            local_file.flush().await?;

            if let Some(remote_options) = self.remote_options.as_ref() {
                Self::copy(self.local_options.clone(), remote_options.clone()).await?;
            }
        }
        Ok(())
    }

    /// Close the local logger. Note that it is not guaranteed that the remote data will be flushed.
    pub async fn close(&mut self) -> Result<(), LogError> {
        if let Some(mut file) = self.local.take() {
            file.close().await?;
        }
        Ok(())
    }

    pub async fn remove(self) -> Result<(), LogError> {
        self.local_options
            .fs
            .remove(&self.local_options.path)
            .await?;
        if let Some(remote_options) = self.remote_options {
            remote_options.fs.remove(&remote_options.path).await?;
        }

        Ok(())
    }

    pub async fn recover(
        fs_option: FsOptions,
        path: Path,
    ) -> Result<impl TryStream<Ok = Vec<R>, Error = LogError> + Unpin, LogError> {
        Options::new(path).fs(fs_option).recover::<R>().await
    }
}

impl<R> Logger<R>
where
    R: Encode + Decode,
{
    #[allow(unused)]
    pub(crate) async fn open_remote_file(&self) -> Result<Option<FusioLogger<R>>, LogError> {
        if let Some(options) = self.remote_options.as_ref() {
            return Ok(Some(options.open_logger().await?));
        }
        Ok(None)
    }

    pub(crate) async fn open_file(options: LoggerOptions) -> Result<FusioLogger<R>, LogError> {
        options.options.build_with_fs(options.fs).await
    }

    pub(crate) async fn recover_from_option(
        options: LoggerOptions,
    ) -> Result<impl TryStream<Ok = Vec<R>, Error = LogError> + Unpin, LogError> {
        options.options.recover_with_fs(options.fs).await
    }

    pub(crate) async fn copy(
        src_options: LoggerOptions,
        dst_options: LoggerOptions,
    ) -> Result<(), LogError> {
        let mut log = Self::open_file(dst_options).await?;

        let mut log_stream = Self::recover_from_option(src_options).await?;

        while let Ok(batch) = log_stream.try_next().await {
            match batch {
                Some(batch) => {
                    log.write_batch(batch.iter()).await?;
                }
                None => break,
            }
        }

        log.close().await
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use fusio::{disk::LocalFs, DynFs};
    use fusio_log::{FsOptions, Path};
    use futures::{StreamExt, TryStreamExt};
    use tempfile::tempdir;

    use super::Logger;
    use crate::fs::{generate_file_id, logger::LoggerOptions};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_local() {
        let tmp_dir = tempdir().unwrap();
        let log_id = generate_file_id();
        let fs = Arc::new(LocalFs {});
        let filename = format!("{}.log", log_id);
        let dir = Path::from_filesystem_path(tmp_dir.path()).unwrap();
        let mut logger = Logger::<String>::open(dir.clone(), fs.clone(), filename.as_str(), 4096)
            .await
            .unwrap();

        logger.write(&"hello tonbo".into()).await.unwrap();
        logger
            .write_batch([&"hello logger".into(), &"hello world".into()].into_iter())
            .await
            .unwrap();

        let file_stream = fs.list(&dir).await.unwrap();
        assert_eq!(file_stream.count().await, 1);

        logger.flush().await.unwrap();

        let mut log_stream =
            Logger::<String>::recover(FsOptions::Local, dir.child(filename.as_str()))
                .await
                .unwrap();
        let record = log_stream.try_next().await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap(), vec!["hello tonbo".to_string()]);

        let record = log_stream.try_next().await.unwrap();
        assert!(record.is_some());
        assert_eq!(
            record.unwrap(),
            vec!["hello logger".to_string(), "hello world".to_string()]
        );

        logger.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_file_num() {
        let tmp_dir = tempdir().unwrap();
        let log_id = generate_file_id();
        let fs = Arc::new(LocalFs {});
        let filename = format!("{}.log", log_id);
        let dir = Path::from_filesystem_path(tmp_dir.path()).unwrap();
        let mut logger = Logger::<String>::open(dir.clone(), fs.clone(), filename.as_str(), 4096)
            .await
            .unwrap();

        logger.close().await.unwrap();

        let file_stream = fs.list(&dir).await.unwrap();
        assert_eq!(file_stream.count().await, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_file_remove() {
        let tmp_dir = tempdir().unwrap();
        let log_id = generate_file_id();
        let fs = Arc::new(LocalFs {});
        let filename = format!("{}.log", log_id);
        let dir = Path::from_filesystem_path(tmp_dir.path()).unwrap();
        let mut logger = Logger::<String>::open(dir.clone(), fs.clone(), filename.as_str(), 4096)
            .await
            .unwrap();

        logger.close().await.unwrap();

        let file_stream = fs.list(&dir).await.unwrap();
        assert_eq!(file_stream.count().await, 1);

        logger.remove().await.unwrap();

        let file_stream = fs.list(&dir).await.unwrap();
        assert_eq!(file_stream.count().await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_remote_file() {
        let log_id = generate_file_id();
        let base_fs = Arc::new(LocalFs {});
        let local_fs = Arc::new(LocalFs {});
        let filename = format!("{}.log", log_id);
        let local_dir = Path::new(std::env::temp_dir())
            .unwrap()
            .child(format!("logger_{}", generate_file_id()));
        let local_path = local_dir.child(filename.as_str());

        local_fs.create_dir_all(&local_dir).await.unwrap();

        let tmp_dir = tempdir().unwrap();
        let base_dir = Path::new(tmp_dir.path()).unwrap();

        // create a logger that both local and remote file is local file different path
        let remote_options = Some(
            LoggerOptions::new(base_fs.clone(), base_dir.child(filename.as_str())).truncate(true),
        );
        let local_options = LoggerOptions::new(local_fs, local_path.clone());

        let mut logger = Logger {
            local: Some(local_options.open_logger::<String>().await.unwrap()),
            local_options,
            remote_options,
        };

        logger.write(&"hello tonbo".into()).await.unwrap();
        logger
            .write_batch([&"hello logger".into(), &"hello world".into()].into_iter())
            .await
            .unwrap();

        let local_file_stream = base_fs.list(&local_dir).await.unwrap();
        assert_eq!(local_file_stream.count().await, 1);

        // remote file has not been created yet
        let remote_file_stream = base_fs.list(&base_dir).await.unwrap();
        assert_eq!(remote_file_stream.count().await, 0);

        logger.flush().await.unwrap();

        // remote file has been created at this time
        let remote_file_stream = base_fs.list(&base_dir).await.unwrap();
        assert_eq!(remote_file_stream.count().await, 1);

        let mut log_stream =
            Logger::<String>::recover(FsOptions::Local, base_dir.child(filename.as_str()))
                .await
                .unwrap();
        let record = log_stream.try_next().await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap(), vec!["hello tonbo".to_string()]);

        let record = log_stream.try_next().await.unwrap();
        assert!(record.is_some());
        assert_eq!(
            record.unwrap(),
            vec!["hello logger".to_string(), "hello world".to_string()]
        );

        logger.close().await.unwrap();

        logger.remove().await.unwrap();

        // both local and remote files are deleted
        let local_file_stream = base_fs.list(&local_dir).await.unwrap();
        assert_eq!(local_file_stream.count().await, 0);

        let remote_file_stream = base_fs.list(&base_dir).await.unwrap();
        assert_eq!(remote_file_stream.count().await, 0);
    }
}
