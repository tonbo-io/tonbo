mod checksum;
mod log;

use std::{io, marker::PhantomData};

use async_stream::stream;
use checksum::{HashReader, HashWriter};
use futures_core::Stream;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{
    io::{AsyncWriteExt, BufReader},
    AsyncBufReadExt,
};
use log::Log;
use thiserror::Error;

use crate::{
    fs::FileId,
    record::Record,
    serdes::{Decode, Encode},
    timestamp::Timestamped,
};

#[derive(Debug)]
pub(crate) struct WalFile<F, R> {
    file: F,
    file_id: FileId,
    _marker: PhantomData<R>,
}

impl<F, R> WalFile<F, R> {
    pub(crate) fn new(file: F, file_id: FileId) -> Self {
        Self {
            file,
            file_id,
            _marker: PhantomData,
        }
    }

    pub(crate) fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl<F, R> WalFile<F, R>
where
    F: AsyncWrite + Unpin,
    R: Record,
{
    async fn write<'r>(
        &mut self,
        log: Log<Timestamped<R::Ref<'r>>>,
    ) -> Result<(), <R::Ref<'r> as Encode>::Error> {
        let mut writer = HashWriter::new(&mut self.file);
        log.encode(&mut writer).await?;
        writer.eol().await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.file.flush().await
    }
}

impl<F, R> WalFile<F, R>
where
    F: AsyncRead + Unpin,
    R: Record,
{
    fn recover(
        &mut self,
    ) -> impl Stream<Item = Result<Log<Timestamped<R>>, RecoverError<<R as Decode>::Error>>> + '_
    {
        stream! {
            let mut file = BufReader::new(&mut self.file);

            loop {
                if file.buffer().is_empty() && file.fill_buf().await?.is_empty() {
                    return;
                }

                let mut reader = HashReader::new(&mut file);

                let record = Log::decode(&mut reader).await.map_err(RecoverError::Decode)?;

                if !reader.checksum().await? {
                    yield Err(RecoverError::Checksum);
                    return;
                }

                yield Ok(record);
            }
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum RecoverError<E: std::error::Error> {
    #[error("wal recover decode error: {0}")]
    Decode(E),
    #[error("wal recover checksum error")]
    Checksum,
    #[error("wal recover io error")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, pin::pin};

    use futures_util::StreamExt;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use super::{log::LogType, FileId, Log, WalFile};
    use crate::timestamp::Timestamped;

    #[tokio::test]
    async fn write_and_recover() {
        let mut file = Vec::new();
        {
            let mut wal = WalFile::<_, String>::new(Cursor::new(&mut file).compat(), FileId::new());
            wal.write(Log::new(LogType::Full, Timestamped::new("hello", 0.into())))
                .await
                .unwrap();
            wal.flush().await.unwrap();
        }
        {
            let mut wal = WalFile::<_, String>::new(Cursor::new(&mut file).compat(), FileId::new());

            {
                let mut stream = pin!(wal.recover());
                let log = stream.next().await.unwrap().unwrap();
                assert_eq!(log.record.ts, 0.into());
                assert_eq!(log.record.value, "hello".to_string());
            }

            wal.write(Log::new(LogType::Full, Timestamped::new("world", 1.into())))
                .await
                .unwrap();
        }

        {
            let mut wal = WalFile::<_, String>::new(Cursor::new(&mut file).compat(), FileId::new());

            {
                let mut stream = pin!(wal.recover());
                let log = stream.next().await.unwrap().unwrap();
                assert_eq!(log.record.ts, 0.into());
                assert_eq!(log.record.value, "hello".to_string());
                let log = stream.next().await.unwrap().unwrap();
                assert_eq!(log.record.ts, 1.into());
                assert_eq!(log.record.value, "world".to_string());
            }
        }
    }
}
