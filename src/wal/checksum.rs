use std::{future::Future, hash::Hasher};

use fusio::{Error, IoBuf, IoBufMut, MaybeSend, Read, Write};

use crate::serdes::{Decode, Encode};

pub(crate) struct HashWriter<W: Write> {
    hasher: crc32fast::Hasher,
    writer: W,
}

impl<W: Write + Unpin> HashWriter<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            writer,
        }
    }

    pub(crate) async fn eol(mut self) -> Result<(), fusio::Error> {
        let i = self.hasher.finish();
        i.encode(&mut self.writer).await
    }
}

impl<W: Write> Write for HashWriter<W> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, buf) = self.writer.write_all(buf).await;
        self.hasher.write(buf.as_slice());

        (result, buf)
    }

    fn sync_data(&self) -> impl Future<Output = Result<(), fusio::Error>> + MaybeSend {
        self.writer.sync_data()
    }

    fn sync_all(&self) -> impl Future<Output = Result<(), fusio::Error>> + MaybeSend {
        self.writer.sync_all()
    }

    fn close(&mut self) -> impl Future<Output = Result<(), fusio::Error>> + MaybeSend {
        self.writer.close()
    }
}

pub(crate) struct HashReader<R: Read> {
    hasher: crc32fast::Hasher,
    reader: R,
}

impl<R: Read + Unpin> HashReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            reader,
        }
    }

    pub(crate) async fn checksum(mut self) -> Result<bool, fusio::Error> {
        let checksum = u64::decode(&mut self.reader).await?;

        Ok(self.hasher.finish() == checksum)
    }
}

impl<R: Read> Read for HashReader<R> {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> (Result<u64, Error>, B) {
        let (result, bytes) = self.reader.read(buf).await;
        if result.is_ok() {
            self.hasher.write(bytes.as_slice());
        }

        (result, bytes)
    }

    async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, bytes) = self.reader.read_exact(buf).await;
        if result.is_ok() {
            self.hasher.write(bytes.as_slice());
        }

        (result, bytes)
    }

    async fn read_to_end(&mut self, buf: Vec<u8>) -> (Result<(), Error>, Vec<u8>) {
        let (result, bytes) = self.reader.read_to_end(buf).await;
        if result.is_ok() {
            self.hasher.write(&bytes);
        }

        (result, bytes)
    }

    async fn size(&self) -> Result<u64, fusio::Error> {
        self.reader.size().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Cursor;

    use fusio::Seek;

    use crate::{
        serdes::{Decode, Encode},
        wal::checksum::{HashReader, HashWriter},
    };

    #[tokio::test]
    async fn test_encode_decode() {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        let mut writer = HashWriter::new(&mut cursor);
        4_u64.encode(&mut writer).await.unwrap();
        3_u32.encode(&mut writer).await.unwrap();
        2_u16.encode(&mut writer).await.unwrap();
        1_u8.encode(&mut writer).await.unwrap();
        writer.eol().await.unwrap();

        cursor.seek(0).await.unwrap();
        let mut reader = HashReader::new(&mut cursor);
        assert_eq!(u64::decode(&mut reader).await.unwrap(), 4);
        assert_eq!(u32::decode(&mut reader).await.unwrap(), 3);
        assert_eq!(u16::decode(&mut reader).await.unwrap(), 2);
        assert_eq!(u8::decode(&mut reader).await.unwrap(), 1);
        assert!(reader.checksum().await.unwrap());
    }
}
