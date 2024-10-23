use fusio::{Error, IoBuf, Write};
use tokio_util::bytes::BufMut;

pub(crate) struct BufferWriter {
    buf: Vec<u8>,
}

impl BufferWriter {
    pub(crate) fn new() -> Self {
        Self { buf: Vec::new() }
    }
}

impl Write for BufferWriter {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        self.buf.put_slice(buf.as_slice());
        (Ok(()), buf)
    }

    async fn sync_data(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl BufferWriter {
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.buf.as_slice()
    }
}
