pub mod manager;

use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use fusio::{dynamic::DynFile, fs::OpenOptions, path::Path, Read, Write};
use ulid::{DecodeError, Ulid};

pub(crate) type FileId = Ulid;

pub enum FileType {
    Wal,
    Parquet,
    Log,
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Wal => write!(f, "wal"),
            FileType::Parquet => write!(f, "parquet"),
            FileType::Log => write!(f, "log"),
        }
    }
}

impl FileType {
    pub(crate) fn open_options(&self, only_read: bool) -> OpenOptions {
        match self {
            FileType::Wal | FileType::Log => OpenOptions::default().create(true).read(true),
            FileType::Parquet => {
                if only_read {
                    OpenOptions::default().read(true)
                } else {
                    OpenOptions::default().create(true).write(true)
                }
            }
        }
    }
}

pub(crate) fn parse_file_id(path: &Path, suffix: FileType) -> Result<Option<FileId>, DecodeError> {
    path.filename()
        .map(|file_name| {
            let file_id = file_name
                .strip_suffix(&format!(".{}", suffix))
                .unwrap_or(file_name);
            FileId::from_str(file_id)
        })
        .transpose()
}

pub struct DynFileWrapper {
    inner: Box<dyn DynFile>,
}

unsafe impl Send for DynFileWrapper {}
unsafe impl Sync for DynFileWrapper {}

pub struct DynWriteWrapper {
    inner: Box<dyn fusio::DynWrite>,
}

unsafe impl Send for DynWriteWrapper {}
unsafe impl Sync for DynWriteWrapper {}
impl DynWriteWrapper {
    #[allow(unused)]
    pub(crate) fn new(inner: Box<dyn fusio::DynWrite>) -> Self {
        Self { inner }
    }
}

impl From<Box<dyn fusio::DynWrite>> for DynWriteWrapper {
    fn from(inner: Box<dyn fusio::DynWrite>) -> Self {
        Self { inner }
    }
}

impl Write for DynWriteWrapper {
    async fn write_all<B: fusio::IoBuf>(&mut self, buf: B) -> (Result<(), fusio::Error>, B) {
        self.inner.write_all(buf).await
    }

    async fn flush(&mut self) -> Result<(), fusio::Error> {
        self.inner.flush().await
    }

    async fn close(&mut self) -> Result<(), fusio::Error> {
        self.inner.close().await
    }
}

impl DynFileWrapper {
    #[allow(unused)]
    pub(crate) fn new(inner: Box<dyn DynFile>) -> Self {
        Self { inner }
    }

    pub(crate) fn file(self) -> Box<dyn DynFile> {
        self.inner
    }
}

impl From<Box<dyn DynFile>> for DynFileWrapper {
    fn from(inner: Box<dyn DynFile>) -> Self {
        Self { inner }
    }
}

impl Read for DynFileWrapper {
    async fn read_exact_at<B: fusio::IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> (Result<(), fusio::Error>, B) {
        self.inner.read_exact_at(buf, pos).await
    }

    async fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> (Result<(), fusio::Error>, Vec<u8>) {
        self.inner.read_to_end_at(buf, pos).await
    }

    async fn size(&self) -> Result<u64, fusio::Error> {
        self.inner.size().await
    }
}

impl Write for DynFileWrapper {
    async fn write_all<B: fusio::IoBuf>(&mut self, buf: B) -> (Result<(), fusio::Error>, B) {
        self.inner.write_all(buf).await
    }

    async fn flush(&mut self) -> Result<(), fusio::Error> {
        self.inner.flush().await
    }

    async fn close(&mut self) -> Result<(), fusio::Error> {
        self.inner.close().await
    }
}
